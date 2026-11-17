"""审计接收端（本地演示用）。

接收“部署完成通知”，按 (env, release_no) 幂等入库，并提供回执查询。
- 相同 (env, release_no) 且内容一致的重试：返回既有回执，不新增记录；
- 相同 (env, release_no) 但内容不同：409 冲突；
- 撤回：按 (env, release_no) 永久保留撤回决定（独立 withdrawals 表，无删除入口）。

撤回与入账在同一类 `BEGIN IMMEDIATE` 写事务中裁决，谁先提交谁生效：
- 撤回先提交：该键此后的任何发送/同键重试都返回 409(kind=withdrawal)，绝不入账；
- 入账先提交：撤回请求返回 409(kind=receipt) 并附上原回执，不产生撤回决定。

故障注入（仅演示）：
- POST simulate=lose_response   提交后丢响应（500）
- POST simulate=slow            提交后慢响应（6s，长于发送端默认 3s 超时）
- POST simulate=delay           提交后慢响应（2s，仍在发送端默认超时内，便于制造交错）
- POST simulate=delay_before_commit  提交前等待 1.5s，制造“请求在途”窗口
- POST simulate=fail_before_commit   提交前失败（503），不留下任何记录
- GET  simulate=delay|slow      先读快照，再延迟返回（制造“迟到/过期的核对响应”）
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

DB_PATH = os.environ.get("DB_PATH", "/data/receiver.db")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

POST_SIMULATE = {"lose_response", "slow", "delay", "delay_before_commit", "fail_before_commit"}
GET_SIMULATE = {"slow", "delay"}
SLOW_SECONDS = 6.0
DELAY_SECONDS = 2.0
PRE_COMMIT_SECONDS = 1.5


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def content_hash(env: str, release_no: str, result: str) -> str:
    canonical = json.dumps(
        {"env": env, "release_no": release_no, "result": result},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                result TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE(env, release_no)
            )
            """
        )
        # 撤回决定永久保留：只插入，不更新、不删除；与 receipts 互斥（谁先提交谁生效）。
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                result TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                withdrawn_at TEXT NOT NULL,
                UNIQUE(env, release_no)
            )
            """
        )
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def fetch_dict(conn: sqlite3.Connection, table: str, rid: int) -> dict:
    row = conn.execute(f"SELECT * FROM {table} WHERE id=?", (rid,)).fetchone()
    return row_to_dict(row)


def respond_after_commit(simulate: str | None, status: int, payload: dict) -> JSONResponse:
    """提交完成后才执行的故障注入：数据已落库，调用方可能拿不到响应。

    对“已提交的拒绝结论”（如撤回在先、入账在先）同样适用，便于演示迟到的响应。
    """
    if simulate == "lose_response":
        return JSONResponse(status_code=500, content={"detail": "模拟故障：结论已提交，响应丢失"})
    if simulate == "slow":
        time.sleep(SLOW_SECONDS)
    elif simulate == "delay":
        time.sleep(DELAY_SECONDS)
    return JSONResponse(status_code=status, content=payload)


def validate_key(env: str, release_no: str, result: str) -> JSONResponse | None:
    if not env or not release_no or not result:
        return JSONResponse(status_code=400, content={"detail": "env / release_no / result 均不能为空"})
    if len(env) > 64 or len(release_no) > 64 or len(result) > 64:
        return JSONResponse(status_code=400, content={"detail": "字段长度不能超过 64 字符"})
    return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="审计接收端", lifespan=lifespan)


class ReceiptIn(BaseModel):
    env: str
    release_no: str
    result: str
    simulate: str | None = None  # lose_response | slow | delay | delay_before_commit


class WithdrawalIn(BaseModel):
    env: str
    release_no: str
    result: str
    simulate: str | None = None


@app.get("/api/health")
def health():
    return {"ok": True}


@app.post("/api/receipts")
def create_receipt(body: ReceiptIn):
    env, release_no, result = body.env.strip(), body.release_no.strip(), body.result.strip()
    bad = validate_key(env, release_no, result)
    if bad is not None:
        return bad
    if body.simulate is not None and body.simulate not in POST_SIMULATE:
        return JSONResponse(status_code=400, content={"detail": f"simulate 仅支持：{sorted(POST_SIMULATE)}"})

    if body.simulate == "fail_before_commit":
        return JSONResponse(status_code=503, content={"detail": "模拟故障：提交前失败，未写入任何记录"})
    if body.simulate == "delay_before_commit":
        time.sleep(PRE_COMMIT_SECONDS)  # 写事务之前等待：制造“入账请求在途”窗口

    ch = content_hash(env, release_no, result)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            # —— 与撤回请求共用同一个写锁：先提交者胜出 ——
            withdrawal = conn.execute(
                "SELECT * FROM withdrawals WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if withdrawal is not None:
                conn.execute("COMMIT")
                return JSONResponse(status_code=409, content={
                    "kind": "withdrawal",
                    "detail": "该环境与发布号已撤回，禁止入账",
                    "existing": row_to_dict(withdrawal),
                    "content_match": withdrawal["content_hash"] == ch,
                })
            row = conn.execute(
                "SELECT * FROM receipts WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is not None:
                conn.execute("COMMIT")
                if row["content_hash"] == ch:
                    # 同内容重试：幂等返回既有回执，绝不重复记账
                    out = row_to_dict(row)
                    out["deduplicated"] = True
                    return respond_after_commit(body.simulate, 200, out)
                return JSONResponse(status_code=409, content={
                    "kind": "receipt",
                    "detail": "相同环境与发布号已存在不同内容",
                    "existing": row_to_dict(row),
                })
            cur = conn.execute(
                "INSERT INTO receipts (env, release_no, result, content_hash, received_at) VALUES (?,?,?,?,?)",
                (env, release_no, result, ch, now_iso()),
            )
            rid = cur.lastrowid
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        receipt = fetch_dict(conn, "receipts", rid)
        return respond_after_commit(body.simulate, 201, receipt)
    except sqlite3.Error as exc:
        return JSONResponse(status_code=500, content={"detail": f"接收端写入失败：{exc}"})
    finally:
        conn.close()


@app.post("/api/withdrawals")
def create_withdrawal(body: WithdrawalIn):
    """按 (env, release_no) 记录永久撤回决定。撤回与入账在同一写锁下裁决。

    - 撤回已存在：同内容幂等返回 200（deduplicated），不同内容 409(kind=withdrawal)；
    - 已入账：409(kind=receipt) + 原回执，且不写入撤回决定（不能报撤回成功）。
    """
    env, release_no, result = body.env.strip(), body.release_no.strip(), body.result.strip()
    bad = validate_key(env, release_no, result)
    if bad is not None:
        return bad
    if body.simulate is not None and body.simulate not in POST_SIMULATE:
        return JSONResponse(status_code=400, content={"detail": f"simulate 仅支持：{sorted(POST_SIMULATE)}"})

    if body.simulate == "delay_before_commit":
        time.sleep(PRE_COMMIT_SECONDS)  # 写事务之前等待：制造“撤回请求在途”窗口

    ch = content_hash(env, release_no, result)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            withdrawal = conn.execute(
                "SELECT * FROM withdrawals WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if withdrawal is not None:
                conn.execute("COMMIT")
                out = row_to_dict(withdrawal)
                if withdrawal["content_hash"] == ch:
                    out["deduplicated"] = True
                    return respond_after_commit(body.simulate, 200, out)
                return JSONResponse(status_code=409, content={
                    "kind": "withdrawal",
                    "detail": "相同环境与发布号已存在不同内容的撤回决定",
                    "existing": out,
                })
            receipt = conn.execute(
                "SELECT * FROM receipts WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if receipt is not None:
                # 入账在先：显示原回执，绝不写入撤回决定
                conn.execute("COMMIT")
                return JSONResponse(status_code=409, content={
                    "kind": "receipt",
                    "detail": "该环境与发布号已入账，撤回未生效",
                    "existing": row_to_dict(receipt),
                })
            cur = conn.execute(
                "INSERT INTO withdrawals (env, release_no, result, content_hash, withdrawn_at) VALUES (?,?,?,?,?)",
                (env, release_no, result, ch, now_iso()),
            )
            wid = cur.lastrowid
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        decision = fetch_dict(conn, "withdrawals", wid)
        return respond_after_commit(body.simulate, 201, decision)
    except sqlite3.Error as exc:
        return JSONResponse(status_code=500, content={"detail": f"接收端写入失败：{exc}"})
    finally:
        conn.close()


@app.get("/api/state")
def key_state(env: str, release_no: str, simulate: str | None = None):
    """按幂等键返回一次一致性快照（回执与撤回决定不可能同时存在）。

    simulate=delay|slow：先读取快照，再延迟返回。用于演示“迟到的核对响应”：
    返回的是读取时刻的状态，调用方不得用它覆盖之后已定案的结论。
    """
    if simulate is not None and simulate not in GET_SIMULATE:
        return JSONResponse(status_code=400, content={"detail": f"GET simulate 仅支持：{sorted(GET_SIMULATE)}"})
    if not env or not release_no:
        return JSONResponse(status_code=400, content={"detail": "env 与 release_no 须同时提供"})
    conn = connect()
    try:
        conn.execute("BEGIN")
        withdrawal = conn.execute(
            "SELECT * FROM withdrawals WHERE env=? AND release_no=?", (env, release_no)
        ).fetchone()
        receipt = conn.execute(
            "SELECT * FROM receipts WHERE env=? AND release_no=?", (env, release_no)
        ).fetchone()
        conn.execute("COMMIT")
        if withdrawal is not None:
            state = "WITHDRAWN"
        elif receipt is not None:
            state = "BOOKED"
        else:
            state = "NONE"
        payload = {
            "env": env,
            "release_no": release_no,
            "state": state,
            "withdrawal": row_to_dict(withdrawal) if withdrawal is not None else None,
            "receipt": row_to_dict(receipt) if receipt is not None else None,
        }
        if simulate == "slow":
            time.sleep(SLOW_SECONDS)
        elif simulate == "delay":
            time.sleep(DELAY_SECONDS)
        return payload
    finally:
        conn.close()


@app.get("/api/receipts")
def list_or_get_receipts(env: str | None = None, release_no: str | None = None):
    conn = connect()
    try:
        if env is not None or release_no is not None:
            if not env or not release_no:
                return JSONResponse(status_code=400, content={"detail": "env 与 release_no 须同时提供"})
            row = conn.execute(
                "SELECT * FROM receipts WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is None:
                return JSONResponse(status_code=404, content={"detail": "无此记录"})
            return row_to_dict(row)
        rows = conn.execute("SELECT * FROM receipts ORDER BY id DESC").fetchall()
        return {"items": [row_to_dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/api/withdrawals")
def list_or_get_withdrawals(env: str | None = None, release_no: str | None = None):
    conn = connect()
    try:
        if env is not None or release_no is not None:
            if not env or not release_no:
                return JSONResponse(status_code=400, content={"detail": "env 与 release_no 须同时提供"})
            row = conn.execute(
                "SELECT * FROM withdrawals WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is None:
                return JSONResponse(status_code=404, content={"detail": "无撤回决定"})
            return row_to_dict(row)
        rows = conn.execute("SELECT * FROM withdrawals ORDER BY id DESC").fetchall()
        return {"items": [row_to_dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
