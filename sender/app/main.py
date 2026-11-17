"""发布值班发送端。

职责：登记“部署完成通知”，调用审计接收端接口，并仅凭接收端回执确认送达。
原则：
- 发送/撤回前先把“已发出、结果未知（待核）”落库，进程崩溃也不会丢失该事实；
- 只有拿到接收端内容一致的回执才置为“已确认”，绝不虚报成功；
- 收回（撤回）是单条、一次性的最终决定：撤回待定期间禁止再次发送原通知；
- 任何 HTTP 响应（发送/撤回/核对）回写本地状态前都要校验“操作纪元”（pending_epoch），
  纪元不符说明该请求已被更新的动作接管，迟到响应不得改写最终结论。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

DB_PATH = os.environ.get("DB_PATH", "/data/sender.db")
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://localhost:8001").rstrip("/")
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "3"))
STARTUP_RECONCILE = os.environ.get("STARTUP_RECONCILE", "1") == "1"
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

STATUS_PENDING = "PENDING"          # 待发送：已登记，尚未发送或已由核对确认对方未记账
STATUS_UNCERTAIN = "UNCERTAIN"      # 待核：已发出但回执缺失/对方不可达，是否送达未知
STATUS_ACKED = "ACKED"              # 已确认：收到接收端内容一致的回执
STATUS_CONFLICT = "CONFLICT"        # 冲突：同环境同发布号内容不一致，需人工处理
STATUS_WITHDRAWING = "WITHDRAWING"  # 撤回待定：撤回请求已发出，结果未确认
STATUS_WITHDRAWN = "WITHDRAWN"      # 已撤回：接收端永久保留撤回决定

DECISION_BOOKED = "BOOKED"          # 最终结论：已入账（附原回执）
DECISION_WITHDRAWN = "WITHDRAWN"    # 最终结论：已撤回

# 已结案：任何发送/撤回/核对都不得再改变这些状态
FINAL_STATUSES = {STATUS_ACKED, STATUS_WITHDRAWN}

SEND_ALLOWED_FROM = {STATUS_PENDING, STATUS_UNCERTAIN, STATUS_CONFLICT}
WITHDRAW_ALLOWED_FROM = {STATUS_PENDING, STATUS_UNCERTAIN, STATUS_CONFLICT}

VALID_SIMULATE = {"lose_response", "slow", "delay", "delay_before_commit", "fail_before_commit"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_epoch() -> str:
    """一次动作的纪元令牌。回写前必须匹配，迟到的响应因此无法覆盖新结论。"""
    return uuid.uuid4().hex


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


def ensure_columns(conn: sqlite3.Connection) -> None:
    """兼容旧库：就地补列，不清空既有数据。"""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(notifications)").fetchall()}
    migrations = {
        "decision": "TEXT",
        "decision_at": "TEXT",
        "pending_epoch": "TEXT",
        "withdraw_attempt_count": "INTEGER NOT NULL DEFAULT 0",
        "withdrawn_at": "TEXT",
    }
    for name, decl in migrations.items():
        if name not in cols:
            conn.execute(f"ALTER TABLE notifications ADD COLUMN {name} {decl}")
    # 旧库中已确认的记录同样是“已入账”的最终结论
    conn.execute(
        "UPDATE notifications SET decision=?, decision_at=COALESCE(decision_at, updated_at) "
        "WHERE status=? AND decision IS NULL",
        (DECISION_BOOKED, STATUS_ACKED),
    )


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                result TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                receipt_id INTEGER,
                receipt_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                decision TEXT,
                decision_at TEXT,
                pending_epoch TEXT,
                withdraw_attempt_count INTEGER NOT NULL DEFAULT 0,
                withdrawn_at TEXT,
                UNIQUE(env, release_no)
            )
            """
        )
        ensure_columns(conn)
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def fetch_row(conn: sqlite3.Connection, nid: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()


def fetch_dict(conn: sqlite3.Connection, nid: int) -> dict:
    row = fetch_row(conn, nid)
    return row_to_dict(row)


def set_fields(conn: sqlite3.Connection, nid: int, **fields) -> None:
    cols = ", ".join(f"{k}=?" for k in fields)
    conn.execute(
        f"UPDATE notifications SET {cols}, updated_at=? WHERE id=?",
        (*fields.values(), now_iso(), nid),
    )


def apply_guarded(
    conn: sqlite3.Connection,
    nid: int,
    guard_statuses: set[str],
    epoch: str | None,
    **fields,
) -> bool:
    """仅当记录仍未结案且纪元未变时回写。

    迟到的发送/撤回/核对响应若发现状态已被接管（纪元不符、已结案），一律丢弃。
    """
    if "decision" in fields:
        fields.setdefault("pending_epoch", None)  # 结案后不再持有任何纪元
    statuses = sorted(guard_statuses)
    marks = ",".join("?" for _ in statuses)
    cols = ", ".join(f"{k}=?" for k in fields)
    cur = conn.execute(
        f"UPDATE notifications SET {cols}, updated_at=? "
        f"WHERE id=? AND status IN ({marks}) AND pending_epoch IS ? AND decision IS NULL",
        (*fields.values(), now_iso(), nid, *statuses, epoch),
    )
    return cur.rowcount > 0


def is_final(row: sqlite3.Row) -> bool:
    return row["status"] in FINAL_STATUSES or row["decision"] is not None


def refusal(row: sqlite3.Row, message: str, code: int = 409, **extra) -> tuple[int, dict]:
    out = row_to_dict(row)
    out.update(extra)
    out["detail"] = message
    return code, out


# ---------------- 核心动作 ----------------

def do_send(nid: int, simulate: str | None = None) -> tuple[int, dict]:
    """发送（或安全重发）一条通知。只有拿到接收端内容一致的回执才置为已确认。

    撤回待定 / 已撤回 / 已确认 时一律拒绝发送，避免把已撤回的通知补送出去。
    普通“待核”（未涉及撤回）的重发仍是接收端幂等的，因此保留原有重试能力。
    """
    conn = connect()
    try:
        row = fetch_row(conn, nid)
        if row is None:
            return 404, {"detail": "通知不存在"}
        if row["status"] == STATUS_ACKED or row["decision"] == DECISION_BOOKED:
            out = row_to_dict(row)
            out["note"] = "已确认，无需重复发送"
            return 200, out
        if row["status"] == STATUS_WITHDRAWN or row["decision"] == DECISION_WITHDRAWN:
            return refusal(row, "该通知已撤回，不得再次发送", kind="withdrawn")
        if row["status"] == STATUS_WITHDRAWING:
            return refusal(row, "撤回结果待定，期间不得再次发送；请核对或重试撤回", kind="withdrawing")
        if is_final(row):
            return refusal(row, "该通知已结案，不得再次发送")

        epoch = new_epoch()
        cur = conn.execute(
            "UPDATE notifications SET status=?, attempt_count=attempt_count+1, pending_epoch=?, "
            "last_error=NULL, updated_at=? "
            "WHERE id=? AND status IN (?,?,?) AND decision IS NULL",
            (STATUS_UNCERTAIN, epoch, now_iso(), nid,
             STATUS_PENDING, STATUS_UNCERTAIN, STATUS_CONFLICT),
        )
        if cur.rowcount == 0:
            fresh = fetch_row(conn, nid)
            return refusal(fresh, "状态已变化，未发送；请刷新后重试")

        payload = {"env": row["env"], "release_no": row["release_no"], "result": row["result"]}
        if simulate:
            payload["simulate"] = simulate
        try:
            resp = httpx.post(f"{RECEIVER_URL}/api/receipts", json=payload, timeout=HTTP_TIMEOUT)
        except httpx.HTTPError as exc:
            apply_guarded(conn, nid, {STATUS_UNCERTAIN}, epoch,
                          last_error=f"接收端不可达或回执丢失（{exc.__class__.__name__}）")
            return 200, fetch_dict(conn, nid)

        if resp.status_code in (200, 201):
            try:
                receipt = resp.json()
            except ValueError:
                receipt = {}
            if receipt.get("content_hash") == row["content_hash"]:
                apply_guarded(
                    conn, nid, {STATUS_UNCERTAIN}, epoch,
                    status=STATUS_ACKED, decision=DECISION_BOOKED, decision_at=now_iso(),
                    last_error=None, receipt_id=receipt.get("id"), receipt_at=receipt.get("received_at"),
                )
            else:
                apply_guarded(conn, nid, {STATUS_UNCERTAIN}, epoch,
                              status=STATUS_CONFLICT, last_error="回执内容与本地登记不一致")
        elif resp.status_code == 409:
            body = _json_or_empty(resp)
            if body.get("kind") == "withdrawal":
                # 接收端撤回在先：旧发送不得入账，本地直接结案为已撤回
                withdrawal = body.get("existing") or {}
                apply_guarded(
                    conn, nid, {STATUS_UNCERTAIN}, epoch,
                    status=STATUS_WITHDRAWN, decision=DECISION_WITHDRAWN, decision_at=now_iso(),
                    withdrawn_at=withdrawal.get("withdrawn_at"),
                    last_error=(
                        None if withdrawal.get("content_hash") == row["content_hash"]
                        else "撤回决定针对的内容与本地登记不一致，需人工核对"
                    ),
                )
            else:
                apply_guarded(conn, nid, {STATUS_UNCERTAIN}, epoch,
                              status=STATUS_CONFLICT,
                              last_error=body.get("detail", "接收端已存在不同内容"))
        else:
            apply_guarded(conn, nid, {STATUS_UNCERTAIN}, epoch,
                          last_error=f"接收端返回 {resp.status_code}，回执缺失")
        out = fetch_dict(conn, nid)
        if out.get("status") != STATUS_UNCERTAIN and out.get("decision") is not None:
            out["note"] = "已凭接收端结论结案"
        return 200, out
    finally:
        conn.close()


def do_withdraw(nid: int, simulate: str | None = None) -> tuple[int, dict]:
    """对单条通知发起（或重试）撤回。

    撤回与入账在接收端由同一写事务裁决；本端在收到结论前保持“撤回待定”，
    绝不先报成功。入账在先时接收端返回原回执，本端如实显示“撤回未生效”。
    """
    conn = connect()
    try:
        row = fetch_row(conn, nid)
        if row is None:
            return 404, {"detail": "通知不存在"}
        if row["status"] == STATUS_WITHDRAWN or row["decision"] == DECISION_WITHDRAWN:
            out = row_to_dict(row)
            out["note"] = "撤回已生效，无需重复撤回"
            return 200, out
        if row["status"] == STATUS_ACKED or row["decision"] == DECISION_BOOKED:
            return refusal(row, f"该通知已入账（原回执 #{row['receipt_id']}），撤回未生效",
                           kind="receipt", receipt_id=row["receipt_id"])
        if is_final(row):
            return refusal(row, "该通知已结案，不能撤回")

        if row["status"] != STATUS_WITHDRAWING:
            epoch = new_epoch()
            cur = conn.execute(
                "UPDATE notifications SET status=?, pending_epoch=?, last_error=NULL, updated_at=? "
                "WHERE id=? AND status IN (?,?,?) AND decision IS NULL",
                (STATUS_WITHDRAWING, epoch, now_iso(), nid,
                 STATUS_PENDING, STATUS_UNCERTAIN, STATUS_CONFLICT),
            )
            if cur.rowcount == 0:
                fresh = fetch_row(conn, nid)
                return refusal(fresh, "状态已变化，未发起撤回；请刷新后重试")
            row = fetch_row(conn, nid)
        epoch = row["pending_epoch"]

        conn.execute(
            "UPDATE notifications SET withdraw_attempt_count=withdraw_attempt_count+1, updated_at=? "
            "WHERE id=? AND status=? AND pending_epoch IS ? AND decision IS NULL",
            (now_iso(), nid, STATUS_WITHDRAWING, epoch),
        )
        payload = {"env": row["env"], "release_no": row["release_no"], "result": row["result"]}
        if simulate:
            payload["simulate"] = simulate
        try:
            resp = httpx.post(f"{RECEIVER_URL}/api/withdrawals", json=payload, timeout=HTTP_TIMEOUT)
        except httpx.HTTPError as exc:
            apply_guarded(conn, nid, {STATUS_WITHDRAWING}, epoch,
                          last_error=f"撤回结果待定：接收端不可达或回执丢失（{exc.__class__.__name__}）")
            out = fetch_dict(conn, nid)
            out["note"] = "撤回结果待定，不得再次发送原通知；可继续核对或重试撤回"
            return 200, out

        if resp.status_code in (200, 201):
            decision = _json_or_empty(resp)
            if decision.get("content_hash") == row["content_hash"]:
                apply_guarded(
                    conn, nid, {STATUS_WITHDRAWING}, epoch,
                    status=STATUS_WITHDRAWN, decision=DECISION_WITHDRAWN, decision_at=now_iso(),
                    withdrawn_at=decision.get("withdrawn_at"), last_error=None,
                )
                out = fetch_dict(conn, nid)
                out["note"] = "撤回已生效，接收端已永久拒绝该键入账"
                return 200, out
            apply_guarded(conn, nid, {STATUS_WITHDRAWING}, epoch,
                          status=STATUS_CONFLICT,
                          last_error="接收端撤回决定的内容与本地登记不一致")
            return 409, fetch_dict(conn, nid)

        body = _json_or_empty(resp)
        if resp.status_code == 409 and body.get("kind") == "receipt":
            existing = body.get("existing") or {}
            apply_guarded(
                conn, nid, {STATUS_WITHDRAWING}, epoch,
                status=STATUS_ACKED, decision=DECISION_BOOKED, decision_at=now_iso(),
                last_error=body.get("detail", "该键已入账，撤回未生效"),
                receipt_id=existing.get("id") or row["receipt_id"],
                receipt_at=existing.get("received_at") or row["receipt_at"],
            )
            out = fetch_dict(conn, nid)
            out["kind"] = "receipt"
            out["note"] = f"该通知已入账（原回执 #{out.get('receipt_id')}），撤回未生效"
            return 409, out
        if resp.status_code == 409:
            apply_guarded(conn, nid, {STATUS_WITHDRAWING}, epoch,
                          status=STATUS_CONFLICT,
                          last_error=body.get("detail", "接收端已存在冲突记录"))
            return 409, fetch_dict(conn, nid)

        apply_guarded(conn, nid, {STATUS_WITHDRAWING}, epoch,
                      last_error=f"撤回结果待定：接收端返回 {resp.status_code}")
        out = fetch_dict(conn, nid)
        out["note"] = "撤回结果待定，不得再次发送原通知"
        return 200, out
    finally:
        conn.close()


def do_reconcile(nid: int, simulate: str | None = None) -> tuple[int, dict]:
    """凭接收端按幂等键返回的一致性快照核对真实状态。

    - SEND 待核：入账→已确认；撤回→已撤回；无记录→回退待发送；
    - 撤回待定：撤回→已撤回；入账→已确认并附原回执；无记录→继续待定（不臆断）。
    """
    conn = connect()
    try:
        row = fetch_row(conn, nid)
        if row is None:
            return 404, {"detail": "通知不存在"}
        if is_final(row):
            out = row_to_dict(row)
            out["note"] = "已结案，核对不会改写最终结论"
            return 200, out

        params = {"env": row["env"], "release_no": row["release_no"]}
        if simulate:
            params["simulate"] = simulate
        try:
            resp = httpx.get(f"{RECEIVER_URL}/api/state", params=params, timeout=HTTP_TIMEOUT)
        except httpx.HTTPError:
            apply_guarded(conn, nid, {row["status"]}, row["pending_epoch"],
                          last_error="核对待定：接收端暂不可达")
            out = fetch_dict(conn, nid)
            out["note"] = "接收端暂不可达，状态保持不变"
            return 200, out

        if resp.status_code != 200:
            apply_guarded(conn, nid, {row["status"]}, row["pending_epoch"],
                          last_error=f"核对失败：接收端返回 {resp.status_code}")
            return 200, fetch_dict(conn, nid)

        snap = _json_or_empty(resp)
        epoch = row["pending_epoch"]
        state = snap.get("state")

        if state == "WITHDRAWN":
            withdrawal = snap.get("withdrawal") or {}
            mismatch = withdrawal.get("content_hash") != row["content_hash"]
            apply_guarded(
                conn, nid, {row["status"]}, epoch,
                status=STATUS_WITHDRAWN, decision=DECISION_WITHDRAWN, decision_at=now_iso(),
                withdrawn_at=withdrawal.get("withdrawn_at"),
                last_error="撤回决定针对的内容与本地登记不一致，需人工核对" if mismatch else None,
            )
            out = fetch_dict(conn, nid)
            out["note"] = "核对确认：接收端已永久撤回该键"
            return 200, out

        if state == "BOOKED":
            receipt = snap.get("receipt") or {}
            if receipt.get("content_hash") == row["content_hash"]:
                apply_guarded(conn, nid, {row["status"]}, epoch,
                              status=STATUS_ACKED, decision=DECISION_BOOKED, decision_at=now_iso(),
                              receipt_id=receipt.get("id"), receipt_at=receipt.get("received_at"),
                              last_error=None)
            else:
                apply_guarded(conn, nid, {row["status"]}, epoch,
                              status=STATUS_CONFLICT,
                              last_error="接收端已存在同环境同发布号的不同内容")
            out = fetch_dict(conn, nid)
            if row["status"] == STATUS_WITHDRAWING:
                out["note"] = f"该通知已入账（原回执 #{out.get('receipt_id')}），撤回未生效"
                out["kind"] = "receipt"
            return 200, out

        # state == NONE
        if row["status"] == STATUS_UNCERTAIN:
            apply_guarded(conn, nid, {STATUS_UNCERTAIN}, epoch,
                          status=STATUS_PENDING, pending_epoch=None, last_error=None)
            out = fetch_dict(conn, nid)
            out["note"] = "接收端无此记录，已恢复待发送，可安全重发"
            return 200, out
        if row["status"] == STATUS_WITHDRAWING:
            apply_guarded(conn, nid, {STATUS_WITHDRAWING}, epoch,
                          last_error="撤回结果待定：接收端暂无记录，可重试撤回")
            out = fetch_dict(conn, nid)
            out["note"] = "撤回仍未确认，继续保持待定；不会据此再次发送"
            return 200, out
        out = fetch_dict(conn, nid)
        out["note"] = "接收端无此记录"
        return 200, out
    finally:
        conn.close()


def do_recover(nid: int) -> tuple[int, dict]:
    """核实“发送结果待定”并恢复为可发送（仍是同键幂等）。

    只有接收端明确回答“无此记录”才恢复待发送；接收端失联或撤回相关时一律不恢复。
    """
    conn = connect()
    try:
        row = fetch_row(conn, nid)
        if row is None:
            return 404, {"detail": "通知不存在"}
        if is_final(row):
            return refusal(row, "已结案，无需恢复")
        if row["status"] == STATUS_WITHDRAWING:
            return refusal(row, "撤回结果待定，不得再次发送；请核对或重试撤回", kind="withdrawing")
        if row["status"] == STATUS_PENDING:
            out = row_to_dict(row)
            out["note"] = "已处于待发送，可直接发送"
            return 200, out
        if row["status"] not in (STATUS_UNCERTAIN, STATUS_CONFLICT):
            return refusal(row, "当前状态不支持恢复")

        epoch = row["pending_epoch"]
        try:
            resp = httpx.get(
                f"{RECEIVER_URL}/api/state",
                params={"env": row["env"], "release_no": row["release_no"]},
                timeout=HTTP_TIMEOUT,
            )
        except httpx.HTTPError:
            return 503, {"detail": "接收端不可达，结果仍待定，不得再次发送", "status": row["status"]}
        if resp.status_code != 200:
            return 503, {"detail": f"接收端核对失败（{resp.status_code}），结果仍待定，不得再次发送",
                         "status": row["status"]}

        snap = _json_or_empty(resp)
        state = snap.get("state")
        if state == "WITHDRAWN":
            withdrawal = snap.get("withdrawal") or {}
            apply_guarded(conn, nid, {row["status"]}, epoch,
                          status=STATUS_WITHDRAWN, decision=DECISION_WITHDRAWN, decision_at=now_iso(),
                          withdrawn_at=withdrawal.get("withdrawn_at"), last_error=None)
            out = fetch_dict(conn, nid)
            out["note"] = "接收端已撤回该键，本地结案"
            return 200, out
        if state == "BOOKED":
            receipt = snap.get("receipt") or {}
            if receipt.get("content_hash") == row["content_hash"]:
                apply_guarded(conn, nid, {row["status"]}, epoch,
                              status=STATUS_ACKED, decision=DECISION_BOOKED, decision_at=now_iso(),
                              receipt_id=receipt.get("id"), receipt_at=receipt.get("received_at"),
                              last_error=None)
            else:
                apply_guarded(conn, nid, {row["status"]}, epoch,
                              status=STATUS_CONFLICT,
                              last_error="接收端已存在同环境同发布号的不同内容")
            out = fetch_dict(conn, nid)
            out["note"] = "接收端已有记录，不能恢复发送"
            return 200, out

        applied = apply_guarded(conn, nid, {row["status"]}, epoch,
                                status=STATUS_PENDING, pending_epoch=None,
                                last_error="核对确认接收端无记录，已恢复待发送")
        out = fetch_dict(conn, nid)
        out["note"] = "接收端明确无此记录，已恢复待发送" if applied else "状态已变化，请刷新"
        return 200, out
    finally:
        conn.close()


def _json_or_empty(resp: httpx.Response) -> dict:
    try:
        data = resp.json()
        return data if isinstance(data, dict) else {}
    except ValueError:
        return {}


def reconcile_all_open() -> int:
    """核对所有未结案记录（含撤回待定），返回处理条数。"""
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT id FROM notifications WHERE status IN (?,?,?)",
            (STATUS_UNCERTAIN, STATUS_PENDING, STATUS_WITHDRAWING),
        ).fetchall()
    finally:
        conn.close()
    for r in rows:
        do_reconcile(r["id"])
    return len(rows)


def _startup_reconcile() -> None:
    """发送端重启后主动核对所有待定记录：只凭接收端快照定案。"""
    time.sleep(2)  # 等待接收端就绪
    try:
        n = reconcile_all_open()
        print(f"[startup] 重启核对完成，共处理 {n} 条待核/待发送/撤回待定记录", flush=True)
    except Exception as exc:  # noqa: BLE001 - 启动核对失败不致命，可稍后手动核对
        print(f"[startup] 重启核对失败（可稍后手动核对）: {exc}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if STARTUP_RECONCILE:
        threading.Thread(target=_startup_reconcile, daemon=True).start()
    yield


app = FastAPI(title="发布值班发送端", lifespan=lifespan)


class NotificationIn(BaseModel):
    env: str
    release_no: str
    result: str


@app.get("/api/health")
def health():
    return {"ok": True}


@app.post("/api/notifications")
def create_notification(body: NotificationIn):
    """登记通知。同 (env, release_no) 同内容幂等去重，内容不同报 409 冲突。"""
    env, release_no, result = body.env.strip(), body.release_no.strip(), body.result.strip()
    if not env or not release_no or not result:
        return JSONResponse(status_code=400, content={"detail": "env / release_no / result 均不能为空"})
    if len(env) > 64 or len(release_no) > 64 or len(result) > 64:
        return JSONResponse(status_code=400, content={"detail": "字段长度不能超过 64 字符"})

    ch = content_hash(env, release_no, result)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM notifications WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is not None:
                conn.execute("COMMIT")
                if row["content_hash"] == ch:
                    out = row_to_dict(row)
                    out["deduplicated"] = True
                    return JSONResponse(status_code=200, content=out)
                return JSONResponse(status_code=409, content={
                    "detail": "相同环境与发布号已存在不同内容",
                    "existing": row_to_dict(row),
                })
            now = now_iso()
            cur = conn.execute(
                """INSERT INTO notifications
                   (env, release_no, result, content_hash, status, attempt_count, created_at, updated_at)
                   VALUES (?,?,?,?,?,0,?,?)""",
                (env, release_no, result, ch, STATUS_PENDING, now, now),
            )
            nid = cur.lastrowid
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        return JSONResponse(status_code=201, content=fetch_dict(conn, nid))
    except sqlite3.Error as exc:
        return JSONResponse(status_code=500, content={"detail": f"本地写入失败：{exc}"})
    finally:
        conn.close()


@app.get("/api/notifications")
def list_notifications():
    conn = connect()
    try:
        rows = conn.execute("SELECT * FROM notifications ORDER BY id DESC").fetchall()
        return {"items": [row_to_dict(r) for r in rows]}
    finally:
        conn.close()


def _validate_simulate(simulate: str | None) -> JSONResponse | None:
    if simulate is not None and simulate not in VALID_SIMULATE:
        return JSONResponse(status_code=400, content={"detail": f"simulate 仅支持：{sorted(VALID_SIMULATE)}"})
    return None


@app.post("/api/notifications/{nid}/send")
def send_notification(nid: int, simulate: str | None = Query(default=None)):
    bad = _validate_simulate(simulate)
    if bad is not None:
        return bad
    code, payload = do_send(nid, simulate)
    return JSONResponse(status_code=code, content=payload)


@app.post("/api/notifications/{nid}/withdraw")
def withdraw_notification(nid: int, simulate: str | None = Query(default=None)):
    bad = _validate_simulate(simulate)
    if bad is not None:
        return bad
    code, payload = do_withdraw(nid, simulate)
    return JSONResponse(status_code=code, content=payload)


@app.post("/api/notifications/{nid}/reconcile")
def reconcile_notification(nid: int, simulate: str | None = Query(default=None)):
    bad = _validate_simulate(simulate)
    if bad is not None:
        return bad
    code, payload = do_reconcile(nid, simulate)
    return JSONResponse(status_code=code, content=payload)


@app.post("/api/notifications/{nid}/recover")
def recover_notification(nid: int):
    code, payload = do_recover(nid)
    return JSONResponse(status_code=code, content=payload)


@app.post("/api/reconcile-all")
def reconcile_all_endpoint():
    n = reconcile_all_open()
    return {"reconciled": n}


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
