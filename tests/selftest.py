#!/usr/bin/env python3
"""关键情形端到端自测（仅依赖 Python 标准库）。

覆盖：
  S1  正常发送 → 已确认
  S2  同内容重试只记一次（两端均不重复）
  S3  同键不同内容 → 409 冲突（发送端登记与接收端直连均验证）
  S4  入库后丢回执 → 待核 → 重发去重 → 已确认
  S5  入库后丢回执 → 待核 → 核对补账 → 已确认
  S6  慢回执（发送端超时）→ 待核 → 核对 → 已确认
  S7  并发同内容提交 → 两端各仅一条
  S8  并发不同内容 → 仅一条，败者 409
  S9  接收端不可用 → 待核且不虚报成功（--lifecycle）
  S10 发送端重启（接收端停机中）→ 待核持久 → 恢复后凭回执补核（--lifecycle）
  S11 发送端重启（接收端正常）→ 启动自动核对凭回执结案（--lifecycle）
  W1  撤回先完成 → 旧发送被拒、同键重试不得入账、接收端永久保留撤回决定
  W2  入账先完成 → 撤回拒绝并显示原回执，接收端不产生撤回决定
  W3  撤回与入账并发交错（真实在途请求）→ 两端结论一致，仅一方生效
  W4  撤回超时/丢回执 → 撤回待定，期间禁止再次发送；核对补回已撤回
  W5  迟到的核对/发送响应不得改写最终结论
  W6  撤回先提交后，迟到的旧发送请求到达 → 拒绝入账
  W7  撤回决定对同键（同内容/异内容）重试永久生效，重复撤回幂等
  W8  接收端重启后撤回决定仍永久生效（--lifecycle）
  W9  撤回待定 + 接收端失联 + 发送端重启 → 待定不丢、禁止发送、恢复后结案（--lifecycle）

用法：
  python3 tests/selftest.py [--lifecycle]
环境变量：
  SENDER_URL / RECEIVER_URL   服务地址（默认 http://127.0.0.1:8000 / :8001）
  RN_LIFECYCLE=1              等价于 --lifecycle
  RN_HOOK_STOP_RECEIVER 等    生命周期钩子命令（默认 docker compose ...）
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

SENDER = os.environ.get("SENDER_URL", "http://127.0.0.1:8000").rstrip("/")
RECEIVER = os.environ.get("RECEIVER_URL", "http://127.0.0.1:8001").rstrip("/")
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN = str(int(time.time()))
ENV = "itest"

PASSED, FAILED = [], []


def check(name, cond, extra=""):
    (PASSED if cond else FAILED).append(name)
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + ("" if cond else f"  | {extra}"), flush=True)


def req(method, url, body=None, timeout=20):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw.decode(errors="replace")}
        return e.code, payload
    except urllib.error.URLError as e:
        return 0, {"error": str(e)}


def sender_create(env, rel, result):
    return req("POST", SENDER + "/api/notifications",
               {"env": env, "release_no": rel, "result": result})


def sender_send(nid, simulate=None):
    url = f"{SENDER}/api/notifications/{nid}/send"
    if simulate:
        url += f"?simulate={simulate}"
    return req("POST", url)


def sender_reconcile(nid, simulate=None):
    url = f"{SENDER}/api/notifications/{nid}/reconcile"
    if simulate:
        url += f"?simulate={simulate}"
    return req("POST", url)


def sender_withdraw(nid, simulate=None):
    url = f"{SENDER}/api/notifications/{nid}/withdraw"
    if simulate:
        url += f"?simulate={simulate}"
    return req("POST", url)


def sender_recover(nid):
    return req("POST", f"{SENDER}/api/notifications/{nid}/recover")


def receiver_state(rel):
    return req("GET", f"{RECEIVER}/api/state?env={ENV}&release_no={rel}", timeout=20)


def receiver_withdrawals(rel):
    _, d = req("GET", RECEIVER + "/api/withdrawals")
    return [x for x in d.get("items", []) if x["env"] == ENV and x["release_no"] == rel]


def sender_rows(rel):
    _, d = req("GET", SENDER + "/api/notifications")
    return [x for x in d.get("items", []) if x["env"] == ENV and x["release_no"] == rel]


def receiver_rows(rel):
    _, d = req("GET", RECEIVER + "/api/receipts")
    return [x for x in d.get("items", []) if x["env"] == ENV and x["release_no"] == rel]


def hook(env_var, default_cmd):
    cmd = os.environ.get(env_var, default_cmd)
    print(f"  >> 钩子: {cmd}", flush=True)
    r = subprocess.run(cmd, shell=True, cwd=PROJECT_ROOT,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if r.returncode != 0:
        print(f"  !! 钩子返回码 {r.returncode}", flush=True)
    # 重启/停机后给服务留出就绪与端口释放时间，避免把冷启动延迟误判为业务结论
    time.sleep(float(os.environ.get("RN_HOOK_SETTLE", "2")))


def wait_up(base, tries=60):
    for _ in range(tries):
        code, _ = req("GET", base + "/api/health", timeout=3)
        if code == 200:
            return True
        time.sleep(0.5)
    return False


def key(tag):
    return f"R{RUN}-{tag}"


# ---------------- 场景 ----------------

def s1_basic():
    k = key("k1")
    code, n = sender_create(ENV, k, "success")
    check("S1 登记通知成功且初始为待发送", code in (200, 201) and n.get("status") == "PENDING", f"code={code} {n}")
    code, n2 = sender_send(n["id"])
    check("S1 发送后已确认且有回执号", n2.get("status") == "ACKED" and bool(n2.get("receipt_id")), f"{n2}")
    check("S1 接收端仅一条记录", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")


def s2_retry_dedup():
    k = key("k1")  # 与 S1 同键：同内容重试
    code, n = sender_create(ENV, k, "success")
    check("S2 同内容重复登记被幂等去重", code == 200 and n.get("deduplicated") is True, f"code={code} {n}")
    code, n2 = sender_send(n["id"])
    check("S2 重发后仍为已确认", n2.get("status") == "ACKED", f"{n2}")
    check("S2 发送端同键仅一条", len(sender_rows(k)) == 1)
    check("S2 接收端同键仍仅一条", len(receiver_rows(k)) == 1)


def s3_conflict():
    k = key("k1")
    code, n = sender_create(ENV, k, "failed")
    check("S3 同键不同内容登记报 409 冲突", code == 409, f"code={code} {n}")
    rows = sender_rows(k)
    check("S3 发送端原记录未被篡改", len(rows) == 1 and rows[0]["result"] == "success", f"{rows}")
    code, _ = req("POST", RECEIVER + "/api/receipts",
                  {"env": ENV, "release_no": k, "result": "failed"})
    check("S3 接收端直连同键不同内容也报 409", code == 409, f"code={code}")
    check("S3 接收端同键仍仅一条", len(receiver_rows(k)) == 1)


def s4_lost_response_resend():
    k = key("k4")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    code, n2 = sender_send(nid, simulate="lose_response")
    check("S4 丢回执后为待核而非成功", n2.get("status") == "UNCERTAIN", f"{n2}")
    check("S4 接收端其实已记账（1 条）", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")
    code, n3 = sender_send(nid)
    check("S4 重发后凭回执确认", n3.get("status") == "ACKED", f"{n3}")
    check("S4 重发未产生重复记录", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")


def s5_lost_response_reconcile():
    k = key("k5")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid, simulate="lose_response")
    check("S5 丢回执后状态为待核", sender_rows(k)[0]["status"] == "UNCERTAIN")
    code, n3 = sender_reconcile(nid)
    check("S5 核对后凭接收端回执确认", n3.get("status") == "ACKED" and bool(n3.get("receipt_id")), f"{n3}")
    check("S5 接收端仍仅一条", len(receiver_rows(k)) == 1)


def s6_slow_response():
    k = key("k6")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    code, n2 = sender_send(nid, simulate="slow")
    check("S6 慢回执超时后为待核", n2.get("status") == "UNCERTAIN", f"{n2}")
    code, n3 = sender_reconcile(nid)
    check("S6 核对后确认", n3.get("status") == "ACKED", f"{n3}")
    check("S6 接收端仅一条", len(receiver_rows(k)) == 1)


def s7_concurrent_same():
    k = key("k7")
    results = []

    def worker():
        try:
            c1, n1 = sender_create(ENV, k, "success")
            c2, n2 = sender_send(n1["id"])
            results.append((c1, c2, n2.get("status")))
        except Exception as e:  # noqa: BLE001
            results.append(("EXC", str(e), None))

    threads = [threading.Thread(target=worker) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("S7 并发同内容：无线程异常", all(r[0] != "EXC" for r in results), f"{results}")
    check("S7 并发同内容：发送端仅一条", len(sender_rows(k)) == 1, f"{sender_rows(k)}")
    check("S7 并发同内容：接收端仅一条", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")
    rows = sender_rows(k)
    check("S7 并发同内容：最终已确认", bool(rows) and rows[0]["status"] == "ACKED", f"{rows}")


def s8_concurrent_diff():
    k = key("k8")
    codes = []

    def worker(i):
        content = "success" if i % 2 == 0 else "failed"
        c, _ = sender_create(ENV, k, content)
        codes.append(c)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ok = sum(1 for c in codes if c in (200, 201))
    conflict = sum(1 for c in codes if c == 409)
    check("S8 并发不同内容：一组入库一组 409", ok == 5 and conflict == 5, f"codes={codes}")
    rows = sender_rows(k)
    check("S8 并发不同内容：发送端仅一条", len(rows) == 1, f"{rows}")
    if rows:
        code, n2 = sender_send(rows[0]["id"])
        check("S8 胜出发送后已确认", n2.get("status") == "ACKED", f"{n2}")
        rrows = receiver_rows(k)
        check("S8 接收端仅一条且内容与发送端一致",
              len(rrows) == 1 and rrows[0]["result"] == rows[0]["result"], f"{rrows} vs {rows}")


def s9_receiver_down():
    k = key("k9")
    hook("RN_HOOK_STOP_RECEIVER", "docker compose stop receiver")
    time.sleep(1)
    code, n = sender_create(ENV, k, "success")
    check("S9 接收端停机时本地可登记", code in (200, 201), f"code={code} {n}")
    nid = n["id"]
    code, n2 = sender_send(nid)
    check("S9 接收端不可达 → 待核而非成功", n2.get("status") == "UNCERTAIN", f"{n2}")
    code, n3 = sender_reconcile(nid)
    check("S9 不可达时核对仍保持待核（不虚报）", n3.get("status") == "UNCERTAIN", f"{n3}")
    return nid


def s10_sender_restart_while_down(nid_k9):
    k = key("k10")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid)  # 接收端仍停机 → 待核
    check("S10 停机期间发送为待核", sender_rows(k)[0]["status"] == "UNCERTAIN")
    hook("RN_HOOK_RESTART_SENDER", "docker compose restart sender")
    if not wait_up(SENDER):
        check("S10 发送端重启后恢复服务", False)
        return
    time.sleep(4)  # 等启动核对线程跑完（接收端仍停机，应保持待核）
    rows = sender_rows(k)
    check("S10 重启后待核状态持久（未丢未虚报）", bool(rows) and rows[0]["status"] == "UNCERTAIN", f"{rows}")
    hook("RN_HOOK_START_RECEIVER", "docker compose start receiver")
    if not wait_up(RECEIVER):
        check("S10 接收端恢复", False)
        return
    code, n4 = sender_reconcile(nid)
    check("S10 恢复后核对判定未送达（回退待发送）", n4.get("status") == "PENDING", f"{n4}")
    code, n4b = sender_send(nid)
    check("S10 安全重发后确认且接收端仅一条",
          n4b.get("status") == "ACKED" and len(receiver_rows(k)) == 1, f"{n4b}")
    code, n5 = sender_reconcile(nid_k9)
    check("S10 此前停机期间的通知核对同为未送达", n5.get("status") == "PENDING", f"{n5}")
    code, n5b = sender_send(nid_k9)
    check("S10 补发后确认", n5b.get("status") == "ACKED", f"{n5b}")
    check("S10 接收端两键各仅一条",
          len(receiver_rows(key("k9"))) == 1 and len(receiver_rows(k)) == 1)


def s11_auto_reconcile_after_restart():
    k = key("k11")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid, simulate="lose_response")
    check("S11 重启前为待核", sender_rows(k)[0]["status"] == "UNCERTAIN")
    hook("RN_HOOK_RESTART_SENDER", "docker compose restart sender")
    if not wait_up(SENDER):
        check("S11 发送端重启后恢复服务", False)
        return
    final = None
    for _ in range(20):  # 启动核对线程约 2s 后触发
        rows = sender_rows(k)
        if rows and rows[0]["status"] == "ACKED":
            final = rows[0]
            break
        time.sleep(0.5)
    check("S11 重启后自动凭接收端回执结案", final is not None, f"{sender_rows(k)}")
    check("S11 接收端仍仅一条", len(receiver_rows(k)) == 1)


def s11_auto_reconcile_after_restart():
    k = key("k11")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid, simulate="lose_response")
    check("S11 重启前为待核", sender_rows(k)[0]["status"] == "UNCERTAIN")
    hook("RN_HOOK_RESTART_SENDER", "docker compose restart sender")
    if not wait_up(SENDER):
        check("S11 发送端重启后恢复服务", False)
        return
    final = None
    for _ in range(20):  # 启动核对线程约 2s 后触发
        rows = sender_rows(k)
        if rows and rows[0]["status"] == "ACKED":
            final = rows[0]
            break
        time.sleep(0.5)
    check("S11 重启后自动凭接收端回执结案", final is not None, f"{sender_rows(k)}")
    check("S11 接收端仍仅一条", len(receiver_rows(k)) == 1)


# ---------------- 撤回场景 ----------------

def new_pending(tag):
    k = key(tag)
    code, n = sender_create(ENV, k, "success")
    assert code in (200, 201), (code, n)
    return k, n["id"]


def w1_withdraw_before_send():
    k, nid = new_pending("w1")
    code, n = sender_withdraw(nid)
    check("W1 撤回成功且结案为已撤回",
          code == 200 and n.get("status") == "WITHDRAWN" and n.get("decision") == "WITHDRAWN", f"{code} {n}")
    check("W1 接收端永久保留撤回决定", len(receiver_withdrawals(k)) == 1, f"{receiver_withdrawals(k)}")

    code, n2 = sender_send(nid)
    check("W1 撤回后旧发送被拒（不得入账）",
          code == 409 and n2.get("kind") == "withdrawn" and not n2.get("receipt_id"), f"{code} {n2}")
    check("W1 接收端仍无回执", len(receiver_rows(k)) == 0, f"{receiver_rows(k)}")

    code, direct = req("POST", RECEIVER + "/api/receipts", {"env": ENV, "release_no": k, "result": "success"})
    check("W1 接收端直连同键重试被拒（kind=withdrawal）",
          code == 409 and direct.get("kind") == "withdrawal", f"{code} {direct}")
    code, other = req("POST", RECEIVER + "/api/receipts", {"env": ENV, "release_no": k, "result": "failed"})
    check("W1 接收端直连同键异内容同样被撤回拒绝",
          code == 409 and other.get("kind") == "withdrawal", f"{code} {other}")
    check("W1 接收端仍无任何回执", len(receiver_rows(k)) == 0, f"{receiver_rows(k)}")
    state, snap = receiver_state(k)
    check("W1 单键快照为 WITHDRAWN 且无回执",
          state == 200 and snap.get("state") == "WITHDRAWN" and snap.get("receipt") is None, f"{snap}")


def w2_booking_first_blocks_withdraw():
    k, nid = new_pending("w2")
    code, n = sender_send(nid)
    check("W2 先入账成功", n.get("status") == "ACKED" and bool(n.get("receipt_id")), f"{n}")
    receipt_id = n.get("receipt_id")
    code, n2 = sender_withdraw(nid)
    check("W2 撤回未生效并显示原回执",
          code == 409 and n2.get("kind") == "receipt" and n2.get("status") == "ACKED"
          and n2.get("receipt_id") == receipt_id, f"{code} {n2}")
    check("W2 接收端不产生撤回决定", len(receiver_withdrawals(k)) == 0, f"{receiver_withdrawals(k)}")
    check("W2 接收端仍仅一条回执", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")
    code, direct = req("POST", RECEIVER + "/api/withdrawals",
                       {"env": ENV, "release_no": k, "result": "success"})
    check("W2 接收端直连撤回同样返回原回执且不入撤回表",
          code == 409 and direct.get("kind") == "receipt"
          and (direct.get("existing") or {}).get("id") == receipt_id
          and len(receiver_withdrawals(k)) == 0, f"{code} {direct}")


def w3_interleaved_booking_vs_withdraw():
    """真实两路并发：入账请求先等 1.5s 再提交，撤回请求立即提交 → 撤回胜。"""
    k, nid = new_pending("w3a")
    results = {}

    def do_book():
        results["send"] = sender_send(nid, simulate="delay_before_commit")

    t = threading.Thread(target=do_book)
    t.start()
    time.sleep(0.3)                     # 让入账请求先“在途”
    code_w, w = sender_withdraw(nid)    # 撤回立刻提交
    t.join()
    code_s, s = results["send"]
    check("W3 撤回先提交：撤回生效",
          code_w == 200 and w.get("decision") == "WITHDRAWN", f"{code_w} {w}")
    check("W3 在途的旧发送不得入账（最终仍为已撤回）",
          code_s == 200 and s.get("decision") == "WITHDRAWN" and not s.get("receipt_id"), f"{code_s} {s}")
    check("W3 接收端无回执、有撤回决定",
          len(receiver_rows(k)) == 0 and len(receiver_withdrawals(k)) == 1,
          f"{receiver_rows(k)} {receiver_withdrawals(k)}")

    """反向（接收端层真实交错）：撤回请求在途 1.5s，期间入账请求立即提交 → 入账胜。"""
    k2 = key("w3b")
    out2 = {}

    def slow_withdrawal():
        out2["withdraw"] = req("POST", RECEIVER + "/api/withdrawals",
                               {"env": ENV, "release_no": k2, "result": "success",
                                "simulate": "delay_before_commit"})

    t2 = threading.Thread(target=slow_withdrawal)
    t2.start()
    time.sleep(0.3)
    code_s2, s2 = req("POST", RECEIVER + "/api/receipts",
                      {"env": ENV, "release_no": k2, "result": "success"})
    t2.join()
    code_w2, w2 = out2["withdraw"]
    check("W3 入账先提交：回执成功入账",
          code_s2 == 201 and s2.get("content_hash"), f"{code_s2} {s2}")
    check("W3 在途撤回不得报成功且返回原回执",
          code_w2 == 409 and w2.get("kind") == "receipt"
          and (w2.get("existing") or {}).get("id") == s2.get("id"), f"{code_w2} {w2}")
    check("W3 接收端仅一条回执、无撤回决定",
          len(receiver_rows(k2)) == 1 and len(receiver_withdrawals(k2)) == 0,
          f"{receiver_rows(k2)} {receiver_withdrawals(k2)}")


def w4_withdraw_uncertain_then_reconcile():
    k, nid = new_pending("w4")
    code, w = sender_withdraw(nid, simulate="slow")
    check("W4 撤回回执超时 → 撤回待定，不报成功",
          w.get("status") == "WITHDRAWING" and w.get("decision") is None, f"{code} {w}")
    check("W4 接收端其实已保留撤回决定", len(receiver_withdrawals(k)) == 1, f"{receiver_withdrawals(k)}")

    code, s = sender_send(nid)
    check("W4 撤回待定期间禁止再次发送",
          code == 409 and s.get("kind") == "withdrawing", f"{code} {s}")
    check("W4 禁止发送后接收端仍无回执", len(receiver_rows(k)) == 0)

    code, rec = sender_reconcile(nid)
    check("W4 核对补回已撤回结论",
          code == 200 and rec.get("status") == "WITHDRAWN" and rec.get("decision") == "WITHDRAWN", f"{rec}")
    check("W4 接收端无回执", len(receiver_rows(k)) == 0)

    # 撤回丢失回执（接收端已提交）也保持待定，重试撤回幂等结案
    k2, nid2 = new_pending("w4b")
    code, w2 = sender_withdraw(nid2, simulate="lose_response")
    check("W4 撤回丢响应 → 撤回待定",
          w2.get("status") == "WITHDRAWING" and w2.get("decision") is None, f"{code} {w2}")
    code, w3 = sender_withdraw(nid2, simulate="lose_response")
    check("W4 再次丢失仍待定（不臆断）",
          w3.get("status") == "WITHDRAWING" and w3.get("decision") is None, f"{code} {w3}")
    check("W4 重试撤回在接收端幂等，仍只一条撤回决定", len(receiver_withdrawals(k2)) == 1)
    code, w4 = sender_withdraw(nid2)
    check("W4 重试撤回拿到回执后结案",
          code == 200 and w4.get("decision") == "WITHDRAWN", f"{code} {w4}")


def w5_late_reconcile_cannot_rewrite():
    """核对响应迟到：快照读到旧状态，期间另一动作已结案 → 迟到响应不得改写。

    用接收端直连制造“发送在途/结果未知”：发送端刚把状态置为待核，
    接收端则用 delay_before_commit 保持无记录；期间完成撤回。
    """
    k, nid = new_pending("w5")
    out = {}

    def slow_send():
        # 接收端 1.5s 后才提交；发送端在此期间为待核（结果未知）
        out["send"] = sender_send(nid, simulate="delay_before_commit")

    t = threading.Thread(target=slow_send)
    t.start()
    time.sleep(0.3)  # 发送端已落“待核”，接收端尚未提交
    rows = sender_rows(k)
    check("W5 发送在途时本地为待核", bool(rows) and rows[0]["status"] == "UNCERTAIN", f"{rows}")

    rec_out = {}

    def slow_reconcile():
        rec_out["rec"] = sender_reconcile(nid, simulate="delay")  # 快照=无记录，2s 后返回

    tr = threading.Thread(target=slow_reconcile)
    tr.start()
    time.sleep(0.4)
    code_w, w = sender_withdraw(nid)  # 此时接收端仍无记录 → 撤回提交
    t.join()
    tr.join()
    code_s, s = out["send"]
    code_r, r = rec_out["rec"]
    check("W5 并发期间撤回已结案",
          code_w == 200 and w.get("decision") == "WITHDRAWN", f"{code_w} {w}")
    check("W5 迟到的发送响应不改写已撤回结论",
          s.get("decision") == "WITHDRAWN" and not s.get("receipt_id"), f"{code_s} {s}")
    check("W5 迟到的核对响应不改写已撤回结论",
          r.get("status") == "WITHDRAWN" and r.get("decision") == "WITHDRAWN", f"{code_r} {r}")
    check("W5 接收端仍无回执且有一条撤回决定",
          len(receiver_rows(k)) == 0 and len(receiver_withdrawals(k)) == 1,
          f"{receiver_rows(k)} {receiver_withdrawals(k)}")

    # 已确认的记录，任何迟到核对同样不改写
    k2, nid2 = new_pending("w5b")
    sender_send(nid2)
    code_r2, r2 = sender_reconcile(nid2, simulate="delay")
    check("W5 已入账记录核对只返回原回执结论",
          code_r2 == 200 and r2.get("decision") == "BOOKED" and r2.get("receipt_id"), f"{r2}")


def w6_late_send_after_withdraw_commit():
    """旧发送请求先发出（在途），撤回随后提交；在途发送到达时接收端必须拒绝入账。"""
    k, nid = new_pending("w6")
    out = {}

    def slow_send():
        out["send"] = sender_send(nid, simulate="delay_before_commit")  # 1.5s 后才提交

    t = threading.Thread(target=slow_send)
    t.start()
    time.sleep(0.2)
    code_w, w = sender_withdraw(nid)
    t.join()
    code_s, s = out["send"]
    check("W6 撤回先完成", code_w == 200 and w.get("decision") == "WITHDRAWN", f"{w}")
    check("W6 迟到的发送响应不得把结论改成已入账",
          s.get("decision") == "WITHDRAWN" and not s.get("receipt_id"), f"{s}")
    check("W6 接收端: 无回执 + 撤回决定永久保留",
          len(receiver_rows(k)) == 0 and len(receiver_withdrawals(k)) == 1)
    state, snap = receiver_state(k)
    check("W6 单键快照仍为 WITHDRAWN", snap.get("state") == "WITHDRAWN", f"{snap}")


def w7_withdrawal_permanent_and_idempotent():
    k, nid = new_pending("w7")
    sender_withdraw(nid)
    code, dup = sender_create(ENV, k, "success")
    check("W7 撤回后同键同内容登记仍幂等返回原记录",
          code == 200 and dup.get("deduplicated") is True and dup.get("decision") == "WITHDRAWN", f"{code} {dup}")
    code, other = sender_create(ENV, k, "failed")
    check("W7 撤回后同键异内容仍 409（不因撤回而放开）", code == 409, f"{code} {other}")
    code, w2 = sender_withdraw(nid)
    check("W7 重复撤回幂等（已生效）", code == 200 and w2.get("decision") == "WITHDRAWN", f"{code} {w2}")
    code, direct = req("POST", RECEIVER + "/api/withdrawals",
                       {"env": ENV, "release_no": k, "result": "success"})
    check("W7 接收端重复撤回幂等（deduplicated）",
          code == 200 and direct.get("deduplicated") is True, f"{code} {direct}")
    code, conflict = req("POST", RECEIVER + "/api/withdrawals",
                         {"env": ENV, "release_no": k, "result": "failed"})
    check("W7 撤回决定异内容 409", code == 409, f"{code} {conflict}")
    check("W7 撤回决定未被改写", len(receiver_withdrawals(k)) == 1)
    code, after = sender_send(nid)
    check("W7 撤回后发送仍被拒", code == 409 and after.get("kind") == "withdrawn", f"{code} {after}")
    check("W7 最终无回执", len(receiver_rows(k)) == 0)


def w8_receiver_restart_keeps_withdrawal():
    k, nid = new_pending("w8")
    code, w = sender_withdraw(nid)
    check("W8 重启前撤回生效", code == 200 and w.get("decision") == "WITHDRAWN", f"{w}")
    hook("RN_HOOK_RESTART_RECEIVER", "docker compose restart receiver")
    if not wait_up(RECEIVER):
        check("W8 接收端重启后恢复服务", False)
        return
    check("W8 重启后撤回决定仍永久保留", len(receiver_withdrawals(k)) == 1)
    code, direct = req("POST", RECEIVER + "/api/receipts", {"env": ENV, "release_no": k, "result": "success"})
    check("W8 重启后同键发送仍被拒", code == 409 and direct.get("kind") == "withdrawal", f"{code} {direct}")
    check("W8 重启后仍无回执", len(receiver_rows(k)) == 0)


def w9_withdraw_uncertain_survives_restart():
    k, nid = new_pending("w9")
    hook("RN_HOOK_STOP_RECEIVER", "docker compose stop receiver")
    time.sleep(1)
    code, w = sender_withdraw(nid)
    check("W9 接收端失联 → 撤回待定",
          w.get("status") == "WITHDRAWING" and w.get("decision") is None, f"{code} {w}")
    code, rec = sender_reconcile(nid)
    check("W9 失联时核对仍保持待定", rec.get("status") == "WITHDRAWING", f"{rec}")
    code, s = sender_send(nid)
    check("W9 待定期间禁止再次发送", code == 409 and s.get("kind") == "withdrawing", f"{code} {s}")

    hook("RN_HOOK_RESTART_SENDER", "docker compose restart sender")
    if not wait_up(SENDER):
        check("W9 发送端重启后恢复服务", False)
        return
    time.sleep(3)
    rows = sender_rows(k)
    check("W9 重启后撤回待定仍在（未丢未虚报）",
          bool(rows) and rows[0]["status"] == "WITHDRAWING" and rows[0]["decision"] is None, f"{rows}")
    code, s2 = sender_send(nid)
    check("W9 重启后待定期间仍禁止发送", code == 409 and s2.get("kind") == "withdrawing", f"{code} {s2}")

    hook("RN_HOOK_START_RECEIVER", "docker compose start receiver")
    if not wait_up(RECEIVER):
        check("W9 接收端恢复", False)
        return
    code, w2 = sender_withdraw(nid)
    check("W9 恢复后重试撤回结案",
          code == 200 and w2.get("decision") == "WITHDRAWN", f"{code} {w2}")
    check("W9 接收端仅一条撤回决定、无回执",
          len(receiver_withdrawals(k)) == 1 and len(receiver_rows(k)) == 0)

    # 反向：接收端已有入账但发送端尚未知悉（丢回执→待核），失联期间撤回待定；
    # 恢复后核对/重试撤回必须显示原回执，绝不报撤回成功
    k2, nid2 = new_pending("w9b")
    sender_send(nid2, simulate="lose_response")  # 接收端已入账，发送端待核
    hook("RN_HOOK_STOP_RECEIVER", "docker compose stop receiver")
    time.sleep(1)
    code, w3 = sender_withdraw(nid2)
    check("W9 已入账后接收端失联，撤回仍待定", w3.get("status") == "WITHDRAWING", f"{w3}")
    hook("RN_HOOK_START_RECEIVER", "docker compose start receiver")
    if not wait_up(RECEIVER):
        check("W9 接收端再次恢复", False)
        return
    code, rec2 = sender_reconcile(nid2)
    check("W9 恢复后核对显示原回执、撤回未生效",
          rec2.get("status") == "ACKED" and rec2.get("decision") == "BOOKED" and rec2.get("receipt_id"),
          f"{rec2}")
    check("W9 接收端仍无撤回决定", len(receiver_withdrawals(k2)) == 0)


def w10_recover_gate():
    """安全重发前必须用接收端快照显式核实；核实前发送端不得臆断“未送达”。"""
    k, nid = new_pending("w10")
    code, s = sender_send(nid, simulate="fail_before_commit")
    check("W10 接收端提交前失败 → 待核而非成功", s.get("status") == "UNCERTAIN", f"{code} {s}")
    check("W10 接收端确无记录", receiver_state(k)[1].get("state") == "NONE")
    code, r = sender_recover(nid)
    check("W10 核实后恢复待发送", code == 200 and r.get("status") == "PENDING", f"{code} {r}")
    code, s2 = sender_send(nid)
    check("W10 恢复后发送成功入账", code == 200 and s2.get("decision") == "BOOKED", f"{code} {s2}")
    code, r2 = sender_recover(nid)
    check("W10 已结案后核实被拒（不改写结论）", code == 409, f"{code} {r2}")

    # 撤回待定时不允许“恢复发送”
    k2, nid2 = new_pending("w10b")
    sender_withdraw(nid2, simulate="lose_response")
    code, r3 = sender_recover(nid2)
    check("W10 撤回待定时核实不恢复发送", code == 409 and r3.get("kind") == "withdrawing", f"{code} {r3}")


def w11_double_withdraw_attempts_bounded_by_receiver():
    """多次撤回尝试只会留下一条永久决定，且时间戳不被后续尝试改写。"""
    k, nid = new_pending("w11")
    code, w = sender_withdraw(nid)
    _, first = receiver_state(k)
    stamp = first["withdrawal"]["withdrawn_at"]
    for _ in range(3):
        sender_withdraw(nid)
    _, later = receiver_state(k)
    check("W11 重复撤回不新增决定", len(receiver_withdrawals(k)) == 1)
    check("W11 首次撤回时间未被改写", later["withdrawal"]["withdrawn_at"] == stamp,
          f"{stamp} vs {later['withdrawal']['withdrawn_at']}")


def main():
    lifecycle = "--lifecycle" in sys.argv or os.environ.get("RN_LIFECYCLE") == "1"
    print(f"发送端 {SENDER}  接收端 {RECEIVER}  lifecycle={lifecycle}", flush=True)
    if not wait_up(SENDER) or not wait_up(RECEIVER):
        print("服务未就绪，请先启动两端。")
        sys.exit(2)
    # 发送端启动后会异步核对一次（约 2s 后）。等它结束再跑场景，
    # 否则“慢回执→待核”等断言会被启动核对抢先结案而变得不确定。
    time.sleep(float(os.environ.get("RN_STARTUP_SETTLE", "4")))

    s1_basic()
    s2_retry_dedup()
    s3_conflict()
    s4_lost_response_resend()
    s5_lost_response_reconcile()
    s6_slow_response()
    s7_concurrent_same()
    s8_concurrent_diff()
    w1_withdraw_before_send()
    w2_booking_first_blocks_withdraw()
    w3_interleaved_booking_vs_withdraw()
    w4_withdraw_uncertain_then_reconcile()
    w5_late_reconcile_cannot_rewrite()
    w6_late_send_after_withdraw_commit()
    w7_withdrawal_permanent_and_idempotent()
    w10_recover_gate()
    w11_double_withdraw_attempts_bounded_by_receiver()
    if lifecycle:
        nid_k9 = s9_receiver_down()
        s10_sender_restart_while_down(nid_k9)
        s11_auto_reconcile_after_restart()
        w8_receiver_restart_keeps_withdrawal()
        w9_withdraw_uncertain_survives_restart()

    print(f"\n通过 {len(PASSED)} 项，失败 {len(FAILED)} 项")
    if FAILED:
        print("失败项：")
        for name in FAILED:
            print(f"  - {name}")
        sys.exit(1)


if __name__ == "__main__":
    main()
