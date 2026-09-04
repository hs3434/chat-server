#!/usr/bin/env python3
"""Go 可靠性机制测试 v2: 全局单调ID + ack 幂等 + 会话唯一ID + msg_state 队列
用法: python3 test_reliability.py <port>
验证:
  1. 单聊消息本体一份, recipient=对方, 收方入队(msg_state pending)
  2. ack_received 幂等 (pending->delivered)
  3. ack_read 幂等 (delivered->read)
  4. 离线消息重新投递 (登录重投 Undelivered)
  5. 群聊: 同一条消息所有成员拿同一个 id (会话唯一)
  6. 已读消息不再重投
"""
import asyncio
import json
import sys

import websockets

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
URL = f"ws://127.0.0.1:{PORT}/ws"
DB = "/workspace/wxlike-server/wxlike_go.db"


async def send_and_collect(actions, read_seconds=1.0):
    got = []
    async with websockets.connect(URL, open_timeout=5) as ws:
        for a in actions:
            await ws.send(json.dumps(a))
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=read_seconds)
                got.append(json.loads(raw))
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
    return got


async def login_conn(user, pwd):
    """返回带登录的连接 + 收到的初始消息"""
    ws = await websockets.connect(URL, open_timeout=5)
    await ws.send(json.dumps({"type": "login", "user": user, "pass": pwd}))
    initial = []
    try:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=0.8)
            m = json.loads(raw)
            initial.append(m)
            if m.get("type") == "login_ok":
                break
    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        pass
    return ws, initial


def state_of(user, msg_id):
    import sqlite3
    con = sqlite3.connect(DB)
    row = con.execute(
        "SELECT state FROM msg_state WHERE user=? AND msg_id=?",
        (user, msg_id)).fetchone()
    con.close()
    return row[0] if row else "MISSING"


async def main():
    ok = True
    def check(name, cond, detail=""):
        nonlocal ok
        if not cond:
            ok = False
        print(f"{'OK' if cond else 'FAIL'} {name} {detail}")

    await send_and_collect([
        {"type": "register", "user": "ra", "pass": "a"},
        {"type": "register", "user": "rb", "pass": "b"},
        {"type": "register", "user": "rc", "pass": "c"},
    ])

    # ---- 1. 单聊离线: ra 发 rb, rb 离线 -> rb 的 msg_state pending ----
    await send_and_collect([
        {"type": "login", "user": "ra", "pass": "a"},
        {"type": "msg", "to": "rb", "body": "hello-msg-1"},
    ])
    await asyncio.sleep(0.3)
    # rb 登录, 应重投该消息
    conn_rb, initial = await login_conn("rb", "b")
    msgs = [m for m in initial if m.get("type") == "msg"]
    check("离线单聊登录重投", any(m.get("body") == "hello-msg-1" for m in msgs),
          str([(m.get('id'), m.get('body')) for m in msgs]))
    single_id = next((m["id"] for m in msgs if m.get("body") == "hello-msg-1"), None)
    check("单聊消息带 id", isinstance(single_id, int))
    st = state_of("rb", single_id)
    check("rbloid 入队 pending", st == "pending", f"state={st}")

    # ---- 2. ack_received 幂等 (pending->delivered) ----
    for _ in range(3):
        await conn_rb.send(json.dumps({"type": "ack_received", "id": single_id}))
    await asyncio.sleep(0.3)
    st = state_of("rb", single_id)
    check("ack_received 幂等(->delivered)", st == "delivered", f"state={st}")

    # ---- 3. ack_read 幂等 (delivered->read) ----
    for _ in range(2):
        await conn_rb.send(json.dumps({"type": "ack_read", "id": single_id}))
    await asyncio.sleep(0.3)
    st = state_of("rb", single_id)
    check("ack_read 幂等(->read)", st == "read", f"state={st}")
    await conn_rb.close()

    # ---- 4. read 后不再重投 ----
    _, initial2 = await login_conn("rb", "b")
    msgs2 = [m for m in initial2 if m.get("type") == "msg"]
    check("已读消息不再重投", all(m.get("id") != single_id for m in msgs2),
          f"count={len(msgs2)}")

    # ---- 5. 群聊: 同一条消息所有成员拿同一个 id ----
    await send_and_collect([
        {"type": "login", "user": "ra", "pass": "a"},
        {"type": "create_group", "gid": "rgrp", "name": "rgrp"},
        {"type": "add_member", "gid": "rgrp", "user": "rb"},
        {"type": "add_member", "gid": "rgrp", "user": "rc"},
    ])
    await asyncio.sleep(0.3)
    # ra 发群消息 (rb, rc 均离线 -> 各自入队)
    await send_and_collect([
        {"type": "login", "user": "ra", "pass": "a"},
        {"type": "msg", "to": "rgrp", "is_group": True, "body": "grp-share"},
    ])
    await asyncio.sleep(0.3)
    # rb 登录收到, 记其 id
    conn_rb, init_b = await login_conn("rb", "b")
    group_msg_b = next((m for m in init_b if m.get("body") == "grp-share"), None)
    await conn_rb.close()
    # rc 登录收到, 记其 id
    conn_rc, init_c = await login_conn("rc", "c")
    group_msg_c = next((m for m in init_c if m.get("body") == "grp-share"), None)
    await conn_rc.close()
    check("群聊成员rb收到", group_msg_b is not None)
    check("群聊成员rc收到", group_msg_c is not None)
    same_id = group_msg_b and group_msg_c and group_msg_b["id"] == group_msg_c["id"]
    check("群消息同一 id (会话唯一)", bool(same_id),
          f"rb={group_msg_b and group_msg_b['id']} rc={group_msg_c and group_msg_c['id']}")
    # 各成员独立状态: rb 未读, rc 未读
    check("rb 群消息未读", state_of("rb", group_msg_b["id"]) in ("pending", "delivered"))
    check("rc 群消息未读", state_of("rc", group_msg_c["id"]) in ("pending", "delivered"))

    print("\n" + ("ALL-PASS" if ok else "HAS-FAIL"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
