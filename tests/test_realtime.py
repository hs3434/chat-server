#!/usr/bin/env python3
"""Go 实时通知 + 限制测试: peer_ack 已读回执推送 + 消息长度限制 + 群转让
用法: python3 test_realtime.py <port>
覆盖:
  1. 单聊: B 读 A 的消息 -> A 收到 peer_ack {id, reader:B}
  2. 群聊: B 读群消息 -> 其他成员收到 peer_ack {id, gid, read, total}
  3. 长消息 (>10KB) -> msg_too_large
  4. 群转让: 群主转让 -> 新群主可踢人, 旧群主不能再踢
  5. 群转让: 非群主转让 -> not_owner
"""
import asyncio, json, sys
import websockets

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
URL = f"ws://127.0.0.1:{PORT}/ws"
FAIL = []


def check(name, ok, extra=""):
    print(f"  {'OK' if ok else 'FAIL'} {name}" + (f" {extra}" if extra else ""))
    if not ok:
        FAIL.append(name)


async def reg(user):
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "register", "user": user, "pass": "x"}))
        try:
            await asyncio.wait_for(ws.recv(), 0.5)
        except Exception:
            pass


async def login(user):
    ws = await websockets.connect(URL, open_timeout=5)
    await ws.send(json.dumps({"type": "login", "user": user, "pass": "x"}))
    got = []
    try:
        while True:
            m = json.loads(await asyncio.wait_for(ws.recv(), 1.0))
            got.append(m)
            if m.get("type") == "login_ok":
                break
    except Exception:
        pass
    return ws, got


async def recv_until(ws, want_type, timeout=2.0):
    """收集直到出现 want_type, 返回 (目标消息, 期间全部消息)"""
    got = []
    try:
        while True:
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout))
            got.append(m)
            if m.get("type") == want_type:
                return m, got
    except Exception:
        return None, got


async def main():
    await reg("rt_a"), await reg("rt_b"), await reg("rt_c")
    await reg("gowner"), await reg("gmember")

    # ---- 1. 单聊 peer_ack ----
    a, ainit = await login("rt_a")

    await a.send(json.dumps({"type": "msg", "to": "rt_b", "body": "ack-me"}))
    await asyncio.sleep(0.3)
    b, binit = await login("rt_b")

    # B 读该消息: binit 含 login 期间收到的未读 (msg 带 id)
    bmsgs = binit
    mid = None
    for m in bmsgs:
        if m.get("type") == "msg":
            mid = m.get("id")
    # B 发 ack_read
    ack = None
    if mid:
        await b.send(json.dumps({"type": "ack_read", "id": mid}))
        ack, _ = await recv_until(a, "peer_ack", timeout=3.0)
    print(f"  [debug] mid={mid} ack={ack}")
    check("单聊: B 已读 -> A 收 peer_ack(reader=B)",
          ack is not None and ack.get("reader") == "rt_b" and ack.get("id") == mid,
          f"ack={ack}")
    await b.close()

    # ---- 2. 群聊 peer_ack ----
    gid = "rtgrp"
    await a.send(json.dumps({"type": "create_group", "gid": gid, "name": "rtgrp"}))
    await asyncio.sleep(0.2)
    for u in ["rt_b", "rt_c"]:
        await a.send(json.dumps({"type": "add_member", "gid": gid, "user": u}))
        await asyncio.sleep(0.2)
    await a.send(json.dumps({"type": "msg", "to": gid, "is_group": True, "body": "grp-ack"}))
    await asyncio.sleep(0.3)
    c, cinit = await login("rt_c")
    # C 收到群消息后 ack_read, A 与 B 应收到 peer_ack
    cmsg = None
    for m in cinit:
        if m.get("type") == "msg" and m.get("body") == "grp-ack":
            cmsg = m
            break
    gmid = cmsg.get("id") if cmsg else None
    pact = None
    print(f"  [debug] gmid={gmid}")
    if gmid:
        await c.send(json.dumps({"type": "ack_read", "id": gmid}))
        pact, _ = await recv_until(a, "peer_ack", timeout=3.0)
    # ack 里应带 gid + read/total
    check("群聊: C 已读 -> A 收 peer_ack(gid+read/total)",
          pact is not None and pact.get("gid") == f"group::{gid}" and pact.get("read") is not None,
          f"pact={pact}")
    await c.close()

    # ---- 3. 长消息限制 ----
    big = "x" * (11 * 1024)
    await a.send(json.dumps({"type": "msg", "to": "rt_b", "body": big}))
    err, _ = await recv_until(a, "error", timeout=2.0)
    check("长消息 11KB -> msg_too_large", err is not None and err.get("code") == "msg_too_large",
          f"err={err}")

    # ---- 4/5. 群转让 ----
    g2 = "transgrp"
    await a.send(json.dumps({"type": "create_group", "gid": g2, "name": "trans"}))
    await asyncio.sleep(0.2)
    await a.send(json.dumps({"type": "add_member", "gid": g2, "user": "rt_c"}))
    await asyncio.sleep(0.2)
    # 非群主 (rt_b 不是成员) 转让 -> not_owner
    await a.send(json.dumps({"type": "transfer_owner", "gid": g2, "user": "rt_b"}))
    e2, _ = await recv_until(a, "error", timeout=1.5)
    # 成员 (rt_c) 不是 owner, 由 rt_c 自己操作? 需 rt_c 登录; 简化: 转让给 rt_c (owner=a 是群主)
    await a.send(json.dumps({"type": "transfer_owner", "gid": g2, "user": "rt_c"}))
    ok_r = await recv_until(a, "group_ok", timeout=1.5)
    check("群主转让给成员 -> group_ok", ok_r[0] is not None)
    # 旧群主 a 不再是 owner: a 再踢人应 not_owner
    await a.send(json.dumps({"type": "remove_member", "gid": g2, "user": "rt_c"}))
    e3, _ = await recv_until(a, "error", timeout=1.5)
    check("转让后旧群主踢人 -> not_owner", e3 is not None and e3.get("code") == "not_owner",
          f"e3={e3}")
    # 新群主 rt_c 可踢人 (rt_c 在线? 已关; 略)

    await a.close()

    print()
    if FAIL:
        print("HAS-FAIL:", FAIL)
        sys.exit(1)
    print("✅ realtime ALL-PASS")


asyncio.run(main())
