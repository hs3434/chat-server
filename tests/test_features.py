#!/usr/bin/env python3
"""Go 新增功能测试: 未读数 badge + 历史分页 + 已读回执
用法: python3 test_features.py <port>
覆盖:
  1. unread: 离线消息 → 登录后 unread 按会话返回正确 count
  2. unread: ack_read 后 unread 清零
  3. history before_id: 翻页返回更早消息 (不重复)
  4. receipts: 已读后 receipts 显示 state=read
"""
import asyncio, json, sys, sqlite3
import websockets

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
URL = f"ws://127.0.0.1:{PORT}/ws"
DB = "/workspace/wxlike-server/wxlike_go.db"
FAIL = []


async def login_conn(user, pwd):
    ws = await websockets.connect(URL, open_timeout=5)
    await ws.send(json.dumps({"type": "login", "user": user, "pass": pwd}))
    initial = []
    try:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            m = json.loads(raw)
            initial.append(m)
            if m.get("type") == "login_ok":
                break
    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        pass
    return ws, initial


async def ask(ws, obj, wait=1.0):
    """在已登录连接上发一个请求, 收集 wait 秒内所有响应"""
    await ws.send(json.dumps(obj))
    got = []
    try:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=wait)
            got.append(json.loads(raw))
    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        pass
    return got


async def main():
    uid = "f1"
    await ask(await websockets.connect(URL, open_timeout=5), {"type": "register", "user": uid, "pass": "x"}) if False else None
    # 注册用独立短连接 (register 不需要登录)
    async with websockets.connect(URL, open_timeout=5) as r1:
        await r1.send(json.dumps({"type": "register", "user": uid, "pass": "x"}))
        try:
            await asyncio.wait_for(r1.recv(), 0.5)
        except Exception:
            pass
    async with websockets.connect(URL, open_timeout=5) as r2:
        await r2.send(json.dumps({"type": "register", "user": "f2", "pass": "x"}))
        try:
            await asyncio.wait_for(r2.recv(), 0.5)
        except Exception:
            pass

    # ---- 1. 未读数 badge: f1 离线时 f2 发 3 条, f1 登录后 unread 应为 3 ----
    sender, _ = await login_conn("f2", "x")
    for i in range(3):
        await sender.send(json.dumps({"type": "msg", "to": uid, "body": f"u-{i}"}))
        await asyncio.sleep(0.15)
    await sender.close()

    recv, initial = await login_conn(uid, "x")
    msgs = [m for m in initial if m.get("type") == "msg"]
    ok1 = len(msgs) == 3
    print(f"  {'OK' if ok1 else 'FAIL'} 离线3条重投 {len(msgs)}")
    if not ok1:
        FAIL.append("离线重投")

    resp = await ask(recv, {"type": "unread"})
    unread_ok = False
    for m in resp:
        if m.get("type") == "unread":
            items = m.get("items", [])
            hit = [i for i in items if i.get("chat") == "f2"]
            unread_ok = bool(hit and hit[0]["count"] == 3)
            print(f"  {'OK' if unread_ok else 'FAIL'} unread badge f2={hit[0]['count'] if hit else 'N/A'} (期望3)")
    if not unread_ok:
        FAIL.append("unread badge")

    # ---- 2. ack_read 后 unread 清零 ----
    for m in msgs:
        await recv.send(json.dumps({"type": "ack_read", "id": m["id"]}))
    await asyncio.sleep(0.3)
    resp2 = await ask(recv, {"type": "unread"})
    cleared = False
    for m in resp2:
        if m.get("type") == "unread":
            items = m.get("items", [])
            hit = [i for i in items if i.get("chat") == "f2"]
            cleared = not hit or hit[0]["count"] == 0
            print(f"  {'OK' if cleared else 'FAIL'} ack_read 后 unread 清零 (hit={hit})")
    if not cleared:
        FAIL.append("ack_read 清零")

    # ---- 3. history 分页 before_id ----
    # f1 发 60 条单聊给 f2; f1 视角查与 f2 的历史
    for i in range(60):
        await recv.send(json.dumps({"type": "msg", "to": "f2", "body": f"p-{i}"}))
        await asyncio.sleep(0.3)  # 真实节奏: 低于限流 5/s
    await asyncio.sleep(0.3)

    h1 = await ask(recv, {"type": "history", "chat": "f2", "limit": 50})
    rows1 = next((m["rows"] for m in h1 if m.get("type") == "history"), [])
    print(f"  history 默认50条: {len(rows1)}")
    if rows1:
        earliest = min(m["id"] for m in rows1)
        h2 = await ask(recv, {"type": "history", "chat": "f2", "limit": 50, "before_id": earliest})
        rows2 = next((m["rows"] for m in h2 if m.get("type") == "history"), [])
        print(f"  history before_id={earliest} 翻页: {len(rows2)} 条")
        page_ok = len(rows2) > 0 and all(m["id"] < earliest for m in rows2)
        ids1 = {m["id"] for m in rows1}
        ids2 = {m["id"] for m in rows2}
        page_ok = page_ok and not (ids1 & ids2)
        print(f"  {'OK' if page_ok else 'FAIL'} 分页 (更早{(rows2[0]['id'] if rows2 else '-')}..{(rows2[-1]['id'] if rows2 else '-')}, 无重叠={not bool(ids1&ids2)})")
        if not page_ok:
            FAIL.append("history before_id 分页")

    # ---- 4. receipts: f2 已读后, f1 查 receipts 应看到 read ----
    f2ws, f2init = await login_conn("f2", "x")
    f2msgs = [m for m in f2init if m.get("type") == "msg"]
    if f2msgs:
        did = f2msgs[0]["id"]
        await f2ws.send(json.dumps({"type": "ack_read", "id": did}))
        await asyncio.sleep(0.3)
        r1 = await ask(recv, {"type": "receipts", "chat": "f2"})
        rows = next((m["rows"] for m in r1 if m.get("type") == "receipts"), [])
        target = [m for m in rows if m.get("id") == did]
        rcpt_ok = bool(target and target[0].get("state") == "read")
        print(f"  {'OK' if rcpt_ok else 'FAIL'} receipts: msg {did} state={target[0].get('state') if target else 'N/A'} (期望 read)")
        if not rcpt_ok:
            FAIL.append("receipts read")
    await f2ws.close()

    # ---- 5. 群聊 unread: key 统一为 group::<gid> (与 history 入参一致) ----
    # f1 建群 gp1, 加 f2; f2 离线, f1 发群消息; f2 登录 unread 应含 group::gp1
    async with websockets.connect(URL, open_timeout=5) as g1:
        await g1.send(json.dumps({"type": "register", "user": "f3", "pass": "x"}))
        try:
            await asyncio.wait_for(g1.recv(), 0.5)
        except Exception:
            pass
    gf1, _ = await login_conn(uid, "x")
    await gf1.send(json.dumps({"type": "create_group", "gid": "gp1", "name": "gp1"}))
    await asyncio.sleep(0.2)
    await gf1.send(json.dumps({"type": "add_member", "gid": "gp1", "user": "f2"}))
    await asyncio.sleep(0.2)
    # f2 离线 (前面已 close)
    await gf1.send(json.dumps({"type": "msg", "to": "gp1", "body": "grp-hello", "is_group": True}))
    await asyncio.sleep(0.3)

    gf2, _ = await login_conn("f2", "x")
    # f2 查 unread: 应含 group::gp1
    ug = await ask(gf2, {"type": "unread"})
    key_ok = False
    for m in ug:
        if m.get("type") == "unread":
            items = m.get("items", [])
            hit = [i for i in items if i.get("chat") == "group::gp1"]
            key_ok = bool(hit and hit[0]["count"] == 1)
            print(f"  {'OK' if key_ok else 'FAIL'} 群聊 unread key=group::gp1 count={hit[0]['count'] if hit else 'N/A'} (期望1, 协议统一)")
    if not key_ok:
        FAIL.append("群聊 unread key 一致性")
    await gf2.close()
    await gf1.close()
    await recv.close()

    print()
    if FAIL:
        print("HAS-FAIL:", FAIL)
        sys.exit(1)
    print("✅ features ALL-PASS")


asyncio.run(main())