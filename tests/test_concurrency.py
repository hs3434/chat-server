#!/usr/bin/env python3
"""Go 并发正确性测试: 多用户并发写 + 并发 ack, 验证不丢消息/不锁错
用法: python3 test_concurrency.py <port>
覆盖:
  1. 3 用户并发向同一目标发消息 (50/用户), 全部落库且 id 不重复
  2. 并发 ack_read, 状态正确
  3. 群聊并发: 多成员同时发群消息, 所有成员都收到 (不丢)
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
            raw = await asyncio.wait_for(ws.recv(), timeout=0.8)
            m = json.loads(raw)
            initial.append(m)
            if m.get("type") == "login_ok":
                break
    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        pass
    return ws, initial


async def reg(user):
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "register", "user": user, "pass": "x"}))
        try:
            await asyncio.wait_for(ws.recv(), 0.5)
        except Exception:
            pass


async def sender_loop(user, target, prefix, count, is_group=False):
    """连续发 count 条消息 (可指定群聊), 返回发送数"""
    ws, _ = await login_conn(user, "x")
    sent = 0
    for i in range(count):
        obj = {"type": "msg", "to": target, "body": f"{prefix}-{i}"}
        if is_group:
            obj["is_group"] = True
        await ws.send(json.dumps(obj))
        sent += 1
        await asyncio.sleep(0.3)  # 真实节奏: 每条 0.3s, 低于限流 5/s 阈值
    await asyncio.sleep(0.5)
    await ws.close()
    return sent


async def main():
    # 3 发送者 + 1 接收者
    users = ["ca", "cb", "cc"]
    target = "cz"
    await reg(target)
    for u in users:
        await reg(u)

    N = 30
    # ---- 1. 并发单聊: 3 用户并发向 cz 发 N 条, 全部落库且 id 唯一 ----
    tasks = [sender_loop(u, target, u, N) for u in users]
    sent_counts = await asyncio.gather(*tasks)
    total_sent = sum(sent_counts)
    await asyncio.sleep(1.0)

    con = sqlite3.connect(DB)
    # cz 收到的单聊消息 (recipient=cz)
    got = con.execute("SELECT COUNT(*), COUNT(DISTINCT id) FROM messages WHERE recipient=?", (target,)).fetchone()
    print(f"  {'OK' if got[0] == total_sent else 'FAIL'} 并发单聊落库 {got[0]}/{total_sent} (期望 {total_sent})")
    if got[0] != total_sent:
        FAIL.append(f"并发单聊 {got[0]}/{total_sent}")
    print(f"  {'OK' if got[1] == total_sent else 'FAIL'} id 唯一 {got[1]}/{total_sent}")
    if got[1] != total_sent:
        FAIL.append("并发 id 唯一")

    # msg_state 也有 90 条 (cz)
    st = con.execute("SELECT COUNT(*) FROM msg_state WHERE user=? AND state='pending'", (target,)).fetchone()
    print(f"  {'OK' if st[0] == total_sent else 'FAIL'} cz msg_state pending {st[0]}/{total_sent}")
    if st[0] != total_sent:
        FAIL.append("cz msg_state")

    # ---- 2. 并发 ack_read: cz 并发把全部标已读 ----
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "login", "user": target, "pass": "x"}))
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                m = json.loads(raw)
                if m.get("type") == "login_ok":
                    break
        except Exception:
            pass
        ids = [r[0] for r in con.execute("SELECT msg_id FROM msg_state WHERE user=?", (target,)).fetchall()]
        # 并发发 ack_read
        for i in range(0, len(ids), 20):
            batch = ids[i:i+20]
            await asyncio.gather(*[ws.send(json.dumps({"type": "ack_read", "id": mid})) for mid in batch])
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.5)
    con2 = sqlite3.connect(DB)
    read_count = con2.execute("SELECT COUNT(*) FROM msg_state WHERE user=? AND state='read'", (target,)).fetchone()[0]
    print(f"  {'OK' if read_count == total_sent else 'FAIL'} 并发 ack_read {read_count}/{total_sent}")
    if read_count != total_sent:
        FAIL.append(f"并发 ack_read {read_count}/{total_sent}")
    con.close()
    con2.close()

    # ---- 3. 群聊并发: 建群 gz, 3 成员并发发, 全部落下且成员都收到 ----
    await reg("cowner")
    owner, _ = await login_conn("cowner", "x")
    await owner.send(json.dumps({"type": "create_group", "gid": "gz", "name": "gz"}))
    await asyncio.sleep(0.2)
    all_users = users + [target]
    for u in all_users:
        await owner.send(json.dumps({"type": "add_member", "gid": "gz", "user": u}))
    await asyncio.sleep(0.3)
    await owner.close()

    M = 15
    tasks2 = [sender_loop(u, "gz", f"g{u}", M, is_group=True) for u in all_users]
    sent2 = await asyncio.gather(*tasks2)
    total2 = sum(sent2)
    await asyncio.sleep(1.0)

    con3 = sqlite3.connect(DB)
    gcount = con3.execute("SELECT COUNT(*) FROM messages WHERE gid='group:gz'").fetchone()[0]
    print(f"  {'OK' if gcount == total2 else 'FAIL'} 并发群聊落库 {gcount}/{total2}")
    if gcount != total2:
        FAIL.append(f"并发群聊 {gcount}/{total2}")
    # 每个成员应有 4*M 条 state (自己的不入队, 3*M)
    for u in all_users:
        mine = con3.execute("SELECT COUNT(*) FROM messages WHERE gid='group:gz' AND sender=?", (u,)).fetchone()[0]
        own = con3.execute("SELECT COUNT(*) FROM msg_state WHERE user=? AND msg_id IN (SELECT id FROM messages WHERE gid='group:gz')", (u,)).fetchone()[0]
        exp = total2 - mine
        ok = own == exp
        print(f"  {'OK' if ok else 'FAIL'} 成员 {u}: msg_state {own}/{exp} (自己的不计)")
        if not ok:
            FAIL.append(f"成员{u} 群state {own}/{exp}")
    con3.close()

    print()
    if FAIL:
        print("HAS-FAIL:", FAIL)
        sys.exit(1)
    print("✅ concurrency ALL-PASS")


asyncio.run(main())