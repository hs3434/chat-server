#!/usr/bin/env python3
"""Go 群生命周期测试: 踢人/退群/解散 + 权限
用法: python3 test_group_lifecycle.py <port>
覆盖:
  1. remove_member: 群主踢人, 被踢者不能再收群消息
  2. remove_member: 非群主踢人 -> error not_owner
  3. leave_group: 成员退群, 退群后不再收消息
  4. dissolve_group: 群主解散, 群不存在 + 成员不能再发
  5. dissolve_group: 非群主解散 -> error not_owner
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


async def ask(ws, obj, wait=1.0):
    await ws.send(json.dumps(obj))
    got = []
    try:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=wait)
            got.append(json.loads(raw))
    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        pass
    return got


async def reg(user):
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "register", "user": user, "pass": "x"}))
        try:
            await asyncio.wait_for(ws.recv(), 0.5)
        except Exception:
            pass


async def members_of(gid):
    con = sqlite3.connect(DB)
    rows = [r[0] for r in con.execute("SELECT user FROM group_members WHERE gid=?", (gid,)).fetchall()]
    con.close()
    return rows


async def main():
    G = "gl"
    await reg("gowner")
    for u in ["g1", "g2", "g3"]:
        await reg(u)

    owner, _ = await login_conn("gowner", "x")
    await owner.send(json.dumps({"type": "create_group", "gid": G, "name": G}))
    await asyncio.sleep(0.2)
    for u in ["g1", "g2", "g3"]:
        await owner.send(json.dumps({"type": "add_member", "gid": G, "user": u}))
    await asyncio.sleep(0.3)

    # ---- 1. 群主踢 g1 ----
    r = await ask(owner, {"type": "remove_member", "gid": G, "user": "g1"})
    ok = any(m.get("type") == "group_ok" for m in r)
    mem = await members_of(G)
    kicked = ok and "g1" not in mem
    print(f"  {'OK' if kicked else 'FAIL'} 群主踢 g1 {mem}")
    if not kicked:
        FAIL.append("remove_member")

    # 被踢的 g1 发群消息 -> 应 not_member
    g1ws, _ = await login_conn("g1", "x")
    r2 = await ask(g1ws, {"type": "msg", "to": G, "body": "i am g1", "is_group": True})
    rejected = any(m.get("type") == "error" and m.get("code") == "not_member" for m in r2)
    print(f"  {'OK' if rejected else 'FAIL'} 被踢者发群消息被拒 not_member")
    if not rejected:
        FAIL.append("被踢者发群被拒")

    # 审查补充: 被踢者查群历史被拒 (微信语义: 退群/被踢后看不到该群)
    r2b = await ask(g1ws, {"type": "history", "chat": "group::" + G, "limit": 10})
    hist_rejected = any(m.get("type") == "error" and m.get("code") == "not_member" for m in r2b)
    print(f"  {'OK' if hist_rejected else 'FAIL'} 被踢者查群历史被拒 not_member")
    if not hist_rejected:
        FAIL.append("被踢者查群历史")

    # ---- 2. 非群主(g2)踢人 -> not_owner ----
    g2ws, _ = await login_conn("g2", "x")
    r3 = await ask(g2ws, {"type": "remove_member", "gid": G, "user": "g3"})
    not_owner = any(m.get("type") == "error" and m.get("code") == "not_owner" for m in r3)
    mem2 = await members_of(G)
    still = "g3" in mem2
    print(f"  {'OK' if not_owner and still else 'FAIL'} 非群主踢人被拒 not_owner (g3仍在={still})")
    if not (not_owner and still):
        FAIL.append("非群主踢人")

    # ---- 3. g2 退群 ----
    r4 = await ask(g2ws, {"type": "leave_group", "gid": G})
    left = any(m.get("type") == "group_ok" for m in r4)
    mem3 = await members_of(G)
    left_ok = left and "g2" not in mem3
    print(f"  {'OK' if left_ok else 'FAIL'} g2 退群 {mem3}")
    if not left_ok:
        FAIL.append("leave_group")

    # 审查补充: 退群后 g2 的群 msg_state 已清 (不再收到该群未读)
    con = sqlite3.connect(DB)
    st_left = con.execute("SELECT COUNT(*) FROM msg_state WHERE user=? AND msg_id IN (SELECT id FROM messages WHERE gid=?)", ("g2", "group:" + G)).fetchone()[0]
    con.close()
    st_ok = st_left == 0
    print(f"  {'OK' if st_ok else 'FAIL'} 退群者群 msg_state 已清 ({st_left} 条)")
    if not st_ok:
        FAIL.append("退群清 msg_state")

    # ---- 4. 群主解散 ----
    r5 = await ask(owner, {"type": "dissolve_group", "gid": G})
    dissolved = any(m.get("type") == "group_ok" for m in r5)
    mem4 = await members_of(G)
    gone = dissolved and len(mem4) == 0
    con = sqlite3.connect(DB)
    gexists = con.execute("SELECT COUNT(*) FROM groups WHERE gid=?", (G,)).fetchone()[0] > 0
    con.close()
    print(f"  {'OK' if gone and not gexists else 'FAIL'} 群主解散 (groups存在={gexists}, 成员={mem4})")
    if not (gone and not gexists):
        FAIL.append("dissolve_group")

    # ---- 5. 非群主解散不存在/他人群 -> not_owner ----
    await reg("g5")
    await owner.send(json.dumps({"type": "create_group", "gid": "gl2", "name": "gl2"}))
    await asyncio.sleep(0.2)
    g5ws, _ = await login_conn("g5", "x")
    r6 = await ask(g5ws, {"type": "dissolve_group", "gid": "gl2"})
    not_owner2 = any(m.get("type") == "error" and m.get("code") == "not_owner" for m in r6)
    con = sqlite3.connect(DB)
    still2 = con.execute("SELECT COUNT(*) FROM groups WHERE gid='gl2'").fetchone()[0] == 1
    con.close()
    print(f"  {'OK' if not_owner2 and still2 else 'FAIL'} 非群主解散被拒 (群仍在={still2})")
    if not (not_owner2 and still2):
        FAIL.append("非群主解散")

    await g5ws.close()
    await g2ws.close()
    await g1ws.close()
    await owner.close()

    print()
    if FAIL:
        print("HAS-FAIL:", FAIL)
        sys.exit(1)
    print("✅ group_lifecycle ALL-PASS")


asyncio.run(main())