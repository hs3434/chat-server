import asyncio, json, time
import websockets

URL = "ws://127.0.0.1:8081/ws"
PASS = []

def ok(name, cond, detail=""):
    PASS.append((name, cond, detail))
    print(("PASS " if cond else "FAIL ") + name + (" | " + str(detail) if detail else ""))

async def req(ws, obj, want, t=5):
    obj = dict(obj); obj["seq"] = int(time.time()*1000) % 1000000
    await ws.send(json.dumps(obj)); end = time.time() + t
    while time.time() < end:
        try:
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout=end-time.time()))
        except asyncio.TimeoutError:
            return None
        if m.get("type") in (want, "error"):
            return m
    return None

async def main():
    U = "fx_a%d" % int(time.time()); V = "fx_b%d" % int(time.time()); W = "fx_c%d" % int(time.time())
    async with websockets.connect(URL) as wa, websockets.connect(URL) as wb, websockets.connect(URL) as wc:
        for ws, u in ((wa, U), (wb, V), (wc, W)):
            r = await req(ws, {"type":"register","user":u,"pass":"p1"}, "register_ok")
            assert r and r.get("type") == "register_ok", r
            r = await req(ws, {"type":"login","user":u,"pass":"p1"}, "login_ok")
            assert r and r.get("type") == "login_ok", r

        # ---- 修复1: get_profile 带 username ----
        r = await req(wa, {"type":"get_profile","to_user":V}, "profile", 3)
        ok("profile带username", r and r.get("profile",{}).get("username") == V, (r or {}).get("profile"))

        # ---- 建群 (U群主, 拉V) ----
        G = "gfx%d" % int(time.time())
        r = await req(wa, {"type":"create_group","gid":G,"name":"修测群"}, "group_ok")
        assert r and r.get("type") == "group_ok", r
        r = await req(wa, {"type":"add_member","gid":G,"user":V}, "group_ok")
        ok("群主拉人", r and r.get("type") == "group_ok", r)

        # ---- 修复4: 非成员拉人被拒 / 拉不存在用户被拒 / 重复拉被拒 / 成员(W拉U前U已在) ----
        r = await req(wc, {"type":"add_member","gid":G,"user":V}, "error", 3)
        ok("非成员拉人拒绝not_member", r and r.get("code")=="not_member", r)
        r = await req(wc, {"type":"add_member","gid":G,"user":U}, "error", 3)
        ok("非成员拉人拒绝(另一路径)", r and r.get("code")=="not_member", r)
        r = await req(wa, {"type":"add_member","gid":G,"user":"ghost_404"}, "error", 3)
        ok("拉不存在用户拒绝no_such_user", r and r.get("code")=="no_such_user", r)
        r = await req(wa, {"type":"add_member","gid":G,"user":V}, "error", 3)
        ok("重复拉人拒绝already_member", r and r.get("code")=="already_member", r)
        # W拉U进群 -> 成员可以拉人 (先让W进群: V是成员, V拉W)
        r = await req(wb, {"type":"add_member","gid":G,"user":W}, "group_ok")
        ok("普通成员V拉W进群", r and r.get("type") == "group_ok", r)
        r = await req(wc, {"type":"add_member","gid":G,"user":U}, "error", 3)
        ok("重复拉人拒绝(W拉已在群U)", r and r.get("code")=="already_member", r)

        # ---- 修复2: ack_read 回包 + 未读清零 ----
        r = await req(wb, {"type":"msg","to":G,"body":"m1","is_group":True}, "msg", 2)
        assert r, "V发消息无回: %r" % r
        # U 收群消息 -> 确认未读=1 -> ack_read 应回包 -> 未读清零
        gmsg = None
        try:
            while True:
                m = json.loads(await asyncio.wait_for(wa.recv(), timeout=3))
                if m.get("type") == "msg" and m.get("from") == V:
                    gmsg = m; break
        except asyncio.TimeoutError:
            pass
        assert gmsg, "U未收到群消息"
        # unread 只统计有 last_ts 的会话? 群刚建可能不出现 -> 查库为准
        import sqlite3 as _sq
        _db = _sq.connect("/workspace/wxlike-server/wxlike_go.db")
        _n = _db.execute("SELECT COUNT(*) FROM msg_state st JOIN messages m ON m.id=st.msg_id WHERE st.user=? AND st.state IN ('pending','delivered') AND m.gid=?", (U, "group:"+G)).fetchone()[0]
        _db.close()
        ok("群未读=1(库)", _n == 1, {"db_unread": _n})
        got_ack = await req(wa, {"type":"ack_read","id":gmsg["id"]}, "ack_read", 3)
        ok("ack_read有回包(带seq)", got_ack is not None and got_ack.get("type")=="ack_read", got_ack)
        r = await req(wa, {"type":"unread"}, "unread")
        gcnt = next((x.get("unread") for x in (r or {}).get("items",[]) if x.get("chat")=="group::"+G), 0)
        ok("群未读清零", gcnt == 0, {"group_unread": gcnt})

        # ---- 修复3: 群主退群拦截 / 普通成员退群OK ----
        r = await req(wa, {"type":"leave_group","gid":G}, "error", 3)
        ok("群主退群被拦截owner_must_transfer", r and r.get("code")=="owner_must_transfer", r)
        r = await req(wc, {"type":"leave_group","gid":G}, "group_ok", 3)
        ok("普通成员退群OK", r and r.get("type") == "group_ok", r)
        # 转让后原群主也能退 (转让 U->V, U退)
        r = await req(wa, {"type":"transfer_owner","gid":G,"user":V}, "group_ok")
        ok("转让群主OK", r and r.get("type") == "group_ok", r)
        r = await req(wa, {"type":"leave_group","gid":G}, "group_ok", 3)
        ok("转让后原群主可退群", r and r.get("type") == "group_ok", r)

    print()
    npass = sum(1 for _,c,_ in PASS if c)
    print("ALL-PASS" if npass == len(PASS) else "HAS-FAIL")
    print("%d/%d" % (npass, len(PASS)))

asyncio.run(main())
