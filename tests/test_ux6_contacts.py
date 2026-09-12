#!/usr/bin/env python3
"""UX6: 好友(联系人) + 搜索 协议测试"""
import asyncio, json, sys, time, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from websockets.asyncio.client import connect
import random
URL = os.environ.get("WSLIKE_URL", "ws://127.0.0.1:8081/ws")
P = "testpass"


async def main():
    rnd = str(random.randint(100000, 999999))
    UA, UB, UC = "fr_a_" + rnd, "fr_b_" + rnd, "fr_c_" + rnd
    results = []

    async def newconn():
        return await connect(URL, open_timeout=5)

    async def req(ws, m, expect=None, timeout=6):
        m = dict(m)
        m["seq"] = int(time.time() * 1000) % 100000000  # 匹配 seq, 不把推送/回显当响应
        await ws.send(json.dumps(m))
        t0 = time.time()
        while time.time() - t0 < timeout:
            r = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
            if r.get("seq") == m["seq"]:
                if expect is None or r.get("type") == expect or r.get("type") == "error":
                    return r
        return None

    async def register_login(ws, u):
        await req(ws, {"type": "register", "user": u, "pass": P}, "register_ok")
        # 注册只建号不置登录态, 必须显式 login
        r = await req(ws, {"type": "login", "user": u, "pass": P}, "login_ok")
        assert r and r["type"] == "login_ok", f"login failed: {r}"
        return r

    ok = lambda name, cond, extra="": results.append((name, bool(cond), extra))

    # B/C 先注册 (注册完即断开; 搜人/加好友不要求对方在线)
    for u in (UB, UC):
        w = await newconn()
        await register_login(w, u)
        await w.close()

    async with await newconn() as wa:
        await register_login(wa, UA)

        # 1. 空好友列表
        r = await req(wa, {"type": "contacts"}, "contacts")
        ok("空好友列表返回 []", r and r.get("friends") == [])

        # 2. 搜索不存在的人
        r = await req(wa, {"type": "search_users", "q": "no_such_" + rnd}, "search_users")
        ok("搜索无结果返回空数组", r and r.get("users") == [])

        # 3. 搜到用户 (按用户名)
        r = await req(wa, {"type": "search_users", "q": "fr_b_" + rnd}, "search_users")
        found = r and any(x["username"] == UB for x in (r.get("users") or []))
        ok("搜索按用户名命中", found, str(r))
        hit = next((x for x in (r.get("users") or []) if x["username"] == UB), {})
        ok("搜索结果带 is_contact=false", hit.get("is_contact") is False, str(hit))

        # 4. 加好友 -> 双向
        r = await req(wa, {"type": "add_contact", "friend": UB}, "contact_ok")
        ok("A 加 B 成功", r and r.get("type") == "contact_ok" and r.get("friend") == UB, str(r))
        r = await req(wa, {"type": "contacts"}, "contacts")
        ok("A 好友列表含 B", r and UB in (r.get("friends") or []), str(r))
        async with await newconn() as wb:
            await register_login(wb, UB)
            r = await req(wb, {"type": "contacts"}, "contacts")
            await wb.close()
        ok("B 好友列表含 A (双向)", r and UA in (r.get("friends") or []), str(r))

        # 5. 重复加幂等
        r = await req(wa, {"type": "add_contact", "friend": UB}, "contact_ok")
        ok("重复加好友幂等成功", r and r.get("type") == "contact_ok", str(r))

        # 6. 搜索结果 is_contact 变 true
        r = await req(wa, {"type": "search_users", "q": "fr_b_" + rnd}, "search_users")
        hit = next((x for x in (r.get("users") or []) if x["username"] == UB), {})
        ok("搜索结果 is_contact=true", hit.get("is_contact") is True, str(hit))

        # 7. 加自己被拒
        r = await req(wa, {"type": "add_contact", "friend": UA}, None)
        ok("加自己被拒 bad_param", r and r.get("type") == "error" and r.get("code") == "bad_param", str(r))

        # 8. 加不存在用户被拒
        r = await req(wa, {"type": "add_contact", "friend": "ghost_" + rnd}, None)
        ok("加不存在用户 no_such_user", r and r.get("type") == "error" and r.get("code") == "no_such_user", str(r))

        # 9. 未登录请求被拒 (服务端对未登录静默丢弃, 无回包 -> 超时也算通过)
        async with await newconn() as wn:
            await wn.send(json.dumps({"type": "contacts", "seq": 999}))
            got = False
            try:
                r2 = json.loads(await asyncio.wait_for(wn.recv(), timeout=2))
                got = (r2.get("type") == "error" and r2.get("code") == "unauthorized")
            except asyncio.TimeoutError:
                got = True  # 静默丢弃 = 未授权请求未被执行, 语义通过
            ok("未登录 contacts 被拒", got)

        # 10. 删好友 -> 双向消失
        r = await req(wa, {"type": "del_contact", "friend": UB}, "contact_ok")
        ok("A 删 B 成功", r and r.get("type") == "contact_ok", str(r))
        r = await req(wa, {"type": "contacts"}, "contacts")
        ok("删后 A 列表不含 B", r and UB not in (r.get("friends") or []), str(r))
        async with await newconn() as wb:
            await register_login(wb, UB)
            r = await req(wb, {"type": "contacts"}, "contacts")
            await wb.close()
        ok("删后 B 列表不含 A (双向)", r and UA not in (r.get("friends") or []), str(r))

    fails = [x for x in results if not x[1]]
    for name, s, extra in results:
        print(("PASS " if s else "FAIL ") + name + ("" if s else " | " + extra))
    print(f"==== UX6: {len(results) - len(fails)}/{len(results)} ALL-PASS ====" if not fails else f"==== UX6: {len(fails)} FAILED ====")
    sys.exit(1 if fails else 0)

asyncio.run(main())
