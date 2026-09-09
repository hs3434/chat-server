#!/usr/bin/env python3
"""Profile 协议测试 (昵称/签名/头像 action 回归覆盖)
用法: python3 test_profile.py <port>
覆盖:
  1. login_ok 携带 nickname/signature/avatar 字段 (空回落)
  2. set_profile 保存昵称/签名 -> profile_ok 回显新资料
  3. get_profile 查他人资料卡
  4. 改资料后, 与其有会话的在线用户收到 profile_evt
  5. 未登录调用 get_profile/set_profile -> error unauthorized
"""
import asyncio, json, sys, time
import websockets

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
URL = f"ws://127.0.0.1:{PORT}/ws"
FAIL = []
su = f"pf{int(time.time())}"


async def login_user(user, pwd="x"):
    ws = await websockets.connect(URL, open_timeout=5)
    await ws.send(json.dumps({"type": "register", "user": user, "pass": pwd}))
    try:
        await asyncio.wait_for(ws.recv(), 0.5)
    except Exception:
        pass
    await ws.send(json.dumps({"type": "login", "user": user, "pass": pwd}))
    return ws


async def recv_until(ws, want, n=12, timeout=1.0):
    got = []
    err = None
    for _ in range(n):
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        except Exception as e:
            err = e
            break
        m = json.loads(raw)
        got.append(m)
        if m.get("type") == want:
            return m, got
    return None, got


async def main():
    a = await login_user(su + "a")
    # 1. login_ok 带 profile 字段
    m, _ = await recv_until(a, "login_ok")
    ok1 = m is not None and "nickname" in m and "avatar" in m and "signature" in m
    print(f"  {'OK' if ok1 else 'FAIL'} login_ok 携带 profile 字段")
    if not ok1:
        FAIL.append("login_ok profile fields")

    # 2. set_profile
    await a.send(json.dumps({"type": "set_profile", "nickname": "阿龙", "signature": "前端工程师", "seq": 1}))
    m2, _ = await recv_until(a, "profile_ok")
    prof = (m2 or {}).get("profile", {}) if m2 else {}
    ok2 = m2 is not None and prof.get("nickname") == "阿龙" and prof.get("signature") == "前端工程师"
    print(f"  {'OK' if ok2 else 'FAIL'} set_profile 保存并列回 (nick={prof.get('nickname')})")
    if not ok2:
        FAIL.append("set_profile")

    # 3. get_profile (他人视角: b 无会话也能查)
    b = await login_user(su + "b")
    await b.send(json.dumps({"type": "get_profile", "to_user": su + "a", "seq": 2}))
    m3, _ = await recv_until(b, "profile")
    p3 = (m3 or {}).get("profile", {}) if m3 else {}
    ok3 = m3 is not None and p3.get("username") == (su + "a") and p3.get("nickname") == "阿龙"
    print(f"  {'OK' if ok3 else 'FAIL'} get_profile 查他人 (nick={p3.get('nickname')})")
    if not ok3:
        FAIL.append("get_profile")

    # 4. a 再改资料 -> b 收到 profile_evt? 需 a/b 有会话 (single msg 往来)
    #    让 a 给 b 发一条(建立会话) 
    await a.send(json.dumps({"type": "msg", "to": su + "b", "body": "hi", "seq": 3}))
    await asyncio.sleep(0.2)
    await a.send(json.dumps({"type": "set_profile", "nickname": "阿龍V2", "seq": 4}))
    # b 应收到 profile_evt (因为 a~b 已是会话)
    m4, got4 = await recv_until(b, "profile_evt")
    evt = m4 if m4 else {}
    ok4 = bool(evt) and evt.get("user") == (su + "a") and evt.get("nickname") == "阿龍V2"
    print(f"  {'OK' if ok4 else 'FAIL'} profile_evt 推给有会话的人 (gots={len(got4)})")
    if not ok4:
        FAIL.append("profile_evt")

    # 5. 未登录 get_profile / set_profile
    anon = await websockets.connect(URL, open_timeout=5)
    await anon.send(json.dumps({"type": "set_profile", "nickname": "x", "seq": 9}))
    man, _ = await recv_until(anon, "error")
    await anon.send(json.dumps({"type": "get_profile", "to_user": su + "a", "seq": 10}))
    man2, _ = await recv_until(anon, "error")
    ok5 = man is not None and man.get("code") == "unauthorized" and man2 is not None and man2.get("code") == "unauthorized"
    print(f"  {'OK' if ok5 else 'FAIL'} 未登录非法操作被拒")
    if not ok5:
        FAIL.append("unauthorized guard")

    for w in (a, b, anon):
        try:
            await w.close()
        except Exception:
            pass

    print()
    if FAIL:
        print("HAS-FAIL:", FAIL)
        sys.exit(1)
    print("✅ profile ALL-PASS")


asyncio.run(main())
