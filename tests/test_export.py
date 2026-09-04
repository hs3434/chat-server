#!/usr/bin/env python3
"""Go 数据导出 + 在线状态测试
用法: python3 test_export.py <port>
前置: 服务器需 --admin exp_admin 启动 (run_all 里配 RATE_FLAG)
覆盖:
  1. 导出: 无 token -> 401
  2. 导出: 非管理员 token -> 401
  3. 导出: 管理员 token -> 200 + 全表 JSON (accounts/messages/audit_log)
  4. presence 查询: 在线/离线状态
  5. presence_evt: 好友上线收到事件 / 下线收到事件
"""
import asyncio, json, sys
import urllib.request
import websockets

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
URL = f"ws://127.0.0.1:{PORT}/ws"
HTTP = f"http://127.0.0.1:{PORT}"
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


def http_get(path):
    try:
        with urllib.request.urlopen(HTTP + path, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, None


async def main():
    await reg("exp_admin"), await reg("exp_user")
    admin_ws, _ = await login("exp_admin")
    # 找 admin token
    admin_token = None
    for m in _:
        if m.get("type") == "login_ok":
            admin_token = m.get("token")
    await admin_ws.close()

    user_ws, umsgs = await login("exp_user")
    user_token = None
    for m in umsgs:
        if m.get("type") == "login_ok":
            user_token = m.get("token")

    # 造一条数据 (exp_user 发消息给自己或互相)
    await user_ws.send(json.dumps({"type": "msg", "to": "exp_admin", "body": "export-me"}))
    await asyncio.sleep(0.3)

    # ---- 1/2/3. 导出 ----
    s1, _ = http_get("/export")
    check("导出无 token -> 401", s1 == 401, f"status={s1}")
    s2, _ = http_get(f"/export?token={user_token}")
    check("导出非管理员 token -> 401", s2 == 401, f"status={s2}")
    s3, data = http_get(f"/export?token={admin_token}")
    ok3 = s3 == 200 and data and isinstance(data.get("messages"), list) and "audit_log" in data
    # messages 里应有 export-me
    has_msg = False
    if data:
        for m in data.get("messages", []):
            if "export-me" in str(m.get("body", "")):
                has_msg = True
    check("导出管理员 token -> 200 + 全表 (含消息)", ok3 and has_msg,
          f"status={s3} messages={len(data.get('messages', [])) if data else None}")

    # ---- 4. presence 查询 ----
    await user_ws.send(json.dumps({"type": "presence", "users": ["exp_admin", "exp_user", "ghost"]}))
    try:
        while True:
            m = json.loads(await asyncio.wait_for(user_ws.recv(), 2.0))
            if m.get("type") == "presence":
                online = m.get("online", {})
                check("presence 查询: 自己在线/admin离线/ghost离线",
                      online.get("exp_user") is True and online.get("exp_admin") is False and online.get("ghost") is False,
                      f"online={online}")
                break
    except Exception:
        check("presence 查询超时", False)

    # ---- 5. presence_evt: 好友上线/下线事件 ----
    # exp_user 登录应已收到 exp_admin 之前的在线事件? admin 已下线. 让 admin 再上线:
    admin2, _ = await login("exp_admin")
    # user_ws 应收到 presence_evt {user:exp_admin, online:true}
    evt_up = None
    try:
        while True:
            m = json.loads(await asyncio.wait_for(user_ws.recv(), 2.0))
            if m.get("type") == "presence_evt" and m.get("user") == "exp_admin":
                evt_up = m
                break
    except Exception:
        pass
    check("好友上线收到 presence_evt online=true",
          evt_up is not None and evt_up.get("online") is True, f"evt={evt_up}")

    # admin 断开 -> user 收 online=false
    await admin2.close()
    evt_down = None
    try:
        while True:
            m = json.loads(await asyncio.wait_for(user_ws.recv(), 2.0))
            if m.get("type") == "presence_evt" and m.get("user") == "exp_admin":
                evt_down = m
                break
    except Exception:
        pass
    check("好友下线收到 presence_evt online=false",
          evt_down is not None and evt_down.get("online") is False, f"evt={evt_down}")

    await user_ws.close()
    print()
    if FAIL:
        print("HAS-FAIL:", FAIL)
        sys.exit(1)
    print("✅ export/presence ALL-PASS")


asyncio.run(main())