#!/usr/bin/env python3
"""Go 设备会话管理测试
用法: python3 test_devices.py <port>
覆盖:
  1. 单设备登录: sessions 列出该设备 (带 sid/ip/since)
  2. 同账号双设备登录: sessions 列 2 台
  3. 新设备登录: 旧设备收到 device_evt
  4. 踢指定设备: 被踢的收到 kicked + 连接断
  5. kick 后 sessions 只剩未踢的
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
                return ws, got
    except Exception:
        return ws, got


async def send_and_get(ws, obj, want_type, timeout=2.0):
    await ws.send(json.dumps(obj))
    try:
        while True:
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout))
            if m.get("type") == want_type or m.get("type") == "error":
                return m
    except Exception:
        return None


async def wait_type(ws, want, timeout=2.0):
    try:
        while True:
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout))
            if m.get("type") == want:
                return m
    except Exception:
        return None


async def main():
    await reg("dev_user")
    # 设备 1
    d1, _ = await login("dev_user")

    # 1. sessions 列出该设备
    s1 = await send_and_get(d1, {"type": "sessions"}, "sessions")
    ok1 = s1 is not None and len(s1.get("devices", [])) == 1 and "sid" in s1["devices"][0]
    sid1 = s1["devices"][0]["sid"] if ok1 else None
    check("单设备: sessions 列 1 台 (sid/ip)", ok1, f"d={s1.get('devices') if s1 else None}")

    # 2. 设备 2 登录
    d2, _ = await login("dev_user")

    # 3. 设备 2 登录时 d1 收到 device_evt (先等事件再查 sessions, 避免被消费)
    evt = await wait_type(d1, "device_evt")
    check("新设备登录: 旧设备收 device_evt", evt is not None and evt.get("sid") and evt.get("sid") != "", f"evt_sid={evt.get('sid') if evt else None}")

    # 2b. sessions 列 2 台
    s2 = await send_and_get(d1, {"type": "sessions"}, "sessions")
    check("双设备: sessions 列 2 台", s2 is not None and len(s2.get("devices", [])) == 2, f"n={len(s2.get('devices',[])) if s2 else None}")

    # 找到 d2 的 sid
    sid2 = None
    if s2:
        for dev in s2["devices"]:
            if dev["sid"] != sid1:
                sid2 = dev["sid"]

    # 4. d1 kick d2 -> d2 收 kicked + 连接断
    k = await send_and_get(d1, {"type": "kick", "sid": sid2}, "kick_ok")
    check("发起 kick -> kick_ok", k is not None and k.get("type") == "kick_ok", f"k={k}")
    kicked = await wait_type(d2, "kicked", timeout=2.0)
    # d2 应收到 kicked, 然后 onclose
    closed = False
    try:
        while True:
            m = json.loads(await asyncio.wait_for(d2.recv(), 3.0))
            if m.get("type") == "kicked":
                kicked = m
    except Exception:
        closed = True
    check("被踢设备收 kicked", kicked is not None and kicked.get("type") == "kicked", f"kicked={kicked}")
    check("被踢设备连接断开", closed, f"closed={closed}")

    # 5. kick 后 sessions 只剩 d1
    s5 = await send_and_get(d1, {"type": "sessions"}, "sessions")
    check("kick 后 sessions 只 1 台", s5 is not None and len(s5.get("devices", [])) == 1, f"n={len(s5.get('devices',[])) if s5 else None}")

    await d1.close()
    try:
        await d2.close()
    except Exception:
        pass
    print()
    if FAIL:
        print("HAS-FAIL:", FAIL)
        sys.exit(1)
    print("✅ devices ALL-PASS")


asyncio.run(main())
