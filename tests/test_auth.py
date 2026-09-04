#!/usr/bin/env python3
"""Go token 认证测试: login 发 token, 后续请求带 token 验证
用法: python3 test_auth.py <port>
覆盖:
  1. login 返回 token
  2. 带有效 token 的请求 (未登录连接) 可执行业务 (history/unread)
  3. 无效 token -> error unauthorized
  4. 无 token -> 请求被忽略 (无响应)
  5. ack_read 带 token 也能生效
"""
import asyncio, json, sys
import websockets

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
URL = f"ws://127.0.0.1:{PORT}/ws"
FAIL = []


async def reg(user):
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "register", "user": user, "pass": "x"}))
        try:
            await asyncio.wait_for(ws.recv(), 0.5)
        except Exception:
            pass


async def main():
    await reg("tuser")
    await reg("tpeer")

    # ---- 1. login 返回 token ----
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "login", "user": "tuser", "pass": "x"}))
        got = []
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                m = json.loads(raw)
                got.append(m)
                if m.get("type") == "login_ok":
                    break
        except Exception:
            pass
        tok = next((m.get("token") for m in got if m.get("type") == "login_ok"), "")
        ok1 = bool(tok and len(tok) >= 16)
        print(f"  {'OK' if ok1 else 'FAIL'} login 返回 token ({tok[:12]}...)" if ok1 else f"  FAIL login 无 token")
        if not ok1:
            FAIL.append("login token")
        # 用这个连接发一条消息给 tpeer (产生历史)
        await ws.send(json.dumps({"type": "msg", "to": "tpeer", "body": "auth-msg"}))
        await asyncio.sleep(0.3)
    # 关闭连接

    # ---- 2. 新连接 (未登录) 带 token 查 history ----
    tok2 = tok
    async with websockets.connect(URL, open_timeout=5) as ws2:
        await ws2.send(json.dumps({"type": "history", "chat": "tpeer", "limit": 20, "token": tok2}))
        got2 = []
        try:
            while True:
                raw = await asyncio.wait_for(ws2.recv(), timeout=1.0)
                got2.append(json.loads(raw))
        except Exception:
            pass
        hrow = next((m for m in got2 if m.get("type") == "history"), None)
        ok2 = bool(hrow and any(r.get("body") == "auth-msg" for r in hrow.get("rows", [])))
        print(f"  {'OK' if ok2 else 'FAIL'} 有效 token 可查 history ({len(hrow.get('rows', [])) if hrow else 0} 条, 含 auth-msg={any(r.get('body')=='auth-msg' for r in (hrow.get('rows',[]) if hrow else []))})")
        if not ok2:
            FAIL.append("token history")

    # ---- 3. 无效 token -> unauthorized ----
    async with websockets.connect(URL, open_timeout=5) as ws3:
        await ws3.send(json.dumps({"type": "history", "chat": "tpeer", "limit": 20, "token": "badtoken-123456"}))
        got3 = []
        try:
            while True:
                raw = await asyncio.wait_for(ws3.recv(), timeout=1.0)
                got3.append(json.loads(raw))
        except Exception:
            pass
        bad = any(m.get("type") == "error" and m.get("code") == "unauthorized" for m in got3)
        print(f"  {'OK' if bad else 'FAIL'} 无效 token -> unauthorized")
        if not bad:
            FAIL.append("无效 token")

    # ---- 4. 无 token -> 请求被忽略 (无响应) ----
    async with websockets.connect(URL, open_timeout=5) as ws4:
        await ws4.send(json.dumps({"type": "history", "chat": "tpeer", "limit": 20}))
        try:
            raw = await asyncio.wait_for(ws4.recv(), timeout=0.8)
            got4 = json.loads(raw)
            ignored = False
            print(f"  {'FAIL' if got4 else 'OK'} 无 token 无响应 (收到: {got4})")
            if got4:
                FAIL.append("无 token 不应有响应")
        except asyncio.TimeoutError:
            ignored = True
            print(f"  OK 无 token 请求被忽略 (超时无响应)")
        except Exception:
            ignored = True

    # ---- 5. ack_read 带 token 生效 ----
    # tpeer 登录 ack_read 那条 auth-msg
    async with websockets.connect(URL, open_timeout=5) as ws5:
        await ws5.send(json.dumps({"type": "login", "user": "tpeer", "pass": "x"}))
        init5 = []
        try:
            while True:
                raw = await asyncio.wait_for(ws5.recv(), timeout=1.0)
                m5 = json.loads(raw)
                init5.append(m5)
                if m5.get("type") == "login_ok":
                    break
        except Exception:
            pass
        msg5 = next((m for m in init5 if m.get("type") == "msg"), None)
        if msg5:
            mid = msg5["id"]
            await ws5.send(json.dumps({"type": "ack_read", "id": mid, "token": tok}))
            await asyncio.sleep(0.3)
            # tuser 用 token 查 receipts 确认 read
            await ws5.send(json.dumps({"type": "logout"}))
            await asyncio.sleep(0.1)
    async with websockets.connect(URL, open_timeout=5) as ws6:
        await ws6.send(json.dumps({"type": "receipts", "chat": "tpeer", "token": tok}))
        got6 = []
        try:
            while True:
                raw = await asyncio.wait_for(ws6.recv(), timeout=1.0)
                got6.append(json.loads(raw))
        except Exception:
            pass
        rrow = next((m for m in got6 if m.get("type") == "receipts"), None)
        target = [r for r in (rrow.get("rows", []) if rrow else []) if r.get("body") == "auth-msg"]
        ok5 = bool(target and target[0].get("state") == "read")
        print(f"  {'OK' if ok5 else 'FAIL'} ack_read 带 token 生效 (auth-msg state={target[0].get('state') if target else 'N/A'})")
        if not ok5:
            FAIL.append("ack_read token")

    print()
    if FAIL:
        print("HAS-FAIL:", FAIL)
        sys.exit(1)
    print("✅ auth ALL-PASS")


asyncio.run(main())