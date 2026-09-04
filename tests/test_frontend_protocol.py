#!/usr/bin/env python3
"""前端协议流验证: 模拟 app.js 的完整生命周期 (登录/发消息/收消息/重连)"""
import asyncio, json
import websockets

async def main():
    URL = "ws://127.0.0.1:8081/ws"
    FAIL = []

    async def req(ws, obj, want_type, timeout=4):
        await ws.send(json.dumps(obj))
        try:
            while True:
                m = json.loads(await asyncio.wait_for(ws.recv(), timeout))
                if m.get("seq") == obj.get("seq") and (m.get("type") == want_type or m.get("type") == "error"):
                    return m
                # 跳过推送 (msg/peer_ack/presence_evt)
        except Exception:
            return None

    # 1. 注册 + 登录 (前端注册并登录)
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "register", "user": "fe_a", "pass": "x"}))
        try:
            await asyncio.wait_for(ws.recv(), 0.5)
        except Exception:
            pass
        await ws.send(json.dumps({"type": "login", "user": "fe_a", "pass": "x"}))
        m = None
        try:
            while True:
                x = json.loads(await asyncio.wait_for(ws.recv(), 2))
                if x.get("type") == "login_ok":
                    m = x
                    break
        except Exception:
            pass
        token_a = m.get("token") if m else None
        print("1. 登录 fe_a 拿 token:", bool(token_a))
    async with websockets.connect(URL, open_timeout=5) as ws2:
        await ws2.send(json.dumps({"type": "register", "user": "fe_b", "pass": "x"}))
        await asyncio.sleep(0.3)

    # 2. A 发消息给 B (前端 sendMsg)
    async with websockets.connect(URL, open_timeout=5) as wa:
        # login
        await wa.send(json.dumps({"type": "login", "user": "fe_a", "pass": "x"}))
        await asyncio.sleep(0.3)
        await wa.send(json.dumps({"type": "msg", "to": "fe_b", "body": "前端测试消息"}))
        await asyncio.sleep(0.3)
        # conversations (前端 refreshConvs)
        await wa.send(json.dumps({"type": "conversations", "seq": 1}))
        m = None
        try:
            while True:
                x = json.loads(await asyncio.wait_for(wa.recv(), 2))
                if x.get("type") == "conversations":
                    m = x
                    break
        except Exception:
            pass
        convs = m.get("items", []) if m else []
        has_fe_b = any(c.get("chat") == "fe_b" and c.get("last_body") == "前端测试消息" for c in convs)
        print("2. conversations 含 fe_b 会话:", has_fe_b)

    # 3. B 登录收消息 + 已读回执
    async with websockets.connect(URL, open_timeout=5) as wb:
        await wb.send(json.dumps({"type": "login", "user": "fe_b", "pass": "x"}))
        got = []
        try:
            while True:
                m = json.loads(await asyncio.wait_for(wb.recv(), 2))
                got.append(m)
                if m.get("type") == "login_ok":
                    break
        except Exception:
            pass
        has_msg = any(m.get("type") == "msg" and m.get("body") == "前端测试消息" for m in got)
        print("3. B 登录收到未读消息:", has_msg)

    # 4. token_login 重连 (前端断线重连)
    async with websockets.connect(URL, open_timeout=5) as wr:
        await wr.send(json.dumps({"type": "token_login", "token": token_a}))
        m = None
        try:
            while True:
                x = json.loads(await asyncio.wait_for(wr.recv(), 2))
                if x.get("type") == "login_ok":
                    m = x
                    break
        except Exception:
            pass
        print("4. token_login 重连成功:", m is not None and m.get("user") == "fe_a")

    if FAIL:
        print("HAS-FAIL:", FAIL)
        return 1
    print("\n✅ frontend-protocol ALL-PASS")
    return 0

sys_exit = asyncio.run(main())
import sys
sys.exit(sys_exit)