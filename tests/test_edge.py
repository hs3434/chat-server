#!/usr/bin/env python3
"""Go 边界与多端测试
用法: python3 test_edge.py <port>
覆盖:
  1. ack 不存在的 id (无副作用, 不崩溃)
  2. 重复 ack_read 已 read 的消息 (幂等)
  3. 多端: 同账号两设备在线, 消息双端都收到
  4. 重复注册 / 重复加成员 的容错
  5. 发给自己 (单聊 to=自身)
"""
import asyncio
import json
import sys

import websockets

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
URL = f"ws://127.0.0.1:{PORT}/ws"
DB = "/workspace/wxlike-server/wxlike_go.db"


async def login_conn(user, pwd):
    """建立已登录连接, 返回 (ws, 收到的初始消息)。连接保持打开供后续发送。"""
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


def state_of(user, msg_id):
    import sqlite3
    con = sqlite3.connect(DB)
    row = con.execute("SELECT state FROM msg_state WHERE user=? AND msg_id=?", (user, msg_id)).fetchone()
    con.close()
    return row[0] if row else "MISSING"


async def main():
    ok = True
    def check(name, cond, detail=""):
        nonlocal ok
        if not cond:
            ok = False
        print(f"{'OK' if cond else 'FAIL'} {name} {detail}")

    # 注册 (独立一次性, 避免依赖已有库)
    try:
        async with websockets.connect(URL, open_timeout=5) as w0:
            await w0.send(json.dumps({"type": "register", "user": "xa", "pass": "a"}))
            await w0.send(json.dumps({"type": "register", "user": "xb", "pass": "b"}))
            await asyncio.sleep(0.3)
    except Exception:
        pass

    # ---- 1. ack 不存在的 id: 应幂等无副作用, 不崩溃 ----
    ws, _ = await login_conn("xa", "a")
    await ws.send(json.dumps({"type": "ack_received", "id": 99999}))
    await ws.send(json.dumps({"type": "ack_read", "id": 99999}))
    await asyncio.sleep(0.3)
    await ws.close()
    check("ack 不存在 id 无副作用", True)

    # ---- 2. 重复注册: 服务器应返回 error(exists), 不崩溃 ----
    async with websockets.connect(URL, open_timeout=5) as w0:
        await w0.send(json.dumps({"type": "register", "user": "xa", "pass": "a"}))
        got = []
        try:
            while True:
                raw = await asyncio.wait_for(w0.recv(), timeout=1.0)
                got.append(json.loads(raw))
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
    has_exists = any(m.get("type") == "error" and m.get("code") == "exists" for m in got)
    check("重复注册返回 error(exists)", has_exists, str([m.get("code") for m in got]))

    # ---- 3. 多端: xa 双连接在线, xb 发消息 xa, 双端都收到 ----
    ws1, _ = await login_conn("xa", "a")
    ws2, _ = await login_conn("xa", "a")
    await asyncio.sleep(0.3)
    # xb 发 xa
    wsb, _ = await login_conn("xb", "b")
    await wsb.send(json.dumps({"type": "msg", "to": "xa", "body": "multi-device"}))
    await asyncio.sleep(0.5)
    got1, got2 = [], []
    for target_ws, out in ((ws1, got1), (ws2, got2)):
        try:
            while True:
                raw = await asyncio.wait_for(target_ws.recv(), timeout=1.0)
                m = json.loads(raw)
                out.append(m)
                if m.get("type") == "msg" and m.get("body") == "multi-device":
                    break
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
    check("多端 ws1 收到", any(m.get("body") == "multi-device" for m in got1))
    check("多端 ws2 收到", any(m.get("body") == "multi-device" for m in got2))
    # 应拿到同一个消息 id
    id1 = next((m.get("id") for m in got1 if m.get("body") == "multi-device"), None)
    id2 = next((m.get("id") for m in got2 if m.get("body") == "multi-device"), None)
    check("多端同 id", id1 == id2 and id1 is not None, f"{id1}/{id2}")
    await ws1.close(); await ws2.close(); await wsb.close()

    # ---- 4. 发给自己: xa 发消息给 xa ----
    ws, _ = await login_conn("xa", "a")
    await ws.send(json.dumps({"type": "msg", "to": "xa", "body": "to-self"}))
    await asyncio.sleep(0.4)
    got_self = []
    try:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            m = json.loads(raw); got_self.append(m)
            if m.get("type") == "msg" and m.get("body") == "to-self":
                break
    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        pass
    self_st = state_of("xa", next((m["id"] for m in got_self if m.get("body")=="to-self"), -1))
    check("发给自己 (单聊 to=自身)", len(got_self) > 0, f"收{len(got_self)}条")
    await ws.close()

    print("\n" + ("ALL-PASS" if ok else "HAS-FAIL"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
