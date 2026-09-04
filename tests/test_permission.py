#!/usr/bin/env python3
"""Go 发送权限测试 (微信规则)
用法: python3 test_permission.py <port>
覆盖:
  1. 单聊给不存在用户 -> error no_such_user
  2. 群聊给不存在群发送 -> error group_not_found
  3. 非成员往已存在群里发 -> error not_member
  4. 成员发群消息正常送达
"""
import asyncio
import json
import sys

import websockets

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
URL = f"ws://127.0.0.1:{PORT}/ws"
URL_WS = URL


async def login_conn(user, pwd):
    ws = await websockets.connect(URL, open_timeout=5)
    await ws.send(json.dumps({"type": "login", "user": user, "pass": pwd}))
    try:
        await asyncio.wait_for(ws.recv(), timeout=1.0)  # login_ok
    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        pass
    return ws


async def main():
    ok = True
    def check(name, cond, detail=""):
        nonlocal ok
        if not cond:
            ok = False
        print(f"{'OK' if cond else 'FAIL'} {name} {detail}")

    # 注册
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "register", "user": "ya", "pass": "a"}))
        await ws.send(json.dumps({"type": "register", "user": "yb", "pass": "b"}))
        await asyncio.sleep(0.3)
    # 建群: ya 建 yg + 加 yb (ya/yb 都是成员)
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "register", "user": "yowner", "pass": "o"}))
        await asyncio.sleep(0.3)
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "login", "user": "yowner", "pass": "o"}))
        await ws.send(json.dumps({"type": "create_group", "gid": "yg", "name": "yg"}))
        await ws.send(json.dumps({"type": "add_member", "gid": "yg", "user": "yb"}))
        await asyncio.sleep(0.3)

    # ---- 1. 单聊给不存在用户 -> no_such_user ----
    ws = await login_conn("ya", "a")
    await ws.send(json.dumps({"type": "msg", "to": "nobody", "body": "hi"}))
    got = []
    try:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            m = json.loads(raw); got.append(m)
            if m.get("type") == "error":
                break
    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        pass
    check("单聊不存在用户 -> error", any(m.get("type") == "error" and m.get("code") == "no_such_user" for m in got),
          str([m.get("code") for m in got if m.get("type") == "error"]))
    await ws.close()

    # ---- 2. 群聊给不存在群 -> group_not_found ----
    ws = await login_conn("ya", "a")
    await ws.send(json.dumps({"type": "msg", "to": "nosuchgroup", "is_group": True, "body": "x"}))
    got2 = []
    try:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            m = json.loads(raw); got2.append(m)
            if m.get("type") == "error":
                break
    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        pass
    check("群不存在 -> error group_not_found", any(m.get("type") == "error" and m.get("code") == "group_not_found" for m in got2),
          str([m.get("code") for m in got2 if m.get("type") == "error"]))
    await ws.close()

    # ---- 3. 非成员(yb 是成员, ya 不是成员)往 yg 发 -> not_member ----
    # ya 不是 yg 成员 (owner+yb 才是)
    ws = await login_conn("ya", "a")
    await ws.send(json.dumps({"type": "msg", "to": "yg", "is_group": True, "body": "imposter"}))
    got3 = []
    try:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            m = json.loads(raw); got3.append(m)
            if m.get("type") == "error":
                break
    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        pass
    check("非成员发群 -> error not_member", any(m.get("type") == "error" and m.get("code") == "not_member" for m in got3),
          str([m.get("code") for m in got3 if m.get("type") == "error"]))
    await ws.close()

    # ---- 4. 成员(owner)发群消息 -> 正常 (yb 离线, 登录补投) ----
    ws_owner = await login_conn("yowner", "o")
    await ws_owner.send(json.dumps({"type": "msg", "to": "yg", "is_group": True, "body": "legit-msg"}))
    await asyncio.sleep(0.4)
    # yb 登录并收集一切 (login_ok 和补投的 msg 都可能来, 不丢)
    got4 = []
    async with websockets.connect(URL_WS, open_timeout=5) as ws_b:
        await ws_b.send(json.dumps({"type": "login", "user": "yb", "pass": "b"}))
        try:
            deadline = asyncio.get_event_loop().time() + 2.0
            while asyncio.get_event_loop().time() < deadline:
                raw = await asyncio.wait_for(ws_b.recv(), timeout=2.0)
                m = json.loads(raw)
                got4.append(m)
                if m.get("type") == "msg" and m.get("body") == "legit-msg":
                    break
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
    check("成员发群消息正常送达", any(m.get("type") == "msg" and m.get("body") == "legit-msg" for m in got4),
          str([m.get("body") for m in got4 if m.get("type") == "msg"]))
    await ws_owner.close()

    print("\n" + ("ALL-PASS" if ok else "HAS-FAIL"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
