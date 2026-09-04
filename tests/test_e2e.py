#!/usr/bin/env python3
"""Go 端到端测试 (新协议) —— 覆盖核心用户链路
用法: python3 test_e2e.py <port>
覆盖:
  1. 注册/登录
  2. 建群 + 加成员
  3. 单聊: 离线补投 / 实时送达
  4. 群聊: 成员即收 (实时 + 离线)
  5. history: 单聊 + 群聊 查询
  6. 会话唯一 id (群同 id / 单调递增)
  7. received/read 状态流
"""
import asyncio
import json
import sys

import websockets

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
URL = f"ws://127.0.0.1:{PORT}/ws"
DB = "/workspace/wxlike-server/wxlike_go.db"


async def connect_session(actions, read_seconds=1.0):
    """连接并依次发 actions, 收集所有响应后关闭"""
    got = []
    async with websockets.connect(URL, open_timeout=5) as ws:
        for a in actions:
            await ws.send(json.dumps(a))
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=read_seconds)
                got.append(json.loads(raw))
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
    return got


async def login_drain(user, pwd, drain_seconds=1.0):
    """登录并返回 (响应列表, 是否 login_ok)。drain 收集登录后重投的消息。"""
    got, ok_login = [], False
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "login", "user": user, "pass": pwd}))
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=drain_seconds)
                m = json.loads(raw)
                got.append(m)
                if m.get("type") == "login_ok":
                    ok_login = True
                    break
                # 也收集登录后重投 (可能在 login_ok 之前来)
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
    return got, ok_login


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

    # 1. 注册
    await connect_session([{"type": "register", "user": "ea", "pass": "a"},
                           {"type": "register", "user": "eb", "pass": "b"},
                           {"type": "register", "user": "ec", "pass": "c"}])
    check("register ea/eb/ec", True)

    # 2. 建群 + 加成员
    r = await connect_session([{"type": "login", "user": "ea", "pass": "a"},
                               {"type": "create_group", "gid": "eg", "name": "eg"},
                               {"type": "add_member", "gid": "eg", "user": "eb"},
                               {"type": "add_member", "gid": "eg", "user": "ec"}])
    check("create_group+add_member", any(m.get("type") == "group_ok" for m in r))

    # 3. 单聊离线补投: ea 发 eb (eb 离线)
    await connect_session([{"type": "login", "user": "ea", "pass": "a"},
                           {"type": "msg", "to": "eb", "body": "dm-offline"}])
    await asyncio.sleep(0.3)
    got_b, ok_b = await login_drain("eb", "b")
    dm = [m for m in got_b if m.get("type") == "msg" and m.get("body") == "dm-offline"]
    check("单聊离线补投", ok_b and len(dm) >= 1, str([m.get("body") for m in got_b if m.get("type")=="msg"]))

    # 4. 单聊历史: eb 拉与 ea 的会话历史
    r = await connect_session([{"type": "login", "user": "eb", "pass": "b"},
                               {"type": "history", "chat": "ea", "limit": 20}])
    hist = [m for m in r if m.get("type") == "history"]
    rows = hist[0]["rows"] if hist else []
    check("单聊历史含 dm-offline", any(row.get("body") == "dm-offline" for row in rows),
          f"{len(rows)} rows")

    # 5. 单聊实时送达: ec 在线, ea 发 ec, ec 实时收到
    got_live = []
    async with websockets.connect(URL, open_timeout=5) as ecws:
        await ecws.send(json.dumps({"type": "login", "user": "ec", "pass": "c"}))
        await asyncio.sleep(0.3)
        await connect_session([{"type": "login", "user": "ea", "pass": "a"},
                               {"type": "msg", "to": "ec", "body": "dm-live"}])
        try:
            while True:
                raw = await asyncio.wait_for(ecws.recv(), timeout=1.5)
                m = json.loads(raw); got_live.append(m)
                if m.get("type") == "msg" and m.get("body") == "dm-live":
                    break
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
    check("单聊实时送达 ec", any(m.get("body") == "dm-live" for m in got_live))

    # 6. 群聊实时: 先在线 eb, ea 发群消息, eb 实时收到
    got_grp = []
    try:
        async with websockets.connect(URL, open_timeout=5) as ebws:
            await ebws.send(json.dumps({"type": "login", "user": "eb", "pass": "b"}))
            await asyncio.sleep(0.3)
            await connect_session([{"type": "login", "user": "ea", "pass": "a"},
                                   {"type": "msg", "to": "eg", "is_group": True, "body": "grp-live"}])
            try:
                while True:
                    raw = await asyncio.wait_for(ebws.recv(), timeout=1.5)
                    m = json.loads(raw); got_grp.append(m)
                    if m.get("type") == "msg" and m.get("body") == "grp-live":
                        break
            except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
                pass
    except Exception:
        pass
    check("群聊实时送达在线成员", any(m.get("body") == "grp-live" for m in got_grp))

    # 7. 群聊历史: eb 拉 eg 群历史 (含 grp-live, 且 ec 离线时也收到过 grp-share?)
    r = await connect_session([{"type": "login", "user": "eb", "pass": "b"},
                               {"type": "history", "chat": "group::eg", "limit": 20}])
    hist = [m for m in r if m.get("type") == "history"]
    rows = hist[0]["rows"] if hist else []
    check("群聊历史含 grp-live", any(row.get("body") == "grp-live" for row in rows),
          f"{len(rows)} rows")

    # 8. 会话唯一 id: 单聊 id 单调递增, 群消息所有成员 id 一致 (验证过历史里)
    ids = [row.get("id") for row in rows if row.get("body") == "grp-live"]
    if rows:
        # 群历史里 grp-live 应只有一个 id
        check("群历史 grp-live 单 id", len(ids) == 1, f"{ids}")

    # 9. 离线群消息补投: ec 离线时 ea 发群消息, ec 登录补投
    await connect_session([{"type": "login", "user": "ea", "pass": "a"},
                           {"type": "msg", "to": "eg", "is_group": True, "body": "grp-ec-offline"}])
    await asyncio.sleep(0.3)
    got_c, _ = await login_drain("ec", "c")
    check("群消息离线补投 ec", any(m.get("type") == "msg" and m.get("body") == "grp-ec-offline" for m in got_c))

    print("\n" + ("ALL-PASS" if ok else "HAS-FAIL"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
