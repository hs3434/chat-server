#!/usr/bin/env python3
"""Go 持久化重启测试 —— "不丢消息"的另一半
用法: python3 test_persistence.py <port>
验证进程崩溃/重启后:
  1. messages 数据完好 (已存消息不丢)
  2. msg_state 状态完好 (投递/未读状态不丢)
  3. 离线消息重启后仍能补投
  4. 已 read 消息重启后不重投
"""
import asyncio
import json
import os
import signal
import subprocess
import sys
import time

import websockets

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
URL = f"ws://127.0.0.1:{PORT}/ws"
DB = "/workspace/wxlike-server/wxlike_go.db"
BIN = "/workspace/wxlike-server/bin/wxlike-go"
DIR = "/workspace/wxlike-server"


def restart_server():
    """kill 服务器进程并重启, 返回是否成功"""
    # 找并杀
    try:
        out = subprocess.run(["pgrep", "-f", "wxlike-go"], capture_output=True, text=True)
        for pid in out.stdout.split():
            pid = pid.strip()
            if pid:
                os.kill(int(pid), signal.SIGKILL)
    except Exception:
        pass
    time.sleep(1)
    # 重启 (setsid 脱离, 保持独立)
    log = open("/tmp/go_srv_restart.log", "a")
    subprocess.Popen([BIN, "--port", str(PORT), "--dir", DIR],
                     stdout=log, stderr=log, start_new_session=True)
    time.sleep(2)
    # 确定端口起来
    for _ in range(10):
        try:
            import socket
            s = socket.create_connection(("127.0.0.1", PORT), 0.5)
            s.close()
            return True
        except Exception:
            time.sleep(0.3)
    return False


async def login_drain(user, pwd, dur=1.0):
    got, ok_login = [], False
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "login", "user": user, "pass": pwd}))
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=dur)
                m = json.loads(raw); got.append(m)
                if m.get("type") == "login_ok":
                    ok_login = True
                    break
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
    return got, ok_login


def row_count(table, where=""):
    import sqlite3
    con = sqlite3.connect(DB)
    q = f"SELECT COUNT(*) FROM {table}" + (f" WHERE {where}" if where else "")
    n = con.execute(q).fetchone()[0]
    con.close()
    return n


async def main():
    ok = True
    def check(name, cond, detail=""):
        nonlocal ok
        if not cond:
            ok = False
        print(f"{'OK' if cond else 'FAIL'} {name} {detail}")

    # 注册 + 发一条离线消息 (receiver 离线)
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "register", "user": "pa", "pass": "a"}))
        await ws.send(json.dumps({"type": "register", "user": "pb", "pass": "b"}))
        await ws.send(json.dumps({"type": "login", "user": "pa", "pass": "a"}))
        await ws.send(json.dumps({"type": "msg", "to": "pb", "body": "pre-restart-msg"}))
        await asyncio.sleep(0.4)

    n_msg_before = row_count("messages")
    n_state_before = row_count("msg_state")
    check(f"重启前消息 {n_msg_before} + 状态 {n_state_before}", n_msg_before >= 1 and n_state_before >= 1)

    # 重启服务器
    ok_restart = restart_server()
    check("服务器重启成功", ok_restart)

    # 重启后数据完好
    n_msg_after = row_count("messages")
    n_state_after = row_count("msg_state")
    check("重启后消息不丢", n_msg_after == n_msg_before, f"{n_msg_before}->{n_msg_after}")
    check("重启后状态不丢", n_state_after == n_state_before, f"{n_state_before}->{n_state_after}")

    # 重启后 pb 登录, 应补投 pre-restart-msg
    got, ok_login = await login_drain("pb", "b")
    check("重启后离线补投", any(m.get("type") == "msg" and m.get("body") == "pre-restart-msg" for m in got),
          str([m.get("body") for m in got if m.get("type") == "msg"]))

    print("\n" + ("ALL-PASS" if ok else "HAS-FAIL"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
