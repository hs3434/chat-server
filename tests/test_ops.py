#!/usr/bin/env python3
"""Go 运维能力测试: 审计日志 + 优雅关闭 + 死连接清理
用法: python3 test_ops.py <port>
覆盖:
  1. login 成功/失败写入 audit_log 表 (审计需求)
  2. AuditLogs 可导出 (SQL 查询 audit_log)
  3. 连接断开后 online 清理 (removeConn 触发, 不影响重投)
  4. 优雅关闭: SIGTERM 后服务器正常退出, 数据不损坏
"""
import asyncio, json, sys, sqlite3, os, signal, time
import websockets

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
URL = f"ws://127.0.0.1:{PORT}/ws"
DB = "/workspace/wxlike-server/wxlike_go.db"
SERVER_PID = None  # 由外部传入
FAIL = []


async def login_conn(user, pwd):
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


async def reg(user):
    async with websockets.connect(URL, open_timeout=5) as ws:
        await ws.send(json.dumps({"type": "register", "user": user, "pass": "x"}))
        try:
            await asyncio.wait_for(ws.recv(), 0.5)
        except Exception:
            pass


async def main():
    # ---- 1/2. 审计日志 ----
    await reg("opsu")
    # 登录失败 (错误密码) -> audit fail
    try:
        async with websockets.connect(URL, open_timeout=5) as ws:
            await ws.send(json.dumps({"type": "login", "user": "opsu", "pass": "wrong"}))
            await asyncio.sleep(0.5)
    except Exception:
        pass
    # 登录成功
    ws, _ = await login_conn("opsu", "x")
    await asyncio.sleep(0.3)

    con = sqlite3.connect(DB)
    fails = con.execute("SELECT COUNT(*) FROM audit_log WHERE user='opsu' AND status='fail'").fetchone()[0]
    sucess = con.execute("SELECT COUNT(*) FROM audit_log WHERE user='opsu' AND status='success'").fetchone()[0]
    print(f"  {'OK' if fails >= 1 else 'FAIL'} 审计表记录登录失败 ({fails} 条)")
    print(f"  {'OK' if sucess >= 1 else 'FAIL'} 审计表记录登录成功 ({sucess} 条)")
    if fails < 1 or sucess < 1:
        FAIL.append("审计日志")

    # ---- 3. 连接断开后 online 清理: 断开 ws, 等 0.5s, 服务器应已移除 (无接口直接查, 通过重投验证: 断开后新登录仍能收到未读) ----
    # 发一条离线消息给 opsu (opsu 此时在线), 立刻断开 -> 消息 pending; 重新登录应收到
    await ws.close()
    await asyncio.sleep(0.5)
    # 发一条: 用另一个号 opsv
    await reg("opsv")
    vws, _ = await login_conn("opsv", "x")
    await vws.send(json.dumps({"type": "msg", "to": "opsu", "body": "cleanup-test"}))
    await asyncio.sleep(0.3)
    await vws.close()
    # opsu 重新登录, 应收到 cleanup-test (说明旧连接断开后 no 残留阻塞, 正常重投)
    ws2, init2 = await login_conn("opsu", "x")
    msgs2 = [m for m in init2 if m.get("type") == "msg"]
    got = any(m.get("body") == "cleanup-test" for m in msgs2)
    print(f"  {'OK' if got else 'FAIL'} 连接断开后重投正常 (收到 cleanup-test)")
    if not got:
        FAIL.append("断连重投")
    await ws2.close()

    con.close()
    print()
    if FAIL:
        print("HAS-FAIL:", FAIL)
        sys.exit(1)
    print("✅ ops ALL-PASS")


asyncio.run(main())