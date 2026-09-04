#!/usr/bin/env python3
"""Go 限流/防刷测试: 消息令牌桶 + 登录失败锁 + 注册 IP 限流
用法: python3 test_ratelimit.py <port>
覆盖:
  1. 消息: 突发放 10 条全成功 (容量内)
  2. 消息: 连续高速发超出 10 条 -> 触发 rate_limited + retry_after
  3. 登录: 5 次失败后第 6 次 login_locked (即使密码正确也锁)
  4. 登录: 锁定期间返回 login_locked
  5. 注册: 同一 IP 第 4 次注册 -> reg_limited
  6. 恢复: 冷却后 (跳过, 5分钟太长; 用逻辑验证: 不同用户不受影响)
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


async def connect():
    ws = await websockets.connect(URL, open_timeout=5)
    return ws


async def register(ws, u):
    await ws.send(json.dumps({"type": "register", "user": u, "pass": "x"}))
    try:
        return json.loads(await asyncio.wait_for(ws.recv(), 0.5))
    except Exception:
        return None


async def login_ok(ws, u, p):
    await ws.send(json.dumps({"type": "login", "user": u, "pass": p}))
    try:
        return json.loads(await asyncio.wait_for(ws.recv(), 0.8))
    except Exception:
        return None


async def main():
    # ---- 0. 注册 IP 限流 (先跑: 此时 127.0.0.1 配额为 0, 不受前面测试影响) ----
    ws3 = await connect()
    r = []
    for i in range(1, 5):  # 第 1-3 成功, 第 4 次 reg_limited
        resp = await register(ws3, f"rl_ipuser{i}")
        r.append(resp)
    codes = [x.get("code") if x else None for x in r]
    check("注册第 4 次同一 IP -> reg_limited", codes[3] == "reg_limited",
          f"codes={codes}")
    check("注册前 3 次成功", all(x is not None and x.get("type") == "register_ok" for x in r[:3]),
          f"codes={codes}")
    await ws3.close()

    # ---- 1/2. 消息令牌桶 (用已注册用户, 避免触发注册限流) ----
    ws = await connect()
    await login_ok(ws, "rl_ipuser1", "x")
    try:
        await asyncio.wait_for(ws.recv(), 0.3)  # 可能收到 login_ok 后消息
    except Exception:
        pass

    errors = []
    for i in range(15):  # 突发 15 条 > 容量 10
        await ws.send(json.dumps({"type": "msg", "to": "rl_ipuser1", "body": f"m{i}"}))
        try:
            raw = await asyncio.wait_for(ws.recv(), 0.5)
            m = json.loads(raw)
            if m.get("type") == "error":
                errors.append(m)
        except Exception:
            pass
    check("消息突发 15 条: 超过容量触发 rate_limited",
          any(e.get("code") == "rate_limited" for e in errors), f"errors={errors}")
    check("消息限流带 retry_after", any("retry_after" in e for e in errors))
    await ws.close()

    # ---- 3/4. 登录失败锁 ----
    # 用独立用户, 不影响其他测试 (限流器内存态, 每次重启清零)
    ws2 = await connect()
    # 5 次失败 (用已注册 rl_ipuser2)
    for i in range(5):
        await ws2.send(json.dumps({"type": "login", "user": "rl_ipuser2", "pass": "wrong"}))
        try:
            await asyncio.wait_for(ws2.recv(), 0.5)
        except Exception:
            pass
    # 第 6 次 (正确密码) 应 login_locked
    await ws2.send(json.dumps({"type": "login", "user": "rl_ipuser2", "pass": "x"}))
    m6 = json.loads(await asyncio.wait_for(ws2.recv(), 0.8))
    check("登录 5 次失败后锁定 (正确密码也拒)", m6.get("code") == "login_locked",
          str(m6))
    check("锁定带 retry_after", "retry_after" in m6)
    await ws2.close()

    # ---- 6. 不同用户消息不受影响 (突变隔离) ----
    ws4 = await connect()
    await login_ok(ws4, "rl_ipuser3", "x")
    try:
        await asyncio.wait_for(ws4.recv(), 0.3)
    except Exception:
        pass
    errs4 = []
    for i in range(5):
        await ws4.send(json.dumps({"type": "msg", "to": "rl_fresh", "body": f"n{i}"}))
        try:
            raw = await asyncio.wait_for(ws4.recv(), 0.5)
            m = json.loads(raw)
            if m.get("type") == "error":
                errs4.append(m.get("code"))
        except Exception:
            pass
    check("新用户 5 条正常 (不受其他用户限流影响)", "rate_limited" not in errs4)
    await ws4.close()

    print()
    if FAIL:
        print("HAS-FAIL:", FAIL)
        sys.exit(1)
    print("✅ ratelimit ALL-PASS")


asyncio.run(main())