#!/usr/bin/env python3
"""Go 群成员查询动作测试
用法: python3 test_group_members.py <port>
覆盖:
  1. 群主查成员: 返回 members + owner + name
  2. 成员查群: 可查
  3. 非成员查群: not_member
  4. 不存在群: group_not_found
  5. 踢人后成员列表更新
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
                break
    except Exception:
        pass
    return ws, got


async def drain(ws, n=50):
    for _ in range(n):
        try:
            await asyncio.wait_for(ws.recv(), 0.1)
        except Exception:
            break


async def send_and_get(ws, obj, want_type, timeout=2.0):
    await drain(ws)
    await ws.send(json.dumps(obj))
    try:
        while True:
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout))
            if m.get("type") == want_type or m.get("type") == "error":
                return m
    except Exception:
        return None


async def main():
    await reg("gm_owner"), await reg("gm_mem"), await reg("gm_out")
    ow, _ = await login("gm_owner")
    mem, _ = await login("gm_mem")
    out, _ = await login("gm_out")
    gid = "gmutest"

    # 建群 + 加人
    r = await send_and_get(ow, {"type": "create_group", "gid": gid, "name": "GM测试群"}, "group_ok")
    check("建群成功", r is not None and r.get("type") == "group_ok")
    await send_and_get(ow, {"type": "add_member", "gid": gid, "user": "gm_mem"}, "group_ok")

    # 1. 群主查成员
    r = await send_and_get(ow, {"type": "group_members", "gid": "group::" + gid}, "group_members")
    ok1 = r is not None and r.get("type") == "group_members" and r.get("owner") == "gm_owner" \
          and r.get("name") == "GM测试群" and "gm_owner" in r.get("members", []) and "gm_mem" in r.get("members", [])
    check("群主查成员: owner+name+members", ok1, f"r={r}")

    # 2. 成员查群
    r2 = await send_and_get(mem, {"type": "group_members", "gid": "group::" + gid}, "group_members")
    check("成员可查", r2 is not None and r2.get("type") == "group_members")

    # 3. 非成员查群 (gm_out 不是成员)
    r3 = await send_and_get(out, {"type": "group_members", "gid": "group::" + gid}, "group_members")
    check("非成员查群 -> not_member", r3 is not None and r3.get("code") == "not_member", f"r3={r3}")

    # 4. 不存在群
    r4 = await send_and_get(ow, {"type": "group_members", "gid": "group::ghostgrp"}, "group_members")
    check("不存在群 -> group_not_found", r4 is not None and r4.get("code") == "group_not_found", f"r4={r4}")

    # 5. 踢人后成员列表更新
    await send_and_get(ow, {"type": "remove_member", "gid": gid, "user": "gm_mem"}, "group_ok")
    r5 = await send_and_get(ow, {"type": "group_members", "gid": "group::" + gid}, "group_members")
    ok5 = r5 is not None and "gm_mem" not in r5.get("members", [])
    check("踢人后成员列表更新", ok5, f"members={r5.get('members') if r5 else None}")

    await ow.close(), await mem.close(), await out.close()
    print()
    if FAIL:
        print("HAS-FAIL:", FAIL)
        sys.exit(1)
    print("✅ group_members ALL-PASS")


asyncio.run(main())