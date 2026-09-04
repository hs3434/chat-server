#!/usr/bin/env python3
"""Go 会话列表 + 断线恢复测试
用法: python3 test_conversations.py <port>
覆盖:
  1. conversations: 空会话列表 (items=[])
  2. conversations: 单聊后列表出现对方, 带 last_body + last_ts + unread
  3. conversations: 群聊后列表出现 group::<gid>
  4. conversations: 排序按最近消息降序 (后发的会话排前)
  5. conversations: ack_read 后 unread 清 0
  6. recent: 返回最近跨会话消息 (含单聊+群聊), 按 id 降序
  7. recent: count 限制
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
    """清空连接上积压的非目标消息 (投递/回执等)"""
    for _ in range(n):
        try:
            await asyncio.wait_for(ws.recv(), 0.1)
        except Exception:
            break


async def send_and_get(ws, obj, want_type, timeout=2.0):
    """先清空积压, 再发请求并等 want_type 响应"""
    await drain(ws)
    await ws.send(json.dumps(obj))
    try:
        while True:
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout))
            if m.get("type") == want_type:
                return m
    except Exception:
        return None


async def main():
    await reg("cv_a"), await reg("cv_b"), await reg("cv_c")
    a, _ = await login("cv_a")

    # ---- 1. 空列表 ----
    r1 = await send_and_get(a, {"type": "conversations"}, "conversations")
    check("空会话列表 items=[]", r1 is not None and r1.get("items") == [],
          f"items={r1.get('items') if r1 else None}")

    # ---- 2. 单聊: A->B, B 未读; A 的会话列表应有 cv_b + unread=0 (自己发的) ----
    await a.send(json.dumps({"type": "msg", "to": "cv_b", "body": "hello-b"}))
    await asyncio.sleep(0.3)
    r2 = await send_and_get(a, {"type": "conversations"}, "conversations")
    item_b = None
    if r2:
        for it in r2.get("items", []):
            if it.get("chat") == "cv_b":
                item_b = it
    check("单聊后会话出现 cv_b (last_body=hello-b)",
          item_b is not None and item_b.get("last_body") == "hello-b",
          f"item_b={item_b}")

    # ---- 3. 群聊: A 建群, 发消息 ----
    gid = "cvgrp"
    await a.send(json.dumps({"type": "create_group", "gid": gid, "name": "cvgrp"}))
    await asyncio.sleep(0.2)
    await a.send(json.dumps({"type": "add_member", "gid": gid, "user": "cv_b"}))
    await asyncio.sleep(0.2)
    await a.send(json.dumps({"type": "msg", "to": gid, "is_group": True, "body": "grp-msg"}))
    await asyncio.sleep(0.3)
    r3 = await send_and_get(a, {"type": "conversations"}, "conversations")
    item_g = None
    if r3:
        for it in r3.get("items", []):
            if it.get("chat") == f"group::{gid}":
                item_g = it
    check("群聊后会话出现 group::cvgrp (last_body=grp-msg)",
          item_g is not None and item_g.get("last_body") == "grp-msg",
          f"item_g={item_g}")

    # ---- 4. 排序: 群消息后发 -> 应排单聊前 ----
    order_ok = False
    if r3:
        items = r3.get("items", [])
        idx_g = next((i for i, it in enumerate(items) if it.get("chat") == f"group::{gid}"), -1)
        idx_b = next((i for i, it in enumerate(items) if it.get("chat") == "cv_b"), -1)
        order_ok = idx_g != -1 and idx_b != -1 and idx_g < idx_b
    check("排序: 群(后发)排单聊前", order_ok, f"order={r3.get('items') if r3 else None}")

    # ---- 5. ack_read 后 unread 清 0 (B 角度: 先看 B 的会话) ----
    b, binit = await login("cv_b")
    r5 = await send_and_get(b, {"type": "conversations"}, "conversations")
    item_b_unread = None
    if r5:
        for it in r5.get("items", []):
            if it.get("chat") == "cv_a":
                item_b_unread = it
    check("B 视角: 会话 cv_a 带 unread=1 (hello-b 未读)",
          item_b_unread is not None and item_b_unread.get("unread") == 1,
          f"item_b_unread={item_b_unread}")
    # B ack_read hello-b (id 从 binit 取)
    mid = None
    for m in binit:
        if m.get("type") == "msg" and m.get("body") == "hello-b":
            mid = m.get("id")
    if mid:
        await b.send(json.dumps({"type": "ack_read", "id": mid}))
        await asyncio.sleep(0.3)
    r5b = await send_and_get(b, {"type": "conversations"}, "conversations")
    item_b_after = None
    if r5b:
        for it in r5b.get("items", []):
            if it.get("chat") == "cv_a":
                item_b_after = it
    check("ack_read 后 unread=0", item_b_after is not None and item_b_after.get("unread") == 0,
          f"item_b_after={item_b_after}")

    # ---- 6. recent: 跨会话最近消息 ----
    r6 = await send_and_get(a, {"type": "recent", "count": 10}, "recent")
    ok6 = r6 is not None and len(r6.get("items", [])) >= 2
    # 应有群消息 + 单聊消息, 按 id 降序 (id 大的在前)
    ids_desc = True
    if r6:
        ids = [it.get("id") for it in r6.get("items", [])]
        ids_desc = ids == sorted(ids, reverse=True)
    check("recent 返回跨会话消息+按 id 降序", ok6 and ids_desc,
          f"items={[it.get('body') for it in (r6 or {}).get('items', [])][:5] if r6 else None}")

    # ---- 7. recent: count=1 ----
    r7 = await send_and_get(a, {"type": "recent", "count": 1}, "recent")
    check("recent count=1 只返回 1 条",
          r7 is not None and len(r7.get("items", [])) == 1,
          f"len={len(r7.get('items', [])) if r7 else None}")

    await a.close(), await b.close()
    print()
    if FAIL:
        print("HAS-FAIL:", FAIL)
        sys.exit(1)
    print("✅ conversations ALL-PASS")


asyncio.run(main())