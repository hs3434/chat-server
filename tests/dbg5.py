import asyncio, json, time, websockets

# 复刻 test_ux6_contacts.py 的连接模式定位卡点
P = "testpass"


async def main():
    rnd = "777001"
    UA, UB = "fr_a_" + rnd, "fr_b_" + rnd

    async def newconn():
        return await websockets.connect("ws://127.0.0.1:8081/ws", open_timeout=5)

    async def rq(ws, m):
        m = dict(m)
        m["seq"] = int(time.time() * 1000) % 100000000
        await ws.send(json.dumps(m))
        t0 = time.time()
        while time.time() - t0 < 5:
            r = json.loads(await asyncio.wait_for(ws.recv(), 5))
            if r.get("seq") == m["seq"]:
                return r
        return None

    # B 先注册并断开
    w = await newconn()
    print("B reg:", (await rq(w, {"type": "register", "user": UB, "pass": P}))["type"], flush=True)
    print("B login:", (await rq(w, {"type": "login", "user": UB, "pass": P}))["type"], flush=True)
    print("B closing...", flush=True)
    await w.close()
    print("B closed", flush=True)

    # A 连接
    wa = await newconn()
    print("A reg:", (await rq(wa, {"type": "register", "user": UA, "pass": P}))["type"])
    print("A login:", (await rq(wa, {"type": "login", "user": UA, "pass": P}))["type"])
    print("A search:", await rq(wa, {"type": "search_users", "q": UB}))
    await wa.close()

asyncio.run(main())
