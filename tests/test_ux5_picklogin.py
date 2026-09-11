#!/usr/bin/env python3
"""UX5: 登录设备/IP历史 + 微信化踢人/转让校验 协议测试"""
import asyncio, json, sys, time
sys.path.insert(0, "/tmp")
from websockets.asyncio.client import connect
import os
URL = os.environ.get("WS", "ws://127.0.0.1:8081/ws")

async def req(wb, payload, want, timeout=4.0):
    """发请求并等待指定 type 的响应 (跳过推送)"""
    if not hasattr(wb, "_seq"):
        wb._seq = int(time.time())
    wb._seq += 1
    seq = wb._seq
    payload = dict(payload, seq=seq)
    await wb.send(json.dumps(payload))
    end = time.time() + timeout
    while time.time() < end:
        try:
            raw = await asyncio.wait_for(wb.recv(), timeout=max(0.1, end - time.time()))
        except asyncio.TimeoutError:
            return None
        m = json.loads(raw)
        if m.get("seq") == seq and m.get("type") == want:
            return m
    return None

async def register(wb, u):
    """注册后必须显式 login: register 只建号不置登录态, 后续请求会被静默丢弃"""
    await req(wb, {"type":"register","user":u,"pass":"pw_"+u}, "register_ok")
    await req(wb, {"type":"login","user":u,"pass":"pw_"+u}, "login_ok")

async def main():
    ok = fail = 0
    def chk(name, cond, extra=""):
        nonlocal ok, fail
        if cond: ok += 1; print("PASS", name, extra)
        else: fail += 1; print("FAIL", name, extra)

    suffix = str(int(time.time()))[-6:]
    U, A, B = "ux5_u"+suffix, "ux5_a"+suffix, "ux5_b"+suffix
    G = "gux5"+suffix

    async with connect(URL) as wu, connect(URL) as wa, connect(URL) as wb:
        await register(wu, U); await register(wa, A); await register(wb, B)

        # --- 登录历史 ---
        r = await req(wu, {"type":"sessions"}, "sessions")
        hist = (r or {}).get("history") or []
        chk("sessions含history", isinstance(hist, list) and len(hist) >= 1, f"n={len(hist)}")
        if hist:
            chk("history含ts+ip", "ts" in hist[0] and "ip" in hist[0], str(hist[0]))

        # token 重连也记一次成功登录
        r1 = await req(wu, {"type":"sessions"}, "sessions")
        tok = (r1 or {}).get("token") or ""
        n_before = len((r1 or {}).get("history") or [])
        async with connect(URL) as wu2:
            if tok:
                r = await req(wu2, {"type":"token_login","token":tok}, "login_ok")
                chk("token_login回包", r is not None)
                r2 = await req(wu2, {"type":"sessions"}, "sessions")
                n_after = len((r2 or {}).get("history") or [])
                chk("token登录也记历史", n_after >= n_before, f"{n_before}->{n_after}")

        # --- 建群 + 微信化踢人校验 ---
        await req(wu, {"type":"create_group","gid":G,"name":"UX5群"}, "group_ok")
        await req(wu, {"type":"add_member","gid":G,"user":A}, "group_ok")
        await req(wu, {"type":"add_member","gid":G,"user":B}, "group_ok")

        # 非群主踢人被拒
        r = await req(wa, {"type":"remove_member","gid":G,"user":B}, "error")
        chk("非群主踢人被拒", r and r.get("code")=="not_owner", str(r and r.get("code")))
        # 群主踢自己被拒
        r = await req(wu, {"type":"remove_member","gid":G,"user":U}, "error")
        chk("群主踢自己被拒", r and r.get("code")=="cannot_kick_self", str(r and r.get("code")))
        # 群主踢不在群的人被拒
        r = await req(wu, {"type":"remove_member","gid":G,"user":"ghost_x"}, "error")
        chk("踢不在群的人被拒", r and r.get("code")=="not_member", str(r and r.get("code")))
        # 群主踢成员成功
        r = await req(wu, {"type":"remove_member","gid":G,"user":B}, "group_ok")
        chk("群主踢成员OK", r is not None)
        # 被踢者收到刷新推送 (B 在线)
        got = False
        try:
            while True:
                m = json.loads(await asyncio.wait_for(wb.recv(), timeout=3))
                if m.get("type") == "conversations_refresh":
                    got = True; break
        except asyncio.TimeoutError:
            pass
        except Exception:
            pass
        chk("被踢者收到刷新推送", got)

        # 转让给自己被拒
        r = await req(wu, {"type":"transfer_owner","gid":G,"user":U}, "error")
        chk("转让给自己被拒", r and r.get("code")=="already_owner", str(r and r.get("code")))
        # 转让给不在群的人被拒
        r = await req(wu, {"type":"transfer_owner","gid":G,"user":"ghost_x"}, "error")
        chk("转让给非成员被拒", r and r.get("code")=="not_member", str(r and r.get("code")))

    print(f"\n{'ALL-PASS' if fail==0 else 'HAS-FAIL'} {ok}/{ok+fail}")
    sys.exit(0 if fail == 0 else 1)

asyncio.run(main())
