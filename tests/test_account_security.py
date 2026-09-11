#!/usr/bin/env python3
# 账号安全协议测试: 自助改密 / 邮箱绑定 / 忘记密码重置
# 用法: python3 test_account_security.py <PORT>
# 说明: 从服务器库直接读验证码(测试环境不真发信)。
import asyncio, json, sys, sqlite3, time, uuid

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081
URL = f"ws://127.0.0.1:{PORT}/ws"
DB = __import__("os").environ.get("WXLIKE_TEST_DB", "/workspace/wxlike-server/wxlike_go.db")

fails = []
def check(name, cond, extra=""):
    if cond: print(f"OK {name}" + (f" {extra}" if extra else ""))
    else: print(f"FAIL {name}" + (f" {extra}" if extra else "")); fails.append(name)

async def req(ws, obj, want, timeout=5):
    obj = dict(obj); obj["seq"] = int(time.time() * 1000) % 1000000
    await ws.send(json.dumps(obj))
    end = time.time() + timeout
    while time.time() < end:
        try:
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout=end - time.time()))
        except asyncio.TimeoutError:
            return None
        if m.get("type") == want or m.get("type") == "error":
            return m
    return None

def code_of(email, purpose):
    con = sqlite3.connect(DB)
    r = con.execute("SELECT code FROM email_codes WHERE email=? AND purpose=? AND used=0 ORDER BY id DESC LIMIT 1", (email, purpose)).fetchone()
    con.close()
    return r[0] if r else None

async def main():
    import websockets
    uname = "sec_" + uuid.uuid4().hex[:6]
    email = f"{uname}@example.com"
    async with websockets.connect(URL, open_timeout=5) as ws:
        # 注册
        r = await req(ws, {"type": "register", "user": uname, "pass": "origpass"}, "register_ok")
        check("注册", r and r.get("type") == "register_ok", str(r))
        r = await req(ws, {"type": "login", "user": uname, "pass": "origpass"}, "login_ok")
        check("登录", r and r.get("type") == "login_ok", str(r))
        token = r.get("token")
        # 登录态用 token 发后续 (新连接模拟)
        # 1) 自助改密: 错旧密码应失败
        r = await req(ws, {"type": "set_password", "old_pass": "WRONG", "new_pass": "np1"}, "password_ok", timeout=3)
        check("改密-错旧密码被拒", r and r.get("type") == "error", str(r))
        # 正确旧密码 -> 成功
        r = await req(ws, {"type": "set_password", "old_pass": "origpass", "new_pass": "newpass1"}, "password_ok")
        check("改密-正确旧密码成功", r and r.get("type") == "password_ok", str(r))
    # 新密码可登录
    async with websockets.connect(URL, open_timeout=5) as ws:
        r = await req(ws, {"type": "login", "user": uname, "pass": "newpass1"}, "login_ok")
        check("改密后新密码可登录", r and r.get("type") == "login_ok", str(r))
        token = r.get("token")
        # 2) 绑定邮箱: 发码
        r = await req(ws, {"type": "send_email_code", "email": email}, "email_code_sent")
        check("绑邮箱-发码", r and r.get("type") == "email_code_sent", str(r))
        bad = await req(ws, {"type": "send_email_code", "email": "not-an-email"}, "email_code_sent", timeout=3)
        check("绑邮箱-非法邮箱拒绝", bad and bad.get("type") == "error", str(bad))
        # 验码绑定
        c = code_of(email, "bind")
        check("绑邮箱-码已入库", bool(c), str(c))
        r = await req(ws, {"type": "verify_email", "email": email, "code": "000000" if c == "000000" else "999999"}, "email_ok", timeout=3)
        check("绑邮箱-错码拒绝", r and r.get("type") == "error", str(r))
        r = await req(ws, {"type": "verify_email", "email": email, "code": c}, "email_ok")
        check("绑邮箱-正确码成功", r and r.get("type") == "email_ok", str(r))
    # 3) 忘记密码: 未登录发重置码
    async with websockets.connect(URL, open_timeout=5) as ws:
        r = await req(ws, {"type": "request_reset", "user": uname}, "reset_sent")
        check("找回-发码(reset_sent)", r and r.get("type") == "reset_sent", str(r))
        # 不存在账号也回 reset_sent (防枚举)
        r = await req(ws, {"type": "request_reset", "user": "no_such_user_xyz"}, "reset_sent")
        check("找回-不存在账号同样响应(防枚举)", r and r.get("type") == "reset_sent", str(r))
        c = code_of(email, "reset")
        check("找回-重置码入库", bool(c), str(c))
        # 错码拒绝
        r = await req(ws, {"type": "reset_password", "user": uname, "code": "111111", "new_pass": "rp1"}, "reset_ok", timeout=3)
        check("找回-错码拒绝", r and r.get("type") == "error", str(r))
        # 正确码重置
        r = await req(ws, {"type": "reset_password", "user": uname, "code": c, "new_pass": "resetpass9"}, "reset_ok")
        check("找回-正确码重置成功", r and r.get("type") == "reset_ok", str(r))
    # 用重置后密码登录
    async with websockets.connect(URL, open_timeout=5) as ws:
        r = await req(ws, {"type": "login", "user": uname, "pass": "resetpass9"}, "login_ok")
        check("重置后新密码可登录", r and r.get("type") == "login_ok", str(r))

    print("\n" + ("ALL-PASS" if not fails else f"HAS-FAIL {fails}"))
    sys.exit(0 if not fails else 1)

asyncio.run(main())
