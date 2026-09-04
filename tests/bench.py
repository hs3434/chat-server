#!/usr/bin/env python3
"""Python vs Go 本地对比基准
用法: python3 bench.py <port> <name>
测: ①注册登录往返延迟 ②连续发消息吞吐 ③服务器进程内存
"""
import asyncio
import json
import os
import statistics
import sys
import time

import websockets

PORT = int(sys.argv[1])
NAME = sys.argv[2] if len(sys.argv) > 2 else f"port{PORT}"
URL = f"ws://127.0.0.1:{PORT}/ws"


async def latency_bench():
    """单条注册+登录往返延迟, 测多次取中位/平均"""
    lat = []
    async with websockets.connect(URL, open_timeout=5) as ws:
        for _ in range(20):
            t0 = time.perf_counter()
            await ws.send(json.dumps({"type": "register", "user": "b01", "pass": "x"}))
            await ws.recv()
            d1 = (time.perf_counter() - t0) * 1000
            t0 = time.perf_counter()
            await ws.send(json.dumps({"type": "login", "user": "b01", "pass": "x"}))
            await ws.recv()
            d2 = (time.perf_counter() - t0) * 1000
            lat.append((d1, d2))
    reg = sorted(x[0] for x in lat)
    log = sorted(x[1] for x in lat)
    return {
        "register_ms_med": round(statistics.median(reg), 2),
        "register_ms_avg": round(statistics.mean(reg), 2),
        "login_ms_med": round(statistics.median(log), 2),
        "login_ms_avg": round(statistics.mean(log), 2),
    }


async def throughput_bench(n=200):
    """吞吐: 发 n 条消息, 测耗时并确认全部收到"""
    async with websockets.connect(URL, open_timeout=5) as ws:
        # 登录为 alice, 和 bob 单聊 (bob 离线 -> 不入队, 但会排队... 用 history 清)
        await ws.send(json.dumps({"type": "login", "user": "b01", "pass": "x"}))
        await ws.recv()
        t0 = time.perf_counter()
        for i in range(n):
            await ws.send(json.dumps({"type": "msg", "to": "b02", "body": f"m{i}"}))
        # 单聊直投没有回执, 发一个 history 强制同步
        await ws.send(json.dumps({"type": "history", "jid": "b02", "limit": n}))
        r = await ws.recv()
        t1 = time.perf_counter()
        hist = json.loads(r)
        rows = len(hist.get("rows", []))
        dt = t1 - t0
        return {
            "sent_sync": n,
            "stored": rows,
            "elapsed_s": round(dt, 3),
            "msg_per_s": round(n / dt, 0),
        }


def rss_bytes(keyword):
    """读服务器进程 RSS (MB)。keyword: 'start_py' 或 'wxlike-go'"""
    try:
        for line in open("/proc/1/cgroup", "r"):
            pass
    except Exception:
        pass
    # 遍历 /proc
    import glob
    best = None
    for pid_dir in glob.glob("/proc/[0-9]*"):
        try:
            cmd = open(f"{pid_dir}/comm").read().strip()
            statm = open(f"{pid_dir}/statm").read().split()
        except Exception:
            continue
        rss_pages = int(statm[1])
        if keyword in cmd or (keyword in _cmdline(pid_dir)):
            rss_mb = rss_pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)
            if best is None or rss_mb > best:
                best = rss_mb
    return best


def _cmdline(pid_dir):
    try:
        return open(f"{pid_dir}/cmdline").read().replace("\0", " ")
    except Exception:
        return ""


async def main():
    print(f"===== {NAME} (port {PORT}) 对比基准 =====")
    lat = await latency_bench()
    print(f"[延迟] register: 中位{lat['register_ms_med']}ms 均{lat['register_ms_avg']}ms | "
          f"login: 中位{lat['login_ms_med']}ms 均{lat['login_ms_avg']}ms")
    th = await throughput_bench()
    print(f"[吞吐] 发{th['sent_sync']}条 存{th['stored']}条 耗时{th['elapsed_s']}s -> {th['msg_per_s']} msg/s")
    # 内存
    time.sleep(0.5)
    if "py" in NAME.lower():
        rss = rss_bytes("impl_py")
        if rss is None:
            rss = rss_bytes("server.py")
    else:
        rss = rss_bytes("wxlike-go")
    print(f"[内存] 服务器 RSS ≈ {rss} MB")


if __name__ == "__main__":
    asyncio.run(main())
