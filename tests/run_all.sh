#!/usr/bin/env bash
# wxlike-server 全部测试统一运行器
# 用法: ./tests/run_all.sh [8081]
# 保证: 每个测试前的干净状态 (清库 + 重启服务器)
set -u
PORT="${1:-8081}"
PY=/workspace/wxlike-server/impl_py/.venv/bin/python
BIN=/workspace/wxlike-server/bin/wxlike-go
DIR=/workspace/wxlike-server
TESTS=/workspace/wxlike-server/tests

echo "==================== wxlike-server 测试 (Go, port $PORT) ===================="
FAIL=0

restart_clean() {
    pkill -f "wxlike-go" 2>/dev/null
    # 等旧进程完全退出 (优雅关闭可能需短暂时间)
    for i in $(seq 1 20); do
        if ! pgrep -f "wxlike-go" >/dev/null 2>&1; then break; fi
        sleep 0.3
    done
    pkill -9 -f "wxlike-go" 2>/dev/null
    sleep 0.5
    rm -f "$DIR/wxlike_go.db"* 2>/dev/null
    nohup "$BIN" --port "$PORT" --dir "$DIR" $RATE_FLAG --web "$DIR/web" > /tmp/go_srv_run.log 2>&1 &
    # 等端口就绪 (最多 8s)
    for i in $(seq 1 40); do
        if (echo > /dev/tcp/127.0.0.1/$PORT) 2>/dev/null; then break; fi
        sleep 0.2
    done
}

run_one() {
    name="$1"; script="$2"
    restart_clean
    echo "--- $name ---"
    if "$PY" "$TESTS/$script" "$PORT" > "/tmp/${name}_out.txt" 2>&1; then
        grep -E "^(OK|FAIL)" "/tmp/${name}_out.txt" | sed 's/^/  /'
        if grep -q "ALL-PASS" "/tmp/${name}_out.txt"; then
            echo "  ✅ $name ALL-PASS"
        else
            echo "  ❌ $name HAS-FAIL"; FAIL=1
        fi
    else
        echo "  ❌ $name CRASH (exit=$?)"; FAIL=1
        tail -4 "/tmp/${name}_out.txt" | sed 's/^/    /'
    fi
}

RATE_FLAG="--reg-limit 0"  # 默认: 关闭注册限流(批量测试注册>3用户); 保留消息限流5/s+登录锁
run_one "e2e"            "test_e2e.py"
run_one "reliability"    "test_reliability.py"
run_one "edge"           "test_edge.py"
run_one "persistence"    "test_persistence.py"
run_one "permission"    "test_permission.py"

run_one "features"      "test_features.py"

RATE_FLAG="--msg-rate 0 --reg-limit 0"  # 并发测试: 关闭限流 (验证并发正确性)
run_one "concurrency"   "test_concurrency.py"

RATE_FLAG="--msg-rate 0 --reg-limit 0"  # 群生命周期测试: 注册多用户, 关闭限流
run_one "grouplifecycle" "test_group_lifecycle.py"

run_one "auth"          "test_auth.py"

run_one "ops"            "test_ops.py"

RATE_FLAG="--reg-limit 3"  # 限流测试: 单独开启注册限流(默认3次/10分钟IP)
run_one "ratelimit"      "test_ratelimit.py"

RATE_FLAG="--reg-limit 0"  # 实时通知测试: 注册多用户
run_one "realtime"       "test_realtime.py"

RATE_FLAG="--reg-limit 0"  # 会话列表测试: 注册多用户
run_one "conversations"  "test_conversations.py"

RATE_FLAG="--reg-limit 0 --admin exp_admin"  # 导出+presence: 注册多用户 + 管理员
run_one "export"         "test_export.py"

RATE_FLAG="--reg-limit 0"  # 前端协议测试
run_one "frontend"       "test_frontend_protocol.py"
run_one "frontend_dom"   "test_frontend_dom.py"

RATE_FLAG="--reg-limit 0"  # 群成员查询测试
run_one "groupmembers"   "test_group_members.py"

RATE_FLAG="--reg-limit 0"  # 设备会话管理
run_one "devices"        "test_devices.py"

echo ""
echo "==================== 结果 ===================="
if [ "$FAIL" -eq 0 ]; then
    echo "🎉 全部测试 ALL-PASS"
    exit 0
else
    echo "❌ 存在失败测试, 见各 /tmp/*_out.txt"
    exit 1
fi
