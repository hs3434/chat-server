#!/bin/bash
# 前端 jsdom 单测 wrapper: node/jsdom 缺失则跳过 (不阻塞套件)
# 严格判定: node 测试失败 -> 输出 FAIL 并 exit 1 (run_one 会判 CRASH 且输出 HAS-FAIL)
if ! command -v node >/dev/null 2>&1; then
  echo "SKIP frontend_ui: node 未安装"
  echo "ALL-PASS (skipped)"
  exit 0
fi
DIR="$(cd "$(dirname "$0")" && pwd)"
if [ ! -d "$DIR/node_modules/jsdom" ]; then
  echo "SKIP frontend_ui: jsdom 未安装 (cd tests/frontend && npm install jsdom)"
  echo "ALL-PASS (skipped)"
  exit 0
fi
cd "$DIR" && node test_frontend_ui.js
CODE=$?
if [ $CODE -ne 0 ]; then
  echo "FAIL frontend_ui: node exit=$CODE"
  exit 1
fi
echo "ALL-PASS"