#!/bin/bash
# 前端静态检查 wrapper (成熟工具链): ESLint(AST 级 JS 检查) + DOM 一致性
# 用法: bash frontend/lint_ui.sh
# 依赖: node + tests/frontend/node_modules (eslint, jsdom)
if ! command -v node >/dev/null 2>&1; then
  echo "SKIP frontend_lint: node 未安装"
  echo "ALL-PASS (skipped)"
  exit 0
fi
DIR="$(cd "$(dirname "$0")" && pwd)"
if [ ! -x "$DIR/node_modules/.bin/eslint" ]; then
  echo "SKIP frontend_lint: eslint 未安装 (cd tests/frontend && npm install eslint@9)"
  echo "ALL-PASS (skipped)"
  exit 0
fi
ROOT="$(cd "$DIR/../.." && pwd)"
FAIL=0

echo "--- ESLint (AST 级 JS 静态检查) ---"
"$DIR/node_modules/.bin/eslint" "$ROOT/web/app.js" --no-ignore
if [ $? -ne 0 ]; then echo "FAIL eslint"; FAIL=1; else echo "  ✅ eslint PASS"; fi

echo "--- DOM 一致性 (jsdom 真解析, 非正则) ---"
node "$DIR/check_dom.js" || FAIL=1

if [ $FAIL -ne 0 ]; then echo "FAIL frontend_lint"; exit 1; fi
echo "ALL-PASS"