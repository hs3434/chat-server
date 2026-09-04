#!/usr/bin/env python3
"""前端 DOM 一致性静态检查 (杜绝 modal 在 script 后被引用导致的 null 崩溃)
用法: python3 test_frontend_dom.py
检查:
  1. app.js 中 $('xxx') 引用的每个 id 必须在 index.html 中存在
  2. script 标签必须在所有带 id 的 modal 之后 (同步执行时 DOM 已就绪)
  3. 所有 id 唯一
  4. 事件绑定目标必须存在
"""
import os, re, sys

BASE = os.path.dirname(os.path.abspath(__file__))
HTML = os.path.join(BASE, "..", "web", "index.html")
JS = os.path.join(BASE, "..", "web", "app.js")

def main():
    fails = []
    html = open(HTML, encoding="utf-8").read()
    js = open(JS, encoding="utf-8").read()

    # 1. index.html 中所有 id
    html_ids = set(re.findall(r'id="([^"]+)"', html))
    # 2. app.js 中所有 $('xxx') / getElementById 引用的 id
    js_refs = set(re.findall(r"\$\('([^']+)'\)", js))
    js_refs |= set(re.findall(r'getElementById\("([^"]+)"\)', js))
    # 3. 动态创建的 id (document.createElement 后 .id=)
    dynamic = set(re.findall(r"\.id\s*=\s*'([^']+)'", js))
    dynamic |= set(re.findall(r'\.id\s*=\s*"([^"]+)"', js))

    missing = js_refs - html_ids - dynamic
    if missing:
        fails.append(f"app.js 引用但 index.html 缺失的 id: {sorted(missing)}")

    # 4. id 唯一性
    dup = {i for i in html_ids if html.count(f'id="{i}"') > 1}
    if dup:
        fails.append(f"index.html 重复 id: {sorted(dup)}")

    # 5. script 标签必须在所有带 id 的 DOM 元素之后 (同步执行 DOM 就绪)
    script_pos = html.find('<script src="/app/app.js">')
    if script_pos == -1:
        fails.append("找不到 <script src=\"/app/app.js\">")
    else:
        late = [i for i in html_ids if html.rfind(f'id="{i}"') > script_pos]
        if late:
            fails.append(f"app.js script 在 {len(late)} 个 DOM 元素之前执行 (同步加载会 null 崩溃): {sorted(late)[:10]}")

    # 6. 事件绑定目标必须存在
    bindings = set(re.findall(r"\$\('([^']+)'\)\.on\w+\s*=", js))
    for b in bindings:
        if b not in html_ids and b not in dynamic:
            fails.append(f"事件绑定目标 {b} 不存在")

    if fails:
        print("FAIL:")
        for f in fails:
            print(" -", f)
        sys.exit(1)
    print(f"✅ frontend-dom 检查通过: {len(html_ids)} 个 HTML id, {len(js_refs)} 个 JS 引用, 全部对应, script 位置正确")
    print("ALL-PASS")

if __name__ == "__main__":
    main()
