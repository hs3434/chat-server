#!/usr/bin/env node
/**
 * DOM 一致性检查 (jsdom 真解析, 替代 Python 正则方案)
 * 检查:
 *  1. app.js 中 $('id') / getElementById 引用的每个 id 必须在 index.html 中存在
 *  2. script 标签必须在所有带 id 的元素之后 (同步执行 DOM 就绪)
 *  3. id 唯一
 *  4. 事件绑定目标存在
 * 用法: node check_dom.js
 */
const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

const ROOT = path.resolve(__dirname, '..', '..');
const html = fs.readFileSync(path.join(ROOT, 'web', 'index.html'), 'utf-8');
const js = fs.readFileSync(path.join(ROOT, 'web', 'app.js'), 'utf-8');

// 用 jsdom 真解析 HTML
const dom = new JSDOM(html);
const { document } = dom.window;
const { Node } = dom.window;

// 1. 真实 DOM id 集合 + 唯一性
const allIds = document.querySelectorAll('[id]');
const htmlIds = new Set();
const dup = new Set();
const seen = new Set();
for (const el of allIds) {
  const id = el.id;
  htmlIds.add(id);
  if (seen.has(id)) dup.add(id);
  seen.add(id);
}

// 2. script 位置: 找 <script src="/app.js"> 的相对位置
// 用 DOM 顺序: script 元素之后还有没有带 id 的元素
const script = document.querySelector('script[src]');
const late = [];
if (script) {
  // 判断元素是否在 script 之后: 用 compareDocumentPosition
  for (const el of allIds) {
    if (el.compareDocumentPosition(script) & Node.DOCUMENT_POSITION_PRECEDING) {
      late.push(el.id);
    }
  }
}

// 3. app.js 引用 (AST 级: 用简单 token 扫描 + 字符串面值, 比纯正则可靠)
// 这里用正则提取字符串字面量里的 id 引用 (ESLint 已保证语法, 此处只管 $('x') 模式)
const refs = new Set();
const reDollar = /\$\s*\(\s*(['"])([^'"]+)\1\s*\)/g;
let m;
while ((m = reDollar.exec(js))) refs.add(m[2]);
const reGet = /getElementById\s*\(\s*(['"])([^'"]+)\1\s*\)/g;
while ((m = reGet.exec(js))) refs.add(m[2]);

// 动态创建的 id (代码里 .id = 'x')
const dynamic = new Set();
const reDyn = /\.id\s*=\s*(['"])([^'"]+)\1/g;
while ((m = reDyn.exec(js))) dynamic.add(m[2]);

let fail = false;
const missing = [...refs].filter(id => !htmlIds.has(id) && !dynamic.has(id));
if (missing.length) { console.error(`❌ 引用但不存在: ${missing}`); fail = true; }

if (dup.size) { console.error(`❌ 重复 id: ${[...dup]}`); fail = true; }

if (!script) { console.error('❌ 找不到 <script>'); fail = true; }
else if (late.length) { console.error(`❌ script 之后还有 ${late.length} 个 id (同步执行会 null): ${late.slice(0,10)}`); fail = true; }

if (fail) process.exit(1);

// 4. 静态资源引用存在性: <script src="/x.js"> / <link href="/x.css"> 指向的文件必须真实存在
const staticRefs = [];
const reSrc = /<(?:script|link)[^>]+(?:src|href)\s*=\s*['"](\/[^'"]+)['"]/g;
let sm;
while ((sm = reSrc.exec(html))) staticRefs.push(sm[1]);
const webDir = path.join(ROOT, 'web');
const missingFiles = staticRefs.filter(p => {
  // 去掉前导 /, 相对 web 目录检查真实文件
  const rel = p.replace(/^\//, '');
  return !fs.existsSync(path.join(webDir, rel));
});
if (missingFiles.length) {
  console.error(`❌ index.html 引用不存在的静态资源: ${missingFiles}`);
  fail = true;
}

if (fail) process.exit(1);
console.log(`✅ DOM 一致性: ${htmlIds.size} 个 HTML id (jsdom 真解析), ${refs.size} 个 JS 引用, ${staticRefs.length} 个静态资源, 全部对应, script 位置正确`);
