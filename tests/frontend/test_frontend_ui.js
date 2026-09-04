#!/usr/bin/env node
/**
 * 前端 DOM/交互 jsdom 单测 (防绑定断链/交互回归)
 * 用法: node tests/frontend/test_frontend_ui.js
 */
const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

const ROOT = path.resolve(__dirname, '..', '..');
const html = fs.readFileSync(path.join(ROOT, 'web', 'index.html'), 'utf-8');
const js = fs.readFileSync(path.join(ROOT, 'web', 'app.js'), 'utf-8');

const dom = new JSDOM(html, { url: 'http://localhost:8081/', runScripts: 'outside-only', pretendToBeVisual: true });
const { window } = dom;
const { document } = window;

class FakeWebSocket {
  static instances = [];
  constructor(url) {
    this.url = url; this.readyState = 0; this.sent = [];
    FakeWebSocket.instances.push(this);
    setTimeout(() => { this.readyState = 1; if (this.onopen) this.onopen({}); }, 5);
  }
  send(data) { this.sent.push(data); }
  close() { this.readyState = 3; if (this.onclose) this.onclose({ code: 1000 }); }
  serverPush(obj) { if (this.onmessage) this.onmessage({ data: JSON.stringify(obj) }); }
  get sentObjs() { return this.sent.map(s => JSON.parse(s)); }
}
window.WebSocket = FakeWebSocket;
window.alert = (m) => { window.__lastAlert = m; };
window.eval(js);

const wait = (ms) => new Promise(r => setTimeout(r, ms));
let pass = 0, fail = 0;
function check(name, cond, extra = '') {
  if (cond) { pass++; console.log(`  ✅ ${name}`); }
  else { fail++; console.log(`  ❌ ${name} ${extra}`); }
}

(async () => {
  console.log('--- 1. 事件绑定完整性 (防 null 崩溃) ---');
  const binds = ['newGrp','devicesBtn','devMask','grpInfo','modalMask','gAdd','gTransfer','gKick','gLeave','gDissolve','loginBtn','regBtn','logout','chatback','sendbtn'];
  for (const id of binds) {
    const el = document.getElementById(id);
    check(`绑定 ${id}`, el && typeof el.onclick === 'function', `Got: ${el ? typeof el.onclick : 'null'}`);
  }

  console.log('--- 2. 登录/注册表单 ---');
  const auth = document.getElementById('auth');
  const app = document.getElementById('app');
  check('初始 auth 可见', auth && !auth.classList.contains('hidden'));
  check('初始 app 隐藏', app && app.classList.contains('hidden'));

  // 等 WS open (eval 时 connect 已建, onopen 5ms)
  await wait(30);
  const ws = FakeWebSocket.instances[0];
  check('WS 连接到 /ws', !!ws && String(ws.url).endsWith('/ws'), `url=${ws && ws.url}`);

  document.getElementById('aUser').value = 'testdom_u';
  document.getElementById('aPass').value = 'testpass';
  document.getElementById('regBtn').onclick();
  await wait(30);

  const s0 = ws.sentObjs[0];
  check('注册发送 {type:register,user,pass}', s0 && s0.type === 'register' && s0.user === 'testdom_u' && s0.pass === 'testpass', JSON.stringify(s0));

  console.log('--- 3. 注册成功 → 自动登录 ---');
  if (s0) ws.serverPush({ type: 'register_ok', user: 'testdom_u', seq: s0.seq });
  await wait(30);
  const s1 = ws.sentObjs[1];
  check('注册后自动发 login', s1 && s1.type === 'login' && s1.user === 'testdom_u', JSON.stringify(s1));
  if (s1) ws.serverPush({ type: 'login_ok', user: 'testdom_u', token: 'tok123', seq: s1.seq });
  await wait(30);
  check('登录成功显示 app', !app.classList.contains('hidden'));
  check('身份显示用户名', document.getElementById('meUser').textContent.includes('testdom_u'));

  console.log('--- 4. 会话列表渲染 ---');
  ws.serverPush({ type: 'conversations', items: [] });
  await wait(20);
  check('conversations 渲染', document.getElementById('convs') !== null);

  console.log('--- 5. 发送消息 ---');
  check('sendbtn 绑定', typeof document.getElementById('sendbtn').onclick === 'function');

  console.log('\n==== 结果 ====');
  console.log(`PASS: ${pass}, FAIL: ${fail}`);
  process.exit(fail > 0 ? 1 : 0);
})();