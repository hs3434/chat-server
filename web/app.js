// wxlike 前端 —— 直连 Go 服务器 WS 协议 (零依赖, 原生 JS)
'use strict';

const $ = (id) => document.getElementById(id);

const State = {
  user: null,
  token: null,
  ws: null,
  convs: [],          // 会话列表 [{chat,last_body,last_ts,last_from,unread}]
  online: {},         // user -> bool (presence)
  view: null,         // 当前打开的 chat (peer user 或 group::gid)
  msgs: {},           // chat -> [{id,from,body,ts,state}] 已加载消息 (id 升序)
  seen: new Set(),    // 已见过的全局消息 id (断线重连去重)
  reconnectAttempt: 0,
  seq: 0,             // 请求序号 (匹配响应)
  pending: {},        // seq -> {type, resolve}
};

// ---------- 服务器通信 ----------
function wsUrl() {
  const proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
  return proto + location.host + '/ws';
}

function connect() {
  const ws = new WebSocket(wsUrl());
  ws.onopen = () => {
    State.reconnectAttempt = 0;
    if (State.token) { tokenLogin(); }
    else console.log('ws open (未登录)');
  };
  ws.onmessage = (e) => {
    let m;
    try { m = JSON.parse(e.data); } catch { return; }
    handleServer(m);
  };
  ws.onclose = () => {
    // 断线重连 (指数退避 1s..10s)
    State.reconnectAttempt++;
    const delay = Math.min(1000 * Math.pow(2, State.reconnectAttempt), 10000);
    setTimeout(connect, delay);
  };
  ws.onerror = () => ws.close();
  State.ws = ws;
}

function send(obj) {
  if (State.ws && State.ws.readyState === 1) State.ws.send(JSON.stringify(obj));
}

// 请求-响应封装: 发动作并等待特定 type 响应 (带等待队列)
function req(obj, wantType, timeout = 4000) {
  return new Promise((resolve) => {
    const id = ++State.seq;
    const timer = setTimeout(() => { delete State.pending[id]; resolve(null); }, timeout);
    State.pending[id] = { type: wantType, resolve: (m) => { clearTimeout(timer); resolve(m); } };
    obj.seq = id;
    send(obj);
  });
}

// ---------- 服务器分发 ----------
function handleServer(m) {
  switch (m.type) {
    case 'login_ok': {
      State.user = m.user;
      State.token = m.token;
      completeLogin();
      break;
    }
    case 'msg': {
      if (!State.seen.has(m.id)) {
        State.seen.add(m.id);
        ingestMsg(m);
      }
      break;
    }
    case 'peer_ack': {
      // 已读回执: 更新已打开会话里对应消息的 state
      if (m.gid) {
        const key = 'group::' + m.gid.split('::')[1];
        updateReadState(key, m.id, m.read, m.total);
      } else if (m.reader && State.view && !isGroup(State.view)) {
        updateReadState(State.view, m.id);
      }
      break;
    }
    case 'presence_evt': {
      State.online[m.user] = !!m.online;
      refreshConversationStatus();
      updateChatHead();
      break;
    }
    case 'device_evt': {
      // 账号在其他设备登录: 提示 (微信语义)
      showToast('你的账号在新设备登录 (' + (m.ip || '未知IP') + ')');
      break;
    }
    case 'kicked': {
      alert('此设备已被踢下线');
      setTimeout(logout, 100);
      break;
    }
    case 'sessions': resolveReq(m.seq, m); break;
    case 'kick_ok': resolveReq(m.seq, m); break;
    case 'conversations': resolveReq(m.seq, m); break;
    case 'history': resolveReq(m.seq, m); break;
    case 'recent': resolveReq(m.seq, m); break;
    case 'unread': resolveReq(m.seq, m); break;
    case 'presence': resolveReq(m.seq, m); break;
    case 'register_ok': resolveReq(m.seq, m); break;
    case 'group_ok': resolveReq(m.seq, m); break;
    case 'error': resolveReq(m.seq, m); break;
    default: console.log('unhandled:', m);
  }
}

function resolveReq(seq, m) {
  const p = State.pending[seq];
  if (p) { delete State.pending[seq]; if (m.type === p.type || m.type === 'error') p.resolve(m); }
}

// 新消息进入: 更新会话列表 + 当前视图
function ingestMsg(m) {
  const key = m.gid ? 'group::' + m.gid.split(':')[1] : m.from;
  if (!State.msgs[key]) State.msgs[key] = [];
  const arr = State.msgs[key];
  if (!arr.some((x) => x.id === m.id)) { arr.push({ id: m.id, from: m.from, body: m.body, ts: m.ts, state: 'delivered' }); arr.sort((a, b) => a.id - b.id); }
  if (State.view && State.view === key) {
    renderMsgs();
    if (m.from !== State.user) ackRead(key, m.id);
  }
  refreshConvs();
}

function isGroup(chat) { return chat.startsWith('group::'); }

// ---------- 会话列表 ----------
async function refreshConvs() {
  const r = await req({ type: 'conversations' }, 'conversations');
  if (!r) return;
  State.convs = r.items || [];
  renderConvs();
  // presence 批量查询: 会话里所有单聊对方
  const users = State.convs.filter((c) => !isGroup(c.chat)).map((c) => c.chat);
  if (users.length) {
    const pr = await req({ type: 'presence', users }, 'presence');
    if (pr) { State.online = { ...State.online, ...pr.online }; renderConvs(); refreshConversationStatus(); }
  }
}

function renderConvs() {
  const box = $('convs');
  if (!State.convs.length) { box.innerHTML = '<div class="empty">暂无会话, 去消息页发一条吧</div>'; return; }
  const sorted = [...State.convs].sort((a, b) => b.last_ts - a.last_ts);
  box.innerHTML = sorted.map((c) => {
    const name = isGroup(c.chat) ? c.chat.slice(7) : c.chat;
    const av = name[0] ? name[0].toUpperCase() : '?';
    const onlineTag = (!isGroup(c.chat) && State.online[c.chat]) ? '🟢' : '';
    const unread = c.unread > 0 ? `<span class="badge">${c.unread}</span>` : '';
    return `<div class="conv" data-chat="${c.chat}">
      <div class="av">${av}</div>
      <div class="cmain">
        <div class="cname"><span>${esc(name)} ${onlineTag}</span><span class="t">${fmtTs(c.last_ts)}</span></div>
        <div class="clast">${esc(c.last_from + ': ' + c.last_body)}</div>
      </div>${unread}
    </div>`;
  }).join('');
  box.querySelectorAll('.conv').forEach((el) => el.onclick = () => openChat(el.dataset.chat));
}

function refreshConversationStatus() {
  // 只重绘名字区 (在线标签) — 简单起见整列表重绘
  if (!State.view) renderConvs();
}

// ---------- 聊天 ----------
async function openChat(chat) {
  State.view = chat;
  if (!State.msgs[chat]) State.msgs[chat] = [];
  $('chatbox').classList.add('open');
  $('chathead').classList.remove('hidden');
  $('inputbar').classList.remove('hidden');
  $('grpInfo').classList.toggle('hidden', !isGroup(chat));
  $('msgs').innerHTML = '';
  updateChatHead();
  renderMsgs();
  // 拉历史 (最近 50 条)
  const r = await req({ type: 'history', chat, limit: 50 }, 'history');
  if (r && r.items) {
    const arr = State.msgs[chat];
    r.items.forEach((m) => { if (!arr.some((x) => x.id === m.id)) arr.push({ id: m.id, from: m.from, body: m.body, ts: m.ts, state: m.state }); });
    arr.sort((a, b) => a.id - b.id);
    State.msgs[chat] = arr;
    renderMsgs();
    // 打开会话: 未读清零 + ack 所有我的未读
    ackAllUnread(chat);
  }
}

function updateChatHead() {
  if (!State.view) return;
  const name = isGroup(State.view) ? State.view.slice(7) : State.view;
  $('chatName').textContent = name;
  const st = isGroup(State.view) ? '群聊' : (State.online[State.view] ? '在线' : '离线');
  $('chatStatus').textContent = st;
}

function renderMsgs() {
  const arr = State.msgs[State.view] || [];
  const box = $('msgs');
  if (!arr.length) { box.innerHTML = '<div class="empty">没有消息</div>'; return; }
  box.innerHTML = arr.map((m) => {
    const mine = m.from === State.user;
    const who = isGroup(State.view) && !mine ? `<div class="who">${esc(m.from)}</div>` : '';
    const recp = (!mine && m.state === 'read') ? '<span class="recp">已读</span>' : '';
    return `<div class="msg ${mine ? 'mine' : 'theirs'}" data-id="${m.id}">
      ${who}${esc(m.body)}<div class="meta">${fmtTs(m.ts)}${mine ? '' : recp}</div>
    </div>`;
  }).join('');
  box.scrollTop = box.scrollHeight;
}

// 更新某会话某消息的已读状态 (peer_ack / 打开时)
function updateReadState(chat, id, read, total) {
  const arr = State.msgs[chat];
  if (!arr) return;
  const m = arr.find((x) => x.id === id);
  if (m && m.state !== 'read') { m.state = 'read'; }
  if (State.view === chat) renderMsgs();
}

async function ackAllUnread(chat) {
  const arr = State.msgs[chat] || [];
  for (const m of arr) {
    if (m.from !== State.user && m.state !== 'read') {
      send({ type: 'ack_received', id: m.id });
      await req({ type: 'ack_read', id: m.id }, 'ack_read').catch(() => {});
      m.state = 'read';
    }
  }
  renderMsgs();
  refreshConvs(); // 未读清零
}

function ackRead(chat, id) {
  send({ type: 'ack_received', id });
  send({ type: 'ack_read', id });
  const arr = State.msgs[chat];
  const m = arr && arr.find((x) => x.id === id);
  if (m) m.state = 'read';
}

function sendMsg() {
  const inp = $('inp');
  const body = inp.value.trim();
  if (!body || !State.view) return;
  const to = State.view;
  const payload = { type: 'msg', to, body };
  if (isGroup(to)) payload.is_group = true;
  send(payload);
  inp.value = '';
  inp.focus();
}

// ---------- 登录/注册 ----------
async function tokenLogin() {
  // 断线重连: 凭 token 恢复会话 (不重输密码, 不存密码)
  const r = await req({ type: 'token_login', token: State.token }, 'login_ok');
  if (!r || r.type === 'error') {
    // token 失效: 回登录页
    logout();
    return;
  }
  State.user = r.user;
  State.token = r.token;
  completeLogin();
}

async function doLogin(regToo) {
  const user = $('aUser').value.trim();
  const pass = $('aPass').value;
  if (!user || !pass) { $('authErr').textContent = '请输入用户名和密码'; return; }
  if (regToo) {
    const rr = await req({ type: 'register', user, pass }, 'register_ok');
    if (!rr || rr.type === 'error') { $('authErr').textContent = '注册失败 (可能已存在)'; return; }
  }
  const r = await req({ type: 'login', user, pass }, 'login_ok');
  if (!r || r.type === 'error') {
    $('authErr').textContent = '登录失败: ' + (r && r.code ? r.code : '未知错误');
    return;
  }
  State.user = r.user;
  State.token = r.token;
  completeLogin();
}

function completeLogin() {
  $('auth').classList.add('hidden');
  $('app').classList.remove('hidden');
  $('meUser').textContent = State.user;
  refreshConvs();
}

function logout() {
  if (State.ws) State.ws.close();
  State.user = null; State.token = null; State.convs = []; State.online = {}; State.view = null;
  State.msgs = {}; State.seen = new Set();
  $('app').classList.add('hidden');
  $('auth').classList.remove('hidden');
  connect(); // 重新连 (未登录态)
}

// ---------- 工具 ----------
function esc(s) { const d = document.createElement('div'); d.textContent = s == null ? '' : String(s); return d.innerHTML; }
function fmtTs(ts) {
  if (!ts) return '';
  const d = new Date(ts);
  const now = new Date();
  if (d.toDateString() === now.toDateString()) return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
  return `${d.getMonth() + 1}/${d.getDate()} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
function pad(n) { return n < 10 ? '0' + n : '' + n; }

// ---------- 群管理 ----------
let currentGid = null;      // 当前群信息面板的 gid (纯 gid)
let currentGMembers = [];   // 群成员列表

function openGrpModal(gid) {
  currentGid = gid;
  showModal();
  refreshGrpInfo();
}

async function refreshGrpInfo() {
  if (!currentGid) return;
  const r = await req({ type: 'group_members', gid: 'group::' + currentGid }, 'group_members');
  if (!r || r.type === 'error') return;
  currentGMembers = r.members || [];
  const isOwner = r.owner === State.user;
  $('gTitle').textContent = r.name || currentGid;
  $('gMembers').innerHTML = currentGMembers.map((u) => {
    const tag = u === r.owner ? '<span class="owner-tag">群主</span>' : '';
    const me = u === State.user ? ' me' : '';
    return `<div class="member${me}"><span>${esc(u)}</span>${tag}</div>`;
  }).join('');
  // 群主才显示管理按钮
  $('gAdd').style.display = isOwner ? 'block' : 'none';
  $('gAddUser').style.display = isOwner ? 'block' : 'none';
  $('gTransfer').style.display = isOwner ? 'block' : 'none';
  $('gKick').style.display = isOwner ? 'block' : 'none';
  $('gLeave').style.display = 'block';
  $('gDissolve').style.display = isOwner ? 'block' : 'none';
}

// 新建群: 输群名 -> 建为会话
async function createGroup() {
  const name = prompt('群名称:');
  if (!name || !name.trim()) return;
  const gid = 'g' + Date.now().toString(36);
  const r = await req({ type: 'create_group', gid, name: name.trim() }, 'group_ok');
  if (r && r.type === 'group_ok') {
    refreshConvs();
    openChat('group::' + gid);
  } else {
    alert('建群失败: ' + (r && r.code || '未知'));
  }
}

// 群操作
async function grpAdd() {
  const u = $('gAddUser').value.trim();
  if (!u || !currentGid) return;
  const r = await req({ type: 'add_member', gid: currentGid, user: u }, 'group_ok');
  if (r && r.type === 'group_ok') { $('gAddUser').value = ''; refreshGrpInfo(); }
  else alert('加人失败: ' + (r && r.code || '未知'));
}

async function grpTransfer() {
  const u = prompt('转让给成员:');
  if (!u || !currentGid) return;
  const r = await req({ type: 'transfer_owner', gid: currentGid, user: u }, 'group_ok');
  if (r && r.type === 'group_ok') refreshGrpInfo();
  else alert('转让失败: ' + (r && r.code || '未知'));
}

async function grpKick() {
  const u = prompt('踢出成员:');
  if (!u || !currentGid) return;
  const r = await req({ type: 'remove_member', gid: currentGid, user: u }, 'group_ok');
  if (r && r.type === 'group_ok') refreshGrpInfo();
  else alert('踢人失败: ' + (r && r.code || '未知'));
}

async function grpLeave() {
  if (!currentGid || !confirm('确认退出该群?')) return;
  const r = await req({ type: 'leave_group', gid: currentGid }, 'group_ok');
  if (r && r.type === 'group_ok') { hideModal(); closeChat(); refreshConvs(); }
  else alert('退群失败: ' + (r && r.code || '未知'));
}

async function grpDissolve() {
  if (!currentGid || !confirm('确认解散该群? 所有人将无法访问')) return;
  const r = await req({ type: 'dissolve_group', gid: currentGid }, 'group_ok');
  if (r && r.type === 'group_ok') { hideModal(); closeChat(); refreshConvs(); }
  else alert('解散失败: ' + (r && r.code || '未知'));
}

function showModal() { $('modal').classList.remove('hidden'); }
function hideModal() { $('modal').classList.add('hidden'); currentGid = null; }
function closeChat() { $('chatbox').classList.remove('open'); State.view = null; $('chathead').classList.add('hidden'); $('inputbar').classList.add('hidden'); }

// ---------- 设备管理 ----------
async function showDevices() {
  $('devModal').classList.remove('hidden');
  $('devList').innerHTML = '<div class="empty">加载中...</div>';
  const r = await req({ type: 'sessions' }, 'sessions');
  if (!r || r.type === 'error') { $('devList').innerHTML = '<div class="empty">无法获取设备</div>'; return; }
  const devs = r.devices || [];
  if (!devs.length) { $('devList').innerHTML = '<div class="empty">无在线设备</div>'; return; }
  // 当前设备 (唯一无 from 自我标记, 简化: 假定最后一台是本机, 因本 conn 刚注册 newest)
  $('devList').innerHTML = devs.map((d) => `
    <div class="dev-item">
      <div class="meta">
        <div><b class="dev-ip">${esc(d.ip || '?')}</b></div>
        <div class="sub">${fmtDate(d.since)}</div>
      </div>
      <button class="kick" data-sid="${esc(d.sid)}">下线</button>
    </div>`).join('');
  $('devList').querySelectorAll('.kick').forEach((b) => b.onclick = () => kickDevice(b.dataset.sid));
}

async function kickDevice(sid) {
  if (!confirm('确认将这台设备下线?')) return;
  const r = await req({ type: 'kick', sid }, 'kick_ok');
  if (r && r.type === 'kick_ok') showDevices();
  else alert('操作失败');
}

function hideDevices() { $('devModal').classList.add('hidden'); }

function fmtDate(ts) {
  if (!ts) return '';
  const d = new Date(ts);
  return `${d.getFullYear()}/${d.getMonth() + 1}/${d.getDate()} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

// Toast 提示 (轻量)
function showToast(msg) {
  let t = $('toast');
  if (!t) { t = document.createElement('div'); t.id = 'toast'; t.style.cssText = 'position:fixed;top:20px;left:50%;transform:translateX(-50%);background:rgba(0,0,0,.75);color:#fff;padding:10px 16px;border-radius:8px;font-size:14px;z-index:100;max-width:80%;'; document.body.appendChild(t); }
  t.textContent = msg;
  clearTimeout(t._timer);
  t._timer = setTimeout(() => { t.remove(); }, 3000);
}

// ---------- 事件绑定 ----------
$('newGrp').onclick = createGroup;
$('devicesBtn').onclick = showDevices;
$('devMask').onclick = hideDevices;
$('grpInfo').onclick = () => { if (State.view && isGroup(State.view)) openGrpModal(State.view.slice(7)); };
$('modalMask').onclick = hideModal;
$('gAdd').onclick = grpAdd;
$('gTransfer').onclick = grpTransfer;
$('gKick').onclick = grpKick;
$('gLeave').onclick = grpLeave;
$('gDissolve').onclick = grpDissolve;
$('loginBtn').onclick = () => doLogin(false);
$('regBtn').onclick = () => doLogin(true);
$('logout').onclick = logout;
$('chatback').onclick = () => { $('chatbox').classList.remove('open'); State.view = null; refreshConvs(); };
$('sendbtn').onclick = sendMsg;
$('inp').addEventListener('keydown', (e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMsg(); } });

connect();
