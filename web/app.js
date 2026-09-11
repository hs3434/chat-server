// wxlike 前端 —— 直连 Go 服务器 WS 协议 (零依赖, 原生 JS)
'use strict';

const $ = (id) => document.getElementById(id);

const State = {
  user: null,
  token: null,
  nick: null,            // 自己的昵称 (空=回落 user)
  avatar: null,          // 自己的头像 /uploads/file (空=首字母占位)
  sig: null,             // 自己签名
  profiles: {},          // username -> {nickname,signature,avatar} (展示用缓存)
  profileQ: {},          // username -> true (get_profile in-flight, 防重复)
  myLastProfile: null,   // 上次拿到的自己的完整资料对象
  chatName: null,       // 当前打开的群名 (来自 conversations.name)
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
    flushOutbox();               // 补发连接建立前排队的请求
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
  else {
    // ws 尚未就绪(刷新/重连瞬间): 排队, 连接打开后补发, 避免静默丢失请求
    State._outbox = State._outbox || [];
    State._outbox.push(obj);
  }
}
// 连接打开后冲刷排队请求
function flushOutbox() {
  if (!State._outbox || !State._outbox.length) return;
  if (!(State.ws && State.ws.readyState === 1)) return;
  const q = State._outbox; State._outbox = [];
  for (const o of q) { try { State.ws.send(JSON.stringify(o)); } catch (e) {} }
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
      resolveReq(m.seq, m);   // 命中 req() 的 pending (token_login/login 都靠它), 否则 await 永不 resolve -> 误判超时登出
      State.user = m.user;
      State.token = m.token;
      // 登录带上自己 profile (服务器 set 过才有; 空回落 username)
      State.nick = m.nickname || '';
      State.avatar = m.avatar || '';
      State.sig = m.signature || '';
      State.email = m.email || '';
      if (State.user) cacheProfile(State.user, { nickname: State.nick, signature: State.sig, avatar: State.avatar });
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
    case 'password_ok': resolveReq(m.seq, m); break;      // 改密成功
    case 'email_code_sent': resolveReq(m.seq, m); break;  // 邮箱验证码已发
    case 'email_ok': resolveReq(m.seq, m); break;         // 邮箱绑定成功
    case 'reset_sent': resolveReq(m.seq, m); break;       // 找回密码: 码已发
    case 'reset_ok': resolveReq(m.seq, m); break;         // 密码重置成功
    case 'error': resolveReq(m.seq, m); break;
    case 'conversations_refresh': // 服务器通知会话列表有变化 (如被拉进群)
      refreshConvs();
      break;
    case 'profile_evt': // 他人改了资料 (推给有会话的人)
      if (m.user) {
        cacheProfile(m.user, { nickname: m.nickname, avatar: m.avatar, signature: m.signature });
        if (m.user === State.user) { State.nick = m.nickname || ''; State.sig = m.signature || ''; State.avatar = m.avatar || ''; updateMeBar(); }
        refreshConvs();
        renderMsgs(); // 可能正显示该成员发的旧消息, 一并更新
        updateChatHead();
      }
      break;
    case 'profile_ok': case 'profile':
      if (m.profile) {
        const p = m.profile;
        // 完整缓存 (含签名)
        cacheProfile(p.username, p);
        // 若是自己的资料改动 (set_profile 回显 / 其它端改我的), 同步本机并刷新"我"
        if (p.username === State.user) {
          State.nick = p.nickname || '';
          State.sig = p.signature || '';
          State.avatar = p.avatar || '';
          updateMeBar();
        }
        // 注意: 服务器 type='profile' 应答也可能来自后台预热 get_profile (warmUsers/hitProfile),
        //       因此这里不自动弹资料卡; 只在用户主动点击(showPeerCard)时渲染。见 showPeerCard。
      }
      resolveReq(m.seq, m); // set_profile/get_profile 的响应走 req 匹配
      break;
    case 'profiles': // 批量 (群成员/会话) 资料
      if (Array.isArray(m.items)) m.items.forEach((p) => cacheProfile(p.username, p));
      break;
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
  warmUsers(users); // 预热单聊对方资料 → 昵称/头像展示
}

function renderConvs() {
  const box = $('convs');
  if (!State.convs.length) { box.innerHTML = '<div class="empty">暂无会话, 去消息页发一条吧</div>'; return; }
  const sorted = [...State.convs].sort((a, b) => b.last_ts - a.last_ts);
  box.innerHTML = sorted.map((c) => {
    const isPub = isGroup(c.chat);
    const pf = isPub ? null : (State.profiles[c.chat] || null); // 单聊对方 (展示用昵称+头像)
    const name = isPub ? (c.name || c.chat.slice(7)) : (pf && pf.nickname ? pf.nickname : c.chat);
    const avImg = pf && avatarUrl(pf.avatar);   // 图
    const av = avImg
      ? `<img src="${esc(avImg)}" alt="" onerror="this.remove()">`
      : `<span class="av-letter">${esc(firstGlyph(name))}</span>`;
    const onlineTag = (!isPub && State.online[c.chat]) ? '🟢' : '';
    const unread = c.unread > 0 ? `<span class="badge">${c.unread}</span>` : '';
    return `<div class="conv" data-chat="${esc(c.chat)}">
      <div class="av">${av}</div>
      <div class="cmain">
        <div class="cname"><span>${esc(name)} ${onlineTag}</span><span class="t">${fmtTs(c.last_ts)}</span></div>
        <div class="clast">${esc((c.last_from ? nickOf(c.last_from) + ': ' : '') + (c.last_body || ''))}</div>
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
  State.chatName = null;
  if (isGroup(chat)) {
    const c = State.convs.find((x) => x.chat === chat);
    if (c && c.name) State.chatName = c.name;
  }
  $('msgs').innerHTML = '';
  updateChatHead();
  renderMsgs();
  // 拉历史 (最近 50 条)
  const r = await req({ type: 'history', chat, limit: 50 }, 'history');
  if (r && (r.items || r.rows)) {
    const arr = State.msgs[chat];
    const list = r.items || r.rows; // 服务器 history 响应字段是 rows (conversations/unread/recent 用 items)
    list.forEach((m) => { if (!arr.some((x) => x.id === m.id)) arr.push({ id: m.id, from: m.from, body: m.body, ts: m.ts, state: m.state }); });
    arr.sort((a, b) => a.id - b.id);
    State.msgs[chat] = arr;
    renderMsgs();
    // 打开会话: 未读清零 + ack 所有我的未读
    ackAllUnread(chat);
    // 预热会话里出现的发送者资料 (群聊昵称/头像)
    const senders = {};
    arr.forEach((mm) => { if (mm.from && mm.from !== State.user) senders[mm.from] = 1; });
    if (!isGroup(chat) && chat !== State.user) senders[chat] = 1;
    warmUsers(Object.keys(senders));
  }
}

function updateChatHead() {
  if (!State.view) return;
  const isPub = isGroup(State.view);
  let name, avRaw;
  if (isPub) { name = State.chatName || State.view.slice(7); avRaw = ''; }
  else { const pf = profileOf(State.view) || {}; name = pf.nickname || State.view; avRaw = pf.avatar; }
  $('chatName').textContent = name;
  const st = isPub ? '群聊' : (State.online[State.view] ? '在线' : '离线');
  $('chatStatus').textContent = st;
  // 头部的群聊/名片入口
  $('grpInfo').classList.toggle('hidden', !isPub);
  $('viewProfile').classList.toggle('hidden', isPub);
  setPfp($('chatHeadAvatar'), avRaw, name);
}

function renderMsgs() {
  const arr = State.msgs[State.view] || [];
  const box = $('msgs');
  if (!arr.length) { box.innerHTML = '<div class="empty">没有消息</div>'; return; }
  box.innerHTML = arr.map((m) => {
    const mine = m.from === State.user;
    const who = isGroup(State.view) && !mine ? `<div class="who">${esc(nickOf(m.from))}</div>` : '';
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
  // 本地回显: 单聊服务器不回显给发送者, 发送后立即本地追加 (微信式: 自己发的立刻可见)
  // 群聊服务端会把消息 deliver 回 sender, 无需本地回显 (避免重复)。
  if (!isGroup(to)) {
    if (!State.msgs[to]) State.msgs[to] = [];
    State.msgs[to].push({ id: -Date.now(), from: State.user, body, ts: Date.now(), state: 'sent' });
    if (State.view === to) renderMsgs();
    refreshConvs();
  }
  inp.value = '';
  inp.focus();
}

// ---------- 登录/注册 ----------
async function tokenLogin() {
  // 断线重连/刷新: 凭 token 恢复会话 (不重输密码, 不存密码)
  // 去重: 已登录(State.user)或已有一次在飞(tokLog)的并发 tokenLogin 一律忽略——
  //  否则多余的一次超时会误把已恢复的会话拉回登录窗。
  if (State._tokLog || State.user) return;
  State._tokLog = true;
  try {
    const r = await req({ type: 'token_login', token: State.token }, 'login_ok', 5000);
    if (!r || r.type === 'error') {
      if (r && r.type === 'error') { logout(); return; }  // 服务端显式拒绝: token 真失效, 登出
      sessionLoss();                     // null = 请求超时/临时网络抖: 不清本地 token, 稍后可重试恢复
      return;
    }
    State.user = r.user;
    State.token = r.token;
    completeLogin();
  } finally { State._tokLog = false; }
}

// 临时无法恢复会话: 只回登录界面, 但保留 localStorage token —— 网络抖动不销毁登录态,
// 下次刷新/重连仍能凭该 token 自动恢复 (仅服务端明确拒绝 token 时才真正登出)。
function sessionLoss() {
  $('app').classList.add('hidden');
  $('auth').classList.remove('hidden');
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
  try {
    localStorage.setItem('wxlike_token', State.token || '');
    localStorage.setItem('wxlike_user', State.user || '');
  } catch (e) { /* 隐私模式等 localStorage 不可用: 仅内存登录态 */ }
  $('auth').classList.add('hidden');
  $('app').classList.remove('hidden');
  $('meUser').textContent = State.user;
  updateMeBar();   // 头像 + 昵称 (含回落用户名)
  refreshConvs();
}

function logout() {
  try { localStorage.removeItem('wxlike_token'); localStorage.removeItem('wxlike_user'); } catch (e) {}
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
    // 本地先插入新群会话 (含 name), 避免 openChat 时 convs 还没刷新拿不到群名
    const key = 'group::' + gid;
    if (!State.convs.some((x) => x.chat === key)) {
      State.convs.unshift({ chat: key, name: name.trim(), last_body: '', last_ts: Date.now(), last_from: '', unread: 0 });
    }
    refreshConvs();
    openChat(key);
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

// ---------- 资料/头像/昵称 (改造新增) ----------
// 取某用户展示昵称
function nickOf(u) {
  if (u === State.user) return State.nick || u;
  const p = State.profiles[u];
  return (p && p.nickname) ? p.nickname : u;
}
// 头像路径归一: 库中 avatar 可能是裸文件名 "a_x.png"(上传落库) 或
// "/uploads/a_x.png"(set_profile/各应答回传); 统一成 <img> 可用的本站路径
function avatarUrl(raw) {
  if (!raw) return '';
  const a = String(raw).trim();
  if (!a) return '';
  if (/^https?:\/\//i.test(a)) return a;   // (预留外链)
  if (a.charAt(0) === '/') return a;        // 已 /uploads/..
  return '/uploads/' + a;                    // 裸文件名 -> 补前缀
}
function firstGlyph(s) {
  const t = String(s == null ? '' : s).trim();
  if (!t) return '?';
  return (Array.from(t)[0] || '?').toUpperCase();
}
// 在 el(如 .avatar div) 里画头像: 有图 -> img(覆盖 textContent 留下安全), 无图 -> 绿调首字
function setPfp(el, raw, glyph) {
  if (!el) return;
  const url = avatarUrl(raw);
  if (url) { el.innerHTML = '<img src="' + esc(url) + '" alt="" onerror="this.remove()">'; return; }
  el.style.background = 'linear-gradient(135deg,#4aa9c9,#07c160)';
  el.style.color = '#fff';
  el.textContent = firstGlyph(glyph || '?');
}
function cacheProfile(u, p) {
  if (!u) return;
  State.profiles[u] = {
    nickname: (p && p.nickname) || '',
    signature: (p && p.signature) || '',
    avatar: (p && p.avatar) || '',
  };
}
function profileOf(u) { return State.profiles[u] || null; }
// 取资料 (缓存命中直接返回; 否则去并发 get_profile, 结果自动进缓存并刷新相关视图)
function hitProfile(u) {
  if (!u) return Promise.resolve(null);
  if (State.profiles[u]) return Promise.resolve(State.profiles[u]);
  if (State.profileQ[u]) return Promise.resolve(null);
  State.profileQ[u] = true;
  return req({ type: 'get_profile', to_user: u }, 'profile', 5000)
    .then((m) => {
      if (m && m.type === 'profile' && m.profile) cacheProfile(m.profile.username || u, m.profile);
      return profileOf(u);
    })
    .catch(() => null)
    .finally(() => { delete State.profileQ[u]; });
}
// 预热若干个展示用用户名资料 (群/会话对方), 全部回来后轻刷视图
function warmUsers(list) {
  (list || []).forEach((u) => {
    if (!u || u === State.user) return;
    if (State.profiles[u] || State.profileQ[u]) return;
    hitProfile(u).then(() => { if (State.view && !isGroup(State.view) && State.view === u) { updateChatHead(); renderMsgs(); } else { renderConvs(); } });
  });
}

// ---- 我的资料 (顶栏 "我" 点击弹出的编辑面板) ----
function openMyProfile() {
  hideModal(); hideDevices(); $('peerModal').classList.add('hidden');
  const nm = $('myName'); if (nm) nm.value = State.nick || '';
  const sg = $('mySig'); if (sg) sg.value = State.sig || '';
  const tip = $('myTip'); if (tip) tip.textContent = '';
  setPfp($('myAvatar'), State.avatar, nickOf(State.user));
  $('myModal').classList.remove('hidden');
}
function closeMyProfile() {
  $('myModal').classList.add('hidden');
  const fi = $('myAvFile'); if (fi) { try { fi.value = ''; } catch (e) {} }
}
// 上传头像: POST /upload?token=<token>, multipart file
async function onAvatarChosen(file) {
  const tip = $('myTip');
  if (!file) return;
  if (tip) { tip.style.color = '#e64340'; tip.textContent = '上传中…'; }
  const fd = new FormData();
  fd.append('file', file);
  try {
    const resp = await fetch('/upload?token=' + encodeURIComponent(State.token || ''), { method: 'POST', body: fd });
    let j = {};
    try { j = await resp.json(); } catch (e) {}
    if (!resp.ok || !j.url) { if (tip) tip.textContent = '上传失败: ' + (resp.status + ''); return; }
    // 服务器已把新头像写进账号; 前端同步自己的头像并刷新展示
    State.avatar = avatarUrl(j.url);
    if (State.user) cacheProfile(State.user, { nickname: State.nick, avatar: State.avatar, signature: State.sig });
    setPfp($('myAvatar'), State.avatar, nickOf(State.user));
    updateMeBar();
    renderConvs(); renderMsgs();
    if (tip) { tip.style.color = '#07c160'; tip.textContent = '头像已上传 ✓ 点"保存"以一并更新昵称/签名'; }
  } catch (e) {
    if (tip) { tip.style.color = '#e64340'; tip.textContent = '上传失败 (网络/格式需 jpg/png/gif)'; }
  }
}
// 保存昵称/签名/头像 (set_profile)
async function saveMyProfile() {
  const tip = $('myTip');
  const nickname = ($('myName').value || '').trim();
  const signature = ($('mySig').value || '').trim();
  if (tip) tip.textContent = '保存中…';
  const r = await req({ type: 'set_profile', nickname, signature, avatar: State.avatar || '' }, 'profile_ok', 5000);
  if (r && r.type === 'profile_ok') {
    if (r.profile) cacheProfile(State.user, r.profile);  // 服务端回显含其落库的 avatar/昵称
    // 用回显(有则)刷新本机自己的资料
    if (r.profile) { State.nick = r.profile.nickname || ''; State.sig = r.profile.signature || ''; State.avatar = r.profile.avatar || ''; }
    if (tip) { tip.style.color = '#07c160'; tip.textContent = '已保存 ✓'; }
    updateMeBar(); renderConvs(); renderMsgs();
  } else if (tip) { tip.style.color = '#e64340'; tip.textContent = '保存失败: ' + ((r && r.code) || '超时'); }
}
// 顶栏 "我" 区渲染: 头像 + 昵称 + 用户名
function updateMeBar() {
  if (!State.user) return;
  const nmEl = $('meName'); if (nmEl) nmEl.textContent = nickOf(State.user);
  const muEl = $('meUser'); if (muEl) muEl.textContent = State.user;
  setPfp($('meAvatar'), State.avatar, nickOf(State.user));
}

// ---- 他人资料卡 (点单聊会话的头部/名片) ----
function renderProfileCard(p) {
  // 兼容 server 应答 {type:'profile', profile:{username,nickname,signature,avatar}}
  const prof = (p && p.profile) ? p.profile : (p || {});
  if (!prof) return;
  const u = prof.username || State.chatPeer || '';
  const nmEl = $('peerName'); if (nmEl) nmEl.textContent = prof.nickname || u;
  const uuEl = $('peerUser'); if (uuEl) uuEl.textContent = u;
  const ssEl = $('peerSig');  if (ssEl) ssEl.textContent = prof.signature || '未填写';
  setPfp($('peerAvatar'), prof.avatar, prof.nickname || u);
  $('peerModal').classList.remove('hidden');
}
async function showPeerCard(user) {
  if (!user) return;
  // 先用已知信息立刻展示
  const p = profileOf(user);
  renderProfileCard(p ? { username: user, nickname: p.nickname || user, signature: p.signature, avatar: p.avatar } : { username: user });
  const got = await hitProfile(user);
  if (got) renderProfileCard(got);
}
function closePeerCard() { $('peerModal').classList.add('hidden'); }

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

// ---- 个人资料 / 头像 / 昵称 (改造新增的绑定) ----
$('meInfo').onclick = openMyProfile;                         // 顶栏头像/昵称 → 我的资料
$('myClose').onclick = closeMyProfile;
$('myMask').onclick = closeMyProfile;
$('mySaveBtn').onclick = () => { saveMyProfile(); };
$('myAvBtn').onclick = () => { const fi = $('myAvFile'); if (fi && fi.click) fi.click(); };
$('myAvFile').addEventListener('change', (e) => { const f = e.target.files && e.target.files[0]; if (f) { onAvatarChosen(f); e.target.value = ''; } });

// ---------- 账号安全: 绑定邮箱 / 修改密码 / 忘记密码 ----------
function refreshEmailRow() {
  const em = State.email || '';
  const inp = $('myEmail'); if (inp) inp.value = em;
  $('myEmailTip').textContent = em ? '已绑定: ' + em : '未绑定邮箱(绑定后可用于找回密码)';
}
$('myEmailBtn').onclick = async () => {
  const em = ($('myEmail').value || '').trim();
  if (!em) { $('myEmailTip').textContent = '请输入邮箱'; return; }
  $('myEmailTip').textContent = '发送中…';
  const r = await req({ type: 'send_email_code', email: em }, 'email_code_sent');
  if (!r || r.type === 'error') {
    $('myEmailTip').textContent = ({ invalid_email: '邮箱格式不正确', email_taken: '该邮箱已被绑定' }[r && r.code] || '发送失败');
    return;
  }
  $('myEmailTip').textContent = '验证码已发送，请查收邮箱';
  $('myCodeRow').classList.remove('hidden');
  $('myCodeRow').style.display = 'flex';
};
$('myCodeBtn').onclick = async () => {
  const em = ($('myEmail').value || '').trim();
  const code = ($('myCode').value || '').trim();
  if (!code) { $('myEmailTip').textContent = '请输入验证码'; return; }
  const r = await req({ type: 'verify_email', email: em, code }, 'email_ok');
  if (!r || r.type === 'error') { $('myEmailTip').textContent = '验证码错误或已过期'; return; }
  State.email = r.email;
  $('myCodeRow').classList.add('hidden');
  refreshEmailRow();
  $('myEmailTip').textContent = '邮箱绑定成功 ✓';
};
$('myPassBtn').onclick = async () => {
  const op = $('myOldPass').value, np = $('myNewPass').value;
  if (!op || !np) { $('myPassTip').textContent = '请填写当前密码与新密码'; return; }
  const r = await req({ type: 'set_password', old_pass: op, new_pass: np }, 'password_ok');
  if (!r || r.type === 'error') {
    $('myPassTip').textContent = ({ wrong_password: '当前密码不正确', bad_request: '新密码不能为空' }[r && r.code] || '修改失败');
    return;
  }
  $('myOldPass').value = ''; $('myNewPass').value = '';
  $('myPassTip').textContent = '密码修改成功 ✓';
};

// 忘记密码弹窗
function openForgot() { $('forgotModal').classList.remove('hidden'); $('fgTip').textContent = ''; }
function closeForgot() { $('forgotModal').classList.add('hidden'); }
$('forgotLink').onclick = openForgot;
$('forgotClose').onclick = closeForgot;
$('forgotMask').onclick = closeForgot;
$('fgSendBtn').onclick = async () => {
  const ident = ($('fgIdent').value || '').trim();
  if (!ident) { $('fgTip').textContent = '请输入用户名或邮箱'; return; }
  $('fgTip').textContent = '发送中…';
  const r = await req({ type: 'request_reset', user: ident }, 'reset_sent');
  if (!r) { $('fgTip').textContent = '网络错误，请重试'; return; }
  $('fgTip').textContent = '若该账号已绑定邮箱，验证码已发送（请查收）';
  $('fgCodeRow').classList.remove('hidden');
};
$('fgResetBtn').onclick = async () => {
  const ident = ($('fgIdent').value || '').trim();
  const code = ($('fgCode').value || '').trim();
  const np = $('fgNewPass').value;
  if (!code || !np) { $('fgTip').textContent = '请填写验证码与新密码'; return; }
  const r = await req({ type: 'reset_password', user: ident, code, new_pass: np }, 'reset_ok');
  if (!r || r.type === 'error') { $('fgTip').textContent = '验证码错误或已过期'; return; }
  $('fgTip').textContent = '密码已重置 ✓ 请返回登录';
  setTimeout(closeForgot, 1200);
};
// 打开我的资料时刷新邮箱显示
const _openMyProfile = openMyProfile;
openMyProfile = function () { _openMyProfile(); State.email = State.email || ''; refreshEmailRow(); };
// 聊天头部左侧 (头像/名字) 在 1:1 会话可点开对方名片
$('chatHeadInfo').onclick = () => { if (State.view && !isGroup(State.view)) showPeerCard(State.view); };
$('viewProfile').onclick = () => { if (State.view && !isGroup(State.view)) showPeerCard(State.view); };
$('peerClose').onclick = closePeerCard;
$('peerMask').onclick = closePeerCard;
// 群/设备弹窗的关闭按钮 (html 里新增的 ×)
$('gClose').onclick = hideModal;
$('devClose').onclick = hideDevices;

// 载入时恢复登录态 (token 持久化在 localStorage): 有 token 则 connect 后自动 tokenLogin
try {
  const t = localStorage.getItem('wxlike_token');
  if (t) State.token = t;
} catch (e) {}
connect();
