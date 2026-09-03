-- wxlike-server 共享数据库结构（三语言实现通用）
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS accounts (
  username TEXT PRIMARY KEY,
  password TEXT NOT NULL,
  created_at INTEGER DEFAULT (unixepoch())
);

CREATE TABLE IF NOT EXISTS groups (
  gid TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  owner TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS group_members (
  gid TEXT NOT NULL,
  user TEXT NOT NULL,
  PRIMARY KEY (gid, user)
);

-- ===== 消息本体 =====
-- id 在"会话"中唯一。群 = 永久长会话：
--   单聊: gid 为空, recipient = 唯一接收方
--   群聊: gid = 'group:<gid>', recipient 为 NULL (消息只有一份, 所有成员共享同一个 id)
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,     -- 会话内唯一 + 全局单调 (ack/seq 用)
  gid TEXT,                                  -- 单聊为空, 群聊 = 'group:<gid>'
  sender TEXT NOT NULL,
  recipient TEXT,                            -- 单聊: 对方; 群聊: NULL
  body TEXT NOT NULL,
  ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_msg_gid ON messages(gid, id);
CREATE INDEX IF NOT EXISTS idx_msg_single ON messages(recipient, id);

-- ===== 接收队列 =====
-- 每个"接收方-消息"一行, 承载投递/未读/已读状态机。这是真正的"投递队列"。
-- state: pending(投递队列) -> delivered(未读队列) -> read(已读, 移除)
-- 群消息: 每个成员一行, 但都用同一个 messages.id -> 成员间 id 一致
CREATE TABLE IF NOT EXISTS msg_state (
  user TEXT NOT NULL,
  msg_id INTEGER NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending',
  PRIMARY KEY (user, msg_id)
);
CREATE INDEX IF NOT EXISTS idx_msgstate_user ON msg_state(user, state, msg_id);

-- ===== 审计日志 =====
-- 原始需求: 服务器对用户完全控制, 满足审计需求 (登录日志/消息全量存档/管理员可导出)
-- 记录每次登录尝试 (成功/失败) + 时间 + 来源 IP
CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL DEFAULT (datetime('now')),
  user TEXT NOT NULL,
  status TEXT NOT NULL,          -- success / fail
  remote TEXT                    -- 来源 IP (可空)
);
CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user, ts);
