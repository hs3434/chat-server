# wxlike-server 架构设计

微信/QQ 式轻量消息服务器（Go 为主实现；Python/Lua 为对比样本）。
面向 772MB 小内存 VPS，几十好友、每秒几条消息的私人场景。

## 目录结构
- `impl_go/` — Go 1.22 (gorilla/websocket + modernc.org/sqlite, 静态单二进制)【主实现】
- `impl_py/` — Python 3.11 + uv (asyncio + websockets)【对比】
- `impl_lua/` — Lua 5.4 (luasocket + luasql + copas + cjson)【探索样本】
- `schema.sql` — SQLite 共享结构
- `tests/` — 端到端 + 可靠性 + 基准

## 核心模型：成员即收 + 离线托管

- **不需要 join**（区别于 XMPP MUC 的 occupant 模型）。
- 群消息投递由 `group_members(gid)` 的成员关系决定——是成员就收。
- 每条消息要么实时投递，要么进入持久队列，**绝不自生自灭**。

## 数据模型（消息本体 + 接收队列分离）

两张核心表，职责分离：

### `messages`（消息本体，一份）
- `id` 主键，**全局单调递增**（AUTOINCREMENT），同时作为**会话内唯一 ID**。
- **单聊**：`gid` 为空，`recipient = 对方`。
- **群聊**：`gid = 'group:<gid>'`，`recipient` 为 NULL。**消息只有一份，所有成员共享同一个 `id`**——这就是"群 = 永久长会话"的体现。

### `msg_state`（接收队列，每接收方一行）
- 主键 `(user, msg_id)`。
- 每个"接收方-消息"一行，承载投递/未读/已读状态机。**这就是真正的"投递队列"**。
- 群消息：给每个成员插一行（`user=成员, msg_id=群消息的同一个 id`），所以**成员间 id 一致**，但**状态各自独立**（未读/已读互不干扰）。
- 单聊：给接收方插一行。

## 消息可靠性协议（全局单调 ID + ack 幂等）

核心是一条**消息状态机**，由 `msg_state` 权威维护：

```
          消息入 messages (生成会话唯一 id)
                    │
                    ▼
        msg_state(user) = pending   ← 投递队列
                    │
   ┌───────────────┴────────────────┐
   │ 尝试投递 (在线)                  │   接收方离线
   ▼                                ▼
客户端 ack_received             留在 pending
   (先接收才能显示)              (登录时重新投)
   │
   ▼
   delivered ← 未读队列
   │
   ▼
客户端 ack_read (用户真正看了)
   │
   ▼
   read ← 未读移除, 只作为历史记录
```

### 关键机制

1. **received 和 read 是两回事**（微信/QQ 也是）：
   - `ack_received`：客户端**接收**这条消息（显示前必须先接收）→ `pending → delivered`（计入未读）
   - `ack_read`：客户端真正**读了** → `delivered → read`（未读移除）
   - **实时推送的消息也一样要走两步**：先 received（显示）再 read（已读）。实时收到 ≠ 已读。

2. **ack 全部幂等**：
   - `AckReceived: UPDATE msg_state SET state='delivered' WHERE user=? AND msg_id=? AND state='pending'`
   - `AckRead: UPDATE msg_state SET state='read' WHERE user=? AND msg_id=? AND state IN ('pending','delivered')`
   - 同一句 SQL 执行任意次，结果一致——重复 ack 不会造成重复移动或错乱。

3. **重新投递**：客户端登录时，服务器取该用户所有 `msg_state.state IN (pending, delivered)` 的消息，补上 `type=msg` + `id` + `state` 推给客户端。客户端按 `id` 去重。

4. **去重与时序**：
   - 数据模型保证服务器按 `id` 升序下发。
   - 客户端维护 `last_seq`（会话内），只接受 `id > last_seq`，`id <= last_seq` 忽略（幂等去重）。

### 为什么用逐条 ack 而不是 ts 游标

| | 全局ID + 逐条 ack（本方案） | ts 单调游标（Prosody mod_readmarks） |
|---|---|---|
| 确认粒度 | 逐条消息 | 只记"已读位置" |
| 未读计数 | 精确到条 | 按游标差估算 |
| 网络开销 | 每条多 1-2 个 ack | 一个游标 |
| 会话唯一 id | 天然支持跨成员/跨端关联 | 无 |
| 适用 | 消息量小、要精确逐条 | 大流量、只求已读位置 |

本项目消息量极小，逐条 ack 更简单精确，且 `messages.id` 天然是会话唯一 ID。

## 存储（冷热两级）

- **内存**：只放连接表（`user → []websocket`）。业务状态全在 SQLite。
- **SQLite**（WAL）：messages + msg_state + 账号 + 群成员。一切持久化落盘。
- **冷热存储决策（明确）**：所有消息即时落库 SQLite，**不做**"内存热层 + 3s 转冷"的两级存储。

  理由（针对本场景）：几十好友、每秒几条消息，消息总量小，直接落盘：
  - 磁盘 IO 完全够（SQLite WAL + 每消息一次 INSERT，<1ms）。
  - 崩溃安全最简单——一切已持久化，无"内存态丢失"。
  - 省掉热层就意味着省掉"喝落盘"的时机问题、内存上限问题、一致性边界。
  - 内存热层是"海量消息 + 冷查询"才需要的优化；本场景引入它只会增加复杂度。
  若未来消息量暴涨（10 万+），再评估：memtable 热层 / 归档分表，届时按真实压力数据决定。

## 协议消息类型

| type | 方向 | 用途 |
|---|---|---|
| `register` | C→S | 注册 |
| `login` | C→S | 登录（触发重新投递）|
| `create_group` | C→S | 建群 |
| `add_member` | C→S | 拉人进群 |
| `msg` | C→S | 发消息（to + is_group + body）|
| `ack_received` | C→S | 确认接收（幂等）|
| `ack_read` | C→S | 确认已读（幂等）|
| `history` | C→S | 拉该会话历史（chat + limit + before_id 翻页）|
| `unread` | C→S | 查未读计数（按会话返回 items:[{chat,count}]）|
| `receipts` | C→S | 查已读回执（chat：单聊=对方状态，群聊=read/total 人数）|
| `login_ok` | S→C | 登录成功 |
| `msg` | S→C | 推送消息（带 id/state/from/body/ts）|
| `history` | S→C | 历史结果（rows）|
| `unread` | S→C | 未读结果（items）|
| `receipts` | S→C | 回执结果（rows）|
| `error` | S→C | 错误 |

### 会话 key（chat 参数）约定

- 单聊：`chat = 对方用户名`（如 `alice`）。
- 群聊：**协议层统一 `chat = 'group::<gid>'`（双冒号）**——`history`/`receipts` 入参、`unread` 返回的 key 都遵循此格式。
- 内部存储层消息本体 `gid = 'group:<gid>'`（单冒号），store 层负责转换。

## 安全边界与已知取舍（设计审查记录）

### 认证模型（当前边界）
- 登录即建立 WS 连接，`route` 以 `user==""` 判断未登录——**无 token/session 过期机制**。
- 适用性：私人服务器（仅好友使用）+ 全站 TLS（nginx 强制 HTTPS）。风险 = 拿到 wss 端口的攻击者需要密码才能建立会话。
- 若未来暴露公网匿名访问，**必须**加 token：login 返回随机 token，后续请求带 token，服务器侧维护 token→user 映射 + 过期。

### 已读回执（简化版）
- 单聊：receipts 返回"我发出的消息 + 对方 read 状态"（查对方 msg_state）。
- 群聊：receipts 返回"我发出的消息 + read/total 人数"（**不是微信的已读详情**——详情需按成员展开，当前以人数代替，够用）。
- 注意：`total` = 该消息的 msg_state 行数 = **其他成员数**（sender 不入队）。新成员不讨老消息，故老消息 total 不含新成员——语义正确。

### 多端与重投
- 多端同账号：离线重投对**每个登录的连接**各发一份，客户端靠 `last_seq`/id 去重（服务器保留 pending/delivered 直到 ack_read）。
- 语义与微信一致：新设备登录会收到旧未读。

### 冷热存储（明确决策）
- 见上文"存储（冷热两级）"：所有消息即时落库 SQLite，不做内存热层（本场景消息量小，直接落盘更简单可靠）。
- 未来若消息量暴涨（10 万+），再评估 memtable 热层 / 归档分表。

### 会话 key 协议约定（第二轮审查修复）
- 单聊 `chat=对方用户名`；群聊 `chat='group::<gid>'`（双冒号）——**unread/history/receipts 三处统一**，内部存储 gid 单冒号由 store 层转换。

## 第三轮审查补充 (2026-08-31)

### 并发正确性
- SQLite 配置：`SetMaxOpenConns(1)`（单连接串行化所有 DB 操作，根除 `database is locked`）+ `PRAGMA busy_timeout=5000`。
- 适用性论证：私人小规模每秒几条消息，单连接吞吐完全够；换来零锁冲突、零 busy 错误。
- `test_concurrency.py` 验证：多用户并发写 90/90 不丢、id 唯一、并发 ack_read 全生效、群并发 60/60。

### 群生命周期（微信语义）
- `remove_member`：仅群主（`not_owner` 拒非群主）；**同时清理被踢者在本群 msg_state**（不再收老消息）。
- `leave_group`：自己退群；**同时清理自己的群 msg_state**。
- `dissolve_group`：仅群主；**清空全群 msg_state** + 删群 + 删成员。
- `history` 群聊分支新增校验：**群不存在或非成员 → `not_member`**（退群/被踢/解散后看不到该群历史，微信语义）。

### token 认证
- `login` 成功返回 `token`（crypto/rand 16 字节 hex）。
- 未登录连接可凭 `token` 字段执行任意业务请求（history/unread/ack 等）；token 无效 → `unauthorized`；无 token → 请求被忽略。
- tokens 内存 map（token→user），当前无过期（每次 login 新 token 可覆盖；重启清空——客户端需重新 login）。已记录为已知边界。

### 测试矩阵（9 套件）
e2e / reliability / edge / persistence / permission / features / concurrency / grouplifecycle / auth —— 全部 ALL-PASS。

## 第四轮审查补充 (2026-08-31)

### 心跳/死连接检测
- 服务器每 54s 发 Ping，`ReadDeadline=60s`；客户端 Pong 刷新 deadline。60s 无帧（含 Pong）→ 判死连接 → 从 online 移除 + 未读留待下次登录重投。
- 作用：断网客户端不泄漏连接；deliver 不再对死连接反复写失败。

### 审计（原始需求）
- `audit_log` 表：登录成功/失败 + 时间 + 来源 IP。`AuditLogin` 写入，`AuditLogs` 可导出。满足"服务器对用户完全控制 + 审计需求"。

### 优雅关闭
- SIGTERM/SIGINT → `closeAll()`（主动断所有 WS 连接，handler 立即返回）→ `http.Shutdown`（5s 超时）→ `store.Close()`。
- 修复：run_all.sh 的 restart_clean 改为"等旧进程退出 + 等端口就绪"（优雅关闭竞态 → e2e/reliability/features/concurrency 曾 ConnectionRefused，修复后 10 套件全绿）。

### 每连接写锁（第四轮审查发现的真实并发 bug）
- **问题**：deliver/send/心跳 Ping 可能并发写同一 WS 连接（A 发消息给 B 走 A 的 goroutine 写 B 的连接，B 自己的心跳 goroutine 也在写）→ gorilla/websocket 并发写不安全（坏帧/panic）。
- **修复**：Server 级 `wsmu map[*websocket.Conn]*sync.Mutex`（每连接写锁），所有写路径统一走 `writeJSON`/`writeControl`；removeConn 时清理。

## 第五轮审查补充 (2026-08-31): 限流/防刷

### 必要性（按威胁分级）
- 公网 wss 对任何人开放；无防护的注册洪水/登录暴破/消息滥用会拖垮 1核772MB VPS。
- 微信语义参考：「发送过于频繁」提示 + 登录异常检测。

### 设计（全内存态, 重启丢失可接受; 与可靠性铁律无冲突）
| 限流器 | 参数 | 错误码 |
|---|---|---|
| 消息令牌桶 | 每用户 cap=10, refill=5/s | `rate_limited` + retry_after |
| 登录失败锁 | 连续 5 次失败锁 5 分钟 (成功后 Reset) | `login_locked` + retry_after |
| 注册 IP 限流 | 每 IP 10 分钟 3 次 | `reg_limited` |

- 启动参数：`--msg-rate 5`（0=关闭）、`--reg-limit 3`（0=关闭）。生产默认开启；批量测试默认 `--reg-limit 0`（测试注册 >3 用户），`concurrency/grouplifecycle` 额外 `--msg-rate 0`；`ratelimit` 测试单独 `--reg-limit 3` 验证。
- 后台清扫 goroutine（每分钟）：清理登录锁过期条目、注册窗口外计数、1h 未用的消息桶（防内存泄漏）。
- 全部走 `sendError` 统一错误码；协议向后兼容（前端只需处理 3 种新错误码）。
- 明确不做：内容审核/敏感词（私人圈子不适用）、Redis 等外部限流（本地 map 足够）。

### 测试
`test_ratelimit.py`：注册第 4 次 reg_limited、前 3 次成功、消息突发 15 条触发 rate_limited + retry_after、登录 5 次失败锁（正确密码也拒）、新用户不受他人限流影响。

## 第六轮审查补充 (2026-08-31): 已读回执推送 / 消息长度 / 群转让

### 已读回执实时推送 (peer_ack)
- 之前 `receipts` 是纯拉取式; 本轮补微信语义的实时推送。
- ack_read 时: 查消息 sender+gid (`MsgSender`) → 单聊推 `peer_ack{id, reader}` 给 sender; 群聊剥离 gid 前缀查成员, 推 `peer_ack{id, reader, gid:"group::xx", read, total}` 给所有在线成员 (含 sender)。
- **协议 bug 修复**: messages.gid 存 `group:xxx`(单冒号), 直接传给 GroupMembers(要纯 gid) 查不到 → 须 `strings.TrimPrefix(gid,"group:")` 再查; peer_ack 对外统一 `group::` 双冒号 (与 history/unread 一致的协议约定)。
- `sendToUser(user, obj)` 辅助: 给用户所有在线连接推送。

### 消息长度限制
- 单条 body > 10KB → `msg_too_large` (微信 ~4KB, 给富余; 防超长消息拖垮内存)。
- 放在 msg 分支开头, 与限流并列。

### 群转让 transfer_owner
- 仅群主 (`IsGroupOwner`) → 目标须是成员 (`IsGroupMember`) → `UPDATE groups SET owner=to WHERE gid AND owner=from`。
- 错误码: `group_not_found` / `not_owner` / `not_member`。
- 语义: 转让后旧群主不再能踢人 (IsGroupOwner 已变)。

### 测试
`test_realtime.py`: 单聊 peer_ack(reader=B)、群聊 peer_ack(gid=group::xx+read+total)、长消息 msg_too_large、转让 group_ok、转让后旧群主 not_owner。

## 第七轮审查补充 (2026-08-31): 会话列表 / 断线恢复

### conversations 会话列表接口
- 前端会话列表 = 每会话 (单聊对方 / 群聊 group::gid) 的最后一条消息预览 + 时间 + 未读数, 按最近降序 (微信语义)。
- SQL: 窗口函数 ROW_NUMBER() OVER (PARTITION BY 会话key ORDER BY id DESC) 取每会话最后一条; 会话范围 = 我参与的 (单聊 sender/recipient=我 + 群聊我在 group_members 或 msg_state)。
- **单连接死锁陷阱**: db.SetMaxOpenConns(1) 下, 主查询 rows 未关闭时不能再发 QueryRow (等 rows.Close 死锁) → 必须先读完所有行 + rows.Close(), 再统一查 unread。
- **切片越界陷阱**: Chat[:7] 判群前缀必须 len(Chat)>=8 (单聊用户名字符串可能 <7 直接 panic)。

### recent 断线恢复接口
- recent {count}: 该用户最近 N 条跨会话消息 (按全 id 降序), 客户端按全局单调 id 去重, 缺的补拉。
- 范围同 conversations (我参与的), 复用同样 WHERE 条件; count 上限 200。

### 测试
`test_conversations.py`: 空列表 / 单聊会话 / 群聊会话 / 降序排序 / B 视角 unread / ack_read 清零 / recent 降序 + count。

## 第八轮审查补充 (2026-08-31): 数据导出 / 在线状态

### 数据导出 (原始需求闭环: 管理员可导出)
- HTTP 端点 `GET /export?token=<login_token>` (非 WS)。
- 鉴权: `--admin <user>` 启动参数指定管理员; 只有管理员用户的 token 才能导出 (否则 401)。
- 返回全量 JSON: accounts/groups/group_members/messages/msg_state/audit_log 六表全量 + exported_at 时间戳。
- Content-Disposition: attachment (浏览器下载)。
- 消息全量存档 (messages 表) + 登录日志 (audit_log) + 导出 = 原始"服务器对用户完全控制, 满足审计"三条全部闭环。

### 在线状态 (presence)
- `presence` 动作: `{users:[...]}` -> `{type:"presence", online:{user:bool}}` 批量查询。
- `presence_evt` 事件: 用户首连接上线/最后连接断开时, 推给与其有往来的人 (Peers = 单聊互发过 + 同群成员)。
- 多端语义: 2 个连接在线时关 1 个不推下线 (只推 0<->1 的跳变)。
- `Peers(user)`: 单聊 (sender/recipient=user) UNION 同群成员 (group_members 自连接)。

### 测试
`test_export.py`: 无 token 401 / 非管理员 401 / 管理员 200+全表含消息 / presence 批量查询 / 好友上线 presence_evt / 好友下线 presence_evt。

## 第九轮审查补充 (2026-08-31): 前端对接层 (手机浏览器直接可用)

### web/ 静态前端 (零依赖原生 JS, 单二进制部署)
- `--web <dir>` 启动参数: 静态文件 serve 到 `/app/` (FileServer + StripPrefix, 与 WS 同端口)。
- 浏览器访问 `http://host:8081/app/` 打开前端 (自动 index.html)。
- index.html: 登录/注册页 + 会话列表侧栏 + 聊天区 (手机优先 max-width 420 + 移动端全屏 slide)。
- app.js: WS 直连 + 完整协议封装:
  - 登录/注册 (login/register) → 会话列表 (conversations + presence 批量) → 聊天 (history/发送 msg)
  - 实时推送处理: msg (去重 seen Set + 会话更新) / peer_ack (已读回执) / presence_evt (在线状态)
  - 打开会话: ack 所有未读 (ack_received + ack_read) → 未读清零
  - 断线重连: 指数退避 (1s..10s) + **token_login** (凭 token 恢复会话, 不存密码)

### token_login 动作 (本轮新增)
- 断线重连/多端恢复: `{type:"token_login", token}` -> 校验 token -> addConn + 重投未读 + 返回 login_ok (原 token 继续有效)。
- 前端不存密码: 重连只靠内存中的 token; token 失效则回登录页。
- 错误码: `unauthorized` (token 无效)。

### 测试
`test_frontend_protocol.py`: 登录拿 token / conversations 含会话 / 对端登录收未读 / token_login 重连成功。

## 第十轮审查补充 (2026-08-31): 群管理前端 UI + group_members 动作

### group_members 动作 (后端新增)
- `{type:"group_members", gid:"group::<gid>"}` -> `{members:[...], owner, name}` 仅成员可查 (非成员 not_member / 不存在 group_not_found)。
- 兼容双冒号/单冒号前缀: `TrimPrefix(TrimPrefix(gid,"group::"),"group:")`。
- `GroupOwner(gid)` store 方法补充。

### 前端群管理 UI (web/)
- 会话列表顶部 "+ 建群" (prompt 输群名 -> create_group -> 自动打开新群会话)。
- 群会话头部 "群信息" 按钮 (仅群聊显示) -> 群信息 modal:
  - 成员列表 (群主标记 + 自己高亮)
  - 加人 (输入用户名 -> add_member)
  - 转让群主 (transfer_owner)
  - 踢人 (remove_member)
  - 退群 (leave_group) / 解散群 (dissolve_group, 需确认)
- 权限: 仅群主显示管理按钮 (加人/转让/踢人/解散); 退群所有人可见。

### 测试
`test_group_members.py`: 群主查成员 (owner+name+members) / 成员可查 / 非成员 not_member / 不存在群 group_not_found / 踢人后列表更新。

## 第十一轮审查补充 (2026-08-31): 设备会话管理 (多端)

### 后端
- ConnMeta (sid/ip/since) 存于 Server.devices[conn], login/token_login 成功后 registerDevice(ws, remote)。
- `sessions` 动作 -> {devices:[{sid,ip,since}]} (只能列自己的在线设备)。
- `kick` 动作 {sid} -> 踢掉我的指定设备: 该 conn 收 `kicked` 事件后 Close; 找不到 kick_ok 不返回而是 not_found。
- 新设备登录: notifyNewDevice 给该用户其他在线 conn 推 `device_evt {sid,ip,since}` (微信"账号在其他设备登录")。
- removeConn 清理 devices[conn]。多端语义类 presence: 新增 dev 推 evt, 不断老连接 (微信允许多端在线)。
- 只踢自己账号: kick 校验 devices[sid] 属于 online[user] (用户拿不到别人 sid)。

### 前端
- 顶栏 "设备" 按钮 -> 设备 modal (IP + 登录时间列表), 每台可 "下线" (kick)。
- 收 `device_evt`: toast "你的账号在新设备登录"; 收 `kicked`: alert + 自动 logout。

### 测试
`test_devices.py`: 单设备 sessions / 双设备 sessions / 新设备登录旧设备收 device_evt / kick -> 被踢收 kicked + 连接断 / kick 后 sessions 只剩在线的。
