# chat-server

自托管轻量聊天服务器（Go 实现）。单二进制 + SQLite，零依赖，手机浏览器直接可用。
仅供私人小团体（几十人内）使用：服务器对用户完全控制，满足审计需求。

## 特性

- **消息**：单聊 / 群聊、未读数、历史分页、已读回执（实时推送）、会话列表、断线恢复
- **群管理**：建群 / 加人 / 踢人 / 退群 / 解散 / 转让群主（微信群权限语义）
- **安全**：token 认证、消息限流、登录失败锁、注册 IP 限流、消息长度限制、心跳检测、审计日志、管理员导出
- **多端**：同一账号多设备在线、设备列表管理、踢下线、新设备登录提醒
- **在线状态**：好友上下线实时通知（presence）
- **前端**：零依赖原生 JS（web/），手机优先，断线自动重连（token 恢复会话）

## 快速开始

```bash
# 编译（需 Go 1.21+）
cd impl_go && go build -o ../bin/wxlike-go .

# 启动（单二进制部署，同端口提供 WS 协议 + 前端静态文件）
bin/wxlike-go --port 8081 --dir . --admin <管理员用户名> --web web

# 浏览器访问
#   http://<host>:8081/app/   前端界面
#   ws://<host>:8081/         WebSocket 协议
```

## 协议

WebSocket JSON 协议，动作动作见 `impl_go/main.go`（route）。
主要动作：`login` / `register` / `token_login` / `msg` / `history` / `unread` /
`receipts` / `ack_received` / `ack_read` / `conversations` / `recent` / `presence` /
`create_group` / `add_member` / `remove_member` / `leave_group` / `dissolve_group` /
`transfer_owner` / `group_members` / `sessions` / `kick`。

## 数据存储

- SQLite 单文件（`wxlike_go.db`），现代 WAL 模式，单连接串行化。
- 6 张表：accounts / groups / group_members / messages / msg_state / audit_log。
- **投递队列 = msg_state 表**：`pending -> delivered -> read`，离线消息落库，重启不丢。
- 消息本体与投递状态分离；群消息只存一份，成员共享同一 id。

## 管理员导出

```bash
curl -o export.json "http://<host>:8081/export?token=<管理员登录token>"
```

返回全量 JSON（accounts/groups/group_members/messages/msg_state/audit_log）。

## 测试

```bash
bash tests/run_all.sh <port>   # 17 个测试套件（e2e/可靠性/并发/群生命周期/限流/设备管理等）
```

## License

AGPL-3.0