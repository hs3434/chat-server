package main

import (
	"database/sql"
	"fmt"
)

// ---------- 数据库 (消息本体 + 接收队列分离) ----------

const (
	StatePending   = "pending"
	StateDelivered = "delivered"
	StateRead      = "read"
)

// MsgRow 是返回给客户端的"一条会话消息"（消息本体 + 当前用户的状态）
type MsgRow struct {
	ID     int64  `json:"id"`
	Gid    string `json:"gid,omitempty"`
	Sender string `json:"from"`
	Body   string `json:"body"`
	Ts     int64  `json:"ts"`
	State  string `json:"state"`
}

// SaveSingle 存一条单聊消息本体 (recipient = 对方), 返回消息 id
func (s *Store) SaveSingle(from, to, body string, ts int64) (int64, error) {
	res, err := s.db.Exec(
		"INSERT INTO messages(gid,sender,recipient,body,ts) VALUES(NULL,?,?,?,?)",
		from, to, body, ts)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// SaveGroup 存一条群消息本体 (recipient=NULL), 返回消息 id
func (s *Store) SaveGroup(gid, from, body string, ts int64) (int64, error) {
	res, err := s.db.Exec(
		"INSERT INTO messages(gid,sender,recipient,body,ts) VALUES(?,?,NULL,?,?)",
		gid, from, body, ts)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// Enqueue 为某接收方建立投递队列条目 (state=pending)。群聊对每个成员各插一行
func (s *Store) Enqueue(user string, msgID int64) {
	s.db.Exec(
		"INSERT OR IGNORE INTO msg_state(user,msg_id,state) VALUES(?,?,?)",
		user, msgID, StatePending)
}

// AckReceived 幂等: pending -> delivered (客户端已接收, 进入未读队列)
func (s *Store) AckReceived(user string, msgID int64) bool {
	_, err := s.db.Exec(
		"UPDATE msg_state SET state=? WHERE user=? AND msg_id=? AND state=?",
		StateDelivered, user, msgID, StatePending)
	return err == nil
}

// AckRead 幂等: delivered -> read (客户端已读, 从未读移除)
// MsgSender 查某消息的 sender + gid (已读回执推送用)
func (s *Store) MsgSender(msgID int64) (sender, gid string) {
	var g sql.NullString
	err := s.db.QueryRow("SELECT sender, gid FROM messages WHERE id=?", msgID).Scan(&sender, &g)
	if err != nil {
		return "", ""
	}
	return sender, g.String
}

// GroupReadCount 群消息已读数 (除 sender 外已 read 的成员数)
func (s *Store) GroupReadCount(msgID int64) int {
	var n int
	s.db.QueryRow("SELECT COUNT(*) FROM msg_state WHERE msg_id=? AND state=?", msgID, StateRead).Scan(&n)
	return n
}

func (s *Store) AckRead(user string, msgID int64) bool {
	_, err := s.db.Exec(
		"UPDATE msg_state SET state=? WHERE user=? AND msg_id=? AND state IN (?,?)",
		StateRead, user, msgID, StatePending, StateDelivered)
	return err == nil
}

// Undelivered: 该用户所有未读消息 (pending+delivered), 按 id 升序 (登录重新投递用)
func (s *Store) Undelivered(user string) []MsgRow {
	rows, err := s.db.Query(`
		SELECT m.id, m.gid, m.sender, m.body, m.ts, st.state
		FROM msg_state st JOIN messages m ON m.id = st.msg_id
		WHERE st.user=? AND st.state IN (?,?)
		ORDER BY m.id`, user, StatePending, StateDelivered)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []MsgRow
	for rows.Next() {
		var m MsgRow
		var gid sql.NullString
		rows.Scan(&m.ID, &gid, &m.Sender, &m.Body, &m.Ts, &m.State)
		m.Gid = gid.String
		out = append(out, m)
	}
	return out
}

// UnreadCounts 该用户所有未读 (pending+delivered) 按会话分组计数 (badge 用)
// 会话 key: 群聊 = group::<gid>, 单聊 = 对方 (我收到的 sender / 我发出的 recipient)
// 单聊会话 key 统一用"对方"：收到的消息 sender=对方, 我发的消息 recipient=对方
func (s *Store) UnreadCounts(user string) map[string]int {
	out := map[string]int{}
	rows, err := s.db.Query(`
		SELECT m.gid, m.sender, m.recipient, COUNT(*)
		FROM msg_state st JOIN messages m ON m.id = st.msg_id
		WHERE st.user=? AND st.state IN (?,?)
		GROUP BY m.gid, m.sender, m.recipient`, user, StatePending, StateDelivered)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var gid, sender, recipient sql.NullString
		var n int
		if err := rows.Scan(&gid, &sender, &recipient, &n); err != nil {
			continue
		}
		if gid.Valid && gid.String != "" {
			// 对外协议统一用 'group::<gid>' (与 history/receipts 入参一致)
			out["group::"+gid.String[6:]] += n
		} else {
			// 单聊: 会话对方 = 我收到的 sender (他人→我) 或 我发往的 recipient (我→他人)
			other := sender.String
			if sender.String == user {
				other = recipient.String
			}
			if other != "" {
				out[other] += n
			}
		}
	}
	return out
}

// ConversationRow 会话列表一行
type ConversationRow struct {
	Chat     string `json:"chat"`      // 单聊=对方 user; 群聊=group::<gid>
	LastBody string `json:"last_body"` // 最后一条消息预览
	LastTs   int64  `json:"last_ts"`   // 最后一条时间 (降序用)
	LastFrom string `json:"last_from"` // 最后一条 sender
	Unread   int    `json:"unread"`    // 未读计数
}

// Conversations 该用户所有会话列表 (七微: 每会话最后一条消息 + 未读数, 按 last_ts 降序)
// 会话范围: 我参与的 = 单聊(我是 sender 或 recipient) + 群聊(我在 group_members 或 msg_state 有我)
func (s *Store) Conversations(user string) []ConversationRow {
	// 每会话最后一条消息: 用窗口函数取最大 id 行
	rows, err := s.db.Query(`
		SELECT key, last_id, body, ts, sender FROM (
			SELECT CASE
					WHEN m.gid IS NOT NULL AND m.gid != '' THEN 'group::' || substr(m.gid, 7)
					ELSE CASE WHEN m.sender = ? THEN m.recipient ELSE m.sender END
				END AS key,
				m.id AS last_id,
				m.body, m.ts, m.sender,
				ROW_NUMBER() OVER (PARTITION BY CASE
					WHEN m.gid IS NOT NULL AND m.gid != '' THEN 'group::' || substr(m.gid, 7)
					ELSE CASE WHEN m.sender = ? THEN m.recipient ELSE m.sender END
				END ORDER BY m.id DESC) AS rn
			FROM messages m
			WHERE m.sender = ? OR m.recipient = ?
			   OR m.gid IN (SELECT 'group:' || gid FROM group_members WHERE user = ?)
			   OR m.id IN (SELECT msg_id FROM msg_state WHERE user = ?)
		) WHERE rn = 1 AND key IS NOT NULL AND key != ''
		ORDER BY last_id DESC`, user, user, user, user, user, user)
	if err != nil {
		return nil
	}
	// 先把主查询所有行读进内存并关闭 rows (单连接串行化: 不关 rows 不能发第二个查询)
	var out []ConversationRow
	for rows.Next() {
		var r ConversationRow
		var lastID int64
		if err := rows.Scan(&r.Chat, &lastID, &r.LastBody, &r.LastTs, &r.LastFrom); err != nil {
			continue
		}
		out = append(out, r)
	}
	rows.Close()

	// 再统一查每会话未读数 (此时主查询已关)
	for i := range out {
		r := &out[i]
		var n int
		if len(r.Chat) >= 8 && r.Chat[:7] == "group::" {
			s.db.QueryRow("SELECT COUNT(*) FROM msg_state st JOIN messages m ON m.id=st.msg_id WHERE st.user=? AND st.state IN (?,?) AND m.gid=?", user, StatePending, StateDelivered, "group:"+r.Chat[7:]).Scan(&n)
		} else {
			s.db.QueryRow("SELECT COUNT(*) FROM msg_state st JOIN messages m ON m.id=st.msg_id WHERE st.user=? AND st.state IN (?,?) AND ((m.sender=? AND m.recipient=?) OR (m.sender=? AND m.recipient=?))", user, StatePending, StateDelivered, r.Chat, user, user, r.Chat).Scan(&n)
		}
		r.Unread = n
	}
	return out
}

// Receipts 已读回执: 查询"我发出的消息, 对方是否已读"
// 单聊: 返回我与 chatKey 的往来消息, state 取对方(chatKey)的 msg_state
//
//	我发的消息 -> 看对方 read 没有; 对方发的 -> 看我 read 没有 (COALESCE read=已读)
//
// 群聊: chatKey = 'group::<gid>', 返回我发的群消息 + 已读人数 "read/total"
func (s *Store) Receipts(user, chatKey string) []MsgRow {
	if len(chatKey) >= 7 && chatKey[:7] == "group::" {
		gid := "group:" + chatKey[7:]
		rows, err := s.db.Query(`
			SELECT m.id, m.gid, m.sender, m.body, m.ts,
			       (SELECT COUNT(*) FROM msg_state st2 WHERE st2.msg_id=m.id AND st2.state='read'),
			       (SELECT COUNT(*) FROM msg_state st3 WHERE st3.msg_id=m.id)
			FROM messages m
			WHERE m.gid=? AND m.sender=?
			ORDER BY m.id DESC LIMIT 500`, gid, user)
		if err != nil {
			return nil
		}
		defer rows.Close()
		var out []MsgRow
		for rows.Next() {
			var m MsgRow
			var gidv sql.NullString
			var rc, tc int
			if err := rows.Scan(&m.ID, &gidv, &m.Sender, &m.Body, &m.Ts, &rc, &tc); err != nil {
				continue
			}
			m.Gid = gidv.String
			m.State = fmt.Sprintf("%d/%d", rc, tc)
			out = append(out, m)
		}
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
		return out
	}
	// 单聊: state 取"对方"的 msg_state (看对方是否已读我发的)
	rows, err := s.db.Query(`
		SELECT m.id, m.gid, m.sender, m.body, m.ts,
		       COALESCE(peer.state, 'read')
		FROM messages m
		LEFT JOIN msg_state peer ON peer.msg_id = m.id AND peer.user=?
		WHERE (m.recipient=? AND m.sender=?)
		   OR (m.recipient=? AND m.sender=?)
		ORDER BY m.id DESC LIMIT 500`,
		chatKey, user, chatKey, chatKey, user)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []MsgRow
	for rows.Next() {
		var m MsgRow
		var gidv sql.NullString
		if err := rows.Scan(&m.ID, &gidv, &m.Sender, &m.Body, &m.Ts, &m.State); err != nil {
			continue
		}
		m.Gid = gidv.String
		out = append(out, m)
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// History 某用户某会话的消息记录 (含 read), 按 id 倒序取 limit 再正序返回
// chatKey: 单聊传对方用户名 (群聊传 'group:<gid>')
// Recent 该用户最近 N 条消息 (跨会话, 按 id 降序) —— 断线重连增量恢复
// 客户端已有消息 id (全局单调), 新收的按 id 去重即可; 漏掉的以 recent 补齐
func (s *Store) Recent(user string, limit int) []MsgRow {
	rows, err := s.db.Query(`
		SELECT m.id, m.gid, m.sender, m.body, m.ts, COALESCE(st.state, 'read')
		FROM messages m
		LEFT JOIN msg_state st ON st.msg_id = m.id AND st.user=?
		WHERE m.sender = ? OR m.recipient = ?
		   OR m.gid IN (SELECT 'group:' || gid FROM group_members WHERE user = ?)
		   OR m.id IN (SELECT msg_id FROM msg_state WHERE user = ?)
		ORDER BY m.id DESC LIMIT ?`, user, user, user, user, user, limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []MsgRow
	for rows.Next() {
		var m MsgRow
		var gid sql.NullString
		if err := rows.Scan(&m.ID, &gid, &m.Sender, &m.Body, &m.Ts, &m.State); err != nil {
			continue
		}
		m.Gid = gid.String
		out = append(out, m)
	}
	return out
}

func (s *Store) History(user, chatKey string, limit int, beforeID int64) []MsgRow {
	var rows *sql.Rows
	var err error
	if len(chatKey) >= 7 && chatKey[:7] == "group::" {
		// 群聊: chatKey = 'group::<gid>', 群消息本体 gid 存 'group:<gid>'
		gid := "group:" + chatKey[7:]
		rows, err = s.db.Query(`
			SELECT m.id, m.gid, m.sender, m.body, m.ts, COALESCE(st.state, 'read')
			FROM messages m LEFT JOIN msg_state st ON st.msg_id = m.id AND st.user=?
			WHERE m.gid=? AND (?=0 OR m.id<?) ORDER BY m.id DESC LIMIT ?`, user, gid, beforeID, beforeID, limit)
	} else {
		// 单聊: 该用户与对方的往来消息
		//  对方发给我的: recipient=user AND sender=chatKey
		//  我发给对方的: recipient=chatKey AND sender=user
		rows, err = s.db.Query(`
			SELECT m.id, m.gid, m.sender, m.body, m.ts, COALESCE(st.state, 'read')
			FROM messages m LEFT JOIN msg_state st ON st.msg_id = m.id AND st.user=?
			WHERE ((m.recipient=? AND m.sender=?)
			   OR (m.recipient=? AND m.sender=?))
			AND (?=0 OR m.id<?)
			ORDER BY m.id DESC LIMIT ?`,
			user, user, chatKey, chatKey, user, beforeID, beforeID, limit)
	}
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []MsgRow
	for rows.Next() {
		var m MsgRow
		var gid sql.NullString
		rows.Scan(&m.ID, &gid, &m.Sender, &m.Body, &m.Ts, &m.State)
		m.Gid = gid.String
		out = append(out, m)
	}
	// reverse
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}
