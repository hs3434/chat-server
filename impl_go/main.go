// wxlike-server Go 实现
// 微信/QQ式轻量消息服务器：单聊/群聊/离线托管/冷热存储
// 消息可靠性: 会话内唯一ID + ack 幂等 (msg_state: pending -> delivered -> read)
// 依赖: gorilla/websocket + modernc.org/sqlite (纯Go, 静态编译)
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	_ "modernc.org/sqlite"
)

// ---------- 数据库 (账号/群成员; 消息见 store_msgstate.go) ----------

type Store struct {
	db *sql.DB
}

// ExportData 全量导出 (原始需求: 管理员可导出)
// 返回 accounts/groups/members/messages/msg_state/audit_log 全表
func (s *Store) ExportData() map[string]interface{} {
	out := map[string]interface{}{}
	tables := []string{"accounts", "groups", "group_members", "messages", "msg_state", "audit_log"}
	for _, t := range tables {
		rows, err := s.db.Query("SELECT * FROM " + t)
		if err != nil {
			out[t] = []interface{}{}
			continue
		}
		cols, _ := rows.Columns()
		var items []map[string]interface{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			rows.Scan(ptrs...)
			m := map[string]interface{}{}
			for i, c := range cols {
				b, _ := vals[i].([]byte)
				if b != nil {
					m[c] = string(b)
				} else if vals[i] != nil {
					m[c] = vals[i]
				} else {
					m[c] = nil
				}
			}
			items = append(items, m)
		}
		rows.Close()
		out[t] = items
	}
	out["exported_at"] = time.Now().Format(time.RFC3339)
	return out
}

func NewStore(dbPath, schemaPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	// SQLite 并发写安全配置:
	//  - SetMaxOpenConns(1): 单连接串行化所有 DB 操作, 根除 "database is locked"
	//    (私人小规模每秒几条消息, 单连接吞吐完全够; 换来零锁冲突/零 busy 错误)
	//  - busy_timeout: 即使有其他进程短暂占用也等待而非立刻报错
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		return nil, err
	}
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(string(schema)); err != nil {
		return nil, err
	}
	// 迁移: 老库(accounts 无个人资料列)加列。CREATE TABLE IF NOT EXISTS 对已有表不生效,
	// 需逐个检查列存在再 ALTER (幂等, 兼容已部署的线上库)。
	cols := map[string]string{
		"nickname":  "TEXT DEFAULT ''",
		"signature": "TEXT DEFAULT ''",
		"avatar":    "TEXT DEFAULT ''",
		"email":     "TEXT DEFAULT ''",
	}
	for col, typ := range cols {
		var n int
		db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('accounts') WHERE name=?", col).Scan(&n)
		if n == 0 {
			if _, err := db.Exec("ALTER TABLE accounts ADD COLUMN " + col + " " + typ); err != nil {
				return nil, err
			}
		}
	}
	// 邮箱验证码 / 密码重置码表 (邮箱绑定与找回密码共用)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS email_codes(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		email TEXT NOT NULL,
		code TEXT NOT NULL,
		purpose TEXT NOT NULL,        -- 'bind' 绑定邮箱 | 'reset' 找回密码
		expires_at INTEGER NOT NULL,  -- 毫秒时间戳
		used INTEGER NOT NULL DEFAULT 0,
		created_at INTEGER NOT NULL
	)`); err != nil {
		return nil, err
	}
	// 用户绑定邮箱的唯一索引(空串允许多行: 未绑定)
	if _, err := db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_email ON accounts(email) WHERE email <> ''"); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close 关闭数据库连接 (优雅关闭时调用)
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Login(user, pwd string) bool {
	var stored string
	err := s.db.QueryRow("SELECT password FROM accounts WHERE username=?", user).Scan(&stored)
	return err == nil && stored == pwd
}

// AuditLogin 记录登录尝试 (成功/失败) 到审计表 (原始需求: 审计)
func (s *Store) AuditLogin(user, status, remote string) {
	s.db.Exec("INSERT INTO audit_log(user, status, remote) VALUES(?,?,?)", user, status, remote)
}

// AuditLogs 管理员导出审计日志
func (s *Store) AuditLogs(limit int) []map[string]interface{} {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.Query("SELECT id, ts, user, status, remote FROM audit_log ORDER BY id DESC LIMIT ?", limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []map[string]interface{}
	for rows.Next() {
		var id int64
		var ts, user, status, remote string
		var remoteN sql.NullString
		if err := rows.Scan(&id, &ts, &user, &status, &remoteN); err != nil {
			continue
		}
		remote = remoteN.String
		out = append(out, map[string]interface{}{"id": id, "ts": ts, "user": user, "status": status, "remote": remote})
	}
	return out
}

func (s *Store) CreateUser(user, pwd string) bool {
	_, err := s.db.Exec("INSERT INTO accounts(username,password) VALUES(?,?)", user, pwd)
	return err == nil
}

func (s *Store) CreateGroup(gid, name, owner string) bool {
	tx, err := s.db.Begin()
	if err != nil {
		return false
	}
	if _, err := tx.Exec("INSERT INTO groups(gid,name,owner) VALUES(?,?,?)", gid, name, owner); err != nil {
		tx.Rollback()
		return false
	}
	if _, err := tx.Exec("INSERT INTO group_members(gid,user) VALUES(?,?)", gid, owner); err != nil {
		tx.Rollback()
		return false
	}
	tx.Commit()
	return true
}

func (s *Store) AddMember(gid, user string) bool {
	_, err := s.db.Exec("INSERT INTO group_members(gid,user) VALUES(?,?)", gid, user)
	return err == nil
}

func (s *Store) GroupMembers(gid string) []string {
	rows, err := s.db.Query("SELECT user FROM group_members WHERE gid=?", gid)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var u string
		rows.Scan(&u)
		out = append(out, u)
	}
	return out
}

// IsGroupOwner 用户是否是群主
func (s *Store) IsGroupOwner(gid, user string) bool {
	var n int
	s.db.QueryRow("SELECT COUNT(*) FROM groups WHERE gid=? AND owner=?", gid, user).Scan(&n)
	return n > 0
}

// TransferOwner 转让群主 (群主 -> 成员)
func (s *Store) TransferOwner(gid, from, to string) bool {
	_, err := s.db.Exec("UPDATE groups SET owner=? WHERE gid=? AND owner=?", to, gid, from)
	return err == nil
}

// RemoveMember 踢人 (仅群主调, 调用前需 IsGroupOwner 校验)
func (s *Store) RemoveMember(gid, user string) bool {
	tx, err := s.db.Begin()
	if err != nil {
		return false
	}
	if _, err := tx.Exec("DELETE FROM group_members WHERE gid=? AND user=?", gid, user); err != nil {
		tx.Rollback()
		return false
	}
	// 同时清理被踢者在本群消息的投递队列 (退群/被踢后不再收老消息)
	if _, err := tx.Exec("DELETE FROM msg_state WHERE user=? AND msg_id IN (SELECT id FROM messages WHERE gid=?)", user, gid); err != nil {
		tx.Rollback()
		return false
	}
	tx.Commit()
	return true
}

// LeaveGroup 自己退群
func (s *Store) LeaveGroup(gid, user string) bool {
	tx, err := s.db.Begin()
	if err != nil {
		return false
	}
	if _, err := tx.Exec("DELETE FROM group_members WHERE gid=? AND user=?", gid, user); err != nil {
		tx.Rollback()
		return false
	}
	// 退群者不再收该群消息 (清理投递队列)
	if _, err := tx.Exec("DELETE FROM msg_state WHERE user=? AND msg_id IN (SELECT id FROM messages WHERE gid=?)", user, gid); err != nil {
		tx.Rollback()
		return false
	}
	tx.Commit()
	return true
}

// DissolveGroup 解散群 (删除群 + 所有成员关系)
func (s *Store) DissolveGroup(gid string) bool {
	tx, err := s.db.Begin()
	if err != nil {
		return false
	}
	if _, err := tx.Exec("DELETE FROM group_members WHERE gid=?", gid); err != nil {
		tx.Rollback()
		return false
	}
	// 解散群: 全群投递队列清空 (无人再收到该群消息)
	if _, err := tx.Exec("DELETE FROM msg_state WHERE msg_id IN (SELECT id FROM messages WHERE gid=?)", gid); err != nil {
		tx.Rollback()
		return false
	}
	if _, err := tx.Exec("DELETE FROM groups WHERE gid=?", gid); err != nil {
		tx.Rollback()
		return false
	}
	tx.Commit()
	return true
}

// GroupExists 群是否存在
func (s *Store) GroupExists(gid string) bool {
	var n int
	s.db.QueryRow("SELECT COUNT(*) FROM groups WHERE gid=?", gid).Scan(&n)
	return n > 0
}

// GroupOwner 群主
func (s *Store) GroupOwner(gid string) string {
	var o string
	s.db.QueryRow("SELECT owner FROM groups WHERE gid=?", gid).Scan(&o)
	return o
}

// IsGroupMember 用户是否是群成员
func (s *Store) IsGroupMember(gid, user string) bool {
	var n int
	s.db.QueryRow("SELECT COUNT(*) FROM group_members WHERE gid=? AND user=?", gid, user).Scan(&n)
	return n > 0
}

// UserExists 用户是否存在
func (s *Store) UserExists(user string) bool {
	var n int
	s.db.QueryRow("SELECT COUNT(*) FROM accounts WHERE username=?", user).Scan(&n)
	return n > 0
}

// Profile 用户公开资料
type Profile struct {
	Username  string `json:"username"`
	Nickname  string `json:"nickname"`
	Signature string `json:"signature"`
	Avatar    string `json:"avatar"` // 头像文件名 (/uploads/<file>), 空=首字母占位
	CreatedAt int64  `json:"created_at"`
}

// GetProfile 取用户资料; 不存在返回 ok=false
func (s *Store) GetProfile(user string) (Profile, bool) {
	var p Profile
	var nickname, signature, avatar string
	var created int64
	err := s.db.QueryRow(
		"SELECT username, COALESCE(nickname,''), COALESCE(signature,''), COALESCE(avatar,''), COALESCE(created_at,0) FROM accounts WHERE username=?",
		user).Scan(&p.Username, &nickname, &signature, &avatar, &created)
	if err != nil {
		return Profile{}, false
	}
	p.Nickname, p.Signature, p.Avatar, p.CreatedAt = nickname, signature, avatar, created
	return p, true
}

// UpdateProfile 保存自己的昵称/签名/头像 (只更新提供非空的字段)
func (s *Store) UpdateProfile(user, nickname, signature, avatar string) {
	if nickname != "" {
		s.db.Exec("UPDATE accounts SET nickname=? WHERE username=?", nickname, user)
	}
	if signature != "" {
		s.db.Exec("UPDATE accounts SET signature=? WHERE username=?", signature, user)
	}
	if avatar != "" {
		s.db.Exec("UPDATE accounts SET avatar=? WHERE username=?", avatar, user)
	}
}

// Peers 与 user 有往来的人: 单聊互发过 或 同群成员 (上线/下线通知用)
func (s *Store) Peers(user string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(u string) {
		if u != "" && u != user && !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	// 单聊: 我是 sender 或 recipient
	rows, err := s.db.Query(`SELECT sender, recipient FROM messages WHERE sender=? OR recipient=?`, user, user)
	if err == nil {
		for rows.Next() {
			var a, b sql.NullString
			rows.Scan(&a, &b)
			if a.Valid {
				add(a.String)
			}
			if b.Valid {
				add(b.String)
			}
		}
		rows.Close()
	}
	// 群: 与我同群的人
	rows2, err := s.db.Query(`SELECT gm2.user FROM group_members gm1 JOIN group_members gm2 ON gm1.gid=gm2.gid WHERE gm1.user=?`, user)
	if err == nil {
		for rows2.Next() {
			var u string
			rows2.Scan(&u)
			add(u)
		}
		rows2.Close()
	}
	return out
}

// ConnMeta 连接会话元数据 (设备管理用)
type ConnMeta struct {
	SID   string // 会话唯一 id (踢指定设备用)
	IP    string // 来源 IP
	Since int64  // 登录时间 (unix ms)
}

type Server struct {
	store   *Store
	mu      sync.Mutex
	online  map[string]map[*websocket.Conn]bool
	tokens  map[string]string               // token -> user (请求级认证)
	wsmu    map[*websocket.Conn]*sync.Mutex // 每连接写锁 (deliver/心跳/业务并发安全)
	devices map[*websocket.Conn]*ConnMeta   // 连接会话元数据 (设备管理)
	upDir   string                          // 头像上传保存目录 (web/uploads)

	// 限流/防刷 (内存态)
	msgLimiter   *MsgRateLimiter
	loginLockout *LoginLockout
	regLimiter   *RegisterLimit
}

func NewServer(store *Store, msgRate float64, regLimit int) *Server {
	srv := &Server{store: store, online: map[string]map[*websocket.Conn]bool{}, tokens: map[string]string{}, wsmu: map[*websocket.Conn]*sync.Mutex{}, devices: map[*websocket.Conn]*ConnMeta{}}
	if msgRate > 0 {
		srv.msgLimiter = NewMsgRateLimiter(10, msgRate) // 消息: 突发 10, refill msgRate/秒
	}
	srv.loginLockout = NewLoginLockout(5, 5*time.Minute) // 登录: 5 次失败锁 5 分钟
	if regLimit > 0 {
		srv.regLimiter = NewRegisterLimit(regLimit, 10*time.Minute) // 注册: 每 IP 10 分钟几个
	}
	// 后台清扫: 每分钟清一次限流器过期条目 (防内存泄漏)
	go srv.cleanupLoop()
	return srv
}

// cleanupLoop 定期清理限流器中的过期条目 (免责: 私人规模, 每分钟一次足够)
func (s *Server) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		// 登录锁: 清掉已过期的
		s.loginLockout.mu.Lock()
		for user, ll := range s.loginLockout.locks {
			if now.After(ll.until) && ll.fails >= s.loginLockout.maxFails {
				delete(s.loginLockout.locks, user)
			}
		}
		s.loginLockout.mu.Unlock()
		// 注册计数: 清掉窗口外的
		if s.regLimiter != nil {
			s.regLimiter.mu.Lock()
			cutoff := now.Add(-10 * time.Minute)
			for ip, c := range s.regLimiter.byIP {
				kept := c.times[:0]
				for _, t := range c.times {
					if t.After(cutoff) {
						kept = append(kept, t)
					}
				}
				if len(kept) == 0 {
					delete(s.regLimiter.byIP, ip)
				} else {
					c.times = kept
				}
			}
			s.regLimiter.mu.Unlock()
		}
		// 消息桶: 清掉 1 小时未用的 (tokens 满的不清, 避免抖)
		if s.msgLimiter != nil {
			s.msgLimiter.mu.Lock()
			for user, b := range s.msgLimiter.buckets {
				if now.Sub(b.last) > 1*time.Hour && b.tokens >= s.msgLimiter.capacity {
					delete(s.msgLimiter.buckets, user)
				}
			}
			s.msgLimiter.mu.Unlock()
		}
	}
}

// writeLock 取某连接的写锁 (不存在则创建)
func (s *Server) writeLock(ws *websocket.Conn) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wsmu[ws] == nil {
		s.wsmu[ws] = &sync.Mutex{}
	}
	return s.wsmu[ws]
}

// writeJSON 加锁写 JSON 文本帧
func (s *Server) writeJSON(ws *websocket.Conn, obj interface{}) {
	data, err := json.Marshal(obj)
	if err != nil {
		return
	}
	mu := s.writeLock(ws)
	mu.Lock()
	defer mu.Unlock()
	if werr := ws.WriteMessage(websocket.TextMessage, data); werr != nil {
		log.Printf("send failed: %v", werr)
	}
}

// writeControl 加锁写控制帧 (心跳 Ping)
func (s *Server) writeControl(ws *websocket.Conn, data []byte) error {
	mu := s.writeLock(ws)
	mu.Lock()
	defer mu.Unlock()
	return ws.WriteControl(websocket.PingMessage, data, time.Now().Add(5*time.Second))
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (s *Server) addConn(user string, ws *websocket.Conn) {
	s.mu.Lock()
	if s.online[user] == nil {
		s.online[user] = map[*websocket.Conn]bool{}
	}
	wasOffline := len(s.online[user]) == 0
	s.online[user][ws] = true
	s.mu.Unlock()
	// 首个连接上线: 通知与其有往来的人 (微信在线状态)
	if wasOffline {
		s.notifyPresence(user, true)
	}
}

// realClientIP 从 HTTP 请求解析客户端真实 IP。
// nginx 反代/负载均衡时 RemoteAddr 是上一跳(nginx)的地址, 真实 IP 在转发头里。
func realClientIP(r *http.Request) string {
	remote := r.RemoteAddr
	if h, _, err := net.SplitHostPort(remote); err == nil {
		remote = h
	}
	// X-Real-IP (nginx proxy_params 标准设置)
	if xr := r.Header.Get("X-Real-IP"); xr != "" {
		if h, _, err := net.SplitHostPort(xr); err == nil {
			return h
		}
		return xr
	}
	// X-Forwarded-For: 逗号分隔, 第一个是最初客户端
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		first := strings.TrimSpace(strings.Split(xff, ",")[0])
		if h, _, err := net.SplitHostPort(first); err == nil {
			return h
		}
		if first != "" {
			return first
		}
	}
	return remote
}

// registerDevice 登录成功后为该连接生成会话元数据 (设备管理)
func (s *Server) registerDevice(ws *websocket.Conn, remote string) *ConnMeta {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		b = []byte(fmt.Sprintf("%d", time.Now().UnixNano()))
	}
	ip := remote
	if h, _, err := net.SplitHostPort(remote); err == nil {
		ip = h
	}
	meta := &ConnMeta{SID: "dev-" + hex.EncodeToString(b), IP: ip, Since: time.Now().UnixMilli()}
	s.mu.Lock()
	s.devices[ws] = meta
	s.mu.Unlock()
	return meta
}

// notifyNewDevice 新设备登录: 通知该用户的其他在线设备 (微信"账号在其他设备登录")
func (s *Server) notifyNewDevice(user string, ws *websocket.Conn, meta *ConnMeta) {
	evt := map[string]interface{}{"type": "device_evt", "sid": meta.SID, "ip": meta.IP, "since": meta.Since}
	for _, c := range s.onlineConns(user) {
		if c != ws {
			s.writeJSON(c, evt)
		}
	}
}

// deviceSessions 列出该用户所有在线设备
func (s *Server) deviceSessions(user string) []map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]interface{}
	for c, meta := range s.devices {
		if s.online[user] != nil && s.online[user][c] {
			out = append(out, map[string]interface{}{"sid": meta.SID, "ip": meta.IP, "since": meta.Since})
		}
	}
	return out
}

// kickDevice 踢掉该用户指定设备: 返回是否找到 (被踢端收到 kicked 并断开)
func (s *Server) kickDevice(user, sid string) bool {
	s.mu.Lock()
	var target *websocket.Conn
	for c, meta := range s.devices {
		if meta.SID == sid && s.online[user] != nil && s.online[user][c] {
			target = c
			break
		}
	}
	s.mu.Unlock()
	if target == nil {
		return false
	}
	s.writeJSON(target, map[string]interface{}{"type": "kicked", "info": "此设备已被其他设备踢下线"})
	target.Close()
	return true
}

// genToken 生成随机 token 绑定用户 (crypto 随机 16 字节 hex)
func (s *Server) genToken(user string) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	tok := hex.EncodeToString(b)
	s.mu.Lock()
	s.tokens[tok] = user
	s.mu.Unlock()
	return tok
}

// userByToken 校验 token, 返回对应用户 ("" = 无效)
func (s *Server) userByToken(tok string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokens[tok]
}

// uploadHandler 头像上传: POST /upload?token=<login_token>, multipart 字段 file
// 服务端自动压缩: 用标准库 image 解码(jpg/png/gif; 网页头像多为这些), 缩放到最长边≤400,
// 统一编码 png 存到 web/uploads/<随机hex>.png, 更新该用户 avatar, 返回 {url}
// 未支持的图(decode 失败, 如动画 webp)返回 400 提示格式。
func (s *Server) uploadHandler() http.HandlerFunc {
	const maxW = 400 // 头像最长边 (压缩后, 兼顾清晰度与体积)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		user := s.userByToken(r.URL.Query().Get("token"))
		if user == "" || s.store.UserExists(user) == false {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if err := r.ParseMultipartForm(16 << 20); err != nil { // 上限 16MB, 之后压缩
			http.Error(w, "bad multipart: "+err.Error(), http.StatusBadRequest)
			return
		}
		file, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "missing file field", http.StatusBadRequest)
			return
		}
		defer file.Close()

		// 解码 (标准库自带 jpg/png/gif decoder; 其它格式如 webp 若不支持 decode 会报错)
		img, _, err := image.Decode(file)
		if err != nil {
			http.Error(w, "unsupported image format (需要 jpg/png/gif): "+err.Error(), http.StatusBadRequest)
			return
		}

		// 缩放: 最长边>maxW 则等比缩小 (双线性近似; 头像足够, 避免额外依赖)
		b := img.Bounds()
		w0, h0 := b.Dx(), b.Dy()
		toEnc := img
		if w0 > maxW || h0 > maxW {
			dst := scaleNearest(img, maxW)
			toEnc = dst
		}

		// 编码 png 存盘
		os.MkdirAll(filepath.Join(s.upDir), 0o755)
		fname := fmt.Sprintf("a_%s_%d.png", hex.EncodeToString([]byte(user))[:min(12, len(hex.EncodeToString([]byte(user))))], time.Now().UnixMilli())
		outPath := filepath.Join(s.upDir, fname)
		fw, err := os.Create(outPath)
		if err != nil {
			http.Error(w, "save failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer fw.Close()
		if err := png.Encode(fw, toEnc); err != nil {
			http.Error(w, "encode failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		s.store.UpdateProfile(user, "", "", fname)
		url := "/uploads/" + fname
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"url":%q,"user":%q}`, url, user)
	}
}

// scaleNearest 等比缩放到最长边不超过 maxW (最近邻采样, 纯标准库无额外依赖)
func scaleNearest(src image.Image, maxW int) image.Image {
	b := src.Bounds()
	w0, h0 := b.Dx(), b.Dy()
	scale := float64(maxW) / float64(maxW) // 占位, 下面按比例
	_ = scale
	nw, nh := w0, h0
	if w0 > h0 {
		nw, nh = maxW, int(float64(h0)*(float64(maxW)/float64(w0)))
	} else {
		nw, nh = int(float64(w0)*(float64(maxW)/float64(h0))), maxW
	}
	if nw < 1 {
		nw = 1
	}
	if nh < 1 {
		nh = 1
	}
	dst := image.NewRGBA(image.Rect(0, 0, nw, nh))
	xr := float64(w0) / float64(nw)
	yr := float64(h0) / float64(nh)
	for y := 0; y < nh; y++ {
		sy := b.Min.Y + int(float64(y)*yr)
		if sy > b.Max.Y-1 {
			sy = b.Max.Y - 1
		}
		for x := 0; x < nw; x++ {
			sx := b.Min.X + int(float64(x)*xr)
			if sx > b.Max.X-1 {
				sx = b.Max.X - 1
			}
			dst.Set(x, y, src.At(sx, sy))
		}
	}
	return dst
}

func (s *Server) removeConn(user string, ws *websocket.Conn) {
	s.mu.Lock()
	becameOffline := false
	if s.online[user] != nil {
		delete(s.online[user], ws)
		if len(s.online[user]) == 0 {
			delete(s.online, user)
			becameOffline = true
		}
	}
	delete(s.wsmu, ws)    // 清理该连接的写锁
	delete(s.devices, ws) // 清理该连接的设备元数据
	s.mu.Unlock()
	// 最后一个连接断开: 通知下线
	if becameOffline {
		s.notifyPresence(user, false)
	}
}

func (s *Server) onlineConns(user string) []*websocket.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*websocket.Conn
	for ws := range s.online[user] {
		out = append(out, ws)
	}
	return out
}

func (s *Server) isOnline(user string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.online[user]) > 0
}

// closeAll 优雅关闭时主动断开所有在线连接 (让 wsHandler 的读循环立即返回)
func (s *Server) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conns := range s.online {
		for ws := range conns {
			ws.Close()
		}
	}
}

// sendToUser 给某用户所有在线连接推一条消息 (peer_ack 用)
func (s *Server) sendToUser(user string, obj map[string]interface{}) {
	for _, c := range s.onlineConns(user) {
		s.writeJSON(c, obj)
	}
}

// notifyPresence 上线/下线通知给与其有往来的人 (微信在线状态)
func (s *Server) notifyPresence(user string, online bool) {
	evt := map[string]interface{}{"type": "presence_evt", "user": user, "online": online}
	for _, p := range s.store.Peers(user) {
		s.sendToUser(p, evt)
	}
}

// deliver 实时投递一条消息给在线接收方 (并发投给它的所有连接)
// 消息本体已存, msgID 是会话内唯一 id
func (s *Server) deliver(recipient string, msgID int64, from, body string, ts int64, gid ...string) {
	conns := s.onlineConns(recipient)
	msg := map[string]interface{}{"type": "msg", "id": msgID, "from": from, "body": body, "ts": ts}
	if len(gid) > 0 && gid[0] != "" {
		msg["gid"] = gid[0] // 群消息: 前端靠 gid 识别群并渲染到群会话
	}
	for _, c := range conns {
		s.writeJSON(c, msg)
	}
}

// exportHandler GET /export?token=xxx —— 仅管理员 token 可导出全量数据
func (s *Server) exportHandler(adminUser string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := r.URL.Query().Get("token")
		s.mu.Lock()
		u, ok := s.tokens[tok]
		s.mu.Unlock()
		if !ok || u != adminUser {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		data := s.store.ExportData()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", `attachment; filename="wxlike-export.json"`)
		json.NewEncoder(w).Encode(data)
	}
}

func (s *Server) wsHandler(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	var user string
	defer ws.Close()

	// 取客户端真实 IP: nginx 反代会把真实 IP 放 X-Real-IP / X-Forwarded-For 头,
	// 不能直接用 RemoteAddr (那是上一跳 nginx/localhost 的地址)
	clientIP := realClientIP(r)

	// 心跳检测: 服务器每 54s 发 Ping, 客户端须回 Pong (浏览器自动回),
	// ReadDeadline 60s: 若 60s 无任何帧(含 Pong) 则 ReadMessage 报错 -> 判死连接
	ws.SetReadDeadline(time.Now().Add(60 * time.Second))
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	ticker := time.NewTicker(54 * time.Second)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			if err := s.writeControl(ws, nil); err != nil {
				ws.Close()
				return
			}
		}
	}()

	for {
		_, raw, err := ws.ReadMessage()
		if err != nil {
			// 读超时/断开: 若是已登录连接, 从 online 移除 (释放内存 + 未读留待下次登录重投)
			if user != "" {
				s.removeConn(user, ws)
			}
			break
		}
		var msg map[string]interface{}
		if json.Unmarshal(raw, &msg) != nil {
			continue
		}
		user = s.route(user, ws, msg, clientIP)
	}
	if user != "" {
		s.removeConn(user, ws)
	}
}

func (s *Server) route(user string, ws *websocket.Conn, msg map[string]interface{}, remote string) string {
	act, _ := msg["type"].(string)

	// 请求序号回显: 前端 req() 靠 seq 匹配 pending, 响应必须带请求的 seq
	seq, _ := msg["seq"].(float64)
	reply := func(obj map[string]interface{}) {
		if seq != 0 {
			obj["seq"] = seq
		}
		s.send(ws, obj)
	}
	errReply := func(code string) {
		reply(map[string]interface{}{"type": "error", "code": code})
	}
	loginOK := func(u, tok string) {
		obj := map[string]interface{}{"type": "login_ok", "user": u, "token": tok}
		if p, ok := s.store.GetProfile(u); ok {
			obj["nickname"] = p.Nickname
			obj["signature"] = p.Signature
			obj["avatar"] = p.Avatar
		}
		obj["email"] = s.store.EmailOf(u)
		reply(obj)
	}

	// get_profile: 查任意用户公开资料卡 (需已登录; toUser 存在才返回)
	if act == "get_profile" {
		if user == "" {
			errReply("unauthorized")
			return user
		}
		toUser, _ := msg["to_user"].(string)
		if toUser == "" {
			toUser, _ = msg["user"].(string)
		}
		if toUser == "" {
			errReply("bad_param")
			return user
		}
		if p, ok := s.store.GetProfile(toUser); ok {
			// 直接回传 Profile struct: username/nickname/signature/avatar/created_at 全带 (json tag 已定义)
			reply(map[string]interface{}{"type": "profile", "profile": p})
		} else {
			errReply("no_such_user")
		}
		return user
	}

	// set_profile: 保存自己的资料 (需已登录成功)
	if act == "set_profile" {
		if user == "" {
			errReply("unauthorized")
			return user
		}
		nick, _ := msg["nickname"].(string)
		sig, _ := msg["signature"].(string)
		av, _ := msg["avatar"].(string)
		s.store.UpdateProfile(user, nick, sig, av)
		// 成功: 回显自己最新资料 + 推送给「有会话的人」实时刷新昵称/头像
		if p, ok := s.store.GetProfile(user); ok {
			reply(map[string]interface{}{"type": "profile_ok", "profile": p})
		} else {
			reply(map[string]interface{}{"type": "profile_ok"})
		}
		peers := s.store.Peers(user)
		evt := map[string]interface{}{"type": "profile_evt", "user": user}
		if p, ok := s.store.GetProfile(user); ok {
			evt["nickname"] = p.Nickname
			evt["signature"] = p.Signature
			evt["avatar"] = p.Avatar
		}
		for _, peer := range peers {
			for _, c := range s.onlineConns(peer) {
				s.writeJSON(c, evt)
			}
		}
		return user
	}

	if act == "login" {
		u, _ := msg["user"].(string)
		p, _ := msg["pass"].(string)
		// 登录失败锁: 连续 5 次失败锁 5 分钟 (防暴力破解)
		if locked, wait := s.loginLocked(u); locked {
			reply(map[string]interface{}{"type": "error", "code": "login_locked", "retry_after": wait})
			return ""
		}
		if s.store.Login(u, p) {
			s.loginLockout.Reset(u)
			s.store.AuditLogin(u, "success", remote)
			s.addConn(u, ws)
			meta := s.registerDevice(ws, remote)
			s.notifyNewDevice(u, ws, meta)
			// 重新投递: 该用户所有未读(pending/delivered), 补 type=msg
			for _, m := range s.store.Undelivered(u) {
				payload := map[string]interface{}{
					"type": "msg", "id": m.ID, "state": m.State,
					"from": m.Sender, "body": m.Body, "ts": m.Ts,
				}
				s.writeJSON(ws, payload)
			}
			loginOK(u, s.genToken(u))
			return u
		}
		s.loginLockout.RecordFail(u)
		s.store.AuditLogin(u, "fail", remote)
		errReply("auth")
		return ""
	}

	if act == "token_login" {
		// token 恢复会话 (断线重连/多端): 不重输密码, 凭 login 时发的 token
		tok, _ := msg["token"].(string)
		s.mu.Lock()
		u, ok := s.tokens[tok]
		s.mu.Unlock()
		if !ok {
			errReply("unauthorized")
			return ""
		}
		s.addConn(u, ws)
		meta := s.registerDevice(ws, remote)
		s.notifyNewDevice(u, ws, meta)
		// 重投未读 (同 login)
		for _, m := range s.store.Undelivered(u) {
			payload := map[string]interface{}{
				"type": "msg", "id": m.ID, "state": m.State,
				"from": m.Sender, "body": m.Body, "ts": m.Ts,
			}
			s.writeJSON(ws, payload)
		}
		loginOK(u, tok) // 原 token 继续有效
		return u
	}

	if act == "register" {
		// 注册 IP 限流: 每 IP 10 分钟限 3 次 (防机器人灌库)
		if !s.regAllowed(remote) {
			errReply("reg_limited")
			return user
		}
		u, _ := msg["user"].(string)
		p, _ := msg["pass"].(string)
		if s.store.CreateUser(u, p) {
			reply(map[string]interface{}{"type": "register_ok"})
		} else {
			errReply("exists")
		}
		return user
	}

	// ---- 找回密码 (未登录可用) ----
	// request_reset: 提交用户名(user 或 email 字段) -> 若绑定了邮箱则发重置码。响应统一不泄露账号是否存在。
	if act == "request_reset" {
		ident, _ := msg["user"].(string)
		if ident == "" {
			ident, _ = msg["email"].(string)
		}
		ident = strings.TrimSpace(ident)
		// 允许用用户名或邮箱发起
		target := s.store.UserByEmail(ident)
		if target == "" {
			// 当作用户名
			var e string
			s.store.db.QueryRow("SELECT COALESCE(email,'') FROM accounts WHERE username=?", ident).Scan(&e)
			if e != "" {
				target = ident
			}
		}
		if target != "" {
			email := s.store.EmailOf(target)
			if email != "" {
				code, err := s.store.CreateCode(email, "reset")
				if err == nil {
					go func() { _ = sendCode(email, code, "reset") }()
				}
			}
		}
		// 无论账号/邮箱是否存在都回 ok (防枚举)
		reply(map[string]interface{}{"type": "reset_sent"})
		return user
	}

	// reset_password: 邮箱/用户名 + 验证码 + 新密码 -> 重置
	if act == "reset_password" {
		ident, _ := msg["user"].(string)
		if ident == "" {
			ident, _ = msg["email"].(string)
		}
		code, _ := msg["code"].(string)
		newPass, _ := msg["new_pass"].(string)
		ident = strings.TrimSpace(ident)
		if code == "" || newPass == "" {
			errReply("bad_request")
			return user
		}
		// 定位用户
		target := s.store.UserByEmail(ident)
		email := ""
		if target != "" {
			email = ident
		} else {
			email = s.store.EmailOf(ident)
			if email != "" {
				target = ident
			}
		}
		if target == "" || email == "" {
			errReply("invalid_code")
			return user
		}
		if !s.store.VerifyCode(email, "reset", code) {
			errReply("invalid_code")
			return user
		}
		if err := s.store.SetPassword(target, newPass); err != nil {
			errReply("server_error")
			return user
		}
		reply(map[string]interface{}{"type": "reset_ok"})
		return user
	}

	// token 认证: 未登录连接可凭 token 执行业务请求 (login/register 已在上方处理)
	if user == "" {
		if tok, ok := msg["token"].(string); ok && tok != "" {
			if tu := s.userByToken(tok); tu != "" {
				user = tu // token 有效, 以 token 绑定的用户身份执行
			} else {
				errReply("unauthorized")
				return ""
			}
		} else {
			return ""
		}
	}

	// ---- 账号安全 (需登录) ----
	// set_password: 旧密码校验 -> 更新新密码 (自助改密)
	if act == "set_password" {
		oldPass, _ := msg["old_pass"].(string)
		newPass, _ := msg["new_pass"].(string)
		if newPass == "" {
			errReply("bad_request")
			return user
		}
		if !s.store.CheckPassword(user, oldPass) {
			errReply("wrong_password")
			return user
		}
		if err := s.store.SetPassword(user, newPass); err != nil {
			errReply("server_error")
			return user
		}
		reply(map[string]interface{}{"type": "password_ok"})
		return user
	}

	// send_email_code: 绑定邮箱 -> 发验证码到该邮箱 (校验格式 + 未被占用)
	if act == "send_email_code" {
		email, _ := msg["email"].(string)
		email = strings.TrimSpace(strings.ToLower(email))
		if !validEmail(email) {
			errReply("invalid_email")
			return user
		}
		if s.store.EmailTaken(email) && s.store.EmailOf(user) != email {
			errReply("email_taken")
			return user
		}
		code, err := s.store.CreateCode(email, "bind")
		if err != nil {
			errReply("server_error")
			return user
		}
		go func() { _ = sendCode(email, code, "bind") }()
		reply(map[string]interface{}{"type": "email_code_sent"})
		return user
	}

	// verify_email: 邮箱 + 验证码 -> 绑定成功
	if act == "verify_email" {
		email, _ := msg["email"].(string)
		code, _ := msg["code"].(string)
		email = strings.TrimSpace(strings.ToLower(email))
		if !validEmail(email) || code == "" {
			errReply("bad_request")
			return user
		}
		if !s.store.VerifyCode(email, "bind", code) {
			errReply("invalid_code")
			return user
		}
		if err := s.store.SetEmail(user, email); err != nil {
			errReply("server_error")
			return user
		}
		reply(map[string]interface{}{"type": "email_ok", "email": email})
		return user
	}

	if act == "ack_received" {
		if id, ok := msg["id"].(float64); ok {
			s.store.AckReceived(user, int64(id))
		}
		return user
	}

	if act == "sessions" {
		// 列出我的所有在线设备 (只能查自己)
		devs := s.deviceSessions(user)
		if devs == nil {
			devs = []map[string]interface{}{}
		}
		reply(map[string]interface{}{"type": "sessions", "devices": devs})
		return user
	}

	if act == "kick" {
		// 主动断开我的指定设备: {sid}
		sid, _ := msg["sid"].(string)
		if sid == "" || !s.kickDevice(user, sid) {
			errReply("not_found")
			return user
		}
		reply(map[string]interface{}{"type": "kick_ok"})
		return user
	}

	if act == "ack_read" {
		if id, ok := msg["id"].(float64); ok {
			s.store.AckRead(user, int64(id))
			// 已读回执实时推送 (微信语义: 对方已读, 发送者立刻知道)
			sender, gid := s.store.MsgSender(int64(id))
			if sender != "" && sender != user {
				ack := map[string]interface{}{"type": "peer_ack", "id": int64(id), "reader": user}
				if gid != "" {
					// 群聊: 推给群里所有在线成员 (含 sender), 附已读数
					// gid 来自 messages 表 (group:xxx 前缀), 剥离前缀后查群成员
					plainGid := strings.TrimPrefix(gid, "group:")
					ack["gid"] = "group::" + plainGid // 对外协议统一 group:: 双冒号
					ack["read"] = s.store.GroupReadCount(int64(id))
					ack["total"] = len(s.store.GroupMembers(plainGid)) - 1 // 除 sender
					for _, m := range s.store.GroupMembers(plainGid) {
						if m != user {
							s.sendToUser(m, ack)
						}
					}
				} else {
					// 单聊: 只推给 sender
					s.sendToUser(sender, ack)
				}
			}
		}
		// 回执确认 (带 seq): 前端 ackAllUnread 逐条 await 本响应, 无回包会每条死等超时
		reply(map[string]interface{}{"type": "ack_read"})
		return user
	}

	if act == "group_members" {
		// 群成员查询: {gid} -> {members:[...], owner, name} (仅成员可查)
		gid, _ := msg["gid"].(string)
		gid = strings.TrimPrefix(strings.TrimPrefix(gid, "group::"), "group:")
		if !s.store.GroupExists(gid) {
			errReply("group_not_found")
			return user
		}
		if !s.store.IsGroupMember(gid, user) {
			errReply("not_member")
			return user
		}
		row := s.store.db.QueryRow("SELECT name, owner FROM groups WHERE gid=?", gid)
		var name, owner string
		row.Scan(&name, &owner)
		members := s.store.GroupMembers(gid)
		reply(map[string]interface{}{
			"type": "group_members", "gid": "group::" + gid,
			"name": name, "owner": owner, "members": members,
		})
		return user
	}

	if act == "transfer_owner" {
		// 群转让: 群主转让 owner 给成员
		gid, _ := msg["gid"].(string)
		to, _ := msg["user"].(string)
		if !s.store.GroupExists(gid) {
			errReply("group_not_found")
			return user
		}
		if !s.store.IsGroupOwner(gid, user) {
			errReply("not_owner")
			return user
		}
		if !s.store.IsGroupMember(gid, to) {
			errReply("not_member")
			return user
		}
		if s.store.TransferOwner(gid, user, to) {
			reply(map[string]interface{}{"type": "group_ok"})
		} else {
			reply(map[string]interface{}{"type": "error"})
		}
		return user
	}

	if act == "create_group" {
		gid, _ := msg["gid"].(string)
		name, _ := msg["name"].(string)
		if name == "" {
			name = gid
		}
		if s.store.CreateGroup(gid, name, user) {
			reply(map[string]interface{}{"type": "group_ok"})
		} else {
			errReply("exists")
		}
		return user
	}

	if act == "add_member" {
		gid, _ := msg["gid"].(string)
		u, _ := msg["user"].(string)
		gid = strings.TrimPrefix(strings.TrimPrefix(gid, "group::"), "group:")
		// 权限与合法性 (微信语义: 群内所有成员都可拉人; 非成员不能拉; 目标必须是真实存在且未入群的用户)
		if !s.store.GroupExists(gid) {
			errReply("group_not_found")
			return user
		}
		if !s.store.UserExists(u) {
			errReply("no_such_user")
			return user
		}
		if !s.store.IsGroupMember(gid, user) {
			errReply("not_member")
			return user
		}
		if s.store.IsGroupMember(gid, u) {
			errReply("already_member")
			return user
		}
		if s.store.AddMember(gid, u) {
			reply(map[string]interface{}{"type": "group_ok"})
			// 被拉入者在线时立即刷新会话列表 (微信语义: 进群立刻看到群)
			for _, c := range s.onlineConns(u) {
				s.writeJSON(c, map[string]interface{}{"type": "conversations_refresh"})
			}
		} else {
			errReply("add_failed")
		}
		return user
	}

	if act == "remove_member" {
		// 群主踢人: 群主才能踢
		gid, _ := msg["gid"].(string)
		u, _ := msg["user"].(string)
		if !s.store.GroupExists(gid) {
			errReply("group_not_found")
			return user
		}
		if !s.store.IsGroupOwner(gid, user) {
			errReply("not_owner")
			return user
		}
		if s.store.RemoveMember(gid, u) {
			reply(map[string]interface{}{"type": "group_ok"})
		} else {
			reply(map[string]interface{}{"type": "error"})
		}
		return user
	}

	if act == "leave_group" {
		// 自己退群。微信语义: 群主不能直接退群, 需先转让群主 (否则群变无主)
		gid, _ := msg["gid"].(string)
		gid = strings.TrimPrefix(strings.TrimPrefix(gid, "group::"), "group:")
		if !s.store.GroupExists(gid) {
			errReply("group_not_found")
			return user
		}
		if s.store.IsGroupOwner(gid, user) {
			errReply("owner_must_transfer")
			return user
		}
		if s.store.LeaveGroup(gid, user) {
			reply(map[string]interface{}{"type": "group_ok"})
		} else {
			errReply("leave_failed")
		}
		return user
	}

	if act == "dissolve_group" {
		// 群主解散群
		gid, _ := msg["gid"].(string)
		if !s.store.GroupExists(gid) {
			errReply("group_not_found")
			return user
		}
		if !s.store.IsGroupOwner(gid, user) {
			errReply("not_owner")
			return user
		}
		if s.store.DissolveGroup(gid) {
			reply(map[string]interface{}{"type": "group_ok"})
		} else {
			reply(map[string]interface{}{"type": "error"})
		}
		return user
	}

	if act == "msg" {
		// 消息令牌桶: 突发 10, 5条/秒; 超限拒绝 (防脚本刷屏拖垮 1核 VPS)
		if ok, wait := s.msgAllowed(user); !ok {
			reply(map[string]interface{}{"type": "error", "code": "rate_limited", "retry_after": wait})
			return user
		}
		to, _ := msg["to"].(string)
		body, _ := msg["body"].(string)
		// 消息长度限制: 单条 <= 10KB (微信 ~4KB, 给富余; 防超长消息拖垮内存)
		if len(body) > 10*1024 {
			reply(map[string]interface{}{"type": "error", "code": "msg_too_large"})
			return user
		}
		ts := time.Now().UnixMilli()
		isGroup, _ := msg["is_group"].(bool)

		if isGroup {
			// 群聊: 必须群存在 + 发送者是成员 (微信要求)
			// 归一化 gid: 前端传 group::<gid> 或 group:<gid> → 无前缀 <gid> (groups/group_members 表格式)
			to = strings.TrimPrefix(strings.TrimPrefix(to, "group::"), "group:")
			if !s.store.GroupExists(to) {
				errReply("group_not_found")
				return user
			}
			if !s.store.IsGroupMember(to, user) {
				errReply("not_member")
				return user
			}
			// 消息本体一份 (messages.gid 带 group: 前缀, 与 ack/conversations 剥离逻辑一致)
			msgID, err := s.store.SaveGroup("group:"+to, user, body, ts)
			if err != nil {
				return user
			}
			for _, member := range s.store.GroupMembers(to) {
				if member == user {
					// sender 自身: 在线时直接回显(服务器权威), 不建 msg_state (避免群已读统计把发送者自己算进去)
					if s.isOnline(member) {
						s.deliver(member, msgID, user, body, ts, "group:"+to)
					}
					continue
				}
				s.store.Enqueue(member, msgID) // pending  (非 sender 成员入队)
				if s.isOnline(member) {
					s.deliver(member, msgID, user, body, ts, "group:"+to)
				}
			}
		} else {
			// 单聊: 对方必须存在
			if !s.store.UserExists(to) {
				errReply("no_such_user")
				return user
			}
			// 一条本体(recipient=to), 收方入队
			msgID, err := s.store.SaveSingle(user, to, body, ts)
			if err != nil {
				return user
			}
			s.store.Enqueue(to, msgID)
			if s.isOnline(to) {
				s.deliver(to, msgID, user, body, ts)
			}
		}
		return user
	}

	if act == "presence" {
		// 在线状态查询: {users:[...]} -> {online:{user:bool}}
		users, _ := msg["users"].([]interface{})
		online := map[string]bool{}
		for _, u := range users {
			if s_, ok := u.(string); ok {
				online[s_] = s.isOnline(s_)
			}
		}
		reply(map[string]interface{}{"type": "presence", "online": online})
		return user
	}

	if act == "conversations" {
		// 会话列表: 每会话最后一条消息 + 未读数, 按最近降序 (微信会话列表)
		rows := s.store.Conversations(user)
		if rows == nil {
			rows = []ConversationRow{}
		}
		reply(map[string]interface{}{"type": "conversations", "items": rows})
		return user
	}

	if act == "unread" {
		// 未读计数 (badge): 服务器权威, 按会话返回 {chat: n}
		counts := s.store.UnreadCounts(user)
		items := []map[string]interface{}{}
		for k, v := range counts {
			items = append(items, map[string]interface{}{"chat": k, "count": v})
		}
		reply(map[string]interface{}{"type": "unread", "items": items})
		return user
	}

	if act == "receipts" {
		// 已读回执: 某会话最近消息附带 state (read=已读)
		chatKey, _ := msg["chat"].(string)
		rows := s.store.Receipts(user, chatKey)
		reply(map[string]interface{}{"type": "receipts", "rows": rows})
		return user
	}

	if act == "recent" {
		// 断线重连恢复: 最近 N 条跨会话消息 (客户端按全局 id 去重)
		limit := 50
		if l, ok := msg["count"].(float64); ok {
			limit = int(l)
		}
		if limit > 200 {
			limit = 200
		}
		rows := s.store.Recent(user, limit)
		if rows == nil {
			rows = []MsgRow{}
		}
		reply(map[string]interface{}{"type": "recent", "items": rows})
		return user
	}

	if act == "history" {
		chatKey, _ := msg["chat"].(string) // 单聊传对方, 群聊传 'group::<gid>'
		// 群聊: 群已解散/非成员 -> 拒绝查历史 (微信语义)
		if len(chatKey) >= 7 && chatKey[:7] == "group::" {
			gid := chatKey[7:]
			if !s.store.GroupExists(gid) || !s.store.IsGroupMember(gid, user) {
				errReply("not_member")
				return user
			}
		}
		limit := 50
		if l, ok := msg["limit"].(float64); ok {
			limit = int(l)
		}
		// 分页: before_id > 0 时只取 id < before_id 的旧消息 (游标翻页)
		beforeID := int64(0)
		if b, ok := msg["before_id"].(float64); ok {
			beforeID = int64(b)
		}
		rows := s.store.History(user, chatKey, limit, beforeID)
		reply(map[string]interface{}{"type": "history", "rows": rows})
		return user
	}

	return user
}

func (s *Server) send(ws *websocket.Conn, obj map[string]interface{}) {
	s.writeJSON(ws, obj)
}

func (s *Server) sendLoginOK(ws *websocket.Conn, user, token string) {
	s.send(ws, map[string]interface{}{"type": "login_ok", "user": user, "token": token})
}

func (s *Server) sendError(ws *websocket.Conn, code string) {
	s.send(ws, map[string]interface{}{"type": "error", "code": code})
}

func main() {
	port := flag.Int("port", 8081, "listen port")
	host := flag.String("host", "0.0.0.0", "listen host")
	dir := flag.String("dir", ".", "project root (for schema + db)")
	msgRate := flag.Float64("msg-rate", 5, "消息限流 refill 速率(条/秒); 0=关闭(测试用)")
	regLimit := flag.Int("reg-limit", 3, "每 IP 10 分钟允许注册数; 0=关闭(测试用)")
	admin := flag.String("admin", "", "管理员用户名 (只有该用户的 token 可导出 /export; 空=关闭导出)")
	web := flag.String("web", "", "前端静态目录 (serve 到 /, 单二进制部署手机浏览器直接可用)")
	flag.Parse()

	base, _ := filepath.Abs(*dir)
	schemaPath := filepath.Join(base, "schema.sql")
	dbPath := filepath.Join(base, "wxlike_go.db")

	store, err := NewStore(dbPath, schemaPath)
	if err != nil {
		log.Fatalf("store init: %v", err)
	}
	srv := NewServer(store, *msgRate, *regLimit)

	// 前端静态文件: / -> web 目录 (手机浏览器直接访问根路径)
	if *web != "" {
		go func() { log.Printf("web static serving %s at /", *web) }()
		srv.upDir = filepath.Join(*web, "uploads")
		os.MkdirAll(srv.upDir, 0o755)
		http.Handle("/", http.FileServer(http.Dir(*web)))
		http.HandleFunc("/ws", srv.wsHandler)           // WS 协议在 /ws
		http.HandleFunc("/upload", srv.uploadHandler()) // 头像上传 (图形压缩)
	} else {
		http.HandleFunc("/", srv.wsHandler) // 纯协议模式: WS 在根
	}

	// 数据导出端点 (管理员 token 鉴权): GET /export?token=xxx
	if *admin != "" {
		http.HandleFunc("/export", srv.exportHandler(*admin))
		log.Printf("export endpoint enabled for admin user %q", *admin)
	}

	addr := fmt.Sprintf("%s:%d", *host, *port)
	log.Printf("wxlike-server(Go) listening %s", addr)

	srvHTTP := &http.Server{Addr: addr, Handler: nil}
	go func() {
		if err := srvHTTP.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	// 优雅关闭: SIGINT/SIGTERM 时关 listener + 等存量连接处理完 + 关 DB
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down...")
	srv.closeAll() // 先断所有 WS 连接, 让 handler 立即返回 (Shutdown 不等 5s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srvHTTP.Shutdown(ctx)
	if err := store.Close(); err != nil {
		log.Printf("db close: %v", err)
	}
	log.Printf("bye")
}
