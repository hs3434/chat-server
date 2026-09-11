package main

// 账号安全相关数据层: 改密、邮箱绑定、验证码(邮箱重绑/找回密码)。
import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// ---- 密码 / 邮箱 ----

// SetPassword 直接更新密码 (调用方负责校验旧密码)
func (s *Store) SetPassword(user, newPass string) error {
	_, err := s.db.Exec("UPDATE accounts SET password=? WHERE username=?", newPass, user)
	return err
}

// CheckPassword 校验用户名+密码 (供改密时验证旧密码)
func (s *Store) CheckPassword(user, pass string) bool {
	return s.Login(user, pass)
}

// SetEmail 绑定/更新用户邮箱
func (s *Store) SetEmail(user, email string) error {
	_, err := s.db.Exec("UPDATE accounts SET email=? WHERE username=?", email, user)
	return err
}

// EmailOf 返回用户已绑定邮箱 (” 表示未绑定)
func (s *Store) EmailOf(user string) string {
	var e string
	s.db.QueryRow("SELECT COALESCE(email,'') FROM accounts WHERE username=?", user).Scan(&e)
	return e
}

// UserByEmail 按邮箱查用户名 (”=不存在)
func (s *Store) UserByEmail(email string) string {
	var u string
	s.db.QueryRow("SELECT username FROM accounts WHERE email=? AND email<>''", email).Scan(&u)
	return u
}

// EmailTaken 该邮箱是否已被别的用户绑定
func (s *Store) EmailTaken(email string) bool {
	var n int
	s.db.QueryRow("SELECT COUNT(*) FROM accounts WHERE email=?", email).Scan(&n)
	return n > 0
}

// ---- 验证码 ----

// validEmail 基础邮箱格式校验 (a@b.c, 无空格)
func validEmail(e string) bool {
	if len(e) < 5 || len(e) > 254 {
		return false
	}
	at := strings.IndexByte(e, '@')
	if at <= 0 || at == len(e)-1 {
		return false
	}
	domain := e[at+1:]
	if strings.IndexByte(domain, '.') <= 0 || strings.HasSuffix(domain, ".") {
		return false
	}
	if strings.ContainsAny(e, " \t\r\n") {
		return false
	}
	return true
}

// genCode 生成 6 位数字验证码
func genCode() string {
	n, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return fmt.Sprintf("%06d", time.Now().UnixNano()%1000000)
	}
	return fmt.Sprintf("%06d", n.Int64())
}

// CreateCode 为邮箱生成验证码 (purpose: bind/reset), 10 分钟有效。
// 同一 email+purpose 的旧未用码作废。
func (s *Store) CreateCode(email, purpose string) (string, error) {
	s.db.Exec("UPDATE email_codes SET used=1 WHERE email=? AND purpose=? AND used=0", email, purpose)
	code := genCode()
	now := time.Now().UnixMilli()
	if _, err := s.db.Exec(
		"INSERT INTO email_codes(email,code,purpose,expires_at,used,created_at) VALUES(?,?,?,?,0,?)",
		email, code, purpose, now+10*60*1000, now,
	); err != nil {
		return "", err
	}
	return code, nil
}

// VerifyCode 校验验证码是否有效并标记已用。成功返回 true。
func (s *Store) VerifyCode(email, purpose, code string) bool {
	var id int64
	err := s.db.QueryRow(
		"SELECT id FROM email_codes WHERE email=? AND purpose=? AND code=? AND used=0 AND expires_at>? ORDER BY id DESC LIMIT 1",
		email, purpose, code, time.Now().UnixMilli(),
	).Scan(&id)
	if err == sql.ErrNoRows || err != nil {
		return false
	}
	s.db.Exec("UPDATE email_codes SET used=1 WHERE id=?", id)
	return true
}
