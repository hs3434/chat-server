package main

// ---------- 限流/防刷 (内存态, 重启丢失可接受; 不破坏可靠性铁律) ----------
//
// 三个独立限流器 (全部内存, 私人小规模够用, 不引入外部依赖):
//   1. 消息令牌桶  每用户 cap=10, refill=5/s   -> 超限 error code="rate_limited"
//   2. 登录失败锁  连续 5 次失败锁 5 分钟      -> error code="login_locked"
//   3. 注册 IP 限流 每 IP 10 分钟限 3 次        -> error code="reg_limited"

import (
	"math"
	"net"
	"strings"
	"sync"
	"time"
)

// ---------- 1. 消息令牌桶 ----------

type msgBucket struct {
	tokens float64
	last   time.Time
}

type MsgRateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*msgBucket
	capacity float64 // 瞬时突发上限
	refill   float64 // 每秒补充
}

func NewMsgRateLimiter(capacity, refill float64) *MsgRateLimiter {
	return &MsgRateLimiter{
		buckets:  map[string]*msgBucket{},
		capacity: capacity,
		refill:   refill,
	}
}

// Allow 尝试消费 1 个令牌; 返回 (是否允许, 还需等多少秒)
func (m *MsgRateLimiter) Allow(key string) (bool, float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	b, ok := m.buckets[key]
	if !ok {
		b = &msgBucket{tokens: m.capacity, last: now}
		m.buckets[key] = b
	}
	// 按经过时间补充令牌
	elapsed := now.Sub(b.last).Seconds()
	b.tokens = math.Min(m.capacity, b.tokens+elapsed*m.refill)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	// 距下一令牌的等待时间
	wait := (1 - b.tokens) / m.refill
	return false, wait
}

// ---------- 2. 登录失败锁 ----------

type loginLock struct {
	fails int
	until time.Time // 锁定截止
}

type LoginLockout struct {
	mu       sync.Mutex
	locks    map[string]*loginLock
	maxFails int           // 连续失败多少次触发锁定
	lockFor  time.Duration // 锁定时长
}

func NewLoginLockout(maxFails int, lockFor time.Duration) *LoginLockout {
	return &LoginLockout{locks: map[string]*loginLock{}, maxFails: maxFails, lockFor: lockFor}
}

// IsLocked 该用户名当前是否被锁; 返回 (锁定, 剩余秒)
func (l *LoginLockout) IsLocked(user string) (bool, float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ll, ok := l.locks[user]
	if !ok {
		return false, 0
	}
	now := time.Now()
	if now.Before(ll.until) {
		return true, ll.until.Sub(now).Seconds()
	}
	return false, 0
}

// RecordFail 记录一次失败; 达到阈值则锁定, 返回新剩余秒数 (0 表示刚触发锁定)
func (l *LoginLockout) RecordFail(user string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ll, ok := l.locks[user]
	if !ok {
		ll = &loginLock{}
		l.locks[user] = ll
	}
	ll.fails++
	if ll.fails >= l.maxFails {
		ll.until = time.Now().Add(l.lockFor)
	}
}

// Reset 登录成功清零失败计数
func (l *LoginLockout) Reset(user string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.locks, user)
}

// ---------- 3. 注册 IP 限流 ----------

type regCounter struct {
	times []time.Time // 近期注册时间戳
}

type RegisterLimit struct {
	mu      sync.Mutex
	byIP    map[string]*regCounter
	maxRegs int           // 窗口内最多注册次数
	window  time.Duration // 时间窗口
}

func NewRegisterLimit(maxRegs int, window time.Duration) *RegisterLimit {
	return &RegisterLimit{byIP: map[string]*regCounter{}, maxRegs: maxRegs, window: window}
}

// Allow 该 IP 是否允许注册; 允许则计数在滑动窗口内
func (r *RegisterLimit) Allow(ip string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-r.window)
	c, ok := r.byIP[ip]
	if !ok {
		c = &regCounter{}
		r.byIP[ip] = c
	}
	// 清掉窗口外
	kept := c.times[:0]
	for _, t := range c.times {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	c.times = kept
	if len(c.times) >= r.maxRegs {
		return false
	}
	c.times = append(c.times, now)
	return true
}

// ---------- Server 集成 ----------

func (s *Server) msgAllowed(user string) (bool, float64) {
	// msg-rate=0 (测试用): 不限流
	if s.msgLimiter == nil {
		return true, 0
	}
	return s.msgLimiter.Allow(user)
}

func (s *Server) loginLocked(user string) (bool, float64) {
	return s.loginLockout.IsLocked(user)
}

func (s *Server) regAllowed(remote string) bool {
	// reg-limit=0 (测试用): 不限注册
	if s.regLimiter == nil {
		return true
	}
	ip := extractIP(remote)
	if ip == "" {
		// 无 IP (本机测试等) 放行
		return true
	}
	return s.regLimiter.Allow(ip)
}

// extractIP 从 RemoteAddr "host:port" 提取 host
func extractIP(remote string) string {
	h, _, err := net.SplitHostPort(remote)
	if err != nil {
		// 可能已是纯 IP (无端口)
		if strings.Contains(remote, ":") {
			return remote
		}
		return remote
	}
	return h
}
