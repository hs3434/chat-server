package main

// 邮件发送: 通过 SMTP 发送验证码 (邮箱绑定 / 找回密码)。
// 凭证从环境变量读取 (不入库/不入 git):
//   WXLIKE_SMTP_HOST  默认 smtp.gmail.com
//   WXLIKE_SMTP_PORT  默认 587 (STARTTLS)
//   WXLIKE_SMTP_USER  发信邮箱 (如 xxx@gmail.com)
//   WXLIKE_SMTP_PASS  应用专用密码 (App Password)
// 未配置时: sendCode 返回 false, 上层降级为「不实际发信」(便于本地/测试)。

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/smtp"
	"os"
	"strings"
)

// smtpConfig 读取环境变量, ok=false 表示未配置发信
func smtpConfig() (host string, port string, user, pass string, ok bool) {
	host = os.Getenv("WXLIKE_SMTP_HOST")
	if host == "" {
		host = "smtp.gmail.com"
	}
	port = os.Getenv("WXLIKE_SMTP_PORT")
	if port == "" {
		port = "587"
	}
	user = os.Getenv("WXLIKE_SMTP_USER")
	pass = os.Getenv("WXLIKE_SMTP_PASS")
	ok = user != "" && pass != ""
	return
}

// sendMail 经 SMTP (STARTTLS) 发送一封纯文本邮件
func sendMail(to, subject, body string) error {
	host, port, user, pass, ok := smtpConfig()
	if !ok {
		return fmt.Errorf("smtp 未配置 (WXLIKE_SMTP_USER/PASS 为空)")
	}
	auth := smtp.PlainAuth("", user, pass, host)
	msg := strings.Join([]string{
		"From: wxlike <" + user + ">",
		"To: " + to,
		"Subject: " + subject,
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=UTF-8",
		"",
		body,
	}, "\r\n")

	addr := host + ":" + port
	if port == "587" {
		// STARTTLS: 先明文连接, 再升级 TLS
		c, err := smtp.Dial(addr)
		if err != nil {
			return err
		}
		defer c.Close()
		if err := c.StartTLS(&tls.Config{ServerName: host}); err != nil {
			return err
		}
		if err := c.Auth(auth); err != nil {
			return err
		}
		if err := c.Mail(user); err != nil {
			return err
		}
		if err := c.Rcpt(to); err != nil {
			return err
		}
		w, err := c.Data()
		if err != nil {
			return err
		}
		if _, err := w.Write([]byte(msg)); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		return c.Quit()
	}
	// 465: 直接 TLS
	return smtp.SendMail(addr, auth, user, []string{to}, []byte(msg))
}

// sendCode 发送 6 位验证码邮件 (purpose: bind/reset)
func sendCode(to, code, purpose string) error {
	var subject, body string
	switch purpose {
	case "bind":
		subject = "wxlike 邮箱绑定验证码"
		body = fmt.Sprintf("你的邮箱绑定验证码是: %s\n\n10 分钟内有效。若非本人操作请忽略。", code)
	case "reset":
		subject = "wxlike 密码重置验证码"
		body = fmt.Sprintf("你的密码重置验证码是: %s\n\n10 分钟内有效。若非本人操作请忽略。", code)
	default:
		subject = "wxlike 验证码"
		body = fmt.Sprintf("你的验证码是: %s", code)
	}
	if err := sendMail(to, subject, body); err != nil {
		log.Printf("[mail] 发送失败 to=%s: %v", to, err)
		return err
	}
	log.Printf("[mail] 已发送 %s 验证码至 %s", purpose, to)
	return nil
}
