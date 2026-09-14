#!/usr/bin/env python3
"""一次性迁移: 把线上所有历史单聊双方自动结为好友 (对齐微信语义: 能聊过天=该有好友关系).

微信语义依据: 微信里单聊必来自好友关系(或同群临时会话), 而本项目旧版无好友功能,
单聊是搜索用户名直接发的 —— 迁移时把这些既有会话关系补成 contacts 双向行.
幂等: INSERT OR IGNORE, 可重复执行.
"""
import sqlite3

DB = '/root/chat-server/wxlike_go.db'
c = sqlite3.connect(DB)

pairs = set()
for (s, r, gid) in c.execute("SELECT sender, recipient, gid FROM messages"):
    if gid:  # 群消息跳过
        continue
    if s and r and s != r:
        pairs.add((s, r))

added = 0
for (a, b) in sorted(pairs):
    for (u, f) in ((a, b), (b, a)):  # 双向各一行
        cur = c.execute(
            "INSERT OR IGNORE INTO contacts(user, friend, created_at) VALUES (?, ?, unixepoch())",
            (u, f),
        )
        added += cur.rowcount

c.commit()
print(f"pairs={len(pairs)} inserted_rows={added}")
for row in c.execute("SELECT user, friend FROM contacts ORDER BY user LIMIT 40"):
    print(row)
