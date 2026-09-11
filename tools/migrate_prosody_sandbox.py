#!/usr/bin/env python3
# 用 Prosody 旧库(/opt/prosody-audit/data/prosody.sqlite)把用户+聊天历史迁移进一个 wxlike 山盘副本
# (绝不碰线上 wxlike_go.db)。目标山盘: 输出文件 .sandbox.mig.db
# 账号密码: Prosody 为 scrypt 不可逆 -> 迁移后用随机密码并写入 <out>_pwlist.txt
# 历史    : 取 store='archive' 里 1:1 chat(<message type=chat><body>), from/to 为 localpart@chat.local
#   群(groupchat/conf)本版仅统计, 不在导入(需群还原设计), 消息中文解码 html.unescape
import sqlite3,re,html,os,sys,random,string,time

OLD='/opt/prosody-audit/data/prosody.sqlite'
# 线上 wxlike 结构模板 (读 schema from 线上库文件以便复制建新库)
TEMPLATE='/root/chat-server/wxlike_go.db'
OUT='/opt/prosody-audit/data/wxlike.sandbox.mig.db'

def conn(f):
    c=sqlite3.connect(f); c.row_factory=sqlite3.Row; return c
# schema clone from template into OUT (drop if exists)
if os.path.exists(OUT): os.remove(OUT)
raw=sqlite3.connect(TEMPLATE)
sqls=[s for s in (raw.execute("select sql from sqlite_master where type='table'").fetchall()) if s[0]]
raw.close()
sb=sqlite3.connect(OUT)
for (s,) in sqls: 
    # 跳过内建/已有
    if 'sqlite_sequence' in (s or ''): continue
    sb.execute(s)
sb.commit()

old=conn(OLD)
# ---------- 导入账号 ----------
users=[]
for r in old.execute("select distinct user from prosody where store='accounts'"):
    u=r['user']
    if not u or u in('','admin@chat.local'): pass
    users.append(u.split('@')[0] if '@' in u else u)
users=sorted(set(users))
def rndpw(): return ''.join(random.choices(string.ascii_letters+string.digits,k=12))
pwlog=[]
n_acc=0
for u in users:
    if sb.execute('select 1 from accounts where username=?',(u,)).fetchone(): 
        pwlog.append(f"SKIP-conflict {u}"); continue
    p=rndpw()
    sb.execute("insert into accounts(username,password,created_at,nickname,signature,avatar) values(?,?,?, ?,?,?)",
               (u, p, int(time.time()*1000), None,None,None))
    pwlog.append(f"{u}  ->  {p}")
    n_acc+=1
sb.commit()

# ---------- 导入 1:1 chat 历史 ----------
# messages(id,gid,sender,recipient,body,ts)  global monotonic id; 但旧消息我们给负id避免与在线现有正id冲突? 山盘中空库无; 依序 id 1..n(ts=when)
body_re=re.compile(r"<body[^>]*>(.*?)</body>",re.S); fromre=re.compile(r"from='([^']+)'"); tore=re.compile(r"to='([^']+)'"); typ=re.compile(r"type='([^']+)'")
rows=old.execute('select "when",value from prosodyarchive where store=\'archive\'').fetchall()
chat=[]; grp=0; skip=0
for r in rows:
    v=r['value']; 
    if not isinstance(v,str): continue
    t=(typ.search(v).group(1) if typ.search(v) else '')
    fr=(fromre.search(v).group(1) if fromre.search(v) else ''); 
    to=(tore.search(v).group(1) if tore.search(v) else '')
    b=(body_re.search(v).group(1) if body_re.search(v) else None)
    if t=='groupchat' or 'conference' in to.lower(): grp+=1; continue
    if b is None: skip+=1; continue
    f=fr.split('@')[0]; tgt=to.split('@')[0]
    chat.append((r['when'],f,tgt,html.unescape(b)))
# 仅保留收发双方都在已导入用户集内的(系统通知如 chat.local->admin 排除)
known=set(users)
chat=[c for c in chat if c[1] in known and c[2] in known]
chat.sort(key=lambda x:x[0])
mid=1
for ts,f,tgt,b in chat:
    sb.execute("insert into messages(id,gid,sender,recipient,body,ts) values(?,?,?,?,?,?)",(mid,None,f,tgt,b,ts*1000))
    mid+=1
sb.commit()
open(OUT+'.pw','w').write("\n".join(pwlog))
print(f"[sandbox import] 库={OUT}")
print(f"  账号导入 {n_acc}/{len(users)}  (冲突跳过见 .pw 文件)")
print(f"  1:1 私聊历史导入 {len(chat)} 条 (群≈{grp} 此版跳过, 无body≈{skip})")
print(f"  密码清单: {OUT}.pw")
