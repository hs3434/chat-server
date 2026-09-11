#!/usr/bin/env python3
# 把 Prosody 旧库用户+私聊历史 落地到【线上】wxlike 库。
# 规则(用户确认): 账号同名已存在则保留线上不动(不覆盖); 聊天历史(1:1)全部导入, id 从线上 max(id)+1 续接。
# 运行前: systemctl stop wxlike-go (避免并发写库)。运行后: start。
# 备份: 已由外部执行 (wxlike_go.db.bak-preimport-*)。
import sqlite3,re,html,time,random,string,sys

OLD='/opt/prosody-audit/data/prosody.sqlite'
LIVE='/root/chat-server/wxlike_go.db'
PWFILE='/root/prosody_import_passwords.txt'

old=sqlite3.connect(OLD); old.row_factory=sqlite3.Row
live=sqlite3.connect(LIVE); live.row_factory=sqlite3.Row

def rndpw(): return ''.join(random.choices(string.ascii_letters+string.digits,k=12))

# 指定账号的固定密码(用户指定): ten -> 1234pass
FIXED_PW = {'ten': '1234pass'}

# 1) 账号: 同名跳过
users=sorted({ (r['user'].split('@')[0] if r['user'] and '@' in r['user'] else r['user']) for r in old.execute("select distinct user from prosody where store='accounts'") if r['user'] })
pwlog=[]; n_add=0; n_skip=0
for u in users:
    if live.execute("select 1 from accounts where username=?",(u,)).fetchone():
        pwlog.append(f"SKIP(线上已存在): {u}"); n_skip+=1; continue
    p=FIXED_PW.get(u) or rndpw()
    live.execute("insert into accounts(username,password,created_at) values(?,?,?)",(u,p,int(time.time()*1000)))
    pwlog.append(f"ADD: {u}  ->  {p}"); n_add+=1
live.commit()

# 2) 1:1 聊天历史: id 从 live max+1 续接
start_id=(live.execute("select coalesce(max(id),0) from messages").fetchone()[0])+1
body_re=re.compile(r"<body[^>]*>(.*?)</body>",re.S); fromre=re.compile(r"from='([^']+)'"); tore=re.compile(r"to='([^']+)'"); typ=re.compile(r"type='([^']+)'")
known=set(users)
chat=[]
for r in old.execute("select \"when\",value from prosodyarchive where store='archive'"):
    v=r['value']
    if not isinstance(v,str): continue
    t=(typ.search(v).group(1) if typ.search(v) else '')
    fr=(fromre.search(v).group(1) if fromre.search(v) else '')
    to=(tore.search(v).group(1) if tore.search(v) else '')
    b=(body_re.search(v).group(1) if body_re.search(v) else None)
    if t=='groupchat' or 'conference' in to.lower() or b is None: continue
    f=fr.split('@')[0]; g=to.split('@')[0]
    if f in known and g in known:
        chat.append((r['when'],f,g,html.unescape(b)))
chat.sort(key=lambda x:x[0])
# 去重(同 when+from+to+body 重复的 Prosody 镜像行, 归并一条)
seen=set(); uniq=[]
for ts,f,g,b in chat:
    k=(ts,f,g,b)
    if k in seen: continue
    seen.add(k); uniq.append((ts,f,g,b))
mid=start_id
for ts,f,g,b in uniq:
    live.execute("insert into messages(id,gid,sender,recipient,body,ts) values(?,?,?,?,?,?)",(mid,None,f,g,b,ts*1000))
    mid+=1
live.commit()

open(PWFILE,'w').write("\n".join(pwlog)+"\n")
print(f"[live import] 账号 新增 {n_add} / 跳过(同名) {n_skip}")
print(f"[live import] 私聊历史 导入 {len(uniq)} 条 (id {start_id}..{mid-1})")
print(f"[live import] 密码清单: {PWFILE}")
