#!/usr/bin/env bash
# sync2vps.sh — 把 admin 本地 wxlike 运行产物(web / bin)增量 rsync 推到 VPS, bin 变化则自动重启服务。
# 用法:
#   bash sync2vps.sh dry      # 仅预览会推什么(listing), 不实际写入
#   bash sync2vps.sh web      # 同步前端静态 web/ -> VPS /root/chat-server/web (保留 VPS uploads, 不删)
#   bash sync2vps.sh bin      # 同步编译产物 bin/wxlike-go -> VPS; 若二进制变化则 systemctl restart wxlike-go
#   bash sync2vps.sh all      # 两者都做(=一次发布: 前端 + 后端二进制)
# 前置: admin 免密登录 VPS:       ssh -p28545 -i ~/.ssh/vps_hs root@vps04.hs3434.top
#       VPS 已装 rsync, 部署目录 /root/chat-server(其 .git 不在此同步范围)
set -u
SRC=/workspace/wxlike-server
SSH="ssh -i $HOME/.ssh/vps_hs -p 28545 -o StrictHostKeyChecking=accept-new"
VPS=root@vps04.hs3434.top
DEST=/root/chat-server
MODE="${1:-dry}"

do_web() {  # 增量推 frontend(不 --delete, 保护 VPS 上运行期产生的 web/uploads)
  rsync -az -e "$SSH" --exclude 'uploads' \
    "$SRC/web/" "$VPS:$DEST/web/" 2>&1 | grep -v setlocale | tail -4
}

do_bin() {  # 推二进制; rsync --itemize 打印变化; 若实际写入则重启
  local out changed
  out=$(rsync -a --itemize-changes -e "$SSH" \
    "$SRC/bin/wxlike-go" "$VPS:$DEST/bin/wxlike-go" 2>&1 | grep -v setlocale)
  echo "$out" | tail -3
  changed=$(echo "$out" | grep -cE '^[<>c]')
  if [ "$MODE" != dry ] && [ "${changed:-0}" -gt 0 ]; then
    echo "[bin] 已更新 -> 重启服务"
    ssh -i "$HOME/.ssh/vps_hs" -p 28545 root@vps04.hs3434.top \
      'systemctl restart wxlike-go && echo RESTARTED || echo RESTART_FAIL' 2>&1 | grep -v setlocale
  else
    echo "[bin] 无变化(或 dry 预览)"
  fi
}

case "$MODE" in
  all) echo "== web =="; do_web; echo "== bin =="; do_bin ;;
  web) echo "== web =="; do_web ;;
  bin) echo "== bin =="; do_bin ;;
  dry) echo "== 预览: web 差异 =="; rsync -an --dry-run -e "$SSH" --exclude 'uploads' "$SRC/web/" "$VPS:$DEST/web/" 2>&1 | grep -v setlocale | tail -6
       echo "== 预览: bin 差异 =="; rsync -an --checksum --dry-run -e "$SSH" "$SRC/bin/wxlike-go" "$VPS:$DEST/bin/wxlike-go" 2>&1 | grep -v setlocale ;;
  *) echo "用法: sync2vps.sh <dry|web|bin|all>"; exit 1;;
esac
echo "sync2vps [$MODE] 完成"
