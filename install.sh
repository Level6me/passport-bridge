#!/usr/bin/env bash
# ==============================================================================
# Passport Bridge 一键部署脚本 (FoloToy AI Passport Standalone Gateway)
# 支持环境: Debian / Ubuntu / Raspberry Pi OS / macOS
# ==============================================================================

set -e

GREEN='\033[032m'
YELLOW='\033[033m'
RED='\033[031m'
BLUE='\033[034m'
PLAIN='\033[0m'

info() {
    echo -e "${GREEN}[INFO]${PLAIN} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${PLAIN} $1"
}

error() {
    echo -e "${RED}[ERROR]${PLAIN} $1"
    exit 1
}

echo -e "${BLUE}"
cat << "BANNER"
  ____                                    _     ____       _     _             
 |  _ \ __ _ ___ ___ _ __   ___  _ __ ___| |_  | __ ) _ __(_) __| | __ _  ___  
 | |_) / _` / __/ __| '_ \ / _ \| '__/ __| __| |  _ \| '__| |/ _` |/ _` |/ _ \ 
 |  __/ (_| \__ \__ \ |_) | (_) | |  \__ \ |_  | |_) | |  | | (_| | (_| |  __/ 
 |_|   \__,_|___/___/ .__/ \___/|_|  |___/\__| |____/|_|  |_|\__,_|\__, |\___| 
                    |_|                                            |___/       
BANNER
echo -e "${PLAIN}"
info "开始部署 FoloToy AI Passport 独立中继网关 (passport-bridge)..."

# 1. 确定安装路径
INSTALL_DIR="${INSTALL_DIR:-$HOME/passport-bridge}"
REPO_URL="https://github.com/Level6me/passport-bridge.git"

if [ -n "$GITHUB_TOKEN" ]; then
    AUTH_REPO_URL="https://${GITHUB_TOKEN}@github.com/Level6me/passport-bridge.git"
else
    AUTH_REPO_URL="$REPO_URL"
fi

# 2. 依赖检查
info "检查系统依赖..."
for cmd in git python3 curl; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        error "未找到系统命令: $cmd，请先安装该基础组件。"
    fi
done

# 3. 拉取或更新仓库代码
if [ -d "$INSTALL_DIR/.git" ]; then
    info "检测到已存在安装目录: $INSTALL_DIR，正在更新最新代码..."
    cd "$INSTALL_DIR"
    git fetch origin main || true
    git reset --hard origin/main || true
else
    info "正在拉取代码至目录: $INSTALL_DIR ..."
    mkdir -p "$(dirname "$INSTALL_DIR")"
    if [ -d "$INSTALL_DIR" ] && [ "$(ls -A "$INSTALL_DIR")" ]; then
        warn "目标目录已存在非空内容，跳过 clone，直接使用当前目录。"
        cd "$INSTALL_DIR"
    else
        git clone "$AUTH_REPO_URL" "$INSTALL_DIR" || error "拉取仓库失败，如果是私有仓库，请在执行命令前附带 GITHUB_TOKEN=xxx 环境变量。"
        cd "$INSTALL_DIR"
    fi
fi

# 4. 创建 Python 虚拟环境与安装依赖
info "配置 Python 虚拟环境..."
if [ ! -d "venv" ]; then
    python3 -m venv venv || error "创建 Python 虚拟环境失败，请确认是否安装了 python3-venv。"
fi

info "安装与更新 Python 运行时依赖..."
./venv/bin/pip install --upgrade pip -q
./venv/bin/pip install -r requirements.txt aiohttp lark-oapi edge-tts -q

# 5. 初始化配置文件
if [ ! -f "config.json" ]; then
    if [ -f "config.example.json" ]; then
        info "未检测到 config.json，正在从模板生成..."
        cp config.example.json config.json
        warn "已生成默认 config.json，请稍后填入真实的飞书 app_id 与 app_secret！"
    fi
else
    info "已存在 config.json，保留现有配置。"
fi

mkdir -p data

# 6. 进程守护托管 (PM2)
if command -v pm2 >/dev/null 2>&1; then
    info "使用 PM2 配置后台常驻服务..."
    if pm2 describe passport-bridge >/dev/null 2>&1; then
        info "重启已有 PM2 服务 (passport-bridge)..."
        pm2 restart passport-bridge --update-env
    else
        info "注册并启动全新 PM2 服务 (passport-bridge)..."
        pm2 start server.py --name passport-bridge --cwd "$INSTALL_DIR" --interpreter "$INSTALL_DIR/venv/bin/python3"
    fi
    pm2 save || true
    info "PM2 守护配置成功！"
else
    warn "未检测到 pm2 命令，跳过自动守护托管。"
    warn "您可以直接通过以下命令前台运行:"
    warn "  cd $INSTALL_DIR && ./venv/bin/python3 server.py"
fi

# 7. 部署完成输出
echo ""
echo -e "${GREEN}====================================================${PLAIN}"
echo -e "${GREEN}🎉 Passport Bridge 部署成功！${PLAIN}"
echo -e "• 安装目录: ${BLUE}$INSTALL_DIR${PLAIN}"
echo -e "• 服务端口: UDP 8765 (自发现) / WebSocket 8765 (数据与音频流)"
if command -v pm2 >/dev/null 2>&1; then
    echo -e "• 查看状态: ${YELLOW}pm2 status passport-bridge${PLAIN}"
    echo -e "• 实时日志: ${YELLOW}pm2 logs passport-bridge${PLAIN}"
    echo -e "• 重启服务: ${YELLOW}pm2 restart passport-bridge${PLAIN}"
fi
echo -e "${GREEN}====================================================${PLAIN}"
