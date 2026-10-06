#!/usr/bin/env bash
# ==============================================================================
# Passport Bridge 插件生命周期构建与环境初始化脚本 (setup.sh)
# 供 feishu-bot-plugin 面板一键安装 / 更新钩子调用
# ==============================================================================

set -e

PLUGIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PLUGIN_DIR"

echo "[PassportBridge:Setup] 正在执行插件初始化配置..."

# 1. 寻找 Python 环境 (优先探测宿主虚拟环境，其次当前虚拟环境，最后系统 Python)
HOST_VENV="../../venv/bin/python3"
LOCAL_VENV="./venv/bin/python3"

if [ -f "$HOST_VENV" ]; then
    PYTHON_BIN="$HOST_VENV"
    echo "[PassportBridge:Setup] 使用飞书机器人宿主虚拟环境: $PYTHON_BIN"
elif [ -f "$LOCAL_VENV" ]; then
    PYTHON_BIN="$LOCAL_VENV"
    echo "[PassportBridge:Setup] 使用本地虚拟环境: $PYTHON_BIN"
else
    PYTHON_BIN="$(command -v python3 || echo "python3")"
    echo "[PassportBridge:Setup] 使用系统 Python: $PYTHON_BIN"
fi

# 2. 安装/更新依赖
if [ -f "requirements.txt" ]; then
    echo "[PassportBridge:Setup] 正在安装 Python 依赖项..."
    "$PYTHON_BIN" -m pip install -r requirements.txt -q || {
        echo "[PassportBridge:Setup] 依赖项安装出现警告，尝试降级容错安装..."
        "$PYTHON_BIN" -m pip install aiohttp edge-tts SpeechRecognition -q || true
    }
fi

# 3. 初始化数据存储目录
mkdir -p data
mkdir -p plugin_data

# 4. 初始化配置文件 (若不存在)
if [ ! -f "config.json" ] && [ -f "config.example.json" ]; then
    cp config.example.json config.json
    echo "[PassportBridge:Setup] 已从模板初始化 config.json"
fi

echo "[PassportBridge:Setup] 插件初始化构建完成！已就绪。"
exit 0
