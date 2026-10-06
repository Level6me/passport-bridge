# 📟 Passport Bridge (`passport-bridge`)

FoloToy AI Passport 硬件随身对讲、全双工通信网关与飞书交互控制中继服务。

---

## 🌟 核心特性
- **双模式支持**：
  - **独立网关服务 (Standalone Gateway)**：通过 `server.py` 启动，PM2 常驻守护，提供 UDP 8765 自动发现与 WebSocket 8765 全双工音频流处理。
  - **飞书插件扩展 (Bot Plugin)**：提供 `plugin.py` 模块化卡片交互与管理。
- **PTT 对讲与语音流**：长按硬件 OK 键实时录音，本地转写并唤醒 AI 回复，实时 TTS 语音流下发硬件播放。
- **飞书卡片控制台**：支持设备配对鉴权、状态监控、音色切换、多状态看板同步与物理紧急熔断。
- **固件工程解耦**：硬件端 ESP-IDF / XiaoZhi 固件工程已拆分至独立项目 `Letgo`（`main/boards/folotoy/ai-passport`）。

---

## 🚀 启动与部署

### 快速一键部署（一行命令）

在目标 Linux / 树莓派设备上直接运行一键安装脚本（自动克隆代码、搭建虚拟环境、安装依赖并配置 PM2 进程守护）：

```bash
# 私有仓库部署（推荐）：
curl -fsSL -H "Authorization: token YOUR_GITHUB_TOKEN" \
  https://raw.githubusercontent.com/Level6me/passport-bridge/main/install.sh | GITHUB_TOKEN=YOUR_GITHUB_TOKEN bash

# 公开仓库部署：
curl -fsSL https://raw.githubusercontent.com/Level6me/passport-bridge/main/install.sh | bash
```

> **提示**：安装完成后，编辑生成的 `config.json` 填入飞书凭证，执行 `pm2 restart passport-bridge` 即可。

---

### 手动部署流程

1. **克隆代码与安装依赖**：
   ```bash
   git clone https://github.com/Level6me/passport-bridge.git
   cd passport-bridge
   python3 -m venv venv
   ./venv/bin/pip install -r requirements.txt aiohttp lark-oapi edge-tts
   ```

2. **配置文件初始化**：
   ```bash
   cp config.example.json config.json
   ```

3. **PM2 守护进程启动**：
   ```bash
   pm2 start server.py --name passport-bridge --cwd $(pwd) --interpreter ./venv/bin/python3
   pm2 save
   ```
