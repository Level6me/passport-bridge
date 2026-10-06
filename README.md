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

### 方式一：飞书机器人插件面板一键安装（推荐）

本仓库原生兼容 `feishu-bot-plugin` 规范，可直接通过飞书机器人的插件管理中心一键接入：

1. **面板一键点击**：
   在飞书发送 `/plugin` 呼出插件管理面板，在市场中找到 `📟 FoloToy AI Passport 硬件随身对讲与看板网关`，点击 **📥 一键安装** 即可。
2. **通过 GitHub 链接安装**：
   在插件面板中点击 **从 GitHub 安装插件**，输入本仓库链接：
   ```text
   https://github.com/Level6me/passport-bridge
   ```
   面板将自动执行克隆、依赖构建（`setup.sh`）与插件装载。
3. **打开硬件控制台**：
   在任意飞书聊天窗口发送 `/passport` 或 `/hardware` 即可呼出可视化硬件交互控制台。

---

### 方式二：独立网关服务器一键部署（一行命令）

在无机器人的独立服务器或树莓派网关设备上，可直接使用独立脚本一键部署与托管：

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
