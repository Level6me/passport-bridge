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

### 1. 配置文件
复制配置模板并填写飞书应用凭证：
```bash
cp config.example.json config.json
```

### 2. 依赖安装
```bash
pip install aiohttp lark-oapi edge-tts
```

### 3. 运行服务 (PM2)
```bash
pm2 start server.py --name passport-bridge --interpreter python3
```
