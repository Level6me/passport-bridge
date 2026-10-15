# FoloToy AI Passport 网关后续架构优化路线规划文档 (Roadmap)

本文档面向随身智能硬件 **FoloToy AI Passport** 与独立网关服务 **passport-bridge**，详细阐述后续进阶功能体系的架构设计、数据流向与落地实施方案。已在线上闭环实现的方案一（Edge-TTS 逐句流式切片抢跑与重叠流水线）与方案二（硬件物理打断 Barge-in 深度熔断）作为基石，与方案三、方案四、方案五共同构成完整的全链路生产级网关技术蓝图。

---

## 目录
1. [已落地架构回顾（方案一与方案二）](#1-已落地架构回顾方案一与方案二)
2. [方案三：局域网极速 OTA 固件分发架构设计](#2-方案三局域网极速-ota-固件分发架构设计)
3. [方案四：多级备用 TTS 智能降级与连接保活架构设计](#3-方案四多级备用-tts-智能降级与连接保活架构设计)
4. [方案五：SQLite WAL 单向只读监听与进程解耦设计](#4-方案五sqlite-wal-单向只读监听与进程解耦设计)
5. [综合实施甘特图与部署规划](#5-综合实施甘特图与部署规划)

---

## 1. 已落地架构回顾（方案一与方案二）

### 1.1 方案一：Edge-TTS 逐句流式切片抢跑与重叠流水线 (已实现)
- **核心机制**：在调用底层 AI 推理引擎时采用 `--output-format stream-json` 流式管道，实时捕获大模型输出的增量分片（`text_delta`）。
- **智能切句器 (`SentenceChunker`)**：依据强断句符号（`。！？\n`）与弱分句符号（`，；`）结合首句短阈值（8字）进行切片，首句成型即刻触发 `sentence_queue` 丢入 TTS 连接池。
- **重叠流水线 (Overlapped Pipeline)**：`tts_worker` 协程边合成边输出 PCM，`playback_worker` 协程以 1920 字节（60ms）严格定速推流至硬件 WebSocket。首字发声延迟直接由全文等待的 6~12 秒压缩至 **1.2~1.8 秒**，且后续分句在硬件播放第一句的 2~3 秒时间内已在后台合成就绪，实现全链路零顿挫播放。

### 1.2 方案二：硬件物理打断 Barge-in 深度熔断机制 (已实现)
- **核心机制**：设备端长按确认键（Barge-in）或开辟新录音时，网关执行 `abort_active_session` 深度熔断。
- **全链路清除**：
  1. 通过 `os.killpg(proc.pid, signal.SIGKILL)` 强行终止正在运行的 `agy` 进程组及所有子工具链，杜绝 CPU、Token 与上下文浪费；
  2. 取消 TTS 语音合成任务及定速推流协程，清空 `sentence_queue` 与 `pcm_queue`；
  3. 清除硬件上传录音缓冲区，下发 `voice_reply_end`、`ai_speech_end` 与 `ai_state: idle`，瞬间恢复硬件为待命状态。

---

## 2. 方案三：局域网极速 OTA 固件分发架构设计

### 2.1 业务痛点
当前固件编译在 GitHub Actions 云端流水线构建完成，设备每次 OTA 需要经由外网 GitHub Releases 下载数十兆的完整固件（`full.bin`）。在弱网或公网抖动环境下，下载缓慢且极易发生中断导致升级失败。

### 2.2 架构拓扑
```mermaid
flowchart LR
    A[GitHub Actions 流水线] -->|自动同步/下载产物| B[网关本地固件仓储目录]
    B --> C[passport-bridge 本地 HTTP OTA 服务 8765/ota]
    C -->|局域网千兆/百兆内网推送| D[FoloToy 硬件设备 10.0.0.23]
    D -->|分块断点续传/校验MD5| E[ESP32 OTA 双分区无缝重启]
```

### 2.3 核心实现方案
1. **本地 OTA 托管端点**：
   在 `passport-bridge` 的 aiohttp 路由器中挂载 `/ota/firmware` 与 `/ota/version`：
   - `GET /ota/version`：返回最新编译版本号、Run ID、SHA256、文件大小及发布说明；
   - `GET /ota/firmware`：采用 `aiohttp.web.FileResponse` 支持 HTTP Range 断点分块传输，局域网下载速率可达 8~12 MB/s，数秒内即可完成整包下载。
2. **主动 OTA 触发通道**：
   网关在检测到本地存在新固件后，通过已建立的 WebSocket 下发指令：
   ```json
   {
     "type": "ota_notify",
     "version": "1.0.4-37423795163",
     "url": "http://10.0.0.2:8765/ota/firmware",
     "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
     "size": 3145728
   }
   ```
3. **安全双分区回退机制**：
   ESP32 固件端使用 `esp_ota_begin()`、`esp_ota_write()`、`esp_ota_end()`，校验 SHA256 成功后标记新分区为 `ESP_OTA_IMG_NEW`，重启若连续 3 次启动失败自动回滚至上一稳定版本。

---

## 3. 方案四：多级备用 TTS 智能降级与连接保活架构设计

### 3.1 业务痛点
公网 Edge-TTS 依赖微软消费级端点，偶尔会受到网络波动、区域限制或服务维护（例如返回 403 / "Our services aren't available right now"）影响。一旦外网 TTS 出现故障，硬件语音播报将完全哑巴。

### 3.2 多级智能降级级联拓扑
```mermaid
flowchart TD
    A[待合成小句切片] --> B{第一级: 常驻 Edge-TTS 连接池}
    B -->|合成成功| S[输出 PCM 流至硬件]
    B -->|超时/403/断连| C{第二级: 本地轻量离线 TTS (Piper / Sherpa-ONNX)}
    C -->|本地毫秒级推理| S
    C -->|若本地模型未装载| D{第三级: 微软 Azure 官方企业 API (备用)}
    D -->|兜底保障| S
    D -->|全部异常| E[硬件播放内置蜂鸣声/提示音兜底]
```

### 3.3 核心实现细节
1. **多后端统一抽象接口 (`BaseTTSEngine`)**：
   定义抽象基类，规范输入文本与 `on_pcm_chunk` 异步回调，实现热插拔无感切换。
2. **本地 Sherpa-ONNX / Piper 轻量引擎部署**：
   - 在树莓派/宿主机安装轻量 CPU 推理依赖 `sherpa-onnx` 或 `piper-tts`（中文 VITS 模型，内存占用仅约 40MB）；
   - 完全离线运行，断网状态下仍能以 0.15 秒实时率合成优质普通话女声，从物理上杜绝外部公网瘫痪风险。
3. **长连接心跳保活与自动重连**：
   针对常驻 Edge-TTS WebSocket 连接，实现指数退避重连机制与定时 ping/pong 保活；在遇到 403 时自动触发时钟漂移自适应校准（Clock Skew Adjustment），防止系统时间偏差导致 token 失效。

---

## 4. 方案五：SQLite WAL 单向只读监听与进程解耦设计

### 4.1 业务痛点
当前 `passport-bridge` 与 `antigravity-feishu-bot` 共享访问 `/home/jiang/github/antigravity-feishu-bot/antigravity_bot.db`。若直接进行并发读写，容易触发 SQLite 数据库锁（`database is locked`），且跨进程强耦合存在相互干扰隐患。

### 4.2 架构解耦设计
```mermaid
flowchart LR
    A[Feishu Bot 主进程 (PM2 ID: 1)] -->|读写操作 (Read/Write)| B[(antigravity_bot.db (WAL模式))]
    B -->|WAL 日志文件变更| C[Linux inotify 异步文件监听]
    C --> D[passport-bridge 只读监听器]
    D -->|URI: file:...db?mode=ro| E[只读瞬时查询/上下文同步]
```

### 4.3 核心实现规范
1. **只读连接模式强制约束**：
   `passport-bridge` 在连接 SQLite 时一律采用 SQLite 官方 URI 只读语法：
   ```python
   conn = sqlite3.connect("file:/path/to/antigravity_bot.db?mode=ro", uri=True)
   ```
   严禁在此连接中执行任何 `INSERT`、`UPDATE`、`DELETE` 操作，杜绝持有写锁。
2. **inotify 事件驱动替代定时轮询**：
   采用 `asyncio` 与 Linux 内核 `inotify` 监听 `antigravity_bot.db-wal` 变更事件；当飞书控制台切换项目或更新对话时，内核级事件直接触发网关同步，将轮询消耗的 CPU 占用降至绝对零，同时同步延迟由 1 秒缩短至 **<10 毫秒**。
3. **独立状态总线持久化**：
   `passport-bridge` 自身的硬件配对白名单、网络状态、设备偏好等数据完全独立持久化于本地 `data/passport_storage.db`，实现系统边界彻底解耦。

---

## 5. 综合实施甘特图与部署规划

| 阶段 | 方案 | 优先级 | 预期周期 | 交付产物 |
| :--- | :--- | :---: | :---: | :--- |
| **已完成** | **方案一：流式切句抢跑与流水线** | P0 | 已交付 | `SentenceChunker` + `stream_pcm_realtime` 流式切片 |
| **已完成** | **方案二：Barge-in 深度熔断** | P0 | 已交付 | `abort_active_session` + `os.killpg` 进程组秒杀 |
| **下一阶段** | **方案四：本地离线 TTS 兜底降级** | P1 | 1~2 天 | 集成 Sherpa-ONNX 离线模型，实现断网语音高可用 |
| **下一阶段** | **方案五：SQLite 只读解耦与 inotify** | P1 | 1 天 | `inotify` 驱动的零延迟项目/状态同步引擎 |
| **后续规划** | **方案三：局域网 OTA 极速分发** | P2 | 2~3 天 | `/ota/firmware` 挂载与 ESP32 固件端双分区校验升级 |

---
*文档归档路径：`/home/jiang/github/passport-bridge/docs/ROADMAP_OPTIMIZATIONS.md`*  
*系统维护责任人：Antigravity Agent Pair Programming*
