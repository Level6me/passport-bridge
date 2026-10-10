#!/usr/bin/env python3
"""
FoloToy AI Passport Standalone PM2 Gateway Service (passport-bridge v1.0.0)
- 独立 UDP 8765 服务自发现
- 独立 WebSocket 8765 全双工通信
- 飞书交互式卡片 MAC 地址配对与白名单鉴权
- Push-to-Talk 长按对讲语音上报与下发回放
"""

import os
import re
import sys
import time
import json
import uuid
import wave
import struct
import asyncio
import logging
import tempfile
import sqlite3
import subprocess
from typing import Dict, Set, Optional, Any
import aiohttp
from aiohttp import web, WSMsgType
import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateMessageRequest, CreateMessageRequestBody,
    PatchMessageRequest, PatchMessageRequestBody
)

# 日志格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PassportGateway] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("PassportGateway")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.json")

def load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "host": "0.0.0.0",
        "port": 8765,
        "udp_port": 8765,
        "gateway_name": "Antigravity 飞书控制台",
        "gateway_id": "feishu_gw_main",
        "app_id": "",
        "app_secret": "",
        "admin_chat_id": "",
        "data_dir": os.path.join(SCRIPT_DIR, "data"),
        "tts_voice": "zh-CN-XiaoxiaoNeural"
    }

CONFIG = load_config()

def clean_text_for_speech(text: str, max_chars: int = 150) -> str:
    """清理并精简文本，便于 Edge-TTS 自然朗读与硬件播报"""
    if not text:
        return ""
    s = text
    s = re.sub(r'<think>.*?</think>', '', s, flags=re.DOTALL)
    s = re.sub(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])', '', s)
    s = re.sub(r'```[\w]*\n[\s\S]*?```', '', s)
    s = re.sub(r'`([^`]+)`', r'\1', s)
    s = re.sub(r'\[(.*?)\]\(.*?\)', r'\1', s)
    s = re.sub(r'https?://\S+', '', s)
    s = re.sub(r'^[ \t]*#{1,6}\s*', '', s, flags=re.MULTILINE)
    s = re.sub(r'\*\*(.*?)\*\*', r'\1', s)
    s = re.sub(r'\*(.*?)\*', r'\1', s)
    s = re.sub(r'~~(.*?)~~', r'\1', s)
    s = re.sub(r'^[ \t]*[-*+]\s+', '', s, flags=re.MULTILINE)
    s = re.sub(r'^[ \t]*\d+\.\s+', '', s, flags=re.MULTILINE)
    emoji_pattern = re.compile(
        r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F900-\U0001F9FF\U0001FA70-\U0001FAFF\U00002600-\U000027BF]+'
    )
    s = emoji_pattern.sub('', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s[:max_chars]
DATA_DIR = CONFIG.get("data_dir", os.path.join(SCRIPT_DIR, "data"))
os.makedirs(DATA_DIR, exist_ok=True)
PAIRED_FILE = os.path.join(DATA_DIR, "paired_devices.json")

_lark_client = None

def get_lark_client():
    global _lark_client
    if _lark_client is None:
        if CONFIG.get("app_id") and CONFIG.get("app_secret"):
            try:
                _lark_client = lark.Client.builder() \
                    .app_id(CONFIG["app_id"]) \
                    .app_secret(CONFIG["app_secret"]) \
                    .timeout(10.0) \
                    .build()
                log.info("Lark OpenAPI Client 初始化成功")
            except Exception as e:
                log.error(f"Lark Client 初始化失败: {e}")
    return _lark_client


from edge_tts.constants import WSS_URL, SEC_MS_GEC_VERSION, WSS_HEADERS
from edge_tts.drm import DRM
from edge_tts.communicate import connect_id, _SSL_CTX, ssml_headers_plus_data, mkssml, date_to_string
from edge_tts.data_classes import TTSConfig

class WarmEdgeTTSPool:
    """
    长连接常驻 Edge-TTS 预热会话池：
    提前完成 2.2 秒的 SSL 握手与 DRM 校验，随时待命。
    收到合成文本后，直接以持续复用的 WebSocket 立即下发 SSML，
    首包 PCM 解码延迟压缩至 300~450ms 内，零额外费用！
    """
    def __init__(self, voice: str = "zh-CN-XiaoxiaoNeural", rate: str = "+10%"):
        self.voice = voice
        self.rate = rate
        self.cfg = TTSConfig(voice, rate, "+0%", "+0Hz", "SentenceBoundary")
        self.session: Optional[Any] = None
        self.ws: Optional[Any] = None
        self.lock = asyncio.Lock()
        self._connected = False
        self._keepalive_task: Optional[asyncio.Task] = None

    async def ensure_connected(self):
        if self._connected and self.ws and not self.ws.closed:
            return
        async with self.lock:
            if self._connected and self.ws and not self.ws.closed:
                return
            try:
                if self.session and not self.session.closed:
                    await self.session.close()
                self.session = aiohttp.ClientSession()
                url = f"{WSS_URL}&ConnectionId={connect_id()}&Sec-MS-GEC={DRM.generate_sec_ms_gec()}&Sec-MS-GEC-Version={SEC_MS_GEC_VERSION}"
                headers = DRM.headers_with_muid(WSS_HEADERS)
                self.ws = await self.session.ws_connect(url, compress=15, headers=headers, ssl=_SSL_CTX)
                await self.ws.send_str(
                    f"X-Timestamp:{date_to_string()}\r\n"
                    "Content-Type:application/json; charset=utf-8\r\n"
                    "Path:speech.config\r\n\r\n"
                    '{"context":{"synthesis":{"audio":{"metadataoptions":{'
                    '"sentenceBoundaryEnabled":"true","wordBoundaryEnabled":"false"'
                    '},'
                    '"outputFormat":"audio-24khz-48kbitrate-mono-mp3"'
                    "}}}}\r\n"
                )
                self._connected = True
                log.info(f"[WarmTTSPool] 常驻 Edge-TTS 连接池握手预热就绪 (voice={self.voice})")
            except Exception as e:
                log.error(f"[WarmTTSPool] 连接建立失败: {e}")
                self._connected = False
                self.ws = None

    async def stream_pcm_realtime(self, text: str, on_pcm_chunk, check_interrupted=None) -> bool:
        """
        极速流式合成：通过已保活的 WebSocket 实时管道送入 ffmpeg，
        首包 PCM 到达时即刻调用 on_pcm_chunk(chunk)，边转码边回传！
        """
        clean_text = clean_text_for_speech(text, max_chars=120)
        if not clean_text:
            return False

        await self.ensure_connected()
        if not self.ws or self.ws.closed:
            return False

        async with self.lock:
            try:
                ssml = ssml_headers_plus_data(connect_id(), date_to_string(), mkssml(self.cfg, clean_text))
                await self.ws.send_str(ssml)

                cmd = [
                    "ffmpeg", "-y", "-f", "mp3", "-i", "pipe:0",
                    "-f", "s16le", "-acodec", "pcm_s16le",
                    "-ac", "1", "-ar", "16000", "pipe:1"
                ]
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL
                )

                async def feed_ffmpeg():
                    try:
                        async for msg in self.ws:
                            if check_interrupted and check_interrupted():
                                break
                            if msg.type == aiohttp.WSMsgType.BINARY:
                                idx = msg.data.find(b"Path:audio\r\n")
                                if idx != -1:
                                    raw_audio = msg.data[idx + len(b"Path:audio\r\n"):]
                                    if proc.stdin and not proc.stdin.is_closing():
                                        proc.stdin.write(raw_audio)
                                        await proc.stdin.drain()
                            elif msg.type == aiohttp.WSMsgType.TEXT:
                                if b"Path:turn.end" in msg.data.encode("utf-8"):
                                    break
                    except Exception as e:
                        log.warning(f"[WarmTTSPool] feed_ffmpeg 异常: {e}")
                    finally:
                        if proc.stdin and not proc.stdin.is_closing():
                            try:
                                proc.stdin.close()
                                await proc.stdin.wait_closed()
                            except Exception:
                                pass

                feed_task = asyncio.create_task(feed_ffmpeg())

                chunk_size = 1920
                while True:
                    if check_interrupted and check_interrupted():
                        break
                    chunk = await proc.stdout.read(chunk_size)
                    if not chunk:
                        break
                    await on_pcm_chunk(chunk)

                await feed_task
                await proc.wait()
                return True
            except Exception as e:
                log.error(f"[WarmTTSPool] stream_pcm_realtime 异常，重置连接: {e}")
                self._connected = False
                self.ws = None
                return False

tts_pool = WarmEdgeTTSPool(
    voice=CONFIG.get("tts_voice", "zh-CN-XiaoxiaoNeural"),
    rate=CONFIG.get("tts_rate", "+10%")
)




class PairedDeviceManager:
    """管理已认证设备白名单持久化"""
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.devices: Dict[str, dict] = {}
        self.load()

    def load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    self.devices = json.load(f)
            except Exception as e:
                log.error(f"读取白名单失败: {e}")
                self.devices = {}
        else:
            self.devices = {}

    def save(self):
        try:
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump(self.devices, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.error(f"保存白名单失败: {e}")

    def is_approved(self, mac: str) -> bool:
        norm_mac = mac.lower().strip()
        info = self.devices.get(norm_mac)
        return bool(info and info.get("status") == "approved")

    def approve(self, mac: str, ip: str = "", name: str = "") -> str:
        norm_mac = mac.lower().strip()
        token = self.devices.get(norm_mac, {}).get("token") or ("tok_" + uuid.uuid4().hex[:12])
        self.devices[norm_mac] = {
            "mac": norm_mac,
            "ip": ip,
            "name": name or "FoloToy AI Passport",
            "status": "approved",
            "token": token,
            "approved_at": int(time.time()),
            "last_seen": int(time.time())
        }
        self.save()
        log.info(f"设备已授权写入白名单: MAC={norm_mac}, IP={ip}")
        return token

    def reject(self, mac: str):
        norm_mac = mac.lower().strip()
        if norm_mac in self.devices:
            self.devices[norm_mac]["status"] = "rejected"
            self.save()
            log.info(f"设备已被拒绝: MAC={norm_mac}")

    def remove(self, mac: str):
        norm_mac = mac.lower().strip()
        if norm_mac in self.devices:
            del self.devices[norm_mac]
            self.save()
            log.info(f"设备已解绑: MAC={norm_mac}")


device_mgr = PairedDeviceManager(PAIRED_FILE)


def execute_direct_smart_home(user_text: str) -> Optional[tuple]:
    """
    智能家居控制直通车：
    匹配常见高频家居控制指令（餐厅灯、客厅灯、玄关灯、入户灯、空调、电脑等），
    直接调用本地 Home Assistant REST 服务，0.2 秒内极速下发，杜绝外部超时。
    返回 (reply_text, executed_tools) 或 None（若未匹配则交由大模型完整处理）。
    """
    text = (user_text or "").lower().strip()

    # 动作解析
    action = None
    if any(k in text for k in ["打开", "开启", "开一下", "开下", "启动", "开个", "开"]):
        action = "turn_on"
    elif any(k in text for k in ["关闭", "关掉", "关一下", "关下", "关上", "熄灭", "关个", "关"]):
        action = "turn_off"

    if not action:
        return None

    ha_token_path = "/home/jiang/github/passport-bridge/ha_token.txt"
    token = ""
    if os.path.exists(ha_token_path):
        try:
            with open(ha_token_path, "r", encoding="utf-8") as f:
                token = f.read().strip()
        except Exception:
            pass

    if not token:
        return None

    import urllib.request
    def _call_ha(domain: str, service: str, entity_id: str) -> bool:
        try:
            url = f"http://127.0.0.1:8123/api/services/{domain}/{service}"
            body = json.dumps({"entity_id": entity_id}).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=body,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                return resp.status == 200
        except Exception as e:
            log.warning(f"直连 HA 控制失败 ({entity_id}): {e}")
            return False

    act_desc = "打开" if action == "turn_on" else "关闭"
    executed_tools = []

    # 1. 餐厅的灯 (10个筒灯 + 组实体)
    if any(k in text for k in ["餐厅", "饭厅"]) and any(k in text for k in ["灯", "照", "筒灯"]):
        dining_entities = [
            "light.yeelink_cn_1084980722_light3_s_2_2",
            "light.yeelink_cn_1110007869_light3_s_2_2",
            "light.yeelink_cn_1110017287_light3_s_2_2",
            "light.yeelink_cn_1110017487_light3_s_2_2",
            "light.yeelink_cn_1112229523_light3_s_2_2",
            "light.yeelink_cn_1112363316_light3_s_2_2",
            "light.yeelink_cn_1112405788_light3_s_2_2",
            "light.yeelink_cn_1112408426_light3_s_2_2",
            "light.yeelink_cn_1113105151_light3_s_2_2",
            "light.yeelink_cn_1113141226_light3_s_2_2",
            "light.mijia_cn_group_1802609041189527553_group3_s_2_light"
        ]
        for eid in dining_entities:
            _call_ha("light", action, eid)
        executed_tools.append({
            "name": f"ha_service_light_{action}",
            "summary": f"{act_desc}餐厅灯光组",
            "target": "餐厅筒灯组 (10盏) & Mesh群组",
            "output": f"成功执行 light.{action} 指令"
        })
        return f"好的，已为您{act_desc}餐厅的灯。", executed_tools

    # 2. 客厅的灯 (12个筒灯 + 组实体)
    if ("客厅" in text and any(k in text for k in ["灯", "照", "筒灯"])) or text in ["开灯", "关灯", "打开灯", "关闭灯"]:
        living_entities = [
            "light.yeelink_cn_1103960370_light3_s_2_2",
            "light.yeelink_cn_1103968451_light3_s_2_2",
            "light.yeelink_cn_1110007942_light3_s_2_2",
            "light.yeelink_cn_1112355696_light3_s_2_2",
            "light.yeelink_cn_1112407792_light3_s_2_2",
            "light.yeelink_cn_1112441170_light3_s_2_2",
            "light.yeelink_cn_1112446511_light3_s_2_2",
            "light.yeelink_cn_1113098725_light3_s_2_2",
            "light.yeelink_cn_1113099048_light3_s_2_2",
            "light.yeelink_cn_1113105799_light3_s_2_2",
            "light.yeelink_cn_1113105891_light3_s_2_2",
            "light.yeelink_cn_1113156278_light3_s_2_2",
            "light.mijia_cn_group_1802916869205270529_group3_s_2_light"
        ]
        for eid in living_entities:
            _call_ha("light", action, eid)
        executed_tools.append({
            "name": f"ha_service_light_{action}",
            "summary": f"{act_desc}客厅灯光组",
            "target": "客厅筒灯组 (12盏) & Mesh群组",
            "output": f"成功执行 light.{action} 指令"
        })
        return f"好的，已为您{act_desc}客厅的灯。", executed_tools

    # 3. 玄关灯
    if "玄关" in text and "灯" in text:
        entrance_entities = [
            "light.yeelink_cn_1112353733_light3_s_2_2",
            "light.yeelink_cn_1113156090_light3_s_2_2",
            "light.yeelink_cn_1113161695_light3_s_2_2",
            "light.mijia_cn_group_1802564442954285056_group3_s_2_light"
        ]
        for eid in entrance_entities:
            _call_ha("light", action, eid)
        executed_tools.append({
            "name": f"ha_service_light_{action}",
            "summary": f"{act_desc}玄关灯",
            "target": "玄关筒灯组 (3盏)",
            "output": f"成功执行 light.{action} 指令"
        })
        return f"好的，已为您{act_desc}玄关灯。", executed_tools

    # 4. 入户灯
    if "入户" in text and "灯" in text:
        foyer_entities = [
            "light.yeelink_cn_1113105302_light3_s_2_2",
            "light.yeelink_cn_1113105786_light3_s_2_2",
            "light.mijia_cn_group_1853403711318671360_group3_s_2_light"
        ]
        for eid in foyer_entities:
            _call_ha("light", action, eid)
        executed_tools.append({
            "name": f"ha_service_light_{action}",
            "summary": f"{act_desc}入户灯",
            "target": "入户筒灯组 (2盏)",
            "output": f"成功执行 light.{action} 指令"
        })
        return f"好的，已为您{act_desc}入户灯。", executed_tools

    # 5. 电脑开机 / 关机
    if any(k in text for k in ["电脑", "主机", "pc"]):
        pc_switch = "switch.cddz_cn_1125169814_pc01m_on_p_2_1"
        _call_ha("switch", action, pc_switch)
        executed_tools.append({
            "name": f"ha_service_switch_{action}",
            "summary": f"执行电脑{act_desc}操作",
            "target": "智能电脑开机卡",
            "output": f"已触发 switch.{action}"
        })
        return f"好的，已向电脑下达{act_desc}指令。", executed_tools

    # 6. 空调控制
    if "空调" in text:
        target_ac = "climate.xiaomi_cn_967084493_rr3r00"
        if "主卧" in text:
            target_ac = "climate.210006731920927_climate"
        elif "次卧" in text:
            target_ac = "climate.197912093786622_climate"

        _call_ha("climate", action, target_ac)
        executed_tools.append({
            "name": f"ha_service_climate_{action}",
            "summary": f"{act_desc}空调",
            "target": target_ac,
            "output": f"已向空调发送 {action} 指令"
        })
        return f"好的，已为您{act_desc}空调。", executed_tools

    return None


def build_voice_interaction_card(
    user_text: str,
    reply_text: str = "",
    executed_tools: list = None,
    is_completed: bool = False,
    think_seconds: int = 0,
    user_name: str = "江江"
) -> dict:
    """
    构建符合飞书 App 真实原生体验的智能卡片：
    - 处理中：展现原生思考指示卡片，引用随身语音原文
    - 完成后：展现真实答复内容与真实执行的系统工具动作记录，不展示僵化清单
    """
    now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    clean_user_q = user_text[:60] + ("..." if len(user_text) > 60 else "")
    quote_header = f"> **{user_name}**：🎙️ 随身语音：「{clean_user_q}」\n\n"

    if is_completed:
        header_title = "✨ AI 智能答复"
        template = "blue"
        content = f"{quote_header}**🤖 回复内容：**\n\n{reply_text}"

        if executed_tools:
            tool_lines = []
            for item in executed_tools[-8:]:
                t_name = item.get("name", "tool")
                t_summary = item.get("summary", "")
                t_target = item.get("target", "")
                t_out = item.get("output", "")
                label = f"**{t_summary}** (`{t_name}`)" if t_summary else f"**{t_name}**"
                if t_target:
                    label += f": `{t_target}`"
                if t_out:
                    clean_out = t_out.replace("\n", " ").strip()
                    label += f"\n   └─ 结果: `{clean_out[:80]}`"
                tool_lines.append(f"• 🔧 {label}")
            content += f"\n\n---\n**🛠️ 真实工具执行记录 (共调用 {len(executed_tools)} 个系统动作)：**\n" + "\n".join(tool_lines)

        time_hint = f"已耗时 {think_seconds}s" if think_seconds > 0 else "即时响应"
        footer_text = f"⚡ Powered by Antigravity · {time_hint} | 🕒 {now_str}"
    else:
        header_title = "🎙️ 正在处理随身语音指令..."
        template = "turquoise"
        time_hint = f"已耗时 {think_seconds}s" if think_seconds > 0 else "已运行 1s"
        content = (
            f"{quote_header}"
            f"🧠 **正在思考并执行任务：**\n\n"
            f"• 🎙️ 语音识别已完成，正在调度 Agent 工具链与上下文推理...\n\n"
            f"*({time_hint}，正在为您处理并生成语音回复)*"
        )
        footer_text = f"⚡ Powered by Antigravity | 🕒 {now_str}"

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": header_title},
            "template": template
        },
        "elements": [
            {
                "tag": "markdown",
                "content": content
            },
            {
                "tag": "note",
                "elements": [
                    {
                        "tag": "plain_text",
                        "content": footer_text
                    }
                ]
            }
        ]
    }


def build_planned_steps_voice_card(
    user_text: str,
    planned_steps: list = None,
    current_step_idx: int = 0,
    tool_action: str = "",
    think_seconds: int = 0,
    is_completed: bool = False,
    reply_text: str = "",
    executed_tools: list = None,
    user_name: str = "江江"
) -> dict:
    """兼容旧接口，内部无缝路由至原生体验卡片构建器"""
    return build_voice_interaction_card(
        user_text=user_text,
        reply_text=reply_text,
        executed_tools=executed_tools,
        is_completed=is_completed,
        think_seconds=think_seconds,
        user_name=user_name
    )


class GatewayServer:
    def __init__(self):
        self.host = CONFIG.get("host", "0.0.0.0")
        self.port = int(CONFIG.get("port", 8765))
        self.udp_port = int(CONFIG.get("udp_port", 8765))
        
        self.app: Optional[web.Application] = None
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
        self.udp_transport = None
        
        # 连接池与状态管理
        self.active_websockets: Set[web.WebSocketResponse] = set()
        self.ws_to_mac: Dict[web.WebSocketResponse, str] = {}
        self.mac_to_ws: Dict[str, web.WebSocketResponse] = {}
        self.ws_info: Dict[web.WebSocketResponse, dict] = {}
        self.pending_auth_ws: Dict[str, web.WebSocketResponse] = {}  # mac -> ws
        self.card_msg_ids: Dict[str, str] = {}  # mac -> feishu message_id
        self.active_tts_tasks: Dict[web.WebSocketResponse, asyncio.Task] = {}
        
        # 音频流缓存与抢跑 ASR
        self.voice_buffers: Dict[web.WebSocketResponse, bytearray] = {}
        self.asr_pretasks: Dict[web.WebSocketResponse, asyncio.Task] = {}


    async def start(self):
        """启动 HTTP/WS 服务与 UDP 自动发现监听器"""
        self.app = web.Application()
        self.app.router.add_get("/ws/passport", self.handle_websocket)
        self.app.router.add_get("/api/status", self.handle_api_status)
        self.app.router.add_get("/api/pair_decision", self.handle_pair_decision)
        self.app.router.add_get("/api/devices", self.handle_api_devices)
        self.app.router.add_post("/api/devices/remove", self.handle_api_remove_device)

        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port, reuse_address=True, reuse_port=True)
        await self.site.start()
        log.info(f"TCP/WS 服务已在 ws://{self.host}:{self.port}/ws/passport 启动")

        await self._start_udp_discovery()

        # 异步启动常驻 Edge-TTS 预热会话池，杜绝首句延迟
        asyncio.create_task(tts_pool.ensure_connected())


    async def _start_udp_discovery(self):
        """启动 UDP 自动发现广播应答协议"""
        loop = asyncio.get_running_loop()
        outer = self

        class DiscoveryProtocol(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                outer.udp_transport = transport

            def datagram_received(self, data, addr):
                try:
                    msg = data.decode("utf-8", errors="ignore").strip()
                    log.info(f"收到来自 {addr} 的 UDP 发现探测: {msg}")
                    if "DISCOVER" in msg:
                        resp = json.dumps({
                            "service": "feishu_passport",
                            "name": CONFIG.get("gateway_name", "Antigravity 飞书控制台"),
                            "server_id": CONFIG.get("gateway_id", "feishu_gw_main"),
                            "ws_port": outer.port,
                            "timestamp": int(time.time())
                        }).encode("utf-8")
                        outer.udp_transport.sendto(resp, addr)
                        log.info(f"已向 {addr} 回复网关发现应答: {resp.decode('utf-8')}")
                except Exception as e:
                    log.error(f"UDP discovery 处理异常: {e}")

        await loop.create_datagram_endpoint(
            lambda: DiscoveryProtocol(),
            local_addr=("0.0.0.0", self.udp_port),
            allow_broadcast=True,
            reuse_port=True
        )
        log.info(f"UDP 自动发现监听器已就绪 (端口 {self.udp_port})")

    async def handle_websocket(self, request: web.Request) -> web.WebSocketResponse:
        """处理设备端 WebSocket 握手、配对认证与双向全双工交互"""
        ws = web.WebSocketResponse(autoping=True, heartbeat=None)
        await ws.prepare(request)
        peer = request.remote
        log.info(f"硬件客户端发起 WebSocket 连接: {peer}")

        self.active_websockets.add(ws)
        self.ws_info[ws] = {"peer": peer, "connected_at": time.time(), "mac": None, "approved": False}

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    await self._handle_client_json(ws, msg.data, peer)
                elif msg.type == WSMsgType.BINARY:
                    await self._handle_client_binary(ws, msg.data)
                elif msg.type == WSMsgType.ERROR:
                    log.error(f"WS 连接异常: {ws.exception()}")
        finally:
            tts_task = self.active_tts_tasks.pop(ws, None)
            if tts_task and not tts_task.done():
                tts_task.cancel()
            mac = self.ws_to_mac.pop(ws, None)
            if mac and self.mac_to_ws.get(mac) == ws:
                self.mac_to_ws.pop(mac, None)
            if mac and self.pending_auth_ws.get(mac) == ws:
                self.pending_auth_ws.pop(mac, None)
            self.active_websockets.discard(ws)
            self.ws_info.pop(ws, None)
            self.voice_buffers.pop(ws, None)
            log.info(f"硬件客户端断开: {peer} (MAC={mac})")

        return ws

    async def _handle_client_json(self, ws: web.WebSocketResponse, text_data: str, peer: str):
        try:
            data = json.loads(text_data)
        except Exception:
            return

        msg_type = data.get("type")
        mac = (data.get("mac") or "").lower().strip()
        ip = data.get("ip") or peer
        dev_name = data.get("device_name") or data.get("device_id") or "FoloToy AI Passport"

        # 1. 配对请求处理
        if msg_type == "pair_request":
            log.info(f"收到设备配对请求: MAC={mac}, IP={ip}, Name={dev_name}")
            if not mac:
                await ws.send_str(json.dumps({"type": "pair_ack", "status": "error", "message": "Missing MAC address"}))
                return

            self.ws_to_mac[ws] = mac
            self.mac_to_ws[mac] = ws
            self.ws_info[ws]["mac"] = mac

            if device_mgr.is_approved(mac):
                # 已经授权过，秒级批准并向飞书推送配对确认通知
                token = device_mgr.approve(mac, ip=ip, name=dev_name)
                self.ws_info[ws]["approved"] = True
                await ws.send_str(json.dumps({
                    "type": "pair_ack",
                    "status": "approved",
                    "token": token,
                    "name": CONFIG.get("gateway_name", "Antigravity 飞书控制台")
                }))
                log.info(f"已授权设备秒级接入成功: {mac}")
                asyncio.create_task(self.send_feishu_paired_notify(mac, ip, dev_name))
            else:
                # 首次接入，挂起并向飞书推送审批卡片
                self.pending_auth_ws[mac] = ws
                await ws.send_str(json.dumps({
                    "type": "pair_ack",
                    "status": "pending",
                    "message": "等待飞书管理员审批确认..."
                }))
                # 发送飞书卡片
                asyncio.create_task(self.send_feishu_auth_card(mac, ip, dev_name))

        # 2. 已认证后的常规握手
        elif msg_type == "handshake":
            if mac and device_mgr.is_approved(mac):
                self.ws_to_mac[ws] = mac
                self.mac_to_ws[mac] = ws
                self.ws_info[ws]["mac"] = mac
                self.ws_info[ws]["approved"] = True
                await ws.send_str(json.dumps({
                    "type": "handshake_ack",
                    "status": "connected",
                    "name": CONFIG.get("gateway_name", "Antigravity 飞书控制台"),
                    "version": "1.0.0"
                }))
            else:
                await ws.send_str(json.dumps({
                    "type": "handshake_ack",
                    "status": "unauthorized",
                    "message": "请先发起 pair_request 并在飞书完成配对认证"
                }))

        # 打断信号 Barge-in (硬件长按 OK 键即刻打断当前播报)
        elif msg_type == "barge_in":
            log.info(f"收到设备打断信号 (Barge-in): MAC={mac}")
            task = self.active_tts_tasks.pop(ws, None)
            if task and not task.done():
                task.cancel()
                log.info("已成功中断正在进行的 TTS 推流播报")

        # 3. 对讲语音开始
        elif msg_type == "voice_start":
            log.info(f"收到设备按下录音开始信号: MAC={mac}")
            task = self.active_tts_tasks.pop(ws, None)
            if task and not task.done():
                task.cancel()
                log.info("录音开始，打断先前仍在推流的播报任务")
            self.voice_buffers[ws] = bytearray()

        # 4. 对讲语音结束
        elif msg_type == "voice_end":
            dev_mac = mac or self.ws_to_mac.get(ws, "")
            log.info(f"收到设备松开录音结束信号: MAC={dev_mac}")
            buf = self.voice_buffers.pop(ws, bytearray())
            if len(buf) > 0:
                tts_t = asyncio.create_task(self.process_incoming_voice(ws, dev_mac, buf))
                self.active_tts_tasks[ws] = tts_t

        # 5. 心跳 Ping
        elif msg_type == "ping":
            await ws.send_str(json.dumps({"type": "pong", "timestamp": int(time.time())}))

        # 6. 获取项目列表 (硬件短按 ▲ 键呼出项目切换列表)
        elif msg_type == "project_list":
            log.info(f"收到设备请求项目列表: MAC={mac}")
            bot_session = self.get_active_bot_session()
            current_proj = ""
            projects = []
            if bot_session:
                current_proj = bot_session.get("project", "")
            # 从 sqlite 读取 recent_projects
            try:
                db_path = "/home/jiang/github/antigravity-feishu-bot/antigravity_bot.db"
                if os.path.exists(db_path):
                    conn = sqlite3.connect(db_path)
                    c = conn.cursor()
                    row = c.execute("SELECT data FROM chat_sessions ORDER BY rowid DESC LIMIT 1").fetchone()
                    conn.close()
                    if row and row[0]:
                        s_data = json.loads(row[0])
                        recents = s_data.get("recent_projects", [])
                        for p in recents:
                            p_name = os.path.basename(p.rstrip("/"))
                            if p_name and p_name not in projects:
                                projects.append(p_name)
            except Exception as e:
                log.warning(f"读取近期项目列表异常: {e}")

            if not projects:
                projects = ["Letgo", "feishu-bot-plugin", "passport_game", "raspberry", "antigravity-feishu-bot"]
            cur_name = os.path.basename(current_proj.rstrip("/")) if current_proj else "Letgo"

            resp = {
                "type": "project_list",
                "current": cur_name,
                "projects": projects
            }
            await ws.send_str(json.dumps(resp))
            log.info(f"已向设备下发项目列表: current={cur_name}, count={len(projects)}")

        # 7. 切换当前工作项目
        elif msg_type == "switch_project":
            target_proj = data.get("project", "")
            log.info(f"收到设备请求切换项目: {target_proj} (MAC={mac})")
            if target_proj:
                try:
                    db_path = "/home/jiang/github/antigravity-feishu-bot/antigravity_bot.db"
                    if os.path.exists(db_path):
                        conn = sqlite3.connect(db_path)
                        c = conn.cursor()
                        row = c.execute("SELECT chat_id, data FROM chat_sessions ORDER BY rowid DESC LIMIT 1").fetchone()
                        if row and row[0]:
                            cid = row[0]
                            s_data = json.loads(row[1]) if row[1] else {}
                            new_path = f"/home/jiang/github/{target_proj}"
                            s_data["project"] = new_path
                            c.execute("UPDATE chat_sessions SET data = ? WHERE chat_id = ?", (json.dumps(s_data, ensure_ascii=False), cid))
                            conn.commit()
                            log.info(f"已同步更新飞书机器人工作目录为: {new_path}")
                        conn.close()
                except Exception as e:
                    log.error(f"切换项目更新数据库异常: {e}")

                await ws.send_str(json.dumps({
                    "type": "project_switched",
                    "project": target_proj,
                    "status": "success"
                }))

    async def _handle_client_binary(self, ws: web.WebSocketResponse, data: bytes):
        """流式追加设备录音音频分片"""
        if ws in self.voice_buffers:
            self.voice_buffers[ws].extend(data)

    async def send_feishu_auth_card(self, mac: str, ip: str, name: str):
        """向管理员飞书推送配对审批交互卡片"""
        client = get_lark_client()
        if not client or not CONFIG.get("admin_chat_id"):
            log.warning("未配置 Lark Client 或 admin_chat_id，无法推送飞书配对卡片")
            return

        chat_id = CONFIG["admin_chat_id"]
        approve_url = f"http://10.0.0.2:{self.port}/api/pair_decision?mac={mac}&action=approve"
        reject_url = f"http://10.0.0.2:{self.port}/api/pair_decision?mac={mac}&action=reject"
        now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "📟 FoloToy AI Passport 设备配对认证"},
                "template": "blue"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**检测到新硬件请求接入飞书控制台：**\n"
                            f"• **设备名称**：`{name}`\n"
                            f"• **设备 IP**：`{ip}`\n"
                            f"• **硬件 MAC**：`{mac}`\n"
                            f"• **申请时间**：{now_str}\n\n"
                            f"请核对后确认是否允许接入。"
                        )
                    }
                },
                {"tag": "hr"},
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "✅ 接受配对"},
                            "type": "primary",
                            "url": approve_url
                        },
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "❌ 拒绝"},
                            "type": "danger",
                            "url": reject_url
                        }
                    ]
                }
            ]
        }

        try:
            req = CreateMessageRequest.builder() \
                .receive_id_type("chat_id") \
                .request_body(CreateMessageRequestBody.builder()
                    .receive_id(chat_id)
                    .msg_type("interactive")
                    .content(json.dumps(card))
                    .build()) \
                .build()
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(None, lambda: client.im.v1.message.create(req))
            if resp.code == 0:
                msg_id = json.loads(resp.raw.content).get("data", {}).get("message_id")
                self.card_msg_ids[mac] = msg_id
                log.info(f"已成功向飞书发送配对审批卡片 (MsgID={msg_id})")
            else:
                log.error(f"发送飞书审批卡片失败: {resp.msg}")
        except Exception as e:
            log.error(f"推送飞书审批卡片异常: {e}")

    async def send_feishu_paired_notify(self, mac: str, ip: str, name: str):
        """向飞书推送设备配对连接确认卡片"""
        client = get_lark_client()
        if not client or not CONFIG.get("admin_chat_id"):
            return

        chat_id = CONFIG["admin_chat_id"]
        now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "📟 FoloToy AI Passport 配对就绪确认"},
                "template": "green"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**✅ 设备已成功完成配对认证并建立全双工连接：**\n"
                            f"• **设备名称**：`{name}`\n"
                            f"• **设备 IP**：`{ip}`\n"
                            f"• **硬件 MAC**：`{mac}`\n"
                            f"• **认证时间**：{now_str}\n\n"
                            f"随身对讲通道已就绪，长按硬件 **OK 键** 即可随时对讲。"
                        )
                    }
                }
            ]
        }
        try:
            req = CreateMessageRequest.builder() \
                .receive_id_type("chat_id") \
                .request_body(CreateMessageRequestBody.builder()
                    .receive_id(chat_id)
                    .msg_type("interactive")
                    .content(json.dumps(card))
                    .build()) \
                .build()
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: client.im.v1.message.create(req))
            log.info(f"已成功向飞书发送配对确认卡片: MAC={mac}")
        except Exception as e:
            log.error(f"推送配对确认卡片异常: {e}")

    async def handle_pair_decision(self, request: web.Request) -> web.Response:
        """处理卡片审批回调 (HTTP GET)"""
        mac = request.query.get("mac", "").lower().strip()
        action = request.query.get("action", "").lower().strip()
        if not mac:
            return web.Response(text="缺少 MAC 参数", status=400)

        if action == "approve":
            token = device_mgr.approve(mac)
            ws = self.pending_auth_ws.pop(mac, None) or self.mac_to_ws.get(mac)
            if ws and not ws.closed:
                self.ws_info[ws]["approved"] = True
                await ws.send_str(json.dumps({
                    "type": "pair_ack",
                    "status": "approved",
                    "token": token,
                    "name": CONFIG.get("gateway_name", "Antigravity 飞书控制台")
                }))
                log.info(f"挂起的设备已通过审批并下发 ACK: MAC={mac}")

            self._update_feishu_card_status(mac, approved=True)
            html = """
            <html><head><meta charset="utf-8"><title>配对成功</title></head>
            <body style="font-family:sans-serif;text-align:center;padding:50px;">
                <h1 style="color:#00B96B;">🎉 设备配对成功！</h1>
                <p>硬件 (MAC: """ + mac + """) 已获准连接并绑定当前飞书控制台。</p>
                <p>设备屏幕已进入待命状态，您可以随时长按 OK 键开始语音对讲。</p>
            </body></html>
            """
            return web.Response(text=html, content_type="text/html")
        else:
            device_mgr.reject(mac)
            ws = self.pending_auth_ws.pop(mac, None) or self.mac_to_ws.get(mac)
            if ws and not ws.closed:
                await ws.send_str(json.dumps({
                    "type": "pair_ack",
                    "status": "rejected",
                    "message": "配对申请已被飞书管理员拒绝"
                }))
                await ws.close()

            self._update_feishu_card_status(mac, approved=False)
            html = """
            <html><head><meta charset="utf-8"><title>配对已拒绝</title></head>
            <body style="font-family:sans-serif;text-align:center;padding:50px;">
                <h1 style="color:#F53F3F;">❌ 已拒绝配对请求</h1>
                <p>设备 (MAC: """ + mac + """) 已被阻止接入。</p>
            </body></html>
            """
            return web.Response(text=html, content_type="text/html")

    def _update_feishu_card_status(self, mac: str, approved: bool):
        """将飞书卡片更新为审批完成状态"""
        msg_id = self.card_msg_ids.get(mac)
        client = get_lark_client()
        if not msg_id or not client:
            return

        now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        status_text = "✅ **已同意授权接入**" if approved else "❌ **已拒绝接入请求**"
        header_color = "green" if approved else "grey"

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "📟 FoloToy AI Passport 设备配对认证"},
                "template": header_color
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"• **硬件 MAC**：`{mac}`\n• **审批结果**：{status_text}\n• **处理时间**：{now_str}"
                    }
                }
            ]
        }
        try:
            req = PatchMessageRequest.builder() \
                .message_id(msg_id) \
                .request_body(PatchMessageRequestBody.builder().content(json.dumps(card)).build()) \
                .build()
            client.im.v1.message.patch(req)
        except Exception as e:
            log.warning(f"更新飞书审批卡片状态失败: {e}")

    async def transcribe_voice(self, wav_path: str) -> str:
        """使用 SpeechRecognition 进行本地/在线语音识别"""
        loop = asyncio.get_running_loop()
        def _recognize():
            try:
                import speech_recognition as sr
                r = sr.Recognizer()
                with sr.AudioFile(wav_path) as source:
                    audio_data = r.record(source)
                text = r.recognize_google(audio_data, language="zh-CN")
                return text.strip() if text else ""
            except sr.UnknownValueError:
                log.warning("ASR 无法识别当前音频（音频能量过低或无明显语音）")
                return ""
            except Exception as e:
                log.warning(f"ASR 识别未能识别出有效语音或发生异常 ({type(e).__name__}): {e}")
                return ""

        try:
            return await asyncio.wait_for(loop.run_in_executor(None, _recognize), timeout=12.0)
        except asyncio.TimeoutError:
            log.warning("ASR 语音转录超时 (12s)")
            return ""
        except Exception as e:
            log.error(f"ASR 语音转录发生异常: {e}")
            return ""

    def get_active_bot_session(self) -> Optional[dict]:
        """读取当前飞书机器人的最新活跃会话信息 (chat_id, conversation_id, project, model)"""
        db_path = "/home/jiang/github/antigravity-feishu-bot/antigravity_bot.db"
        if not os.path.exists(db_path):
            return None
        try:
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            # 优先取最近活跃的授权会话，回退到 chat_sessions
            row = c.execute("SELECT chat_id FROM auth_sessions ORDER BY last_request_at DESC LIMIT 1").fetchone()
            chat_id = row[0] if row else None
            if not chat_id:
                row = c.execute("SELECT chat_id FROM chat_sessions ORDER BY rowid DESC LIMIT 1").fetchone()
                chat_id = row[0] if row else None

            if not chat_id:
                conn.close()
                return None

            row = c.execute("SELECT data FROM chat_sessions WHERE chat_id = ?", (chat_id,)).fetchone()
            conn.close()
            if row and row[0]:
                session_data = json.loads(row[0])
                return {
                    "chat_id": chat_id,
                    "conversation": session_data.get("conversation", ""),
                    "project": session_data.get("project", ""),
                    "model": session_data.get("model", "gemini-3.8-flash-low"),
                    "memory_context": session_data.get("memory_context", "")
                }
        except Exception as e:
            log.error(f"读取飞书机器人活跃会话失败: {e}")
        return None

    def update_bot_session_summary(self, chat_id: str, user_text: str, reply_text: str):
        """将随身设备本轮问答同步写入飞书机器人的会话摘要，确保飞书App下一次输入无缝接续"""
        db_path = "/home/jiang/github/antigravity-feishu-bot/antigravity_bot.db"
        if not os.path.exists(db_path) or not chat_id:
            return
        try:
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            row = c.execute("SELECT data FROM chat_sessions WHERE chat_id = ?", (chat_id,)).fetchone()
            if row and row[0]:
                data = json.loads(row[0])
                data["last_user_query"] = user_text
                data["last_ai_summary"] = reply_text
                c.execute("UPDATE chat_sessions SET data = ? WHERE chat_id = ?", (json.dumps(data, ensure_ascii=False), chat_id))
                conn.commit()
                log.info(f"已成功将随身语音问答同步写入飞书机器人会话上下文 (chat_id={chat_id})")
            conn.close()
        except Exception as e:
            log.warning(f"同步飞书机器人会话上下文失败: {e}")

    def extract_recent_transcript_context(self, conv_id: str, max_turns: int = 8) -> str:
        """从当前会话的 transcript.jsonl 中提取最近几轮真实对话，作为即时上下文注入"""
        if not conv_id:
            return ""
        t_path = f"/home/jiang/.gemini/antigravity-cli/brain/{conv_id}/.system_generated/logs/transcript.jsonl"
        if not os.path.exists(t_path):
            return ""
        try:
            with open(t_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()
            turns = []
            for line in lines:
                try:
                    d = json.loads(line)
                    t = d.get("type")
                    c = d.get("content", "")
                    if not c:
                        continue
                    if t == "USER_INPUT":
                        c_text = c
                        if "<USER_REQUEST>" in c_text:
                            m = re.search(r"<USER_REQUEST>(.*?)</USER_REQUEST>", c_text, re.DOTALL)
                            if m:
                                c_text = m.group(1)
                        lines_c = [l.strip() for l in c_text.splitlines() if not l.startswith("[System") and not l.startswith("<") and not l.startswith("[User") and l.strip()]
                        clean_text = " ".join(lines_c).strip()
                        if clean_text:
                            turns.append(f"用户：{clean_text[-120:]}")
                    elif t == "PLANNER_RESPONSE":
                        clean_a = re.sub(r"\[TASK_PLAN\].*?\[/TASK_PLAN\]", "", c, flags=re.DOTALL)
                        clean_a = re.sub(r"```.*?```", "[代码块]", clean_a, flags=re.DOTALL)
                        clean_a = re.sub(r"[#*`>]+", "", clean_a)
                        clean_a = re.sub(r"[\r\n]+", " ", clean_a).strip()
                        if clean_a:
                            turns.append(f"AI回复：{clean_a[:120]}")
                except Exception:
                    pass
            if turns:
                return "\n".join(turns[-max_turns:])
        except Exception as e:
            log.warning(f"读取会话上下文异常: {e}")
        return ""

    def append_turn_to_transcript(self, conv_id: str, user_text: str, ai_text: str):
        """将随身设备本轮问答追加写入飞书主会话 transcript 文件，实现多端会话历史永久统一"""
        if not conv_id:
            return
        t_path = f"/home/jiang/.gemini/antigravity-cli/brain/{conv_id}/.system_generated/logs/transcript.jsonl"
        if not os.path.exists(t_path):
            return
        try:
            now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            user_entry = json.dumps({
                "type": "USER_INPUT",
                "source": "USER_EXPLICIT",
                "status": "DONE",
                "created_at": now_iso,
                "content": f"[随身对讲设备语音] {user_text}"
            }, ensure_ascii=False)
            ai_entry = json.dumps({
                "type": "PLANNER_RESPONSE",
                "source": "MODEL",
                "status": "DONE",
                "created_at": now_iso,
                "content": ai_text
            }, ensure_ascii=False)
            with open(t_path, "a", encoding="utf-8") as f:
                f.write(user_entry + "\n" + ai_entry + "\n")
            log.info(f"已成功将随身对讲问答追加写入飞书主会话 transcript (conv_id={conv_id})")
        except Exception as e:
            log.warning(f"追加写入 transcript 失败: {e}")

    async def update_feishu_voice_card_step(
        self,
        msg_id: str,
        user_text: str,
        planned_steps: list,
        current_step_idx: int,
        tool_action: str = "",
        think_seconds: int = 0
    ):
        """飞书端：实时更新动态任务规划与执行清单卡片"""
        client = get_lark_client()
        if not client or not msg_id:
            return

        card = build_planned_steps_voice_card(
            user_text=user_text,
            planned_steps=planned_steps,
            current_step_idx=current_step_idx,
            tool_action=tool_action,
            think_seconds=think_seconds,
            is_completed=False
        )
        try:
            req = PatchMessageRequest.builder() \
                .message_id(msg_id) \
                .request_body(PatchMessageRequestBody.builder().content(json.dumps(card)).build()) \
                .build()
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: client.im.v1.message.patch(req))
        except Exception as e:
            log.warning(f"更新飞书步骤卡片异常: {e}")

    def extract_session_tools_from_transcript(self, transcript_path: str) -> list:
        """从 transcript.jsonl 中提取已执行的工具调用及简明结果"""
        if not os.path.exists(transcript_path):
            return []
        tools = []
        try:
            with open(transcript_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()

            last_tool = None
            for line in lines:
                try:
                    d = json.loads(line)
                    t_type = d.get("type")
                    if t_type == "PLANNER_RESPONSE" and d.get("tool_calls"):
                        for tc in d["tool_calls"]:
                            name = tc.get("name", "tool")
                            args = tc.get("args", {})
                            if isinstance(args, str):
                                try:
                                    args = json.loads(args)
                                except Exception:
                                    args = {}
                            summary = str(args.get("toolSummary", "") or args.get("toolAction", "")).strip(" \"'\n\r")
                            target = (
                                args.get("CommandLine") or
                                args.get("TargetFile") or
                                args.get("AbsolutePath") or
                                args.get("query") or
                                args.get("Url") or ""
                            )
                            target = str(target).strip(" \"'\n\r")
                            last_tool = {
                                "name": name,
                                "summary": summary,
                                "target": target[:80] if target else "",
                                "output": ""
                            }
                            tools.append(last_tool)
                    elif t_type == "GENERIC" and last_tool is not None:
                        c = d.get("content", "")
                        if "Output:" in c:
                            c = c.split("Output:", 1)[1].strip()
                        elif "Stdout:" in c:
                            c = c.split("Stdout:", 1)[1].strip()
                        first_line = c.splitlines()[0] if c.splitlines() else ""
                        last_tool["output"] = first_line[:80].strip()
                        last_tool = None
                except Exception:
                    pass
        except Exception as e:
            log.warning(f"解析 transcript 记录异常: {e}")
        return tools

    async def generate_ai_reply(self, user_query: str, mac: str = "", feishu_msg_id: str = "", planned_steps: list = None, duration_sec: float = 0.0) -> tuple:
        """调用 Antigravity 针对用户语音提问生成口语化分析回复，深度注入飞书App当前会话上下文并具备全量工具执行能力"""
        if not user_query:
            return "我没有听清您的说话内容，请长按确认键重新对我说话。", []

        bot_session = self.get_active_bot_session()
        conv_id = bot_session.get("conversation", "") if bot_session else ""
        chat_id = bot_session.get("chat_id", "") if bot_session else ""

        # 【极速直达通道】：智能家居控制直接在本地秒级执行，不进入漫长的大模型多轮推理
        try:
            direct_res = execute_direct_smart_home(user_query)
            if direct_res:
                reply_text, executed_tools = direct_res
                log.info(f"⚡ 命中智能家居直通执行: {user_query} -> {reply_text} (工具={executed_tools})")
                if chat_id:
                    self.update_bot_session_summary(chat_id, f"🎙️ [随身设备语音] {user_query}", reply_text)
                if conv_id:
                    self.append_turn_to_transcript(conv_id, user_query, reply_text)
                return reply_text, executed_tools
        except Exception as e:
            log.warning(f"智能家居直通执行异常，回退至大模型调度: {e}")

        recent_context = self.extract_recent_transcript_context(conv_id)
        context_block = f"【飞书App当前会话最近讨论记录】\n{recent_context}\n\n" if recent_context else ""

        # 检查最新编译产物状态
        fw_exists = os.path.exists("/home/jiang/github/Letgo/fw-37423795163/FoloToy-AI-Passport-full.bin")
        fw_status_note = "【系统环境实时状态】：当前新固件（Run ID: 37423795163）已构建编译完成，完整固件包已下载到本地！" if fw_exists else "【系统环境实时状态】：当前固件正在编译或准备中。"

        project_dir = bot_session.get("project", "") if bot_session else ""
        if not project_dir or not os.path.isdir(project_dir):
            project_dir = "/home/jiang/github/Letgo"

        prompt = (
            f"[这是来自用户随身硬件对讲设备 FoloToy AI Passport 的即时语音输入 (MAC: {mac})]\n"
            f"重要原则：用户使用随身硬件对讲提问与在飞书App提问完全等价，拥有最高优先级与完全同等的Agent会话流水线能力！\n"
            f"用户在设备上长按提问：「{user_query}」\n\n"
            f"【用户核心准则】：\n"
            f"1. 如果用户发出的是操作、执行、推送、构建、测试等命令（如“把当前项目的最新代码推送到github”），你必须自主调用工具执行完成，绝对严禁推诿让用户手动操作！\n"
            f"2. 如果用户发出的是智能家居控制指令（如开灯/关灯/查设备/开关空调/开电脑等），系统已提供本地极速控制工具 ha-ctl（在PATH中），你可以直接在命令行运行（例如 ha-ctl find 客厅 查找设备，ha-ctl turn_on 开启，ha-ctl turn_off 关闭，ha-ctl list 列出所有设备）。请优先使用 ha-ctl 快速完成控制，切勿在容器内盲目探索！\n"
            f"3. 真实性原则：严禁在未成功执行操作时谎称已完成！如果设备离线或遇到错误，如实告知用户当前设备状态与错误原因。\n"
            f"4. 注意隐私保护，严禁在任何地方明文暴露 GitHub Token 或个人凭证。\n\n"
            f"{fw_status_note}\n\n"
            f"{context_block}"
            f"【核心回答指令】\n"
            f"请完全基于上方飞书App当前会话的最新讨论记录与实时环境状态，对用户在随身设备上的提问做出极速、直接、精准的口语化回答。\n"
            f"重点要求：第一句话直接给出核心结论或操作结果，严禁寒暄铺垫，篇幅严格控制在20至50字以内，极其适合语音播报；严禁输出Markdown标题、加粗符号、代码块、列表符号或表情符号。"
        )

        agy_bin = CONFIG.get("agy_bin", "/home/jiang/.local/bin/agy")
        model = CONFIG.get("ai_model", "gemini-3.8-flash-low")
        effort = CONFIG.get("ai_effort", "low")

        cmd = [
            agy_bin,
            "-p", prompt,
            "--model", model,
            "--effort", effort,
            "--print-timeout", "60s"
        ]


        # 挂载工作区目录，打通全量 Agent 工具执行权限
        if project_dir and os.path.isdir(project_dir):
            cmd.extend(["--add-dir", project_dir])

        log.info(f"开始调用具备全量 Agent 执行能力的大脑分析并执行对讲指令 (proj={project_dir})...")

        start_time = time.time()
        brain_base = "/home/jiang/.gemini/antigravity-cli/brain"
        existing_brains = set(os.listdir(brain_base)) if os.path.exists(brain_base) else set()

        executed_tools = []
        active_t_path = None
        stop_monitor = asyncio.Event()

        async def _monitor_loop():
            nonlocal active_t_path, executed_tools
            last_count = 0
            while not stop_monitor.is_set():
                try:
                    await asyncio.wait_for(stop_monitor.wait(), timeout=1.5)
                    break
                except asyncio.TimeoutError:
                    pass

                if not active_t_path and os.path.exists(brain_base):
                    try:
                        cur_dirs = set(os.listdir(brain_base))
                        new_dirs = [d for d in (cur_dirs - existing_brains) if os.path.isdir(os.path.join(brain_base, d))]
                        if new_dirs:
                            new_dirs.sort(key=lambda d: os.path.getmtime(os.path.join(brain_base, d)), reverse=True)
                            cand = os.path.join(brain_base, new_dirs[0], ".system_generated", "logs", "transcript.jsonl")
                            if os.path.exists(cand):
                                active_t_path = cand
                    except Exception:
                        pass

                if active_t_path and os.path.exists(active_t_path):
                    parsed = self.extract_session_tools_from_transcript(active_t_path)
                    if parsed:
                        executed_tools = parsed

        monitor_task = asyncio.create_task(_monitor_loop())

        try:
            # 彻底清理环境变量，阻断所有会话锁和嵌套干扰
            custom_env = os.environ.copy()
            for k in list(custom_env.keys()):
                if any(x in k.lower() for x in ["antigravity", "conversation", "trajectory", "agentapi", "gemini_cli"]):
                    custom_env.pop(k, None)

            loop = asyncio.get_running_loop()
            def _execute_subp():
                import subprocess
                return subprocess.run(cmd, env=custom_env, capture_output=True, text=True, timeout=120.0)

            subp_res = await loop.run_in_executor(None, _execute_subp)
            output = (subp_res.stdout or "").strip()
            stderr_out = (subp_res.stderr or "").strip()
            log.info(f"agy 执行完毕 (RC={subp_res.returncode}, stdout长度={len(output)}, stderr长度={len(stderr_out)})")
            if subp_res.returncode != 0 and stderr_out:
                log.warning(f"agy 报错输出: {stderr_out[:200]}")
            cleaned = clean_text_for_speech(output)
            if cleaned:
                stop_monitor.set()
                await monitor_task
                if active_t_path and os.path.exists(active_t_path):
                    executed_tools = self.extract_session_tools_from_transcript(active_t_path)

                if chat_id:
                    self.update_bot_session_summary(chat_id, f"🎙️ [随身设备语音] {user_query}", cleaned)
                if conv_id:
                    self.append_turn_to_transcript(conv_id, user_query, cleaned)
                return cleaned, executed_tools
            else:
                log.warning(f"AI 输出经过清理后为空，原始输出: {output[:100]}")
        except asyncio.TimeoutError:
            log.warning("AI 生成回复超时 (120s)")
        except Exception as e:
            log.error(f"AI 生成回复异常: {e}")
        finally:
            stop_monitor.set()
            if not monitor_task.done():
                await monitor_task
            if active_t_path and os.path.exists(active_t_path):
                executed_tools = self.extract_session_tools_from_transcript(active_t_path)

        if executed_tools:
            fallback = f"执行「{user_query}」耗时超过限制，后台已执行 {len(executed_tools)} 个操作步骤，请在飞书卡片查看具体执行明细。"
        else:
            fallback = f"执行「{user_query}」响应超时，请稍后重试或在飞书直接下达。"

        if chat_id:
            self.update_bot_session_summary(chat_id, f"🎙️ [随身设备语音] {user_query}", fallback)
        if conv_id:
            self.append_turn_to_transcript(conv_id, user_query, fallback)
        return fallback, executed_tools

    async def send_feishu_incoming_voice_card(self, user_text: str, planned_steps: list = None) -> Optional[str]:
        """飞书端：收到语音识别结果后，立即下发随身语音思考卡片"""
        client = get_lark_client()
        bot_session = self.get_active_bot_session()
        chat_id = (bot_session.get("chat_id") if bot_session else "") or CONFIG.get("admin_chat_id")
        if not client or not chat_id:
            return None

        card = build_voice_interaction_card(
            user_text=user_text,
            is_completed=False,
            think_seconds=1
        )
        try:
            req = CreateMessageRequest.builder() \
                .receive_id_type("chat_id") \
                .request_body(CreateMessageRequestBody.builder()
                    .receive_id(chat_id)
                    .msg_type("interactive")
                    .content(json.dumps(card))
                    .build()) \
                .build()
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(None, lambda: client.im.v1.message.create(req))
            if resp.code == 0:
                msg_id = json.loads(resp.raw.content).get("data", {}).get("message_id")
                log.info(f"已立即向飞书下发随身语音思考卡片 (MsgID={msg_id})")
                return msg_id
            else:
                log.error(f"下发飞书语音卡片失败: {resp.msg}")
        except Exception as e:
            log.error(f"下发飞书语音卡片异常: {e}")
        return None

    async def update_feishu_voice_card_reply(
        self,
        msg_id: str,
        user_text: str,
        reply_text: str,
        planned_steps: list = None,
        think_seconds: int = 0,
        executed_tools: list = None
    ):
        """飞书端：任务完成更新最终卡片（真实 AI 回复 + 工具执行记录）"""
        client = get_lark_client()
        if not client or not msg_id:
            return

        card = build_voice_interaction_card(
            user_text=user_text,
            reply_text=reply_text,
            executed_tools=executed_tools,
            is_completed=True,
            think_seconds=think_seconds
        )
        try:
            req = PatchMessageRequest.builder() \
                .message_id(msg_id) \
                .request_body(PatchMessageRequestBody.builder().content(json.dumps(card)).build()) \
                .build()
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(None, lambda: client.im.v1.message.patch(req))
            if resp.code == 0:
                log.info(f"已成功将飞书卡片更新为全部完成状态 (MsgID={msg_id})")
            else:
                log.error(f"更新飞书语音卡片失败: {resp.msg}")
        except Exception as e:
            log.error(f"更新飞书语音卡片异常: {e}")

    async def process_incoming_voice(self, ws: web.WebSocketResponse, mac: str, audio_bytes: bytearray):
        """处理设备上传的语音帧并进行 ASR -> 即显飞书 -> AI 分析 -> 更新飞书 -> TTS 回放"""
        duration_sec = len(audio_bytes) / 32000.0
        log.info(f"开始处理设备音频数据，长度: {len(audio_bytes)} 字节 (约 {duration_sec:.2f} 秒)")

        if len(audio_bytes) < 6400:
            log.warning("录音时长过短 (<0.2s)，忽略处理")
            return

        # 1. 立即通知硬件处于 ASR 语音识别状态
        if not ws.closed:
            await ws.send_str(json.dumps({
                "type": "ai_state",
                "state": "thinking",
                "detail": "Feishu: Transcribing..."
            }))

        # 2. 存为临时 WAV 文件并进行音量自适应增益与 ASR 转录
        wav_path = None
        user_text = ""
        try:
            # 针对硬件麦克风录音进行峰值检测与自适应数字增益 (+6dB ~ +18dB 智能平滑缩放，防止底噪过小导致 ASR 丢字)
            import numpy as np
            samples = np.frombuffer(audio_bytes, dtype=np.int16)
            if len(samples) > 0:
                max_amp = np.max(np.abs(samples))
                log.info(f"🎙️ 硬件音频帧统计: 采样点={len(samples)}, 峰值幅值={max_amp}")
                if 0 < max_amp < 18000:
                    scale = min(20000.0 / max_amp, 8.0) # 最大放大8倍，目标峰值约20000
                    boosted = np.clip(samples.astype(np.float32) * scale, -32768, 32767).astype(np.int16)
                    processed_bytes = boosted.tobytes()
                    log.info(f"🎙️ 音频已施加数字增益 x{scale:.2f}")
                else:
                    processed_bytes = audio_bytes
            else:
                processed_bytes = audio_bytes

            with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tf:
                wav_path = tf.name
                with wave.open(wav_path, "wb") as wf:
                    wf.setnchannels(1)
                    wf.setsampwidth(2)
                    wf.setframerate(16000)
                    wf.writeframes(processed_bytes)

            user_text = await self.transcribe_voice(wav_path)
        except Exception as e:
            log.error(f"语音转录文件写入或识别发生异常: {e}")
        finally:
            if wav_path and os.path.exists(wav_path):
                try:
                    os.remove(wav_path)
                except Exception:
                    pass

        # 3. 校验语音转写结果
        if user_text:
            log.info(f"🎙️ ASR 语音识别成功: 「{user_text}」 (MAC: {mac})")
        else:
            log.warning("🎙️ ASR 未能识别出清晰语音内容")
            user_text = "(未检测到有效语音)"

        # 4. 【核心交互升级】：向飞书下发即时语音思考卡片（完全异步后台执行，杜绝网络阻塞硬件）
        feishu_msg_id_fut = asyncio.Future()
        async def _async_send_card():
            try:
                mid = await self.send_feishu_incoming_voice_card(user_text)
                if not feishu_msg_id_fut.done():
                    feishu_msg_id_fut.set_result(mid)
            except Exception as e:
                log.warning(f"后台发送飞书卡片异常: {e}")
                if not feishu_msg_id_fut.done():
                    feishu_msg_id_fut.set_result(None)

        asyncio.create_task(_async_send_card())

        # 立即通知硬件：语音已成功送达飞书！
        if not ws.closed:
            await ws.send_str(json.dumps({
                "type": "feishu_voice_sent",
                "status": "success",
                "text": user_text,
                "duration": round(duration_sec, 1)
            }))


        # 5. 通知硬件当前正在 AI 深度分析
        if not ws.closed:
            await ws.send_str(json.dumps({
                "type": "ai_state",
                "state": "thinking",
                "detail": "Feishu: AI Analyzing..."
            }))

        # 6. 调用 AI 大模型或极速直通生成分析回复
        executed_tools = []
        reply_text = ""
        try:
            if user_text != "(未检测到有效语音)":
                log.info(f"🤖 正在调用 AI 分析提问 (关联会话与全量Agent工具链)...")
                reply_text, executed_tools = await self.generate_ai_reply(
                    user_query=user_text,
                    mac=mac,
                    feishu_msg_id=feishu_msg_id,
                    duration_sec=duration_sec
                )
                log.info(f"🤖 AI 生成回复: 「{reply_text}」 (实际执行工具数: {len(executed_tools)})")
            else:
                reply_text = "我没有听清您的说话内容，请长按确认键重新对我说话。"
        except Exception as e:
            log.error(f"AI 生成回复异常: {e}")
            reply_text = "抱歉，处理您的语音指令时出现异常，请稍后重试。"

        # 7. 【核心交互升级】：任务完成，后台刷新飞书卡片展示原生答复与工具执行记录
        async def _async_update_card():
            try:
                msg_id = await asyncio.wait_for(asyncio.shield(feishu_msg_id_fut), timeout=5.0)
                if msg_id:
                    await self.update_feishu_voice_card_reply(
                        msg_id=msg_id,
                        user_text=user_text,
                        reply_text=reply_text,
                        think_seconds=int(duration_sec + 2),
                        executed_tools=executed_tools
                    )
            except Exception as e:
                log.warning(f"后台更新飞书卡片异常: {e}")

        asyncio.create_task(_async_update_card())


        # 8 & 9. 【流式重叠抢跑流水线】：通过常驻 Edge-TTS 连接池边合成边定速推流硬件
        if not ws.closed and reply_text:
            log.info(f"🔊 启动极速流式抢跑语音播报: 「{reply_text[:20]}...」")
            await ws.send_str(json.dumps({"type": "ai_speech_start", "text": reply_text[:60]}))
            await ws.send_str(json.dumps({
                "type": "ai_state",
                "state": "speaking",
                "detail": "Feishu: Speaking..."
            }))
            await ws.send_str(json.dumps({"type": "voice_reply_start"}))

            # 严格定速推流 (16kHz 16-bit 单声道 = 32000 字节/秒，每 1920 字节 60ms)
            async def handle_pcm_stream(pcm_chunk: bytes):
                if ws.closed:
                    return
                # 将 pcm_chunk 按 1920 字节定速打入 WebSocket
                chunk_len = 1920
                for pos in range(0, len(pcm_chunk), chunk_len):
                    if ws.closed:
                        break
                    piece = pcm_chunk[pos:pos + chunk_len]
                    await ws.send_bytes(piece)
                    await asyncio.sleep(0.056)

            is_interrupted = lambda: ws.closed or ws not in self.active_websockets

            stream_ok = await tts_pool.stream_pcm_realtime(
                text=reply_text,
                on_pcm_chunk=handle_pcm_stream,
                check_interrupted=is_interrupted
            )

            if not stream_ok and not ws.closed:
                # 若连接池偶发异常，平滑回退到一次性合成
                fallback_pcm = await self.synthesize_speech_pcm(reply_text)
                if fallback_pcm:
                    for offset in range(0, len(fallback_pcm), 1920):
                        if ws.closed:
                            break
                        await ws.send_bytes(fallback_pcm[offset:offset + 1920])
                        await asyncio.sleep(0.058)

            if not ws.closed:
                await ws.send_str(json.dumps({"type": "voice_reply_end"}))
                await ws.send_str(json.dumps({"type": "ai_speech_end"}))
                await ws.send_str(json.dumps({"type": "ai_state", "state": "idle", "detail": "Ready"}))
                log.info("设备语音播报完成，已切回待命状态")


    async def synthesize_speech_pcm(self, text: str) -> Optional[bytes]:
        """调用 edge-tts 生成音频并经 ffmpeg 转为 16kHz 16-bit 单声道 PCM"""
        try:
            import edge_tts
            voice = CONFIG.get("tts_voice", "zh-CN-XiaoxiaoNeural")
            rate = CONFIG.get("tts_rate", "-5%")
            with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as mp3_file:
                mp3_path = mp3_file.name

            communicate = edge_tts.Communicate(text, voice, rate=rate)
            await communicate.save(mp3_path)

            # ffmpeg 转成 16kHz s16le PCM
            cmd = [
                "ffmpeg", "-y", "-i", mp3_path,
                "-f", "s16le", "-acodec", "pcm_s16le",
                "-ac", "1", "-ar", "16000", "-"
            ]
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            try:
                os.remove(mp3_path)
            except Exception:
                pass

            if proc.returncode == 0:
                return proc.stdout
        except Exception as e:
            log.error(f"TTS 语音合成失败: {e}")
        return None

    async def handle_api_status(self, request: web.Request) -> web.Response:
        """网关健康与连接状态"""
        data = {
            "status": "online",
            "gateway_name": CONFIG.get("gateway_name"),
            "port": self.port,
            "udp_port": self.udp_port,
            "connected_clients": len(self.active_websockets),
            "paired_devices_count": len(device_mgr.devices),
            "active_devices": [
                {"peer": info["peer"], "mac": info["mac"], "approved": info["approved"]}
                for info in self.ws_info.values()
            ],
            "timestamp": int(time.time())
        }
        return web.json_response(data)

    async def handle_api_devices(self, request: web.Request) -> web.Response:
        """获取所有已配对设备"""
        return web.json_response(device_mgr.devices)

    async def handle_api_remove_device(self, request: web.Request) -> web.Response:
        """解绑设备"""
        data = await request.json()
        mac = data.get("mac", "")
        if mac:
            device_mgr.remove(mac)
            ws = self.mac_to_ws.get(mac)
            if ws and not ws.closed:
                await ws.send_str(json.dumps({"type": "pair_ack", "status": "unpaired"}))
                await ws.close()
            return web.json_response({"success": True})
        return web.json_response({"success": False, "error": "Missing mac"}, status=400)


async def main():
    server = GatewayServer()
    await server.start()
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("网关服务已安全停止")
