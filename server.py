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
import signal
from typing import Dict, Set, Optional, Any, Tuple, Callable, Awaitable, List
import aiohttp
from aiohttp import web, WSMsgType
import urllib.request
import urllib.error
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


class SentenceChunker:
    """
    流式切句器：
    实时接收大模型输出的文本增量 (text_delta)，
    智能按标点（。！？；，及换行）以及长度阈值进行首句极速切片抢跑与后续平滑分句。
    """
    def __init__(self):
        self.buffer = ""
        self.is_first_sentence = True
        self.major_delims = set("。！？!?\n")
        self.minor_delims = set("，；,;：:")

    def feed(self, delta: str) -> List[str]:
        if not delta:
            return []
        self.buffer += delta
        sentences = []

        while True:
            cut_idx = -1
            cut_len = 0

            for idx, ch in enumerate(self.buffer):
                if ch in self.major_delims:
                    if idx >= 2:
                        cut_idx = idx
                        cut_len = 1
                        break
                elif ch in self.minor_delims:
                    min_len = 8 if self.is_first_sentence else 14
                    if idx >= min_len:
                        cut_idx = idx
                        cut_len = 1
                        break

            if cut_idx == -1 and len(self.buffer) >= 28:
                cut_idx = 25
                cut_len = 0

            if cut_idx != -1:
                sentence = self.buffer[:cut_idx + cut_len].strip()
                self.buffer = self.buffer[cut_idx + cut_len:].lstrip()
                if sentence:
                    self.is_first_sentence = False
                    sentences.append(sentence)
            else:
                break

        return sentences

    def flush(self) -> List[str]:
        sentences = []
        rem = self.buffer.strip()
        self.buffer = ""
        if rem:
            sentences.append(rem)
        return sentences


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

        async with self.lock:
            try:
                if not self._connected or not self.ws or self.ws.closed:
                    self._connected = False
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
                interrupted = False
                while True:
                    if check_interrupted and check_interrupted():
                        interrupted = True
                        break
                    chunk = await proc.stdout.read(chunk_size)
                    if not chunk:
                        break
                    await on_pcm_chunk(chunk)

                if interrupted:
                    if not feed_task.done():
                        feed_task.cancel()
                    if proc.returncode is None:
                        try:
                            proc.kill()
                        except Exception:
                            pass
                    self._connected = False
                    if self.ws and not self.ws.closed:
                        try:
                            await self.ws.close()
                        except Exception:
                            pass
                    self.ws = None
                    return False

                try:
                    await feed_task
                except Exception:
                    pass
                try:
                    await proc.wait()
                except Exception:
                    pass
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
    user_name: str = "江江",
    planned_steps: list = None,
    current_step_idx: int = 0,
    active_tool: dict = None
) -> dict:
    """
    构建与飞书 App 完全一模一样的任务执行与步骤进度卡片：
    - 处理中（有规划）：展示与飞书端完全一致的道路清单（已完成打勾✅，当前步骤⏳高亮执行工具，待执行⚪）
    - 处理中（无规划但有工具）：展示多步骤工具执行清单（✅ 已完成，⏳ 正在执行）
    - 完成后：展示真实答复内容与真实执行的系统工具动作记录
    """
    now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    clean_user_q = user_text[:60] + ("..." if len(user_text) > 60 else "")
    quote_header = f"> **{user_name}**：🎙️ 随身语音：「{clean_user_q}」\n\n"
    time_hint = f"已运行 {think_seconds}s" if think_seconds > 0 else "请稍候..."

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

        footer_text = f"⚡ Powered by Antigravity · 已耗时 {think_seconds}s | 🕒 {now_str}"
    elif planned_steps:
        total = len(planned_steps)
        current_step_idx = max(0, min(current_step_idx, total - 1))
        curr_step_name = planned_steps[current_step_idx].strip()
        curr_step_clean = re.sub(r'^[•\-\*\s✅⏳⚪\d\.\、\[\]]+\s*', '', curr_step_name).strip()
        curr_step_clean = re.sub(r'^(?:正在执行|正在运行|正在|已执行|已运行|已)\s*', '', curr_step_clean).strip()

        header_title = f"🛠️ [{current_step_idx + 1}/{total}] {curr_step_clean}"
        if len(header_title) > 28:
            header_title = header_title[:25] + "..."
        template = "turquoise"

        lines = []
        for i, raw_step in enumerate(planned_steps):
            step = raw_step.strip()
            core = re.sub(r'^[•\-\*\s✅⏳⚪\d\.\、\[\]]+\s*', '', step).strip()
            core = re.sub(r'^(?:正在执行|正在运行|正在|已执行|已运行|已)\s*', '', core).strip()

            if i < current_step_idx:
                completed_text = f"已{core}" if not core.startswith("已") else core
                lines.append(f"• ✅ **{i+1}. {completed_text}**")
            elif i == current_step_idx:
                active_text = f"正在{core}" if not core.startswith("正在") else core
                if active_tool:
                    act_summary = active_tool.get("summary") or active_tool.get("name") or ""
                    act_target = active_tool.get("target") or ""
                    act_desc = f"{act_summary} {act_target}".strip()
                    lines.append(f"• ⏳ **{i+1}. {active_text}** `({act_desc[:30]})`...")
                else:
                    lines.append(f"• ⏳ **{i+1}. {active_text}**...")
            else:
                lines.append(f"• ⚪ {i+1}. {core}")

        steps_block = "\n".join(lines)
        content = (
            f"{quote_header}"
            f"**📋 任务规划与执行清单 (第 {current_step_idx + 1}/{total} 步)：**\n\n"
            f"{steps_block}\n\n"
            f"*({time_hint}，随身语音指令全速推进中)*"
        )
        footer_text = f"⚡ Powered by Antigravity | 🕒 {now_str}"
    elif executed_tools or active_tool:
        all_tools = list(executed_tools or [])
        total_steps = len(all_tools) + (1 if active_tool and active_tool not in all_tools else 0)
        curr_tool = active_tool or (all_tools[-1] if all_tools else {})
        tool_act = curr_tool.get("summary") or curr_tool.get("name") or "执行操作"

        header_title = f"🛠️ [{total_steps}] {tool_act}"
        if len(header_title) > 28:
            header_title = header_title[:25] + "..."
        template = "turquoise"

        lines = []
        # 最多显示最近 4 个已完成步骤
        history_tools = all_tools[:-1] if active_tool and active_tool in all_tools else all_tools
        if len(history_tools) > 4:
            lines.append(f"*(已完成前 {len(history_tools) - 4} 个执行步骤)*")
            history_tools = history_tools[-4:]

        for item in history_tools:
            name = item.get("name", "tool")
            summary = item.get("summary") or name
            target = item.get("target", "")
            target_str = f" `({target[:30]})`" if target else ""
            lines.append(f"• ✅ **已完成: {summary}**{target_str}")

        if active_tool:
            a_name = active_tool.get("name", "tool")
            a_summary = active_tool.get("summary") or a_name
            a_target = active_tool.get("target", "")
            a_target_str = f" `({a_target[:30]})`" if a_target else ""
            lines.append(f"• ⏳ **正在执行: {a_summary}**{a_target_str}...")
        else:
            lines.append("• ⏳ 正在整理并生成最终回复...")

        steps_block = "\n".join(lines)
        content = (
            f"{quote_header}"
            f"**🤖 任务多步骤执行中 (第 {total_steps} 步)：**\n\n"
            f"{steps_block}\n\n"
            f"*({time_hint}，AI 正在运行底层命令或操作文件)*"
        )
        footer_text = f"⚡ Powered by Antigravity | 🕒 {now_str}"
    else:
        header_title = "🎙️ 正在处理随身语音指令..."
        template = "turquoise"
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
        user_name=user_name,
        planned_steps=planned_steps,
        current_step_idx=current_step_idx
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
        self.active_ai_procs: Dict[web.WebSocketResponse, asyncio.subprocess.Process] = {}
        self._active_broadcast_task: Optional[asyncio.Task] = None
        
        # 音频流缓存与抢跑 ASR
        self.voice_buffers: Dict[web.WebSocketResponse, bytearray] = {}
        self.asr_pretasks: Dict[web.WebSocketResponse, asyncio.Task] = {}
        self._last_synced_project: Optional[str] = None

        # 固件 OTA 版本检测与文件分发
        self.ota_card_msg_ids: Dict[str, str] = {}  # "mac:tag" -> feishu message_id
        self.firmware_dir = os.path.join(CONFIG.get("data_dir", SCRIPT_DIR), "firmware")
        os.makedirs(self.firmware_dir, exist_ok=True)
        self._ignored_ota_file = os.path.join(CONFIG.get("data_dir", SCRIPT_DIR), "ignored_ota.json")
        self._cached_release = None
        self._cached_release_time = 0
        self._last_ota_check_time = 0

    async def abort_active_session(self, ws: web.WebSocketResponse, reason: str = "barge_in"):
        """
        【硬件物理打断 Barge-in 深度熔断机制】：
        当设备长按确认键打断、开始新录音或断开连接时：
        1. 立即 kill 正在运行的底层 AI 推理子进程组 (agy)，彻底截断 CPU/Token 消耗；
        2. 取消并中止正在执行的 TTS 与推流协程任务；
        3. 清理硬件录音积压缓存与 ASR 预热任务；
        4. 向硬件下发语音结束及待命状态重置帧，恢复硬件为 Ready。
        """
        log.info(f"⚡ [深度熔断] 触发会话打断熔断 ({reason}): peer={self.ws_info.get(ws, {}).get('peer')}")

        # 1. 深度终止底层 AI 推理子进程 (agy 进程组)
        proc = self.active_ai_procs.pop(ws, None)
        if proc:
            try:
                if proc.returncode is None:
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                        log.info(f"⚡ [深度熔断] 已通过 killpg 彻底清除 AI 进程组 (PGID={proc.pid})")
                    except Exception:
                        proc.kill()
            except Exception as e:
                log.warning(f"终止 AI 子进程组异常: {e}")

        # 2. 取消并中止正在执行的 TTS 与语音处理协程任务
        tts_task = self.active_tts_tasks.pop(ws, None)
        if tts_task and not tts_task.done():
            tts_task.cancel()
            log.info("⚡ [深度熔断] 已中断正在进行的 TTS 与音频推流任务")
        if hasattr(self, "_active_broadcast_task") and self._active_broadcast_task and not self._active_broadcast_task.done():
            self._active_broadcast_task.cancel()
            log.info("⚡ [深度熔断] 已中断正在进行的全局语音广播推流任务")

        # 3. 清理录音缓存与 ASR 任务
        self.voice_buffers.pop(ws, None)
        asr_task = self.asr_pretasks.pop(ws, None)
        if asr_task and not asr_task.done():
            asr_task.cancel()

        # 4. 下发硬件状态重置帧 (voice_start 或 barge_in 时硬件正处于按键主动接管状态，绝不可回送 voice_reply_end、ai_speech_end 或 idle 干扰硬件录音状态)
        if not ws.closed and reason not in ("voice_start", "barge_in"):
            try:
                await ws.send_str(json.dumps({"type": "voice_reply_end"}))
                await ws.send_str(json.dumps({"type": "ai_speech_end"}))
                await ws.send_str(json.dumps({
                    "type": "ai_state",
                    "state": "idle",
                    "detail": "Ready"
                }))
                log.info("⚡ [深度熔断] 已成功向硬件下发状态重置帧 (ai_state: idle)")
            except Exception as e:
                log.warning(f"向硬件下发状态重置异常: {e}")


    async def start(self):
        """启动 HTTP/WS 服务与 UDP 自动发现监听器"""
        self.app = web.Application()
        self.app.router.add_get("/ws/passport", self.handle_websocket)
        self.app.router.add_get("/api/status", self.handle_api_status)
        self.app.router.add_get("/api/pair_decision", self.handle_pair_decision)
        self.app.router.add_get("/api/ota_action", self.handle_ota_action)
        self.app.router.add_get("/firmware/{tag}/{filename}", self.handle_firmware_download)
        self.app.router.add_get("/api/devices", self.handle_api_devices)
        self.app.router.add_post("/api/devices/remove", self.handle_api_remove_device)
        self.app.router.add_post("/api/ring", self.handle_api_ring)
        self.app.router.add_post("/api/voice", self.handle_api_voice)
        self.app.router.add_post("/api/speak", self.handle_api_speak)

        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port, reuse_address=True, reuse_port=True)
        await self.site.start()
        log.info(f"TCP/WS 服务已在 ws://{self.host}:{self.port}/ws/passport 启动")

        await self._start_udp_discovery()

        # 异步启动常驻 Edge-TTS 预热会话池，杜绝首句延迟
        asyncio.create_task(tts_pool.ensure_connected())

        # 异步启动与飞书机器人活跃项目的实时双向同步巡检
        asyncio.create_task(self._project_sync_loop())


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
        ws = web.WebSocketResponse(autoping=True, heartbeat=15.0)
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
            await self.abort_active_session(ws, reason="disconnect")
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
        log.info(f"📩 收到设备文本消息 [{peer}]: {text_data[:120]}")
        try:
            data = json.loads(text_data)
        except Exception as e:
            log.warning(f"解析设备 JSON 异常: {e}, 原始内容: {text_data}")
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
                cur_name, projects = self.get_available_projects()
                await ws.send_str(json.dumps({
                    "type": "project_list",
                    "current": cur_name,
                    "projects": projects
                }))
                await ws.send_str(json.dumps({
                    "type": "project_switched",
                    "project": cur_name,
                    "status": "connected"
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
                self.ws_info[ws]["data"] = data
                await ws.send_str(json.dumps({
                    "type": "handshake_ack",
                    "status": "connected",
                    "name": CONFIG.get("gateway_name", "Antigravity 飞书控制台"),
                    "version": "1.0.0"
                }))
                cur_name, projects = self.get_available_projects()
                await ws.send_str(json.dumps({
                    "type": "project_list",
                    "current": cur_name,
                    "projects": projects
                }))
                await ws.send_str(json.dumps({
                    "type": "project_switched",
                    "project": cur_name,
                    "status": "connected"
                }))
                # 开机/握手成功后异步检查最新固件版本并推送飞书卡片
                asyncio.create_task(self.check_device_ota(ws, mac, data))
            else:
                await ws.send_str(json.dumps({
                    "type": "handshake_ack",
                    "status": "unauthorized",
                    "message": "请先发起 pair_request 并在飞书完成配对认证"
                }))

        # 打断信号 Barge-in (硬件长按 OK 键即刻打断当前播报与推理)
        elif msg_type == "barge_in":
            log.info(f"收到设备打断信号 (Barge-in): MAC={mac}")
            await self.abort_active_session(ws, reason="barge_in")

        # 3. 对讲语音开始 (打断先前正在进行的播报与推理并开辟新录音缓冲)
        elif msg_type == "voice_start":
            log.info(f"收到设备按下录音开始信号: MAC={mac}")
            await self.abort_active_session(ws, reason="voice_start")
            self.voice_buffers[ws] = bytearray()

        # 4. 对讲语音结束
        elif msg_type == "voice_end":
            dev_mac = mac or self.ws_to_mac.get(ws, "")
            buf = self.voice_buffers.pop(ws, bytearray())
            log.info(f"收到设备松开录音结束信号: MAC={dev_mac}, 音频缓冲大小: {len(buf)} 字节")
            if len(buf) > 0:
                tts_t = asyncio.create_task(self.process_incoming_voice(ws, dev_mac, buf))
                self.active_tts_tasks[ws] = tts_t
            else:
                log.warning("⚠️ 录音缓冲为空 (0 字节)，无法进行语音处理！")

        # 5. 心跳 Ping
        elif msg_type == "ping":
            await ws.send_str(json.dumps({"type": "pong", "timestamp": int(time.time())}))

        # 6. 获取项目列表 (硬件短按 ▲ 键呼出项目切换列表)
        elif msg_type == "project_list":
            log.info(f"收到设备请求项目列表: MAC={mac}")
            cur_name, projects = self.get_available_projects()
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
                new_path = f"/home/jiang/github/{target_proj}"
                cid = None
                s_data = {}
                try:
                    db_path = "/home/jiang/github/antigravity-feishu-bot/antigravity_bot.db"
                    if os.path.exists(db_path):
                        conn = sqlite3.connect(db_path)
                        c = conn.cursor()
                        row = c.execute("SELECT chat_id, data FROM chat_sessions ORDER BY rowid DESC LIMIT 1").fetchone()
                        if row and row[0]:
                            cid = row[0]
                            s_data = json.loads(row[1]) if row[1] else {}
                            ws_root = s_data.get("workspace_root", "/home/jiang/github")
                            if target_proj.startswith("/") and os.path.isdir(target_proj):
                                new_path = target_proj
                            elif os.path.isdir(os.path.join(ws_root, target_proj)):
                                new_path = os.path.join(ws_root, target_proj)
                            else:
                                matched = None
                                for rp in s_data.get("recent_projects", []):
                                    if os.path.basename(rp.rstrip("/")) == target_proj:
                                        matched = rp
                                        break
                                new_path = matched if matched else os.path.join(ws_root, target_proj)

                            s_data["project"] = new_path
                            recents = s_data.get("recent_projects", [])
                            if new_path in recents:
                                recents.remove(new_path)
                            recents.insert(0, new_path)
                            s_data["recent_projects"] = recents[:10]

                            c.execute("UPDATE chat_sessions SET data = ? WHERE chat_id = ?", (json.dumps(s_data, ensure_ascii=False), cid))
                            conn.commit()
                            log.info(f"已同步更新飞书机器人工作目录为: {new_path}")
                            self._last_synced_project = new_path
                        conn.close()
                except Exception as e:
                    log.error(f"切换项目更新数据库异常: {e}")

                cur_name = os.path.basename(new_path.rstrip("/")) if new_path else target_proj
                _, projects = self.get_available_projects()
                await ws.send_str(json.dumps({
                    "type": "project_switched",
                    "project": cur_name,
                    "status": "success"
                }))
                await ws.send_str(json.dumps({
                    "type": "project_list",
                    "current": cur_name,
                    "projects": projects
                }))

                if cid:
                    card = self.build_project_switch_card(new_path, s_data)
                    asyncio.create_task(self.send_feishu_card_to_chat(cid, card))

    async def _handle_client_binary(self, ws: web.WebSocketResponse, data: bytes):
        """流式追加设备录音音频分片并进行最大长度安全截断保护"""
        if ws in self.voice_buffers:
            # 单次录音上限保护：16kHz 16bit 单声道 60秒 = 1,920,000 字节 (~1.83MB)
            max_audio_bytes = CONFIG.get("max_audio_buffer_bytes", 1920000)
            buf = self.voice_buffers[ws]
            cur_len = len(buf)

            if cur_len >= max_audio_bytes:
                # 已经达到或超过上限，主动截断丢弃后续分片，防止内存无上限膨胀
                if cur_len == max_audio_bytes or (cur_len - len(data) < max_audio_bytes):
                    log.warning(f"⚠️ 硬件音频缓冲已达到最大上限 ({max_audio_bytes} 字节, 约 {max_audio_bytes/32000:.0f}s)，后续音频分片已自动截断抛弃")
                return

            remaining = max_audio_bytes - cur_len
            if len(data) > remaining:
                buf.extend(data[:remaining])
                log.warning(f"⚠️ 硬件音频缓冲写入触发截断保护，达到上限 {max_audio_bytes} 字节")
            else:
                buf.extend(data)

            cur_len = len(buf)
            if cur_len % 32000 < len(data):
                log.info(f"🎙️ 正在流式接收音频，当前累积: {cur_len} 字节 (约 {cur_len/32000:.1f}s)")
        else:
            log.warning(f"⚠️ 收到二进制音频分片 ({len(data)} 字节)，但 ws 不在 voice_buffers 中！")

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
                cur_name, projects = self.get_available_projects()
                await ws.send_str(json.dumps({
                    "type": "project_list",
                    "current": cur_name,
                    "projects": projects
                }))
                await ws.send_str(json.dumps({
                    "type": "project_switched",
                    "project": cur_name,
                    "status": "connected"
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

    def _load_ignored_ota(self) -> dict:
        try:
            if os.path.exists(self._ignored_ota_file):
                with open(self._ignored_ota_file, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            log.warning(f"[OTA] 读取已忽略固件版本异常: {e}")
        return {}

    def _save_ignored_ota(self, data: dict):
        try:
            with open(self._ignored_ota_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.warning(f"[OTA] 保存已忽略固件版本异常: {e}")

    async def get_latest_github_release(self) -> Optional[dict]:
        """获取 GitHub 最新的固件 Release 发布信息 (带内存缓存 60 秒)"""
        now = time.time()
        if self._cached_release and (now - self._cached_release_time < 60):
            return self._cached_release

        token = CONFIG.get("github_token", "")
        url = "https://api.github.com/repos/Level6me/Letgo/releases?per_page=1"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "PassportBridge-OTA"
        }

        loop = asyncio.get_running_loop()
        def _fetch():
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                if data and isinstance(data, list):
                    return data[0]
                return None

        try:
            release = await loop.run_in_executor(None, _fetch)
            if release:
                self._cached_release = release
                self._cached_release_time = now
                return release
        except Exception as e:
            log.warning(f"[OTA] 获取 GitHub 最新 Release 失败: {e}")
        return None

    async def check_device_ota(self, ws: web.WebSocketResponse, mac: str, data: dict):
        """当设备握手连接时，检查固件版本并向飞书推送更新交互卡片"""
        if not mac:
            return

        # 延迟 1 秒，等待握手与看板同步稳定完成
        await asyncio.sleep(1.0)

        release = await self.get_latest_github_release()
        if not release:
            log.info(f"[OTA] 未能获取到最新 Release 信息，跳过检测: MAC={mac}")
            return

        latest_tag = release.get("tag_name", "").strip()
        if not latest_tag:
            return

        dev_version = data.get("version", "未知版本")
        fw_tag = data.get("firmware_tag", "")
        compile_date = data.get("compile_date", "")
        compile_time = data.get("compile_time", "")

        # 1. 检查是否已经是该版本 (需双重核验实际内核编译日期，防止 NVS fw_tag 提前写入后实际 OTA 失败或回滚导致的假升级现象)
        is_up_to_date = (fw_tag == latest_tag or (dev_version and dev_version == latest_tag))
        if is_up_to_date and "Oct 12" in compile_date:
            log.warning(f"[OTA] 设备上报 tag 虽然为 {fw_tag}，但真实内核编译日期滞后 ({compile_date})，判定为未真正刷入，允许重新推送升级: MAC={mac}")
            is_up_to_date = False

        if is_up_to_date:
            log.info(f"[OTA] 设备已是最新固件 ({latest_tag})，无需提醒: MAC={mac}")
            return

        # 2. 检查是否已经向飞书推送过该版本的卡片（杜绝重复刷屏）
        card_key = f"{mac}:{latest_tag}"
        if card_key in self.ota_card_msg_ids:
            return

        # 3. 检查用户是否在此设备上忽略过此版本
        ignored_map = self._load_ignored_ota()
        if latest_tag in ignored_map.get(mac, []):
            log.info(f"[OTA] 固件版本 {latest_tag} 已被用户设置为忽略: MAC={mac}")
            return

        # 3. 推送交互卡片到飞书
        client = get_lark_client()
        admin_chat_id = CONFIG.get("admin_chat_id")
        if not client or not admin_chat_id:
            log.warning("[OTA] 缺少 Lark Client 或 admin_chat_id，无法推送固件更新卡片")
            return

        created_at = release.get("created_at", "")
        if created_at:
            try:
                t_obj = time.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
                t_local = time.localtime(time.mktime(t_obj) + 8 * 3600)
                created_str = time.strftime("%Y-%m-%d %H:%M:%S", t_local)
            except Exception:
                created_str = created_at
        else:
            created_str = "未知时间"

        body_snippet = release.get("body", "").strip()
        if not body_snippet:
            body_snippet = "FoloToy AI Passport 固件更新包"
        else:
            body_snippet = body_snippet[:150] + ("..." if len(body_snippet) > 150 else "")

        server_ip = CONFIG.get("external_ip") or "10.0.0.2"
        upgrade_url = f"http://{server_ip}:{self.port}/api/ota_action?action=upgrade&mac={mac}&tag={latest_tag}"
        ignore_url = f"http://{server_ip}:{self.port}/api/ota_action?action=ignore&mac={mac}&tag={latest_tag}"

        curr_info = f"`{dev_version}`"
        if compile_date or compile_time:
            curr_info += f" ({compile_date} {compile_time})"

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "🚀 检测到 AI Passport 固件新版本"},
                "template": "blue"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**📟 设备开机连接就绪，检测到云端有新编译固件：**\n"
                            f"• **设备名称**：`FoloToy AI Passport`\n"
                            f"• **硬件 MAC**：`{mac}`\n"
                            f"• **当前固件版本**：{curr_info}\n"
                            f"• **最新固件版本**：**`{latest_tag}`**\n"
                            f"• **构建发布时间**：{created_str}\n"
                            f"• **更新摘要**：{body_snippet}\n\n"
                            f"点击下方按钮可向设备直接推送 OTA 无线升级："
                        )
                    }
                },
                {"tag": "hr"},
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "🚀 推送更新"},
                            "type": "primary",
                            "url": upgrade_url
                        },
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "⚪ 忽略本次更新"},
                            "type": "default",
                            "url": ignore_url
                        }
                    ]
                }
            ]
        }

        try:
            req = CreateMessageRequest.builder() \
                .receive_id_type("chat_id") \
                .request_body(CreateMessageRequestBody.builder()
                    .receive_id(admin_chat_id)
                    .msg_type("interactive")
                    .content(json.dumps(card))
                    .build()) \
                .build()
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(None, lambda: client.im.v1.message.create(req))
            if resp.code == 0:
                msg_id = json.loads(resp.raw.content).get("data", {}).get("message_id")
                self.ota_card_msg_ids[f"{mac}:{latest_tag}"] = msg_id
                log.info(f"[OTA] 已成功向飞书发送固件升级卡片 (MsgID={msg_id}, Tag={latest_tag})")
            else:
                log.error(f"[OTA] 发送飞书固件升级卡片失败: {resp.msg}")
        except Exception as e:
            log.error(f"[OTA] 推送飞书固件升级卡片异常: {e}")

    async def handle_ota_action(self, request: web.Request) -> web.Response:
        """处理飞书卡片上的【推送更新】与【忽略本次更新】操作"""
        action = request.query.get("action", "").lower().strip()
        mac = request.query.get("mac", "").lower().strip()
        tag = request.query.get("tag", "").strip()

        if not mac or not tag:
            return web.Response(text="缺少必要参数 (mac, tag)", status=400)

        card_key = f"{mac}:{tag}"
        msg_id = self.ota_card_msg_ids.get(card_key)
        client = get_lark_client()

        if action == "ignore":
            ignored_map = self._load_ignored_ota()
            mac_ignored = set(ignored_map.get(mac, []))
            mac_ignored.add(tag)
            ignored_map[mac] = list(mac_ignored)
            self._save_ignored_ota(ignored_map)
            log.info(f"[OTA] 用户已忽略固件版本 {tag}: MAC={mac}")

            if msg_id and client:
                now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                patch_card = {
                    "config": {"wide_screen_mode": True},
                    "header": {
                        "title": {"tag": "plain_text", "content": "⚪ AI Passport 固件更新已忽略"},
                        "template": "grey"
                    },
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": (
                                    f"• **硬件 MAC**：`{mac}`\n"
                                    f"• **版本号**：`{tag}`\n"
                                    f"• **处理结果**：⚪ **已忽略本次更新**\n"
                                    f"• 该版本后续开机连接将不再向飞书重复推送卡片提醒。\n"
                                    f"• **处理时间**：{now_str}"
                                )
                            }
                        }
                    ]
                }
                try:
                    req = PatchMessageRequest.builder() \
                        .message_id(msg_id) \
                        .request_body(PatchMessageRequestBody.builder().content(json.dumps(patch_card)).build()) \
                        .build()
                    client.im.v1.message.patch(req)
                except Exception as e:
                    log.warning(f"[OTA] 更新飞书忽略卡片失败: {e}")

            html = f"""
            <html><head><meta charset="utf-8"><title>已忽略本次更新</title></head>
            <body style="font-family:sans-serif;text-align:center;padding:50px;">
                <h1 style="color:#86909C;">⚪ 已忽略固件更新</h1>
                <p>设备 (MAC: {mac}) 后续开机连接将不再重复提醒版本 <b>{tag}</b>。</p>
            </body></html>
            """
            return web.Response(text=html, content_type="text/html")

        elif action == "upgrade":
            ws = self.mac_to_ws.get(mac)
            if not ws or ws.closed:
                return web.Response(text="❌ 设备当前未连接，无法下发 OTA 升级指令，请确保设备在线后重试。", status=400)

            tag_dir = os.path.join(self.firmware_dir, tag)
            os.makedirs(tag_dir, exist_ok=True)
            ota_file = os.path.join(tag_dir, "ota.bin")

            if not os.path.exists(ota_file):
                log.info(f"[OTA] 本地尚未缓存 {tag} 固件，开始从 GitHub Release 获取...")
                release = await self.get_latest_github_release()
                download_url = None
                is_full_bin = False
                for asset in (release.get("assets", []) if release else []):
                    if asset.get("name") in ["FoloToy-AI-Passport-ota.bin", "xiao_zhi.bin"]:
                        download_url = asset.get("browser_download_url")
                        is_full_bin = False
                        break
                    elif asset.get("name") == "FoloToy-AI-Passport-full.bin":
                        download_url = asset.get("browser_download_url")
                        is_full_bin = True

                if not download_url:
                    return web.Response(text=f"无法从 Release {tag} 中获取固件下载地址", status=500)

                token = CONFIG.get("github_token", "")
                tmp_file = os.path.join(tag_dir, "download.bin")
                loop = asyncio.get_running_loop()
                def _dl():
                    req = urllib.request.Request(download_url, headers={
                        "Authorization": f"Bearer {token}",
                        "Accept": "application/octet-stream",
                        "User-Agent": "PassportBridge-OTA"
                    })
                    with urllib.request.urlopen(req) as resp, open(tmp_file, "wb") as f_out:
                        f_out.write(resp.read())

                await loop.run_in_executor(None, _dl)

                if is_full_bin:
                    with open(tmp_file, "rb") as sf:
                        sf.seek(0x1A0000)
                        ota_bytes = sf.read()
                    with open(ota_file, "wb") as df:
                        df.write(ota_bytes)
                    try:
                        os.remove(tmp_file)
                    except Exception:
                        pass
                else:
                    os.rename(tmp_file, ota_file)
                log.info(f"[OTA] 固件准备完毕: {ota_file} ({os.path.getsize(ota_file)} 字节)")

            server_ip = CONFIG.get("external_ip") or "10.0.0.2"
            firmware_url = f"http://{server_ip}:{self.port}/firmware/{tag}/ota.bin"

            ota_cmd = {
                "type": "ota_upgrade",
                "url": firmware_url,
                "version": tag
            }
            await ws.send_str(json.dumps(ota_cmd))
            log.info(f"[OTA] 成功向设备下发 OTA 升级指令: url={firmware_url}, version={tag}")

            if msg_id and client:
                now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                patch_card = {
                    "config": {"wide_screen_mode": True},
                    "header": {
                        "title": {"tag": "plain_text", "content": "🚀 AI Passport 固件更新进行中"},
                        "template": "turquoise"
                    },
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": (
                                    f"• **硬件 MAC**：`{mac}`\n"
                                    f"• **目标版本**：**`{tag}`**\n"
                                    f"• **升级状态**：✅ **已成功下发 OTA 指令！**\n"
                                    f"• 设备正在高速下载固件并写入 Flash。\n"
                                    f"• 屏幕正在显示百分比进度条，写入完成后将自动重启生效。\n"
                                    f"• **推送时间**：{now_str}"
                                )
                            }
                        }
                    ]
                }
                try:
                    req = PatchMessageRequest.builder() \
                        .message_id(msg_id) \
                        .request_body(PatchMessageRequestBody.builder().content(json.dumps(patch_card)).build()) \
                        .build()
                    client.im.v1.message.patch(req)
                except Exception as e:
                    log.warning(f"[OTA] 更新飞书升级进行中卡片失败: {e}")

            html = f"""
            <html><head><meta charset="utf-8"><title>固件升级已推送</title></head>
            <body style="font-family:sans-serif;text-align:center;padding:50px;">
                <h1 style="color:#00B96B;">🚀 固件 OTA 升级指令已下发！</h1>
                <p>设备 (MAC: {mac}) 已收到升级指令并开始从局域网下载 <b>{tag}</b>。</p>
                <p>请留意设备屏幕上的百分比进度，下载写入完毕后将自动重启完成升级。</p>
            </body></html>
            """
            return web.Response(text=html, content_type="text/html")

        return web.Response(text="未知操作", status=400)

    async def handle_firmware_download(self, request: web.Request) -> web.StreamResponse:
        """为设备提供 OTA 固件静态下载服务"""
        tag = request.match_info.get("tag")
        filename = request.match_info.get("filename")
        if not tag or not filename:
            return web.Response(text="参数错误", status=400)

        filepath = os.path.join(self.firmware_dir, tag, filename)
        if not os.path.exists(filepath):
            return web.Response(text="固件文件不存在", status=404)

        return web.FileResponse(filepath)

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
                    "memory_context": session_data.get("memory_context", ""),
                    "workspace_root": session_data.get("workspace_root", "/home/jiang/github"),
                    "recent_projects": session_data.get("recent_projects", []),
                    "ignored_projects": session_data.get("ignored_projects", []),
                    "raw_session_data": session_data
                }
        except Exception as e:
            log.error(f"读取飞书机器人活跃会话失败: {e}")
        return None

    def get_available_projects(self) -> tuple[str, list[str]]:
        """获取当前活跃项目及工作区所有可用项目列表 (当前项置顶，近期项目优先，并按字母排序)"""
        bot_session = self.get_active_bot_session()
        current_proj = bot_session.get("project", "") if bot_session else ""
        cur_name = os.path.basename(current_proj.rstrip("/")) if current_proj else "Letgo"

        projects = []
        if cur_name:
            projects.append(cur_name)

        ws_root = "/home/jiang/github"
        recents = []
        ignored = []
        if bot_session:
            ws_root = bot_session.get("workspace_root", "/home/jiang/github")
            recents = bot_session.get("recent_projects", [])
            ignored = bot_session.get("ignored_projects", [])

        # 优先追加近期活跃项目
        for p in recents:
            p_name = os.path.basename(p.rstrip("/"))
            if p_name and p_name not in projects and p not in ignored:
                projects.append(p_name)

        # 扫描工作区目录补充其余工程
        if os.path.exists(ws_root):
            try:
                for n in sorted(os.listdir(ws_root)):
                    if n.startswith(".") or n in ["venv", "downloads"]:
                        continue
                    fp = os.path.join(ws_root, n)
                    if fp in ignored:
                        continue
                    if os.path.isdir(fp) and n not in projects:
                        projects.append(n)
            except Exception as e:
                log.warning(f"扫描工作区项目目录失败: {e}")

        if not projects:
            projects = ["Letgo", "passport_game", "feishu-bot-plugin", "antigravity-feishu-bot"]

        return cur_name, projects[:12]

    def get_context_stats_fast(self, session_data: dict = None) -> dict:
        """快速估算当前会话上下文使用情况"""
        model_name = session_data.get("model", "gemini-3.8-flash-high") if session_data else "gemini-3.8-flash-high"
        max_tokens = 1000000
        if "claude" in model_name.lower():
            max_tokens = 200000
        elif any(k in model_name.lower() for k in ["gpt-4", "gpt4", "gpt"]):
            max_tokens = 128000

        conv_id = session_data.get("conversation") if session_data else ""
        transcript_path = ""
        if conv_id:
            p = f"/home/jiang/.gemini/antigravity-cli/brain/{conv_id}/.system_generated/logs/transcript.jsonl"
            if os.path.exists(p):
                transcript_path = p

        total_tokens = 0
        if transcript_path and os.path.exists(transcript_path):
            try:
                def est_tok(txt):
                    if not txt:
                        return 0
                    if not isinstance(txt, str):
                        txt = json.dumps(txt, ensure_ascii=False)
                    cjk = sum(1 for c in txt if '\u4e00' <= c <= '\u9fff')
                    return int(cjk * 1.5 + (len(txt) - cjk) / 3.8)

                with open(transcript_path, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            d = json.loads(line)
                            total_tokens += est_tok(d.get("content", "")) + est_tok(d.get("thinking", "")) + est_tok(d.get("tool_calls", ""))
                        except Exception:
                            pass
            except Exception:
                pass

        if total_tokens == 0:
            total_tokens = 2500
        free_tokens = max(0, max_tokens - total_tokens)
        free_pct = max(0.0, 100.0 - (total_tokens / max_tokens * 100.0))
        return {
            "total_tokens": total_tokens,
            "free_tokens": free_tokens,
            "max_tokens": max_tokens,
            "free_pct": free_pct
        }

    def build_project_switch_card(self, target_path: str, session_data: dict) -> dict:
        """构建与飞书 App /project 面板完全一致的响应通知卡片"""
        project_name_only = os.path.basename(target_path) if target_path else "默认"
        current_model = session_data.get("model", "Default") if session_data else "Default"
        success_text = (
            f"📂 **工作区项目切换成功！**\n\n"
            f"当前已将活跃目录设定为：\n`{target_path}`"
        )
        elements = [{
            "tag": "markdown",
            "content": success_text
        }]

        stats = self.get_context_stats_fast(session_data)
        def format_k(num):
            if num >= 1000000:
                return f"{num / 1000000:.1f}M"
            elif num >= 1000:
                return f"{num / 1000:.1f}K"
            return str(num)

        raw_free_pct = stats.get("free_pct", 100.0)
        total_tokens = stats.get("total_tokens", 0)
        free_tokens = stats.get("free_tokens", 1000000)
        max_tokens = stats.get("max_tokens", 1000000)

        if total_tokens == 0 or raw_free_pct >= 100.0:
            free_pct_str = "100.0"
        elif raw_free_pct >= 99.9:
            free_pct_str = f"{raw_free_pct:.2f}" if raw_free_pct < 99.99 else "99.99"
        else:
            free_pct_str = f"{raw_free_pct:.1f}"

        context_str = f"🧠 上下文剩余: {free_pct_str}% ({format_k(free_tokens)}/{format_k(max_tokens)})"
        meta_parts = [f"🤖 模型: {current_model}", f"🗂️ 项目: {project_name_only}", context_str]
        elements.append({
            "tag": "markdown",
            "content": f"<font color='grey'>{' | '.join(meta_parts)}</font>"
        })

        now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        elements.append({
            "tag": "note",
            "elements": [
                {
                    "tag": "plain_text",
                    "content": f"⚡ Powered by Antigravity | 🕒 {now_str}"
                }
            ]
        })

        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": "blue",
                "title": {"content": "✨ AI 回复", "tag": "plain_text"}
            },
            "elements": elements
        }

    async def send_feishu_card_to_chat(self, chat_id: str, card: dict):
        """向指定的飞书会话推送交互式卡片"""
        client = get_lark_client()
        if not client:
            log.warning("Lark client 不可用，无法发送项目切换卡片")
            return
        try:
            req = CreateMessageRequest.builder() \
                .receive_id_type("chat_id") \
                .request_body(CreateMessageRequestBody.builder() \
                    .receive_id(chat_id) \
                    .msg_type("interactive") \
                    .content(json.dumps(card)) \
                    .build()) \
                .build()
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(None, lambda: client.im.v1.message.create(req))
            if resp.code == 0:
                log.info(f"已成功向飞书会话 ({chat_id}) 推送项目切换通知卡片")
            else:
                log.error(f"推送项目切换卡片失败: code={resp.code}, msg={resp.msg}")
        except Exception as e:
            log.error(f"推送飞书卡片异常: {e}")

    async def _project_sync_loop(self):
        """定期巡检飞书机器人数据库中的活跃项目变动，并在发生切换时主动广播同步至硬件"""
        while True:
            try:
                await asyncio.sleep(1.0)
                bot_session = self.get_active_bot_session()
                if not bot_session:
                    continue
                cur_proj = bot_session.get("project", "")
                if not cur_proj:
                    continue
                if self._last_synced_project is None:
                    self._last_synced_project = cur_proj
                    continue
                if cur_proj != self._last_synced_project:
                    log.info(f"[ProjectSync] 检测到飞书控制台切换项目: {self._last_synced_project} -> {cur_proj}")
                    self._last_synced_project = cur_proj
                    cur_name, projects = self.get_available_projects()
                    switched_msg = json.dumps({
                        "type": "project_switched",
                        "project": cur_name,
                        "status": "success"
                    })
                    list_msg = json.dumps({
                        "type": "project_list",
                        "current": cur_name,
                        "projects": projects
                    })
                    for ws in list(self.active_websockets):
                        try:
                            await ws.send_str(switched_msg)
                            await ws.send_str(list_msg)
                        except Exception:
                            pass

                # 定期巡检已连接设备的固件升级 (每300秒)
                now_ts = time.time()
                if now_ts - self._last_ota_check_time > 300:
                    self._last_ota_check_time = now_ts
                    for m, w in list(self.mac_to_ws.items()):
                        if not w.closed:
                            d = self.ws_info.get(w, {}).get("data", {})
                            asyncio.create_task(self.check_device_ota(w, m, d))
            except Exception as e:
                log.warning(f"项目同步巡检异常: {e}")

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

    async def generate_ai_reply(
        self,
        user_query: str,
        mac: str = "",
        feishu_msg_id: str = "",
        feishu_msg_id_fut: Optional[asyncio.Future] = None,
        planned_steps: list = None,
        duration_sec: float = 0.0,
        on_sentence: Optional[Callable[[str], Awaitable[None]]] = None,
        ws: Optional[web.WebSocketResponse] = None
    ) -> tuple:
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
                if on_sentence:
                    await on_sentence(reply_text)
                return reply_text, executed_tools
        except Exception as e:
            log.warning(f"智能家居直通执行异常，回退至大模型调度: {e}")

        project_dir = bot_session.get("project", "") if bot_session else ""
        if not project_dir or not os.path.isdir(project_dir):
            project_dir = "/home/jiang/github/Letgo"

        recent_context = self.extract_recent_transcript_context(conv_id)
        context_block = f"【飞书App当前会话最近讨论记录】\n{recent_context}\n\n" if recent_context else ""

        # 动态扫描最新编译产物状态
        fw_status_note = "【系统环境实时状态】：当前固件正在编译或准备中。"
        fw_out_dir = os.path.join(project_dir, "firmware_output")
        if os.path.exists(fw_out_dir):
            try:
                runs = [d for d in os.listdir(fw_out_dir) if d.startswith("fw-") and os.path.isdir(os.path.join(fw_out_dir, d))]
                if runs:
                    runs.sort(key=lambda d: os.path.getmtime(os.path.join(fw_out_dir, d)), reverse=True)
                    latest_run = runs[0]
                    bin_path = os.path.join(fw_out_dir, latest_run, "FoloToy-AI-Passport-full.bin")
                    if os.path.exists(bin_path):
                        sz_mb = round(os.path.getsize(bin_path) / (1024 * 1024), 2)
                        fw_status_note = f"【系统环境实时状态】：当前最新固件（{latest_run}，{sz_mb}MB）已构建编译完成，完整固件包已下载到本地！"
            except Exception:
                pass

        prompt = (
            f"[这是来自用户随身硬件对讲设备 FoloToy AI Passport 的即时语音输入 (MAC: {mac})]\n"
            f"重要原则：用户使用随身硬件对讲提问与在飞书App提问完全等价，拥有最高优先级与完全同等的Agent会话流水线能力！\n"
            f"用户在设备上长按提问：「{user_query}」\n\n"
            f"【用户核心准则与全量工具执行权限】：\n"
            f"1. 你拥有当前工程（项目目录：{project_dir}）的全部工具执行权限（包括运行命令 run_command、查看/搜索/修改代码 view_file/replace_file_content/write_to_file、Git操作等）。\n"
            f"2. 当用户发出的是操作、执行、排查问题、检查固件、定位Bug、推送代码、构建测试等任何工程命令时，你必须立即自主调用工具链进行真实排查与执行，绝对严禁推诿、绝对严禁让用户手动操作、绝对严禁在未成功执行时谎称已完成！\n"
            f"3. 【任务规划强制规范】：只要判断当前任务需要调用工具执行操作（读写代码、运行终端命令、多步排错等），必须在回复最开始输出一行任务规划：\n"
            f"[TASK_PLAN] 步骤1名称 | 步骤2名称 | 步骤3名称 [/TASK_PLAN]\n"
            f"规划精炼（3~4步为宜），飞书界面会自动识别并为用户实时渲染动态步骤进度清单！\n"
            f"4. 如果用户发出的是智能家居控制指令（如开灯/关灯/查设备/开关空调/开电脑等），系统已提供本地极速控制工具 ha-ctl（在PATH中），你可以直接在命令行运行（例如 ha-ctl find 客厅 查找设备，ha-ctl turn_on 开启，ha-ctl turn_off 关闭，ha-ctl list 列出所有设备）。请优先使用 ha-ctl 快速完成控制，切勿在容器内盲目探索！\n"
            f"5. 注意隐私保护，严禁在任何地方明文暴露 GitHub Token 或个人凭证。\n\n"
            f"{fw_status_note}\n\n"
            f"{context_block}"
            f"【核心回答指令】\n"
            f"如果用户指令需要排查或执行操作，先调用工具完成操作，然后再生成播报文本。\n"
            f"最终播报文本要求：第一句话直接给出核心结论或操作结果，篇幅控制在20至50字以内，极简口语化，适合硬件喇叭朗读；严禁输出Markdown标题、加粗符号、代码块、列表符号或表情符号。"
        )

        agy_bin = CONFIG.get("agy_bin", "/home/jiang/.local/bin/agy")
        raw_model = (bot_session.get("model") if bot_session else "") or "gemini-3.8-flash-medium"

        cmd = [
            agy_bin,
            "-p", prompt,
            "--model", raw_model,
            "--dangerously-skip-permissions",
            "--output-format", "stream-json",
            "--print-timeout", "300s"
        ]

        # 仅当模型名称未自带 -low/-medium/-high 等固定 effort 后缀时，才附加 --effort，杜绝 agy 参数冲突报错
        if not any(raw_model.endswith(f"-{e}") for e in ["low", "medium", "high", "xhigh", "max"]):
            eff = (bot_session.get("effort") if bot_session else "") or "medium"
            if eff:
                cmd.extend(["--effort", eff])

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

        chunker = SentenceChunker()
        accumulated_deltas = []
        final_result_response = ""
        emitted_sentences = 0
        proc = None

        current_step_idx = 0
        planned_steps = list(planned_steps or [])
        last_patch_time = 0.0

        async def maybe_patch_card(active_tool=None, force=False):
            nonlocal last_patch_time, current_step_idx
            now_t = time.time()
            if not force and now_t - last_patch_time < 0.25 and active_tool is None:
                return

            mid = feishu_msg_id
            if not mid and feishu_msg_id_fut and feishu_msg_id_fut.done():
                mid = feishu_msg_id_fut.result()

            if not mid:
                return

            last_patch_time = now_t
            card = build_voice_interaction_card(
                user_text=user_query,
                executed_tools=executed_tools,
                is_completed=False,
                think_seconds=int(now_t - start_time),
                planned_steps=planned_steps,
                current_step_idx=current_step_idx,
                active_tool=active_tool
            )
            try:
                client = get_lark_client()
                if client:
                    req = PatchMessageRequest.builder() \
                        .message_id(mid) \
                        .request_body(PatchMessageRequestBody.builder().content(json.dumps(card)).build()) \
                        .build()
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, lambda: client.im.v1.message.patch(req))
            except Exception as e:
                log.warning(f"实时更新飞书步骤卡片异常: {e}")

        try:
            # 彻底清理环境变量，阻断所有会话锁和嵌套干扰
            custom_env = os.environ.copy()
            for k in list(custom_env.keys()):
                if any(x in k.lower() for x in ["antigravity", "conversation", "trajectory", "agentapi", "gemini_cli"]):
                    custom_env.pop(k, None)

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=project_dir,
                env=custom_env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True
            )
            if ws:
                self.active_ai_procs[ws] = proc

            async def read_stream():
                nonlocal final_result_response, emitted_sentences, current_step_idx, planned_steps
                while True:
                    line = await proc.stdout.readline()
                    if not line:
                        break
                    line_str = line.decode("utf-8", errors="ignore").strip()
                    if not line_str:
                        continue
                    try:
                        evt = json.loads(line_str)
                        evt_name = evt.get("event")
                        if evt_name == "step_update":
                            su = evt.get("step_update", {})
                            st = su.get("step_type")
                            delta = su.get("text_delta", "")
                            if delta and st == "agent_response":
                                accumulated_deltas.append(delta)
                                cur_acc_text = "".join(accumulated_deltas)
                                if "[TASK_PLAN]" in cur_acc_text and "[/TASK_PLAN]" in cur_acc_text:
                                    m_plan = re.search(r"\[TASK_PLAN\](.*?)\[/TASK_PLAN\]", cur_acc_text, re.DOTALL)
                                    if m_plan:
                                        p_text = m_plan.group(1).strip()
                                        steps = [s.strip() for s in p_text.split("|") if s.strip()]
                                        if steps and steps != planned_steps:
                                            planned_steps = steps
                                            current_step_idx = 0
                                            log.info(f"📋 [随身语音识别出任务规划] 共 {len(steps)} 步: {steps}")
                                            await maybe_patch_card(force=True)

                                if on_sentence:
                                    clean_delta = re.sub(r"\[TASK_PLAN\].*?\[/TASK_PLAN\]", "", delta, flags=re.DOTALL)
                                    clean_delta = re.sub(r"\[TASK_PLAN\].*", "", clean_delta)
                                    if clean_delta:
                                        s_list = chunker.feed(clean_delta)
                                        for s in s_list:
                                            c_s = clean_text_for_speech(s)
                                            if c_s:
                                                emitted_sentences += 1
                                                await on_sentence(c_s)
                            elif st == "tool":
                                t_state = su.get("state", "ACTIVE")
                                t_name = su.get("tool_name") or su.get("name") or "tool"
                                t_info = su.get("tool_info", {})
                                if not isinstance(t_info, dict):
                                    t_info = {}
                                t_params = t_info.get("parameters") or t_info.get("args") or su.get("parameters") or su.get("args") or {}
                                if not isinstance(t_params, dict):
                                    t_params = {}
                                summary = str(t_params.get("toolSummary", "") or t_params.get("toolAction", "")).strip(" \"'\n\r")
                                target = (
                                    t_params.get("CommandLine") or
                                    t_params.get("TargetFile") or
                                    t_params.get("AbsolutePath") or
                                    t_params.get("query") or
                                    t_params.get("Url") or ""
                                )
                                target = str(target).strip(" \"'\n\r")
                                tool_item = {
                                    "name": t_name,
                                    "summary": summary or f"执行 {t_name}",
                                    "target": target[:80] if target else "",
                                    "output": ""
                                }
                                if t_state != "DONE":
                                    executed_tools.append(tool_item)
                                    if planned_steps:
                                        current_step_idx = min(len(planned_steps) - 1, max(0, len(executed_tools) - 1))
                                    log.info(f"🛠️ [硬件触发工具执行] {t_name}: {summary} ({target[:40]})")
                                    await maybe_patch_card(active_tool=tool_item, force=True)
                                else:
                                    log.info(f"✅ [硬件触发工具完成] {t_name}: {summary}")
                                    await maybe_patch_card(active_tool=None, force=True)
                        elif evt_name == "result":
                            res = evt.get("result", {})
                            final_result_response = res.get("response", "")
                    except Exception:
                        pass

                # 流结束时刷新剩余句子
                if on_sentence:
                    tail_list = chunker.flush()
                    for s in tail_list:
                        c_s = clean_text_for_speech(s)
                        if c_s:
                            emitted_sentences += 1
                            await on_sentence(c_s)

            await asyncio.wait_for(read_stream(), timeout=300.0)
            await proc.wait()
            stderr_bytes = await proc.stderr.read()
            stderr_out = stderr_bytes.decode("utf-8", errors="ignore").strip()
            log.info(f"agy 执行完毕 (RC={proc.returncode}, 发送分句数={emitted_sentences}, stderr={len(stderr_out)})")
            if proc.returncode != 0 and stderr_out:
                log.warning(f"agy 报错输出: {stderr_out[:200]}")

            raw_full = final_result_response or "".join(accumulated_deltas)
            cleaned = clean_text_for_speech(raw_full)

            # 如果从未发射过分句（例如模型一次性给出），则发射整句
            if emitted_sentences == 0 and cleaned and on_sentence:
                emitted_sentences += 1
                await on_sentence(cleaned)

            if cleaned:
                stop_monitor.set()
                await monitor_task
                if active_t_path and os.path.exists(active_t_path):
                    parsed_tools = self.extract_session_tools_from_transcript(active_t_path)
                    if parsed_tools:
                        executed_tools = parsed_tools

                if chat_id:
                    self.update_bot_session_summary(chat_id, f"🎙️ [随身设备语音] {user_query}", cleaned)
                if conv_id:
                    self.append_turn_to_transcript(conv_id, user_query, cleaned)
                return cleaned, executed_tools
            else:
                log.warning(f"AI 输出经过清理后为空，原始输出: {raw_full[:100]}")
        except asyncio.CancelledError:
            log.info("AI 推理任务被外部中断取消 (Barge-in / 打断)")
            if proc and proc.returncode is None:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
            raise
        except asyncio.TimeoutError:
            log.warning("AI 生成回复超时 (300s)")
            if proc and proc.returncode is None:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
        except Exception as e:
            log.error(f"AI 生成回复异常: {e}")
        finally:
            if ws:
                self.active_ai_procs.pop(ws, None)
            stop_monitor.set()
            if not monitor_task.done():
                await monitor_task
            if active_t_path and os.path.exists(active_t_path):
                executed_tools = self.extract_session_tools_from_transcript(active_t_path)

        if executed_tools:
            fallback = f"执行「{user_query}」耗时超过限制，后台已执行 {len(executed_tools)} 个操作步骤，请在飞书卡片查看具体执行明细。"
        else:
            fallback = f"执行「{user_query}」响应超时，请稍后重试或在飞书直接下达。"

        if emitted_sentences == 0 and on_sentence:
            emitted_sentences += 1
            await on_sentence(fallback)

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

        # 6. 【流式切片抢跑与重叠流水线】：创建句子队列与音频推流流水线
        sentence_queue: asyncio.Queue = asyncio.Queue()
        pcm_queue: asyncio.Queue = asyncio.Queue()
        interrupted_ev = asyncio.Event()

        def is_interrupted() -> bool:
            return interrupted_ev.is_set() or ws.closed or ws not in self.active_websockets

        # 协程A：TTS 合成流水线 (逐句取文本 -> Edge-TTS 预热连接池流式转码 -> 写入 pcm_queue)
        async def tts_worker():
            try:
                while not is_interrupted():
                    sentence = await sentence_queue.get()
                    if sentence is None:
                        # 所有分句已全部送达
                        break
                    clean_s = clean_text_for_speech(sentence)
                    if not clean_s:
                        sentence_queue.task_done()
                        continue

                    log.info(f"🔊 [流式TTS切片抢跑] 正在合成小句: 「{clean_s}」")

                    async def _on_pcm_chunk(pcm_data: bytes):
                        if not is_interrupted():
                            await pcm_queue.put(pcm_data)

                    stream_ok = await tts_pool.stream_pcm_realtime(
                        text=clean_s,
                        on_pcm_chunk=_on_pcm_chunk,
                        check_interrupted=is_interrupted
                    )
                    if not stream_ok and not is_interrupted():
                        # 若连接池偶发异常，降级到单句一次性合成
                        fallback_pcm = await self.synthesize_speech_pcm(clean_s)
                        if fallback_pcm and not is_interrupted():
                            await pcm_queue.put(fallback_pcm)

                    sentence_queue.task_done()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.warning(f"TTS 流水线合成异常: {e}")
            finally:
                # 告知播放端：所有音频切片已合成完毕
                await pcm_queue.put(None)

        # 协程B：定速推流流水线 (从 pcm_queue 获取音频帧 -> 1920字节/0.056s 推送到硬件)
        async def playback_worker():
            speech_started = False
            try:
                while not is_interrupted():
                    chunk = await pcm_queue.get()
                    if chunk is None:
                        # 全部音频播放完毕
                        break
                    if not speech_started and not ws.closed:
                        speech_started = True
                        log.info("🔊 [流水线播报] 首包音频到达，硬件即刻开播！")
                        await ws.send_str(json.dumps({
                            "type": "ai_state",
                            "state": "speaking",
                            "detail": "Feishu: Speaking..."
                        }))
                        await ws.send_str(json.dumps({"type": "voice_reply_start"}))

                    # 严格定速推流 (16kHz 16-bit 单声道 = 32000 字节/秒，每 1920 字节 60ms)
                    chunk_len = 1920
                    for pos in range(0, len(chunk), chunk_len):
                        if is_interrupted():
                            break
                        piece = chunk[pos:pos + chunk_len]
                        if len(piece) < chunk_len:
                            piece = piece + b"\x00" * (chunk_len - len(piece))
                        await ws.send_bytes(piece)
                        await asyncio.sleep(0.056)
                    pcm_queue.task_done()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.warning(f"定速推流硬件异常: {e}")
            finally:
                if speech_started and not is_interrupted() and not ws.closed:
                    await ws.send_str(json.dumps({"type": "voice_reply_end"}))
                    await ws.send_str(json.dumps({"type": "ai_speech_end"}))
                    await ws.send_str(json.dumps({"type": "ai_state", "state": "idle", "detail": "Ready"}))
                    log.info("设备语音流式播报完成，已切回待命状态")
                elif not speech_started and not is_interrupted() and not ws.closed:
                    log.warning("未触发音频流式播报，向硬件下发结束待命帧防止看门狗超时")
                    if reply_text:
                        await ws.send_str(json.dumps({"type": "ai_speech_start", "text": reply_text}))
                    await ws.send_str(json.dumps({"type": "voice_reply_end"}))
                    await ws.send_str(json.dumps({"type": "ai_speech_end"}))
                    await ws.send_str(json.dumps({"type": "ai_state", "state": "idle", "detail": "Ready"}))

        tts_task = asyncio.create_task(tts_worker())
        play_task = asyncio.create_task(playback_worker())

        first_slice_sent = False
        async def on_sentence_slice(slice_text: str):
            nonlocal first_slice_sent
            if not is_interrupted() and slice_text:
                await sentence_queue.put(slice_text)
                if not first_slice_sent and not ws.closed:
                    first_slice_sent = True
                    await ws.send_str(json.dumps({"type": "ai_speech_start", "text": slice_text}))

        # 7. 调用 AI 大模型生成分析回复 (流式逐句回调抢跑)
        executed_tools = []
        reply_text = ""
        current_task = asyncio.current_task()
        self.active_tts_tasks[ws] = current_task

        try:
            if user_text != "(未检测到有效语音)":
                log.info(f"🤖 正在调用 AI 分析提问 (关联会话与全量Agent工具链)...")
                reply_text, executed_tools = await self.generate_ai_reply(
                    user_query=user_text,
                    mac=mac,
                    feishu_msg_id="",
                    feishu_msg_id_fut=feishu_msg_id_fut,
                    duration_sec=duration_sec,
                    on_sentence=on_sentence_slice,
                    ws=ws
                )
                log.info(f"🤖 AI 生成回复完毕: 「{reply_text[:60]}...」 (工具={len(executed_tools)})")
            else:
                reply_text = "我没有听清您的说话内容，请长按确认键重新对我说话。"
                await on_sentence_slice(reply_text)

            if not ws.closed and reply_text:
                await ws.send_str(json.dumps({"type": "ai_speech_start", "text": reply_text}))

        except asyncio.CancelledError:
            interrupted_ev.set()
            tts_task.cancel()
            play_task.cancel()
            raise
        except Exception as e:
            log.error(f"AI 生成回复异常: {e}")
            reply_text = "抱歉，处理您的语音指令时出现异常，请稍后重试。"
            await on_sentence_slice(reply_text)
        finally:
            # 发送分句结束标识符
            await sentence_queue.put(None)

        # 等待流水线全部播放完毕或被打断
        try:
            await asyncio.gather(tts_task, play_task, return_exceptions=True)
        except Exception:
            pass
        finally:
            self.active_ai_procs.pop(ws, None)
            if self.active_tts_tasks.get(ws) == current_task:
                self.active_tts_tasks.pop(ws, None)

        # 8. 刷新飞书卡片展示最终原生答复与工具执行明细
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


    async def synthesize_speech_pcm(self, text: str) -> Optional[bytes]:
        """调用 edge-tts 生成音频并经 ffmpeg 转为 16kHz 16-bit 单声道 PCM"""
        try:
            import edge_tts
            voice = CONFIG.get("tts_voice", "zh-CN-XiaoxiaoNeural")
            rate = CONFIG.get("tts_rate", "+10%")
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

    def split_text_for_broadcast(self, text: str) -> List[str]:
        """将待播报长文本智能切分成适合朗读与屏幕滚动展示的短句列表"""
        if not text:
            return []
        cleaned_full = clean_text_for_speech(text, max_chars=4000)
        if not cleaned_full:
            return []
        chunker = SentenceChunker()
        raw_sentences = chunker.feed(cleaned_full)
        raw_sentences.extend(chunker.flush())
        sentences = []
        for s in raw_sentences:
            cs = s.strip()
            if cs:
                sentences.append(cs)
        return sentences

    async def play_voice_to_all(self, text: str):
        """
        向所有在线硬件设备流式分句播放文本语音：
        1. 采用 SentenceChunker 智能分句，单句控制在适度长度；
        2. 双协程异步预合成流水线 (Prefetch Pipeline)：边播放当前句，边预加载下一句，杜绝卡顿；
        3. 每句只向硬件发送当前句字幕，杜绝超长字符串撑爆 LVGL 或触发硬件看门狗；
        4. 硬件端连续接收 PCM 音频流无缝拼接，播放顺滑连贯。
        """
        if hasattr(self, "_active_broadcast_task") and self._active_broadcast_task and not self._active_broadcast_task.done():
            self._active_broadcast_task.cancel()
            log.info("📢 中止上一轮未完成的语音广播推流任务")

        task = asyncio.current_task()
        self._active_broadcast_task = task

        try:
            sentences = self.split_text_for_broadcast(text)
            if not sentences:
                log.warning("播报文本切句后为空，取消播报")
                return

            log.info(f"📢 [流式语音广播] 待播报文本切分为 {len(sentences)} 个分句，启动流水线推流")

            audio_packet_queue: asyncio.Queue = asyncio.Queue(maxsize=2)
            interrupted_ev = asyncio.Event()

            def is_interrupted() -> bool:
                if interrupted_ev.is_set():
                    return True
                alive = [w for w in self.active_websockets if not w.closed]
                return len(alive) == 0

            # 协程1：后台异步预合成流水线
            async def producer():
                try:
                    for idx, sentence_text in enumerate(sentences):
                        if is_interrupted():
                            break
                        log.info(f"🎙️ [广播合成] 正在预合成第 {idx+1}/{len(sentences)} 句: 「{sentence_text}」")
                        
                        pcm_buf = bytearray()
                        async def _on_pcm_chunk(chunk: bytes):
                            pcm_buf.extend(chunk)

                        stream_ok = False
                        try:
                            stream_ok = await tts_pool.stream_pcm_realtime(
                                text=sentence_text,
                                on_pcm_chunk=_on_pcm_chunk,
                                check_interrupted=is_interrupted
                            )
                        except Exception as e_stream:
                            log.warning(f"tts_pool 流式合成异常: {e_stream}")

                        if (not stream_ok or len(pcm_buf) == 0) and not is_interrupted():
                            log.info(f"降级采用单句直接合成: 「{sentence_text}」")
                            fallback_pcm = await self.synthesize_speech_pcm(sentence_text)
                            if fallback_pcm:
                                pcm_buf = bytearray(fallback_pcm)

                        if len(pcm_buf) > 0 and not is_interrupted():
                            await audio_packet_queue.put((sentence_text, bytes(pcm_buf)))
                        elif not is_interrupted():
                            log.warning(f"分句合成音频为空，跳过该句: 「{sentence_text}」")
                except asyncio.CancelledError:
                    pass
                except Exception as e_prod:
                    log.error(f"广播音频合成流水线异常: {e_prod}")
                finally:
                    await audio_packet_queue.put(None)

            # 协程2：定速推流消费者
            async def consumer():
                speech_started = False
                try:
                    while not is_interrupted():
                        packet = await audio_packet_queue.get()
                        if packet is None:
                            break
                        sentence_text, pcm_data = packet

                        alive_ws = [w for w in list(self.active_websockets) if not w.closed]
                        if not alive_ws:
                            break

                        if not speech_started:
                            speech_started = True
                            for ws in alive_ws:
                                try:
                                    await ws.send_str(json.dumps({
                                        "type": "ai_state",
                                        "state": "speaking",
                                        "detail": "Broadcast: Speaking..."
                                    }))
                                    await ws.send_str(json.dumps({"type": "voice_reply_start"}))
                                except Exception:
                                    pass

                        # 向硬件下发当前短句字幕
                        for ws in alive_ws:
                            try:
                                await ws.send_str(json.dumps({
                                    "type": "ai_speech_start",
                                    "text": sentence_text
                                }))
                            except Exception:
                                pass

                        # 严格定速推流当前句 PCM 帧 (每 1920 字节 60ms)
                        chunk_len = 1920
                        for pos in range(0, len(pcm_data), chunk_len):
                            if is_interrupted():
                                break
                            piece = pcm_data[pos:pos + chunk_len]
                            if len(piece) < chunk_len:
                                piece = piece + b"\x00" * (chunk_len - len(piece))
                            for ws in list(self.active_websockets):
                                if not ws.closed:
                                    try:
                                        await ws.send_bytes(piece)
                                    except Exception:
                                        pass
                            await asyncio.sleep(0.056)

                        audio_packet_queue.task_done()
                except asyncio.CancelledError:
                    pass
                except Exception as e_cons:
                    log.error(f"广播推流异常: {e_cons}")
                finally:
                    alive_ws = [w for w in list(self.active_websockets) if not w.closed]
                    if speech_started and not is_interrupted():
                        for ws in alive_ws:
                            try:
                                await ws.send_str(json.dumps({"type": "voice_reply_end"}))
                                await ws.send_str(json.dumps({"type": "ai_speech_end"}))
                                await ws.send_str(json.dumps({
                                    "type": "ai_state",
                                    "state": "idle",
                                    "detail": "Ready"
                                }))
                            except Exception:
                                pass
                        log.info("📢 流式语音广播顺利完成，所有设备已切回待命")
                    elif speech_started and is_interrupted():
                        for ws in alive_ws:
                            try:
                                await ws.send_str(json.dumps({"type": "voice_reply_end"}))
                                await ws.send_str(json.dumps({"type": "ai_speech_end"}))
                                await ws.send_str(json.dumps({
                                    "type": "ai_state",
                                    "state": "idle",
                                    "detail": "Ready"
                                }))
                            except Exception:
                                pass

            prod_task = asyncio.create_task(producer())
            cons_task = asyncio.create_task(consumer())
            try:
                await asyncio.gather(prod_task, cons_task)
            except asyncio.CancelledError:
                interrupted_ev.set()
                prod_task.cancel()
                cons_task.cancel()
                raise
        except asyncio.CancelledError:
            log.info("📢 语音广播任务已安全取消")
        except Exception as e:
            log.error(f"流式分句广播全局异常: {e}")

    async def handle_api_ring(self, request: web.Request) -> web.Response:
        """寻机声光呼叫：向所有设备弹出居中报警弹窗并播报鸣叫语音"""
        count = 0
        for ws in list(self.active_websockets):
            if not ws.closed:
                count += 1
                try:
                    await ws.send_str(json.dumps({
                        "type": "alert_popup",
                        "title": "🔔 寻机呼叫",
                        "content": "飞书控制台正呼叫本设备！"
                    }))
                except Exception:
                    pass
        if count > 0:
            asyncio.create_task(self.play_voice_to_all("寻机呼叫！我在这里！"))
            return web.json_response({"success": True, "alerted_clients": count, "message": "已下发寻机弹窗与语音播报"})
        return web.json_response({"success": False, "error": "当前无在线设备连接"}, status=400)

    async def handle_api_voice(self, request: web.Request) -> web.Response:
        """动态切换播报音色并立即向设备播报测试语音"""
        try:
            data = await request.json()
            new_voice = data.get("voice", "")
            if not new_voice:
                return web.json_response({"success": False, "error": "Missing voice"}, status=400)
            CONFIG["tts_voice"] = new_voice
            try:
                with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                    json.dump(CONFIG, f, indent=2, ensure_ascii=False)
            except Exception:
                pass
            global tts_pool
            tts_pool.voice = new_voice
            tts_pool.cfg = TTSConfig(new_voice, tts_pool.rate, "+0%", "+0Hz", "SentenceBoundary")
            tts_pool._connected = False
            asyncio.create_task(self.play_voice_to_all("已切换播报音色"))
            return web.json_response({"success": True, "voice": new_voice})
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def handle_api_speak(self, request: web.Request) -> web.Response:
        """向所有在线硬件播报指定文本并弹窗显示"""
        try:
            data = await request.json()
            text = (data.get("text") or "").strip()
            if not text:
                return web.json_response({"success": False, "error": "文本内容不能为空"}, status=400)
            count = 0
            for ws in list(self.active_websockets):
                if not ws.closed:
                    count += 1
                    try:
                        await ws.send_str(json.dumps({
                            "type": "alert_popup",
                            "title": "📢 语音播报",
                            "content": text[:80]
                        }))
                    except Exception:
                        pass
            if count > 0:
                asyncio.create_task(self.play_voice_to_all(text))
                return web.json_response({"success": True, "count": count, "text": text})
            return web.json_response({"success": False, "error": "当前无在线硬件连接，无法播放"}, status=400)
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)


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
