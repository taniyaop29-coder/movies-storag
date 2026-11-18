#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
NH DEVELOPER — TELEGRAM FREE VIDEO HOST (STREAM + DOWNLOAD LINK GENERATOR)
File: bot_server.py
Architecture: Pyrogram MTProto (1 MB Chunk Streamer) + aiohttp HTTP 206 Server
Compatible with: NH BOX Android App (Media3 ExoPlayer), VLC, MX Player, Web, IDM
Official Channel: https://t.me/htmlmovievg (@htmlmovievg)
================================================================================
"""

import os
import re
import hmac
import time
import math
import hashlib
import logging
import asyncio
import mimetypes
from urllib.parse import quote
from typing import Dict, List, Optional, Tuple

from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait

# ------------------------------------------------------------------------------
# LOGGING SETUP
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("NH-Host")

# ------------------------------------------------------------------------------
# ENVIRONMENT VARIABLES CONFIGURATION
# ------------------------------------------------------------------------------
API_ID = int(os.environ.get("API_ID", "0").strip() or "0")
API_HASH = os.environ.get("API_HASH", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()

raw_bin = os.environ.get("BIN_CHANNEL", "0").strip()
try:
    BIN_CHANNEL = int(raw_bin)
except ValueError:
    BIN_CHANNEL = 0

SECRET_KEY = os.environ.get("SECRET_KEY", "nh_super_secret_hmac_key_2026").strip()
BASE_URL = os.environ.get("BASE_URL", "http://localhost:8080").strip().rstrip("/")
PORT = int(os.environ.get("PORT", "8080").strip() or "8080")

# Optional Pro Scaling & Access Control Env Vars (Scene 16)
MULTI_TOKENS_RAW = os.environ.get("MULTI_TOKENS", "").strip()
ALLOWED_USERS_RAW = os.environ.get("ALLOWED_USERS", "").strip()

ALLOWED_USERS = set()
if ALLOWED_USERS_RAW:
    for uid in re.split(r"[,\s]+", ALLOWED_USERS_RAW):
        uid = uid.strip()
        if uid.lstrip("-").isdigit():
            ALLOWED_USERS.add(int(uid))

CHUNK_SIZE = 1024 * 1024  # Pyrogram stream_media yields exact 1 MiB (1,048,576 bytes) chunks
START_TIME = time.time()

# Worker Pool & Metadata Cache
clients: List[Client] = []
client_loads: Dict[int, int] = {}
message_cache: Dict[Tuple[int, int], Message] = {}


# ------------------------------------------------------------------------------
# HELPER FUNCTIONS (HMAC-SHA256 & FORMATTING)
# ------------------------------------------------------------------------------
def sign_file_id(msg_id: int) -> str:
    """Generates a 16-char HMAC-SHA256 signature for a BIN_CHANNEL message ID."""
    return hmac.new(
        SECRET_KEY.encode("utf-8"),
        str(msg_id).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()[:16]


def verify_signature(msg_id: int, sig: str) -> bool:
    """Constant-time verification of the signed stream/download link."""
    if not sig:
        return False
    clean_sig = sig.split(".")[0].strip().lower()
    expected = sign_file_id(msg_id).lower()
    return hmac.compare_digest(clean_sig, expected)


def human_size(num_bytes: int) -> str:
    """Formats raw byte count into human-readable KB / MB / GB."""
    if num_bytes <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = min(int(math.log(num_bytes, 1024)), len(units) - 1)
    val = num_bytes / (1024 ** idx)
    return f"{val:.2f} {units[idx]}"


def extract_media_info(msg: Message) -> Optional[ dict ]:
    """Extracts media object, filename, file size, and MIME type from a Telegram Message."""
    media = msg.video or msg.document or msg.audio or msg.animation
    if not media:
        return None

    file_name = getattr(media, "file_name", None)
    mime_type = getattr(media, "mime_type", None) or "video/mp4"
    file_size = getattr(media, "file_size", 0) or 0

    if not file_name:
        ext = mimetypes.guess_extension(mime_type) or ".mp4"
        file_name = f"NH_Video_{msg.id}{ext}"

    # Ensure safe extension for URLs (.mp4, .mkv, .webm, etc.)
    _, ext = os.path.splitext(file_name)
    if not ext:
        ext = ".mp4"
        file_name += ext

    if mime_type == "application/octet-stream" and ext.lower() in (".mp4", ".mkv", ".webm", ".mov"):
        mime_type = "video/mp4" if ext.lower() != ".webm" else "video/webm"

    return {
        "media": media,
        "file_name": file_name,
        "file_size": file_size,
        "mime_type": mime_type,
        "ext": ext.lower(),
    }


def pick_least_busy_client() -> Tuple[int, Client]:
    """Selects the MTProto worker client with the lowest active stream count (MULTI_TOKENS)."""
    best_idx = min(client_loads, key=lambda k: client_loads[k])
    return best_idx, clients[best_idx]


async def get_cached_message(client_idx: int, client: Client, msg_id: int) -> Optional[Message]:
    """Fetches and caches the BIN_CHANNEL message for a specific worker client."""
    cache_key = (client_idx, msg_id)
    if cache_key in message_cache:
        return message_cache[cache_key]

    try:
        msg = await client.get_messages(BIN_CHANNEL, msg_id)
        if msg and not msg.empty:
            if len(message_cache) > 1000:
                message_cache.clear()
            message_cache[cache_key] = msg
            return msg
    except FloodWait as e:
        log.warning(f"FloodWait of {e.value}s while fetching message {msg_id}")
        await asyncio.sleep(e.value)
        msg = await client.get_messages(BIN_CHANNEL, msg_id)
        if msg and not msg.empty:
            message_cache[cache_key] = msg
            return msg
    except Exception as exc:
        log.error(f"Error fetching message {msg_id} from BIN_CHANNEL: {exc}")
    return None


# ------------------------------------------------------------------------------
# HTTP 206 PARTIAL CONTENT STREAMING & DIRECT DOWNLOAD HANDLERS
# ------------------------------------------------------------------------------
def parse_range_header(range_header: Optional[str], file_size: int) -> Tuple[int, int, bool]:
    """
    Parses HTTP Range header (e.g. 'bytes=0-', 'bytes=1048576-2097151').
    Returns (start_byte, end_byte, is_partial).
    """
    if not range_header or not range_header.startswith("bytes="):
        return 0, file_size - 1, False

    try:
        range_spec = range_header.replace("bytes=", "").split(",")[0].strip()
        start_str, end_str = range_spec.split("-")
        if start_str == "":
            # Suffix range: bytes=-500
            suffix_len = int(end_str)
            start_byte = max(0, file_size - suffix_len)
            end_byte = file_size - 1
        else:
            start_byte = int(start_str)
            end_byte = int(end_str) if end_str else (file_size - 1)
    except Exception:
        return 0, file_size - 1, False

    start_byte = max(0, min(start_byte, file_size - 1))
    end_byte = max(start_byte, min(end_byte, file_size - 1))
    return start_byte, end_byte, True


async def handle_media_stream(request: web.Request, force_download: bool = False) -> web.StreamResponse:
    """
    Streams video/file directly from Telegram BIN_CHANNEL in 1 MiB MTProto chunks
    with full HTTP 206 Partial Content byte-range support for instant seeking.
    """
    try:
        msg_id = int(request.match_info["msg_id"])
    except (KeyError, ValueError):
        raise web.HTTPBadRequest(text="Invalid message ID")

    sig_raw = request.match_info.get("sig", "")
    if not verify_signature(msg_id, sig_raw):
        raise web.HTTPForbidden(text="Invalid or expired HMAC-SHA256 signature")

    client_idx, worker = pick_least_busy_client()
    msg = await get_cached_message(client_idx, worker, msg_id)
    if not msg:
        raise web.HTTPNotFound(text="Media file not found in BIN_CHANNEL")

    info = extract_media_info(msg)
    if not info:
        raise web.HTTPNotFound(text="No streamable media found in message")

    file_size = info["file_size"]
    file_name = info["file_name"]
    mime_type = info["mime_type"]

    range_hdr = request.headers.get("Range")
    start_byte, end_byte, is_partial = parse_range_header(range_hdr, file_size)
    content_length = end_byte - start_byte + 1

    # Calculate Pyrogram 1 MiB chunk offset and chunk count
    offset_chunks = start_byte // CHUNK_SIZE
    last_chunk = end_byte // CHUNK_SIZE
    chunk_limit = last_chunk - offset_chunks + 1
    first_chunk_cut = start_byte % CHUNK_SIZE

    status_code = 206 if is_partial else 200
    disposition_type = "attachment" if force_download else "inline"
    encoded_name = quote(file_name)

    headers = {
        "Content-Type": mime_type,
        "Accept-Ranges": "bytes",
        "Content-Length": str(content_length),
        "Content-Disposition": f'{disposition_type}; filename="{file_name}"; filename*=UTF-8''{encoded_name}',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Range, Content-Type, Accept",
        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
        "Cache-Control": "public, max-age=86400",
    }
    if is_partial:
        headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{file_size}"

    # Handle HEAD requests immediately (used by ExoPlayer / IDM / ADM to probe file size)
    if request.method == "HEAD":
        return web.Response(status=status_code, headers=headers)

    resp = web.StreamResponse(status=status_code, headers=headers)
    await resp.prepare(request)

    client_loads[client_idx] += 1
    bytes_remaining = content_length
    is_first_chunk = True

    try:
        async for chunk in worker.stream_media(msg, limit=chunk_limit, offset=offset_chunks):
            if bytes_remaining <= 0:
                break

            if is_first_chunk:
                is_first_chunk = False
                if first_chunk_cut > 0:
                    chunk = chunk[first_chunk_cut:]

            if len(chunk) > bytes_remaining:
                chunk = chunk[:bytes_remaining]

            if chunk:
                await resp.write(chunk)
                bytes_remaining -= len(chunk)
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    except Exception as exc:
        log.warning(f"Stream interrupted for msg_id={msg_id}: {exc}")
    finally:
        client_loads[client_idx] = max(0, client_loads[client_idx] - 1)

    return resp


async def stream_handler(request: web.Request) -> web.StreamResponse:
    return await handle_media_stream(request, force_download=False)


async def download_handler(request: web.Request) -> web.StreamResponse:
    return await handle_media_stream(request, force_download=True)


async def watch_handler(request: web.Request) -> web.Response:
    """Built-in responsive HTML5 Web Video Player page (Scene 14 Watch Online button)."""
    try:
        msg_id = int(request.match_info["msg_id"])
    except (KeyError, ValueError):
        raise web.HTTPBadRequest(text="Invalid message ID")

    sig_raw = request.match_info.get("sig", "")
    if not verify_signature(msg_id, sig_raw):
        raise web.HTTPForbidden(text="Invalid or expired HMAC-SHA256 signature")

    client_idx, worker = pick_least_busy_client()
    msg = await get_cached_message(client_idx, worker, msg_id)
    if not msg:
        raise web.HTTPNotFound(text="Media file not found in BIN_CHANNEL")

    info = extract_media_info(msg)
    if not info:
        raise web.HTTPNotFound(text="No streamable media found")

    sig = sign_file_id(msg_id)
    ext = info["ext"]
    stream_url = f"{BASE_URL}/stream/{msg_id}/{sig}{ext}"
    dl_url = f"{BASE_URL}/dl/{msg_id}/{sig}{ext}"
    size_str = human_size(info["file_size"])
    title = info["file_name"]

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} — NH Telegram Cloud Streamer</title>
<style>
  body {{
    margin: 0; background: #050811; color: #f8fafc;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    display: flex; flex-direction: column; align-items: center; min-height: 100vh; padding: 24px;
  }}
  .card {{
    width: 100%; max-width: 960px; background: #0d1526; border: 1px solid #243556;
    border-radius: 16px; padding: 20px; box-shadow: 0 20px 50px rgba(0,0,0,0.7);
  }}
  .badge {{
    display: inline-block; background: #24A1DE; color: #fff; font-weight: 800;
    font-size: 12px; padding: 5px 12px; border-radius: 999px; margin-bottom: 10px;
  }}
  h1 {{ font-size: 20px; margin: 0 0 6px; word-break: break-all; }}
  .meta {{ color: #94a3b8; font-size: 14px; margin-bottom: 16px; }}
  video {{
    width: 100%; aspect-ratio: 16/9; background: #000; border-radius: 12px;
    border: 1px solid #334155; margin-bottom: 18px;
  }}
  .actions {{ display: flex; flex-wrap: wrap; gap: 12px; }}
  .btn {{
    flex: 1; min-width: 200px; text-align: center; padding: 12px 18px;
    border-radius: 10px; font-weight: 800; font-size: 15px; text-decoration: none;
    cursor: pointer; border: none;
  }}
  .btn-dl {{ background: #10b981; color: #04130d; }}
  .btn-copy {{ background: #24A1DE; color: #ffffff; }}
</style>
</head>
<body>
  <div class="card">
    <span class="badge">NH DEVELOPER • HTTP 206 CLOUD STREAM</span>
    <h1>{title}</h1>
    <div class="meta">Size: {size_str} • File ID: #{msg_id} • Zero-Buffering Byte-Range Stream</div>
    <video controls autoplay preload="metadata" src="{stream_url}"></video>
    <div class="actions">
      <a class="btn btn-dl" href="{dl_url}">📥 Fast Direct Download ({size_str})</a>
      <button class="btn btn-copy" onclick="navigator.clipboard.writeText('{stream_url}');this.textContent='✓ Copied Stream URL!';">📋 Copy Stream Link (NH BOX / VLC)</button>
    </div>
  </div>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


async def health_handler(request: web.Request) -> web.Response:
    """UptimeRobot 24x7 Keep-Alive Endpoint (/health — Scene 13)."""
    uptime_sec = int(time.time() - START_TIME)
    return web.json_response({
        "status": "ok",
        "service": "NH Telegram Free Video Host",
        "uptime_seconds": uptime_sec,
        "workers": len(clients),
        "active_streams": sum(client_loads.values()),
    })


async def index_handler(request: web.Request) -> web.Response:
    """Root landing page verifying server LIVE status."""
    uptime_sec = int(time.time() - START_TIME)
    return web.json_response({
        "status": "LIVE",
        "server": "NH DEVELOPER — Telegram Free Video Host (HTTP 206 MTProto Streamer)",
        "workers_online": len(clients),
        "uptime_seconds": uptime_sec,
        "health_endpoint": f"{BASE_URL}/health",
        "channel": "https://t.me/htmlmovievg",
    })


# ------------------------------------------------------------------------------
# TELEGRAM BOT HANDLERS (SCENE 14 LIVE DEMO)
# ------------------------------------------------------------------------------
def register_bot_handlers(primary_bot: Client):
    @primary_bot.on_message(filters.private & filters.command(["start", "help"]))
    async def start_cmd(client: Client, message: Message):
        text = (
            "🚀 **NH DEVELOPER — Telegram Free Video Host is LIVE!**\n\n"
            "Forward or upload any **Video, Movie (1 GB – 4 GB MP4/MKV), Audio, or Document** "
            "to me and I will instantly generate:\n"
            "• 🚀 **Direct Stream Link** (`HTTP 206` for NH BOX / Media3 ExoPlayer / VLC)\n"
            "• 📥 **Direct Download Link** (High-Speed Browser / IDM / ADM)\n"
            "• ▶️ **Watch Online Web Player**\n\n"
            "📢 **Official Channel:** @htmlmovievg"
        )
        await message.reply_text(
            text,
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📢 Join NH Developer Telegram", url="https://t.me/htmlmovievg")]
            ]),
        )

    @primary_bot.on_message(
        filters.private & (filters.video | filters.document | filters.audio | filters.animation)
    )
    async def media_receive_handler(client: Client, message: Message):
        user_id = message.from_user.id if message.from_user else 0
        if ALLOWED_USERS and user_id not in ALLOWED_USERS:
            await message.reply_text(
                "⛔ **Access Restricted:** Your Telegram User ID is not in `ALLOWED_USERS`."
            )
            return

        if not BIN_CHANNEL:
            await message.reply_text(
                "⚠️ `BIN_CHANNEL` is not configured correctly in Environment Variables!"
            )
            return

        try:
            # Archive file into private BIN_CHANNEL
            archived = await message.copy(chat_id=BIN_CHANNEL)
        except Exception as exc:
            log.error(f"Failed to copy media to BIN_CHANNEL ({BIN_CHANNEL}): {exc}")
            await message.reply_text(
                f"❌ **Failed to save file to BIN_CHANNEL (`{BIN_CHANNEL}`)**\n"
                f"Make sure this bot is added as an **Administrator** in your private channel with "
                f"`Post Messages` permission enabled!\n\nError: `{exc}`"
            )
            return

        info = extract_media_info(archived)
        if not info:
            await message.reply_text("⚠️ Could not read media metadata.")
            return

        msg_id = archived.id
        sig = sign_file_id(msg_id)
        ext = info["ext"]
        file_name = info["file_name"]
        size_str = human_size(info["file_size"])

        stream_link = f"{BASE_URL}/stream/{msg_id}/{sig}{ext}"
        dl_link = f"{BASE_URL}/dl/{msg_id}/{sig}{ext}"
        watch_link = f"{BASE_URL}/watch/{msg_id}/{sig}{ext}"

        # Cache immediately for instant first playback
        message_cache[(0, msg_id)] = archived

        reply_text = (
            "✅ **Your File is Ready for Unlimited Streaming & Download!**\n\n"
            f"🎬 **File Name:** `{file_name}`\n"
            f"📦 **File Size:** `{size_str}`  •  **ID:** `#{msg_id}`\n\n"
            f"🚀 **Direct Stream Link (NH BOX / ExoPlayer / VLC):**\n`{stream_link}`\n\n"
            f"📥 **Direct Download Link (Browser / ADM / IDM):**\n`{dl_link}`"
        )

        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("▶️ Watch Online (Web Player)", url=watch_link),
                InlineKeyboardButton("📥 Fast Direct Download", url=dl_link),
            ]
        ])

        await message.reply_text(
            reply_text,
            disable_web_page_preview=True,
            reply_markup=buttons,
        )


# ------------------------------------------------------------------------------
# MAIN APPLICATION BOOTSTRAP
# ------------------------------------------------------------------------------
async def start_services():
    if not API_ID or not API_HASH or not BOT_TOKEN:
        log.error("Missing required environment variables: API_ID, API_HASH, or BOT_TOKEN!")
        raise SystemExit(1)

    log.info("[NH-Host] Connecting MTProto Client to Telegram DC...")

    primary = Client(
        name="nh_primary_bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True,
    )
    register_bot_handlers(primary)
    await primary.start()
    me = await primary.get_me()

    clients.append(primary)
    client_loads[0] = 0
    log.info(f"[NH-Host] ✅ Primary bot started. (@{me.username} connected to BIN_CHANNEL {BIN_CHANNEL})")

    # Start extra worker bots if MULTI_TOKENS is provided (Scene 16 Pro Scaling Tip)
    if MULTI_TOKENS_RAW:
        extra_tokens = [t.strip() for t in re.split(r"[,\s]+", MULTI_TOKENS_RAW) if ":" in t.strip()]
        for idx, tok in enumerate(extra_tokens, start=1):
            try:
                worker = Client(
                    name=f"nh_worker_bot_{idx}",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    bot_token=tok,
                    in_memory=True,
                )
                await worker.start()
                w_me = await worker.get_me()
                clients.append(worker)
                client_loads[len(clients) - 1] = 0
                log.info(f"[NH-Host] ⚡ Extra MULTI_TOKEN worker #{idx} started (@{w_me.username})")
            except Exception as exc:
                log.warning(f"[NH-Host] Failed to start extra worker #{idx}: {exc}")

    # Configure aiohttp Web Server
    app = web.Application()
    app.router.add_get("/", index_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_route("*", "/stream/{msg_id}/{sig}", stream_handler)
    app.router.add_route("*", "/dl/{msg_id}/{sig}", download_handler)
    app.router.add_get("/watch/{msg_id}/{sig}", watch_handler)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()
    log.info(f"[NH-Host] ✅ HTTP Stream Server running on port {PORT} (0.0.0.0:{PORT})")

    await idle()

    log.info("[NH-Host] Shutting down services...")
    await runner.cleanup()
    for c in clients:
        try:
            await c.stop()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(start_services())
    except KeyboardInterrupt:
        pass
