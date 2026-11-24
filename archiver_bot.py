"""Personal Telegram Message Archiver & Audit Bot.

Containerized service for archiving personal Telegram message history,
tracking message edits/deletions for audit continuity, and managing
time-bounded media cache retention.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import html
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time
from typing import Any, Iterator

import requests

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", str(BASE_DIR / "data")))
CONFIG_PATH = BASE_DIR / "config.json"
DB_PATH = DATA_DIR / "messages_cache.db"
MEDIA_DIR = DATA_DIR / "media_cache"
LOG_PATH = DATA_DIR / "archiver.log"
SESSION_PATH = DATA_DIR / "personal_sync"


logger = logging.getLogger("TelegramAuditArchiver")


def _configure_logging() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    handlers: list[logging.Handler] = [logging.FileHandler(LOG_PATH, encoding="utf-8")]
    if sys.stdout is not None:
        handlers.append(logging.StreamHandler(sys.stdout))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )


def _parse_chat_ids(raw: Any) -> list[int]:
    if isinstance(raw, list):
        result: list[int] = []
        for item in raw:
            try:
                result.append(int(item))
            except (TypeError, ValueError):
                continue
        return result
    if isinstance(raw, str) and raw.strip():
        result = []
        for part in raw.split(","):
            part = part.strip()
            if part:
                try:
                    result.append(int(part))
                except ValueError:
                    continue
        return result
    return []


def load_config() -> dict[str, Any]:
    """Load configuration from environment variables with optional local config.json fallback."""
    file_cfg: dict[str, Any] = {}
    if CONFIG_PATH.exists():
        try:
            file_cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.error("Failed to parse config.json: %s", exc)

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN") or file_cfg.get("bot_token", "")
    api_id_raw = os.environ.get("TELEGRAM_API_ID") or file_cfg.get("api_id")
    api_hash = os.environ.get("TELEGRAM_API_HASH") or file_cfg.get("api_hash", "")

    env_chats = os.environ.get("NOTIFY_CHAT_IDS")
    notify_chat_ids = (
        _parse_chat_ids(env_chats)
        if env_chats is not None
        else _parse_chat_ids(file_cfg.get("notify_chat_ids", []))
    )

    ttl_raw = os.environ.get("CACHE_TTL_HOURS") or file_cfg.get("cache_ttl_hours", 48)
    try:
        cache_ttl_hours = max(1, int(ttl_raw))
    except (TypeError, ValueError):
        cache_ttl_hours = 48

    userbot_env = os.environ.get("USERBOT_ENABLED")
    if userbot_env is not None:
        userbot_enabled = userbot_env.strip().lower() in ("1", "true", "yes", "on")
    else:
        userbot_enabled = bool(file_cfg.get("userbot_enabled", False))

    try:
        api_id = int(api_id_raw) if api_id_raw else None
    except (TypeError, ValueError):
        api_id = None

    return {
        "bot_token": str(bot_token).strip(),
        "notify_chat_ids": notify_chat_ids,
        "cache_ttl_hours": cache_ttl_hours,
        "userbot_enabled": userbot_enabled,
        "api_id": api_id,
        "api_hash": str(api_hash).strip(),
    }


class MessageStore:
    """Thread-safe SQLite message store with TTL-based retention cleanup."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self.lock = threading.Lock()
        self._init_db()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    source TEXT NOT NULL,
                    connection_id TEXT NOT NULL,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    sender_id INTEGER,
                    sender_name TEXT,
                    sender_username TEXT,
                    chat_title TEXT,
                    text TEXT,
                    media_type TEXT,
                    file_id TEXT,
                    local_path TEXT,
                    created_at INTEGER NOT NULL,
                    PRIMARY KEY (source, chat_id, message_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages (created_at)"
            )

    def save_message(
        self,
        source: str,
        connection_id: str,
        chat_id: int,
        message_id: int,
        sender_id: int,
        sender_name: str,
        sender_username: str,
        chat_title: str,
        text: str,
        media_type: str | None = None,
        file_id: str | None = None,
        local_path: str | None = None,
        created_at: int | None = None,
    ) -> None:
        now_ts = int(time.time()) if created_at is None else int(created_at)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO messages
                (source, connection_id, chat_id, message_id, sender_id, sender_name,
                 sender_username, chat_title, text, media_type, file_id, local_path, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source,
                    connection_id or "",
                    chat_id,
                    message_id,
                    sender_id,
                    sender_name,
                    sender_username,
                    chat_title,
                    text or "",
                    media_type,
                    file_id,
                    local_path,
                    now_ts,
                ),
            )

    def get_message(
        self, source: str, chat_id: int | None, message_id: int
    ) -> dict[str, Any] | None:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            if chat_id is not None:
                cur.execute(
                    """
                    SELECT * FROM messages
                    WHERE source = ? AND chat_id = ? AND message_id = ?
                    """,
                    (source, chat_id, message_id),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM messages
                    WHERE source = ? AND message_id = ?
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (source, message_id),
                )
            row = cur.fetchone()
            return dict(row) if row else None

    def update_text(
        self, source: str, chat_id: int | None, message_id: int, new_text: str
    ) -> None:
        with self._connect() as conn:
            if chat_id is not None:
                conn.execute(
                    """
                    UPDATE messages SET text = ?
                    WHERE source = ? AND chat_id = ? AND message_id = ?
                    """,
                    (new_text, source, chat_id, message_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE messages SET text = ?
                    WHERE source = ? AND message_id = ?
                    """,
                    (new_text, source, message_id),
                )

    def cleanup_old(self, ttl_hours: int = 48, now_ts: int | None = None) -> int:
        ref_ts = int(time.time()) if now_ts is None else int(now_ts)
        cutoff = ref_ts - int(ttl_hours * 3600)
        removed = 0
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                "SELECT local_path FROM messages WHERE created_at < ?", (cutoff,)
            )
            rows = cur.fetchall()
            for row in rows:
                path = row["local_path"]
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                    except OSError as exc:
                        logger.warning("Could not remove cached media %s: %s", path, exc)
            cur = conn.execute(
                "DELETE FROM messages WHERE created_at < ?", (cutoff,)
            )
            removed = cur.rowcount
        return removed

    def count_messages(self) -> int:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM messages")
            return int(cur.fetchone()[0])


class TelegramAuditBot:
    """Telegram Bot API client archiving Business chat updates and edit/delete audit logs."""

    MEDIA_METHOD_MAP = {
        "photo": ("sendPhoto", "photo"),
        "video": ("sendVideo", "video"),
        "voice": ("sendVoice", "voice"),
        "video_note": ("sendVideoNote", "video_note"),
        "audio": ("sendAudio", "audio"),
        "document": ("sendDocument", "document"),
    }

    def __init__(self, config: dict[str, Any], store: MessageStore) -> None:
        self.config = config
        self.token = str(config.get("bot_token", "")).strip()
        self.bot_id = int(self.token.split(":")[0]) if ":" in self.token else 0
        self.store = store
        self.api_url = f"https://api.telegram.org/bot{self.token}"
        self.file_url = f"https://api.telegram.org/file/bot{self.token}"
        self.offset = 0
        self.running = True
        self.business_owners: dict[str, int] = {}
        self.sync_active = False

    def api_call(
        self,
        method: str,
        data: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        timeout: int = 35,
    ) -> dict[str, Any]:
        try:
            resp = requests.post(
                f"{self.api_url}/{method}", data=data, files=files, timeout=timeout
            )
            return resp.json()
        except Exception as exc:
            logger.error("Telegram API error (%s): %s", method, exc)
            return {"ok": False}

    def download_bot_file(self, file_id: str, ext: str = ".bin") -> str | None:
        try:
            res = self.api_call("getFile", {"file_id": file_id})
            if not res.get("ok"):
                return None
            file_path = res["result"].get("file_path")
            if not file_path:
                return None
            _, orig_ext = os.path.splitext(file_path)
            if orig_ext:
                ext = orig_ext
            safe_suffix = os.path.basename(file_id)[-10:]
            local_full = MEDIA_DIR / f"{int(time.time() * 1000)}_{safe_suffix}{ext}"
            resp = requests.get(f"{self.file_url}/{file_path}", timeout=30)
            if resp.status_code == 200:
                local_full.write_bytes(resp.content)
                return str(local_full)
        except Exception as exc:
            logger.error("Media download failed: %s", exc)
        return None

    def get_recipients(self, connection_id: str | None = None) -> list[int]:
        recipients = set(self.config.get("notify_chat_ids", []))
        if connection_id and connection_id in self.business_owners:
            recipients.add(self.business_owners[connection_id])
        return sorted(recipients)

    def send_notification(
        self,
        text: str,
        media_type: str | None = None,
        file_id: str | None = None,
        local_path: str | None = None,
        connection_id: str | None = None,
    ) -> None:
        recipients = self.get_recipients(connection_id)
        if not recipients:
            logger.warning("No notification recipients configured. Send /start to the bot.")
            return

        for chat_id in recipients:
            sent_media = False
            if (
                media_type
                and local_path
                and os.path.exists(local_path)
                and media_type in self.MEDIA_METHOD_MAP
            ):
                method, field = self.MEDIA_METHOD_MAP[media_type]
                with open(local_path, "rb") as media_fh:
                    payload: dict[str, Any] = {"chat_id": chat_id, "parse_mode": "HTML"}
                    if media_type != "video_note":
                        payload["caption"] = text
                    res = self.api_call(method, data=payload, files={field: media_fh})
                    if res.get("ok"):
                        sent_media = True
                        if media_type == "video_note":
                            self.api_call(
                                "sendMessage",
                                {"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                            )

            if not sent_media and media_type and file_id and media_type in self.MEDIA_METHOD_MAP:
                method, field = self.MEDIA_METHOD_MAP[media_type]
                payload = {"chat_id": chat_id, field: file_id, "parse_mode": "HTML"}
                if media_type != "video_note":
                    payload["caption"] = text
                res = self.api_call(method, payload)
                if res.get("ok"):
                    sent_media = True
                    if media_type == "video_note":
                        self.api_call(
                            "sendMessage",
                            {"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                        )

            if not sent_media:
                self.api_call(
                    "sendMessage",
                    {"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                )

    @staticmethod
    def extract_media(msg: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
        if msg.get("photo"):
            return "photo", msg["photo"][-1]["file_id"], ".jpg"
        for mtype, ext in (
            ("video", ".mp4"),
            ("voice", ".ogg"),
            ("video_note", ".mp4"),
            ("audio", ".mp3"),
            ("document", ".bin"),
        ):
            if msg.get(mtype):
                return mtype, msg[mtype]["file_id"], ext
        return None, None, None

    @staticmethod
    def format_user(user_obj: dict[str, Any] | None) -> tuple[str, str]:
        if not user_obj:
            return "Unknown", ""
        first = user_obj.get("first_name", "") or ""
        last = user_obj.get("last_name", "") or ""
        name = f"{first} {last}".strip() or "Unnamed"
        uname = user_obj.get("username", "") or ""
        return name, uname

    def handle_update(self, upd: dict[str, Any]) -> None:
        if "message" in upd:
            msg = upd["message"]
            chat_id = msg["chat"]["id"]
            text = msg.get("text", "")
            if text.startswith("/start") or text.startswith("/status"):
                if chat_id not in self.config["notify_chat_ids"]:
                    self.config["notify_chat_ids"].append(chat_id)
                count = self.store.count_messages()
                ttl = self.config.get("cache_ttl_hours", 48)
                sync_state = "Active" if self.sync_active else "Standby / Business API only"
                reply = (
                    "<b>Personal Telegram Audit Archiver</b>\n\n"
                    f"Chat ID: <code>{chat_id}</code>\n"
                    f"Personal Sync: <b>{sync_state}</b>\n"
                    f"Cached Messages: <b>{count}</b>\n"
                    f"Retention TTL: <b>{ttl}h</b>"
                )
                self.api_call(
                    "sendMessage",
                    {"chat_id": chat_id, "text": reply, "parse_mode": "HTML"},
                )

        if "business_connection" in upd:
            bc = upd["business_connection"]
            conn_id = bc["id"]
            user = bc.get("user", {})
            user_chat_id = bc.get("user_chat_id", user.get("id"))
            is_enabled = bc.get("is_enabled", False)
            name, uname = self.format_user(user)
            if user_chat_id:
                self.business_owners[conn_id] = int(user_chat_id)
                if int(user_chat_id) not in self.config["notify_chat_ids"]:
                    self.config["notify_chat_ids"].append(int(user_chat_id))
            status_str = "CONNECTED" if is_enabled else "DISCONNECTED"
            uname_str = f" (@{html.escape(uname)})" if uname else ""
            self.send_notification(
                f"<b>Telegram Business Status: {status_str}</b>\n"
                f"Account: <b>{html.escape(name)}</b>{uname_str}",
                connection_id=conn_id,
            )

        if "business_message" in upd:
            msg = upd["business_message"]
            conn_id = msg.get("business_connection_id", "")
            chat = msg.get("chat", {})
            chat_id = chat.get("id")
            msg_id = msg.get("message_id")
            sender = msg.get("from", {})
            sender_name, sender_uname = self.format_user(sender)
            chat_name, chat_uname = self.format_user(chat)
            chat_title = f"{chat_name} (@{chat_uname})" if chat_uname else chat_name

            text = msg.get("text") or msg.get("caption") or ""
            media_type, file_id, ext = self.extract_media(msg)
            local_path = (
                self.download_bot_file(file_id, ext or ".bin")
                if media_type and file_id
                else None
            )
            self.store.save_message(
                source="business",
                connection_id=conn_id,
                chat_id=chat_id,
                message_id=msg_id,
                sender_id=sender.get("id", 0),
                sender_name=sender_name,
                sender_username=sender_uname,
                chat_title=chat_title,
                text=text,
                media_type=media_type,
                file_id=file_id,
                local_path=local_path,
            )

        if "edited_business_message" in upd:
            msg = upd["edited_business_message"]
            conn_id = msg.get("business_connection_id", "")
            chat = msg.get("chat", {})
            chat_id = chat.get("id")
            msg_id = msg.get("message_id")
            new_text = msg.get("text") or msg.get("caption") or ""

            old = self.store.get_message("business", chat_id, msg_id)
            sender_name, sender_uname = self.format_user(msg.get("from", {}))
            chat_name, chat_uname = self.format_user(chat)
            chat_title = f"{chat_name} (@{chat_uname})" if chat_uname else chat_name

            old_text = old["text"] if old else "(not in retention window)"
            if old_text != new_text:
                uname_str = f" (@{html.escape(sender_uname)})" if sender_uname else ""
                alert = (
                    "<b>Audit Event: Message Edited</b>\n"
                    f"Author: <b>{html.escape(sender_name)}</b>{uname_str}\n"
                    f"Dialog: <b>{html.escape(chat_title)}</b>\n\n"
                    f"<b>Previous:</b>\n<blockquote>{html.escape(old_text or '[no text]')}</blockquote>\n\n"
                    f"<b>Updated:</b>\n<blockquote>{html.escape(new_text or '[no text]')}</blockquote>"
                )
                self.send_notification(alert, connection_id=conn_id)
                self.store.update_text("business", chat_id, msg_id, new_text)

        if "deleted_business_messages" in upd:
            del_info = upd["deleted_business_messages"]
            conn_id = del_info.get("business_connection_id", "")
            chat = del_info.get("chat", {})
            chat_id = chat.get("id")
            chat_name, chat_uname = self.format_user(chat)
            chat_title = f"{chat_name} (@{chat_uname})" if chat_uname else chat_name

            for mid in del_info.get("message_ids", []):
                saved = self.store.get_message("business", chat_id, mid)
                if saved:
                    dt_str = datetime.fromtimestamp(
                        saved["created_at"], tz=timezone.utc
                    ).strftime("%Y-%m-%d %H:%M:%S UTC")
                    s_uname = (
                        f" (@{html.escape(saved['sender_username'])})"
                        if saved["sender_username"]
                        else ""
                    )
                    text_part = (
                        f"\n\n<b>Archived Text:</b>\n<blockquote>{html.escape(saved['text'])}</blockquote>"
                        if saved["text"]
                        else ""
                    )
                    media_label = (
                        f"\n<b>Attachment:</b> {html.escape(saved['media_type'])}"
                        if saved["media_type"]
                        else ""
                    )
                    alert = (
                        "<b>Audit Event: Message Deleted</b>\n"
                        f"Author: <b>{html.escape(saved['sender_name'])}</b>{s_uname}\n"
                        f"Dialog: <b>{html.escape(chat_title)}</b>\n"
                        f"Timestamp: <code>{dt_str}</code>"
                        f"{media_label}{text_part}"
                    )
                    self.send_notification(
                        alert,
                        media_type=saved["media_type"],
                        file_id=saved["file_id"],
                        local_path=saved["local_path"],
                        connection_id=conn_id,
                    )

    def run_forever(self) -> None:
        if not self.token:
            logger.error(
                "TELEGRAM_BOT_TOKEN is not set. Configure environment variables before starting."
            )
            return

        me = self.api_call("getMe")
        if not me.get("ok"):
            logger.error("Failed to authenticate bot token with Telegram API.")
            return
        bot_info = me["result"]
        logger.info(
            "Bot online: @%s (id=%s)", bot_info.get("username"), bot_info.get("id")
        )

        last_cleanup = time.time()
        while self.running:
            try:
                res = self.api_call(
                    "getUpdates",
                    {
                        "offset": self.offset,
                        "timeout": 25,
                        "allowed_updates": json.dumps(
                            [
                                "message",
                                "business_connection",
                                "business_message",
                                "edited_business_message",
                                "deleted_business_messages",
                            ]
                        ),
                    },
                    timeout=35,
                )
                if res.get("ok"):
                    for upd in res.get("result", []):
                        self.offset = upd["update_id"] + 1
                        try:
                            self.handle_update(upd)
                        except Exception as exc:
                            logger.error("Error processing update: %s", exc)

                if time.time() - last_cleanup > 1800:
                    ttl = int(self.config.get("cache_ttl_hours", 48))
                    self.store.cleanup_old(ttl)
                    last_cleanup = time.time()
            except Exception as exc:
                logger.error("Polling loop error: %s", exc)
                time.sleep(3)


def start_personal_sync_thread(
    config: dict[str, Any], store: MessageStore, bot: TelegramAuditBot
) -> None:
    """Optional MTProto personal-account archive sync for authorized local sessions."""
    if not config.get("userbot_enabled"):
        return
    if not config.get("api_id") or not config.get("api_hash"):
        logger.warning(
            "Personal sync enabled, but TELEGRAM_API_ID or TELEGRAM_API_HASH is missing."
        )
        return

    session_file = Path(f"{SESSION_PATH}.session")
    if not session_file.exists():
        logger.warning(
            "Session file %s not found; skipping personal sync thread.", session_file
        )
        return

    import asyncio
    from telethon import TelegramClient, events

    def runner() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = TelegramClient(
            str(SESSION_PATH), int(config["api_id"]), str(config["api_hash"]), loop=loop
        )

        @client.on(events.NewMessage(func=lambda e: e.is_private))
        async def on_new_private_msg(event: Any) -> None:
            try:
                if bot.bot_id and event.chat_id == bot.bot_id:
                    return
                sender = await event.get_sender()
                chat = await event.get_chat()
                if getattr(sender, "bot", False) or getattr(chat, "bot", False):
                    return

                s_name = (
                    f"{getattr(sender, 'first_name', '') or ''} "
                    f"{getattr(sender, 'last_name', '') or ''}"
                ).strip() or "Unnamed"
                s_uname = getattr(sender, "username", "") or ""
                c_name = (
                    f"{getattr(chat, 'first_name', '') or ''} "
                    f"{getattr(chat, 'last_name', '') or ''}"
                ).strip() or s_name
                c_uname = getattr(chat, "username", "") or ""
                chat_title = f"{c_name} (@{c_uname})" if c_uname else c_name

                text = event.raw_text or ""
                media_type = None
                local_path = None
                if event.media:
                    for attr, mtype, ext in (
                        ("photo", "photo", ".jpg"),
                        ("voice", "voice", ".ogg"),
                        ("video_note", "video_note", ".mp4"),
                        ("video", "video", ".mp4"),
                        ("audio", "audio", ".mp3"),
                        ("document", "document", ".bin"),
                    ):
                        if getattr(event, attr, None):
                            media_type = mtype
                            fname = f"sync_{int(time.time() * 1000)}_{event.id}{ext}"
                            dest = MEDIA_DIR / fname
                            await event.download_media(file=str(dest))
                            local_path = str(dest)
                            break

                store.save_message(
                    source="personal_sync",
                    connection_id="personal_sync",
                    chat_id=event.chat_id,
                    message_id=event.id,
                    sender_id=event.sender_id or 0,
                    sender_name=s_name,
                    sender_username=s_uname,
                    chat_title=chat_title,
                    text=text,
                    media_type=media_type,
                    local_path=local_path,
                )
            except Exception as exc:
                logger.error("[PersonalSync NewMessage] %s", exc)

        @client.on(events.MessageEdited(func=lambda e: e.is_private))
        async def on_edit(event: Any) -> None:
            try:
                if bot.bot_id and event.chat_id == bot.bot_id:
                    return
                old = store.get_message("personal_sync", event.chat_id, event.id)
                new_text = event.raw_text or ""
                if old and old["text"] != new_text:
                    s_uname = (
                        f" (@{html.escape(old['sender_username'])})"
                        if old["sender_username"]
                        else ""
                    )
                    bot.send_notification(
                        "<b>Audit Event: Message Edited</b>\n"
                        f"Author: <b>{html.escape(old['sender_name'])}</b>{s_uname}\n"
                        f"Dialog: <b>{html.escape(old['chat_title'])}</b>\n\n"
                        f"<b>Previous:</b>\n<blockquote>{html.escape(old['text'] or '[no text]')}</blockquote>\n\n"
                        f"<b>Updated:</b>\n<blockquote>{html.escape(new_text or '[no text]')}</blockquote>"
                    )
                    store.update_text("personal_sync", event.chat_id, event.id, new_text)
            except Exception as exc:
                logger.error("[PersonalSync Edit] %s", exc)

        @client.on(events.MessageDeleted())
        async def on_delete(event: Any) -> None:
            try:
                for mid in event.deleted_ids:
                    saved = store.get_message("personal_sync", event.chat_id, mid)
                    if saved:
                        dt_str = datetime.fromtimestamp(
                            saved["created_at"], tz=timezone.utc
                        ).strftime("%Y-%m-%d %H:%M:%S UTC")
                        s_uname = (
                            f" (@{html.escape(saved['sender_username'])})"
                            if saved["sender_username"]
                            else ""
                        )
                        text_part = (
                            f"\n\n<b>Archived Text:</b>\n<blockquote>{html.escape(saved['text'])}</blockquote>"
                            if saved["text"]
                            else ""
                        )
                        media_label = (
                            f"\n<b>Attachment:</b> {html.escape(saved['media_type'])}"
                            if saved["media_type"]
                            else ""
                        )
                        bot.send_notification(
                            "<b>Audit Event: Message Deleted</b>\n"
                            f"Author: <b>{html.escape(saved['sender_name'])}</b>{s_uname}\n"
                            f"Dialog: <b>{html.escape(saved['chat_title'])}</b>\n"
                            f"Timestamp: <code>{dt_str}</code>"
                            f"{media_label}{text_part}",
                            media_type=saved["media_type"],
                            local_path=saved["local_path"],
                        )
            except Exception as exc:
                logger.error("[PersonalSync Delete] %s", exc)

        async def run_client() -> None:
            await client.connect()
            if not await client.is_user_authorized():
                logger.warning("[PersonalSync] Session is not authorized.")
                return
            bot.sync_active = True
            logger.info("[PersonalSync] Personal archive sync connected.")
            await client.run_until_disconnected()

        loop.run_until_complete(run_client())

    threading.Thread(target=runner, daemon=True).start()


def start_health_server() -> None:
    """Lightweight HTTP health endpoint for Docker / cloud platforms."""
    port = int(os.environ.get("PORT", 7860))

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Telegram Audit Archiver is healthy.")

        def log_message(self, format: str, *args: object) -> None:
            pass

    def serve() -> None:
        try:
            server = HTTPServer(("0.0.0.0", port), HealthHandler)
            logger.info("Healthcheck server listening on port %s", port)
            server.serve_forever()
        except Exception as exc:
            logger.error("Healthcheck server failed: %s", exc)

    threading.Thread(target=serve, daemon=True).start()


def main() -> None:
    _configure_logging()
    start_health_server()
    cfg = load_config()
    store = MessageStore(DB_PATH)
    bot = TelegramAuditBot(cfg, store)
    start_personal_sync_thread(cfg, store, bot)
    bot.run_forever()


if __name__ == "__main__":
    main()
