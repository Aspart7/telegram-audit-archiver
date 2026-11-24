"""Unit tests for thread-safe MessageStore and configuration hygiene."""

from __future__ import annotations

import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from archiver_bot import MessageStore, load_config


class TestMessageStore(unittest.TestCase):
    def test_save_retrieve_and_update(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            store = MessageStore(db_path)

            store.save_message(
                source="business",
                connection_id="conn-1",
                chat_id=1001,
                message_id=42,
                sender_id=555,
                sender_name="Alice",
                sender_username="alice",
                chat_title="Alice (@alice)",
                text="Original text",
                created_at=1_700_000_000,
            )

            self.assertEqual(store.count_messages(), 1)
            msg = store.get_message("business", 1001, 42)
            self.assertIsNotNone(msg)
            assert msg is not None
            self.assertEqual(msg["text"], "Original text")

            store.update_text("business", 1001, 42, "Edited text")
            updated = store.get_message("business", 1001, 42)
            assert updated is not None
            self.assertEqual(updated["text"], "Edited text")

    def test_ttl_cleanup_removes_expired_rows_and_media(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            media_old = Path(tmpdir) / "old_photo.jpg"
            media_new = Path(tmpdir) / "new_photo.jpg"
            media_old.write_bytes(b"old")
            media_new.write_bytes(b"new")

            store = MessageStore(db_path)
            now_ts = 1_700_200_000
            old_ts = now_ts - (49 * 3600)
            fresh_ts = now_ts - (2 * 3600)

            store.save_message(
                source="business",
                connection_id="conn-1",
                chat_id=1001,
                message_id=1,
                sender_id=11,
                sender_name="Old User",
                sender_username="olduser",
                chat_title="Old User",
                text="Expired message",
                media_type="photo",
                local_path=str(media_old),
                created_at=old_ts,
            )
            store.save_message(
                source="business",
                connection_id="conn-1",
                chat_id=1001,
                message_id=2,
                sender_id=12,
                sender_name="Fresh User",
                sender_username="freshuser",
                chat_title="Fresh User",
                text="Fresh message",
                media_type="photo",
                local_path=str(media_new),
                created_at=fresh_ts,
            )

            removed = store.cleanup_old(ttl_hours=48, now_ts=now_ts)
            self.assertEqual(removed, 1)
            self.assertEqual(store.count_messages(), 1)
            self.assertFalse(media_old.exists())
            self.assertTrue(media_new.exists())
            self.assertIsNone(store.get_message("business", 1001, 1))
            self.assertIsNotNone(store.get_message("business", 1001, 2))

    def test_concurrent_writes_are_thread_safe(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = MessageStore(Path(tmpdir) / "concurrent.db")
            errors: list[Exception] = []

            def worker(worker_id: int) -> None:
                try:
                    for idx in range(25):
                        store.save_message(
                            source="business",
                            connection_id="conn-thread",
                            chat_id=2000 + worker_id,
                            message_id=idx,
                            sender_id=worker_id,
                            sender_name=f"Worker {worker_id}",
                            sender_username=f"w{worker_id}",
                            chat_title="Thread Test",
                            text=f"Message {idx}",
                        )
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(errors, [])
            self.assertEqual(store.count_messages(), 150)

    def test_env_config_loading_without_hardcoded_secrets(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TELEGRAM_BOT_TOKEN": "12345:sample_token",
                "NOTIFY_CHAT_IDS": "101, 202",
                "CACHE_TTL_HOURS": "24",
                "USERBOT_ENABLED": "false",
                "TELEGRAM_API_ID": "",
                "TELEGRAM_API_HASH": "",
            },
            clear=False,
        ):
            cfg = load_config()
            self.assertEqual(cfg["bot_token"], "12345:sample_token")
            self.assertEqual(cfg["notify_chat_ids"], [101, 202])
            self.assertEqual(cfg["cache_ttl_hours"], 24)
            self.assertFalse(cfg["userbot_enabled"])
            self.assertIsNone(cfg["api_id"])
            self.assertEqual(cfg["api_hash"], "")


if __name__ == "__main__":
    unittest.main()
