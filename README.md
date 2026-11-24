# Personal Telegram Message Archiver & Audit Bot

Containerized 24/7 personal message backup and change-audit service built with **Python**, **SQLite**, **Telethon**, and **Docker**. Designed to maintain personal audit trails of message edits and deletions on accounts owned and authorized by the operator, with automated TTL retention cleanup.

> **Scope & Privacy Notice:** Intended strictly for personal message backup and self-audit on accounts you own. Never deploy with unauthorized accounts, and never commit `.env`, `config.json`, or `.session` files to version control.

---

## Architecture & Key Engineering Practices

- **Environment-First Secret Management:** Zero hardcoded API keys, session tokens, or credentials; all secrets load from environment variables (`.env.example`) or an untracked local `config.json`.
- **Thread-Safe SQLite Persistence (`MessageStore`):** Parameterized SQL queries protected by `threading.Lock` with indexed timestamps.
- **Automated Data Minimization (TTL Cleanup):** Periodic sweep removes cached messages and orphaned local media files older than `CACHE_TTL_HOURS` (default `48h`).
- **Safe HTML Escaping:** Untrusted message bodies and usernames are escaped with `html.escape()` prior to formatting Telegram HTML audit alerts.
- **Container Hardening:** `Dockerfile` runs on `python:3.12-slim` under an unprivileged non-root `appuser`, exposing only a lightweight `/health` HTTP check on port `7860`.

---

## Project Structure

```text
telegram-audit-archiver/
├── archiver_bot.py      # MessageStore, Business API bot, optional personal sync & health server
├── tests/
│   └── test_store.py    # Unit tests for SQLite persistence, TTL cleanup, concurrency & env config
├── config.example.json  # Template config (no secrets)
├── .env.example         # Environment variable template
├── Dockerfile           # Non-root container build for 24/7 deployment
├── requirements.txt
└── .gitignore           # Excludes .env, config.json, *.session, *.db, media_cache/
```

---

## Quick Start

### Local Setup
```bash
cp .env.example .env
# Edit .env with your TELEGRAM_BOT_TOKEN and NOTIFY_CHAT_IDS
pip install -r requirements.txt
python archiver_bot.py
```

### Docker Deployment
```bash
docker build -t telegram-audit-archiver .
docker run -d --name telegram-archiver \
  --env-file .env \
  -p 7860:7860 \
  telegram-audit-archiver
```

---

## Running Tests

```bash
python -m unittest discover -s tests -v
```
