#!/usr/bin/env python3
"""
Standalone Bulk Firebase Health Checker — v2.3 Pro
Manual + Auto mutator + Telegram notifications
+ Firebase device verification (only notify if total > 0)
+ Auto valid.txt file every 30 minutes

Telegram env vars:
    TELEGRAM_BOT_TOKEN   Bot token from @BotFather
    TELEGRAM_CHAT_ID     Your chat/group ID

Run:
    python run.py
Then open:
    http://127.0.0.1:8080
"""
from __future__ import annotations
import http.client
import json
import os
import random
import re
import secrets
import sqlite3
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qsl, urlencode
from urllib.request import Request, urlopen

import requests  # for device verify + telegram file upload

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════
BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
DATA.mkdir(exist_ok=True)
DB = DATA / "bulk.sqlite3"
VERIFIED_FILE = DATA / "valid.txt"

HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8080"))
WORKERS = max(1, min(16, int(os.getenv("BULK_WORKERS", "6"))))
MAX_URLS = max(1, min(5000, int(os.getenv("BULK_MAX_URLS", "2000"))))
MAX_BYTES = max(4096, min(2_000_000, int(os.getenv("BULK_MAX_BYTES", "300000"))))
TIMEOUT = max(2, min(30, int(os.getenv("BULK_TIMEOUT", "8"))))
MAX_PROBE = 64 * 1024
RATE_WINDOW = 300
RATE_LIMIT = 20
RATE: dict = {}
RATE_LOCK = threading.Lock()
STOP = threading.Event()
LOG_LOCK = threading.Lock()
GEN_LOCK = threading.Lock()

# Auto mutator
AUTO_MAX_URLS = max(100, min(500000, int(os.getenv("BULK_AUTO_MAX_URLS", "50000"))))
AUTO_BATCH = max(50, min(1000, int(os.getenv("BULK_AUTO_BATCH", "200"))))
AUTO_EXPAND = os.getenv("BULK_AUTO_EXPAND", "1") == "1"

# Firebase device verification (default key = "12")
FIREBASE_DEFAULT_KEY = os.getenv("FIREBASE_DEFAULT_KEY", "12")
VERIFY_TIMEOUT = 12

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
FILE_INTERVAL = 30 * 60  # 30 minutes

# Verified URL tracking
VERIFIED_URLS: set = set()
VERIFIED_LOCK = threading.Lock()

# Mutation ingredients
PREFIXES = [
    "admin-", "panel-", "test-", "new-", "my-", "the-", "official-",
    "real-", "pro-", "super-", "best-", "indian-",
]
MUT_SUFFIXES = [
    "-app", "-panel", "-admin", "-pro", "-v1", "-v2", "-official",
    "-test", "-new", "-india", "-in", "123", "2024", "2025",
    "01", "02", "-bot", "-user",
]
MUT_NUMBERS = [
    "", "0", "1", "2", "3", "7", "9", "11", "22", "99",
    "01", "02", "07", "09", "10", "12", "123", "007", "786",
]

FB_SUFFIXES = ["-default-rtdb.firebaseio.com"]


# ═══════════════════════════════════════════════════════════════
#  SCHEMA
# ═══════════════════════════════════════════════════════════════
SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 token TEXT UNIQUE NOT NULL,
 filename TEXT NOT NULL,
 total INTEGER NOT NULL,
 processed INTEGER NOT NULL DEFAULT 0,
 active INTEGER NOT NULL DEFAULT 0,
 inactive INTEGER NOT NULL DEFAULT 0,
 no_data INTEGER NOT NULL DEFAULT 0,
 unreachable INTEGER NOT NULL DEFAULT 0,
 too_large INTEGER NOT NULL DEFAULT 0,
 status TEXT NOT NULL DEFAULT 'pending',
 error TEXT,
 job_type TEXT NOT NULL DEFAULT 'manual',
 seeds TEXT,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 started_at TEXT,
 completed_at TEXT,
 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS items (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 job_id INTEGER NOT NULL,
 url TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending',
 http_status INTEGER,
 error TEXT,
 latency_ms INTEGER,
 checked_at TEXT,
 FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_items_pending ON items(job_id,status,id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status,id);
"""


# ═══════════════════════════════════════════════════════════════
#  UTILITIES
# ═══════════════════════════════════════════════════════════════
def log(msg: str):
    with LOG_LOCK:
        print(f"[BULK] {msg}", flush=True)


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def db():
    c = sqlite3.connect(DB, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    return c


def init_db():
    c = db()
    c.executescript(SCHEMA)
    cols = {r["name"] for r in c.execute("PRAGMA table_info(jobs)").fetchall()}
    if "job_type" not in cols:
        try:
            c.execute("ALTER TABLE jobs ADD COLUMN job_type TEXT NOT NULL DEFAULT 'manual'")
        except sqlite3.OperationalError:
            pass
    if "seeds" not in cols:
        try:
            c.execute("ALTER TABLE jobs ADD COLUMN seeds TEXT")
        except sqlite3.OperationalError:
            pass
    c.execute("UPDATE jobs SET status='pending', started_at=NULL "
              "WHERE status='processing' AND job_type='manual'")
    c.execute("UPDATE jobs SET status='stopped' "
              "WHERE status='processing' AND job_type='auto'")
    c.commit()
    c.close()


def load_verified():
    """Load previously verified URLs from valid.txt into memory."""
    if not VERIFIED_FILE.exists():
        return
    try:
        with open(VERIFIED_FILE, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u:
                    VERIFIED_URLS.add(u)
        log(f"Loaded {len(VERIFIED_URLS)} verified URLs from {VERIFIED_FILE.name}")
    except Exception as e:
        log(f"load_verified error: {e}")


def save_verified(url: str) -> bool:
    """Append verified URL to valid.txt. Returns True if it was new."""
    with VERIFIED_LOCK:
        if url in VERIFIED_URLS:
            return False
        VERIFIED_URLS.add(url)
        try:
            with open(VERIFIED_FILE, "a", encoding="utf-8") as f:
                f.write(url + "\n")
        except Exception as e:
            log(f"save_verified error: {e}")
        return True


# ═══════════════════════════════════════════════════════════════
#  FIREBASE DEVICE VERIFICATION
# ═══════════════════════════════════════════════════════════════
def verify_devices(url: str, key: str = None, timeout: int = VERIFY_TIMEOUT) -> int:
    """
    Fetch /clients.json and return device count.
    Returns:
        >= 0  → number of devices found (0 means empty)
        -1    → verification failed (auth, network, invalid JSON, etc.)
    """
    if key is None:
        key = FIREBASE_DEFAULT_KEY

    endpoint = f"{url.rstrip('/')}/clients.json"
    params = {"auth": key} if key else {}
    headers = {
        "Accept": "application/json",
        "User-Agent": "Firebase-Device-Checker/1.0",
    }

    try:
        r = requests.get(endpoint, params=params, headers=headers, timeout=timeout)
        if r.status_code in (401, 403):
            return -1
        if r.status_code == 404:
            return -1
        r.raise_for_status()

        try:
            data = r.json()
        except ValueError:
            return -1

        if data is None:
            return 0
        if not isinstance(data, dict):
            return -1

        return len(data)
    except Exception as e:
        log(f"verify_devices error [{url}]: {type(e).__name__}: {str(e)[:100]}")
        return -1


# ═══════════════════════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════════════════════
def _tg_send(text: str, disable_preview: bool = False):
    if not TELEGRAM_ENABLED:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_preview,
        }
        data = json.dumps(payload).encode("utf-8")
        req = Request(url, data=data, headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=8) as resp:
            resp.read()
    except Exception as e:
        log(f"Telegram error: {type(e).__name__}: {str(e)[:120]}")


def tg_notify(text: str):
    if not TELEGRAM_ENABLED:
        return
    threading.Thread(target=_tg_send, args=(text, True), daemon=True).start()


def tg_send_document(filepath: str, caption: str = ""):
    """Upload a file to Telegram."""
    if not TELEGRAM_ENABLED:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        with open(filepath, "rb") as f:
            files = {"document": (os.path.basename(filepath), f, "text/plain")}
            data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption,
                    "parse_mode": "HTML"}
            r = requests.post(url, files=files, data=data, timeout=60)
            r.raise_for_status()
    except Exception as e:
        log(f"Telegram file upload error: {type(e).__name__}: {str(e)[:150]}")


def tg_notify_active(url: str, http_code: int, latency_ms, device_count: int):
    """Notify when an ACTIVE URL is verified to have devices."""
    if not TELEGRAM_ENABLED:
        return
    msg = (
        f"🔥 <b>VERIFIED FIREBASE FOUND</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>URL:</b>\n<code>{url}</code>\n\n"
        f"<b>Devices:</b> {device_count} ✅\n"
        f"<b>HTTP:</b> {http_code}\n"
        f"<b>Latency:</b> {latency_ms if latency_ms is not None else '—'} ms\n\n"
        f'🔗 <a href="{url}/.json?shallow=true">Open Firebase JSON</a>\n'
        f"━━━━━━━━━━━━━━━━━━━━━━━"
    )
    tg_notify(msg)


def tg_startup_msg():
    if not TELEGRAM_ENABLED:
        return
    msg = (
        f"🚀 <b>Bulk Firebase Checker v2.3 started</b>\n"
        f"Workers: {WORKERS}\n"
        f"Auto cap: {AUTO_MAX_URLS}\n"
        f"Suffix: <code>{FB_SUFFIXES[0]}</code>\n"
        f"Loaded verified: <b>{len(VERIFIED_URLS)}</b>\n"
        f"File interval: 30 min\n"
        f"Server: http://{HOST}:{PORT}"
    )
    tg_notify(msg)


# ═══════════════════════════════════════════════════════════════
#  PERIODIC FILE SENDER (every 30 min)
# ═══════════════════════════════════════════════════════════════
def periodic_file_sender():
    """Send valid.txt to Telegram every 30 minutes."""
    log("File sender thread started (every 30 min)")
    next_send = time.time() + FILE_INTERVAL
    while not STOP.is_set():
        wait = next_send - time.time()
        if wait > 0:
            # sleep in small chunks so we can react to STOP quickly
            if STOP.wait(min(wait, 5)):
                return
            continue

        # send now
        try:
            if TELEGRAM_ENABLED and VERIFIED_FILE.exists():
                with VERIFIED_LOCK:
                    count = len(VERIFIED_URLS)
                if count > 0:
                    caption = (
                        f"📄 <b>Verified Firebase URLs</b>\n"
                        f"Total: <b>{count}</b>\n"
                        f"Time: {now()}"
                    )
                    tg_send_document(str(VERIFIED_FILE), caption)
                    log(f"Sent valid.txt to Telegram ({count} URLs)")
                else:
                    log("Skipped file send — no verified URLs yet")
        except Exception as e:
            log(f"periodic_file_sender error: {e}")

        next_send = time.time() + FILE_INTERVAL


# ═══════════════════════════════════════════════════════════════
#  URL NORMALIZE
# ═══════════════════════════════════════════════════════════════
def normalize(line: str):
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    candidate = line.split(",", 1)[0].strip().strip('"').strip("'")
    if candidate.lower() in ("url", "firebase_url", "firebase url"):
        return None
    if not re.match(r"^https://", candidate, re.I):
        return None
    try:
        u = urlparse(candidate)
        host = (u.hostname or "").lower().rstrip(".")
        if not host or not (host.endswith(".firebaseio.com")
                            or host.endswith(".firebasedatabase.app")):
            return None
        if u.username or u.password:
            return None
        return candidate.rstrip("/")
    except Exception:
        return None


def parse_urls(content: str):
    seen = set()
    out = []
    for line in content.splitlines():
        u = normalize(line)
        if u and u not in seen:
            seen.add(u)
            out.append(u)
            if len(out) >= MAX_URLS:
                break
    return out


# ═══════════════════════════════════════════════════════════════
#  PROBE
# ═══════════════════════════════════════════════════════════════
def probe(url: str):
    started = time.monotonic()
    try:
        u = urlparse(url)
        host = u.hostname
        port = u.port or 443
        base = u.path or "/"
        if not base.endswith(".json"):
            base = base.rstrip("/") + "/.json" if base != "/" else "/.json"
        q = [(k, v) for k, v in parse_qsl(u.query, keep_blank_values=True)
             if k.lower() not in {"orderby", "limittofirst"}]
        q += [("orderBy", '"$key"'), ("limitToFirst", "1")]
        path = base + "?" + urlencode(q)

        conn = http.client.HTTPSConnection(host, port, timeout=TIMEOUT)
        conn.request("GET", path, headers={
            "Accept": "application/json",
            "User-Agent": "Standalone-Firebase-Health-Checker/2.3",
            "Connection": "close",
        })
        resp = conn.getresponse()
        code = resp.status
        latency = int((time.monotonic() - started) * 1000)

        if code != 200:
            resp.read(16 * 1024)
            conn.close()
            return "inactive", code, f"HTTP {code}", latency

        chunks = []
        total = 0
        while total <= MAX_PROBE:
            chunk = resp.read(min(8192, MAX_PROBE + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_PROBE:
                conn.close()
                return "response_too_large", code, "Probe response exceeded 64KB", latency
        conn.close()

        body = b"".join(chunks)
        if not body.strip():
            return "invalid_no_data", code, "Empty response", latency

        try:
            parsed = json.loads(body.decode("utf-8", "strict"))
        except Exception:
            return "invalid_no_data", code, "Invalid JSON", latency

        if parsed in (None, "", {}, [], False, 0):
            return "invalid_no_data", code, "No data", latency

        return "active", code, "HTTP 200 + meaningful probe data", latency

    except Exception as e:
        return ("unreachable", None,
                f"{type(e).__name__}: {str(e)[:180]}",
                int((time.monotonic() - started) * 1000))


# ═══════════════════════════════════════════════════════════════
#  AUTO MUTATOR
# ═══════════════════════════════════════════════════════════════
def sanitize_seed(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9\-_]", "", s)
    return s[:60]


def mutate_word(word: str):
    out = {word}
    for p in PREFIXES:
        if p:
            out.add(p + word)
    for s in MUT_SUFFIXES:
        if s:
            out.add(word + s)
    for _ in range(5):
        n = random.choice(MUT_NUMBERS)
        s = random.choice(MUT_SUFFIXES)
        if n or s:
            out.add(f"{word}{n}{s}")
    for _ in range(4):
        p = random.choice(PREFIXES)
        s = random.choice(MUT_SUFFIXES)
        if p or s:
            out.add(f"{p}{word}{s}")
    return list(out)


def extract_project(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return ""
    for fs in FB_SUFFIXES:
        if host.endswith(fs):
            return host[:-len(fs)]
    if "-default-rtdb" in host:
        return host.split("-default-rtdb")[0]
    return host


def generate_auto_batch(job_id: int, seeds: list, batch_size: int) -> int:
    try:
        c = db()
        existing = {r["url"] for r in c.execute(
            "SELECT url FROM items WHERE job_id=?", (job_id,)).fetchall()}
        active_projects = []
        if AUTO_EXPAND:
            for r in c.execute(
                "SELECT url FROM items WHERE job_id=? AND status='active' LIMIT 200",
                (job_id,)
            ).fetchall():
                p = extract_project(r["url"])
                if p and p not in active_projects:
                    active_projects.append(p)
        c.close()

        pool = list(seeds) + active_projects
        if not pool:
            return 0

        new_items = []
        attempts = 0
        max_attempts = batch_size * 30
        while len(new_items) < batch_size and attempts < max_attempts:
            attempts += 1
            base = random.choice(pool)
            mutations = mutate_word(base)
            random.shuffle(mutations)
            for m in mutations:
                for fs in FB_SUFFIXES:
                    u = f"https://{m}{fs}"
                    if u in existing:
                        continue
                    existing.add(u)
                    new_items.append((job_id, u))
                    if len(new_items) >= batch_size:
                        break
                if len(new_items) >= batch_size:
                    break

        if new_items:
            c = db()
            c.executemany("INSERT INTO items(job_id,url) VALUES(?,?)", new_items)
            c.execute("UPDATE jobs SET total=total+?, updated_at=? WHERE id=?",
                      (len(new_items), now(), job_id))
            c.commit()
            c.close()
        return len(new_items)
    except Exception as e:
        log(f"generate_auto_batch error: {e}")
        return 0


# ═══════════════════════════════════════════════════════════════
#  JOB CLAIM / RESULT
# ═══════════════════════════════════════════════════════════════
def claim_job():
    c = db()
    try:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute(
            "SELECT id FROM jobs WHERE status='pending' ORDER BY id LIMIT 1"
        ).fetchone()
        if not row:
            c.rollback()
            return None
        c.execute(
            "UPDATE jobs SET status='processing', started_at=COALESCE(started_at,?), "
            "updated_at=? WHERE id=? AND status='pending'",
            (now(), now(), row["id"])
        )
        if c.total_changes != 1:
            c.rollback()
            return None
        c.commit()
        return row["id"]
    finally:
        c.close()


def claim_item(job_id: int):
    c = db()
    try:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute(
            "SELECT * FROM items WHERE job_id=? AND status='pending' "
            "ORDER BY id LIMIT 1", (job_id,)
        ).fetchone()
        if not row:
            c.rollback()
            return None
        c.execute("UPDATE items SET status='checking' WHERE id=? AND status='pending'",
                  (row["id"],))
        if c.total_changes != 1:
            c.rollback()
            return None
        c.commit()
        return row
    finally:
        c.close()


def save_result(item, result):
    status, code, detail, latency = result
    c = db()
    try:
        c.execute("BEGIN")
        c.execute(
            "UPDATE items SET status=?,http_status=?,error=?,latency_ms=?,checked_at=? "
            "WHERE id=?", (status, code, detail, latency, now(), item["id"])
        )
        col = {
            "active": "active",
            "inactive": "inactive",
            "invalid_no_data": "no_data",
            "unreachable": "unreachable",
            "response_too_large": "too_large",
        }.get(status)
        if col:
            c.execute(
                f"UPDATE jobs SET processed=processed+1,{col}={col}+1,"
                f"updated_at=? WHERE id=?", (now(), item["job_id"])
            )
        c.commit()
    finally:
        c.close()

    log(f"JOB #{item['job_id']} RESULT -> {status.upper()} | {item['url']} | HTTP {code or '-'}")

    # 🔥 ACTIVE → verify devices first
    if status == "active":
        device_count = verify_devices(item["url"])
        log(f"  └─ Verify {item['url']} → devices={device_count}")

        if device_count > 0:
            # Only notify if devices > 0
            is_new = save_verified(item["url"])
            if is_new:
                log(f"  └─ ✅ VERIFIED & SAVED ({device_count} devices)")
            tg_notify_active(item["url"], code or 200, latency, device_count)
        else:
            log(f"  └─ ❌ Skipped (no devices / verify failed)")


def finish_if_done(job_id: int) -> bool:
    c = db()
    try:
        pending = c.execute(
            "SELECT COUNT(*) n FROM items WHERE job_id=? AND "
            "status IN ('pending','checking')", (job_id,)
        ).fetchone()["n"]
        if pending == 0:
            c.execute(
                "UPDATE jobs SET status='completed',completed_at=?,updated_at=? "
                "WHERE id=? AND status='processing'", (now(), now(), job_id)
            )
            c.commit()
            return True
        return False
    finally:
        c.close()


def find_running_auto_job():
    c = db()
    row = c.execute(
        "SELECT id FROM jobs WHERE status='processing' AND "
        "job_type='auto' ORDER BY id LIMIT 1"
    ).fetchone()
    c.close()
    return row["id"] if row else None


# ═══════════════════════════════════════════════════════════════
#  WORKERS
# ═══════════════════════════════════════════════════════════════
def process_manual_job(job_id: int):
    log(f"JOB #{job_id} STARTED (manual)")
    while not STOP.is_set():
        item = claim_item(job_id)
        if not item:
            if finish_if_done(job_id):
                log(f"JOB #{job_id} COMPLETED")
                return
            time.sleep(.15)
            continue
        save_result(item, probe(item["url"]))


def process_auto_tick(job_id: int) -> bool:
    item = claim_item(job_id)
    if item:
        save_result(item, probe(item["url"]))
        return True

    with GEN_LOCK:
        c = db()
        pending = c.execute(
            "SELECT COUNT(*) n FROM items WHERE job_id=? AND status='pending'",
            (job_id,)
        ).fetchone()["n"]
        job = c.execute("SELECT seeds,total,status FROM jobs WHERE id=?",
                        (job_id,)).fetchone()
        c.close()

        if not job or job["status"] != "processing":
            return False
        if pending > 0:
            return True
        if job["total"] >= AUTO_MAX_URLS:
            c = db()
            c.execute(
                "UPDATE jobs SET status='completed',completed_at=?,updated_at=? "
                "WHERE id=?", (now(), now(), job_id)
            )
            c.commit()
            c.close()
            log(f"AUTO JOB #{job_id} CAP REACHED ({AUTO_MAX_URLS})")
            return False

        try:
            seeds = json.loads(job["seeds"] or "[]")
        except Exception:
            seeds = []
        if not seeds:
            c = db()
            c.execute(
                "UPDATE jobs SET status='completed',completed_at=?,updated_at=? "
                "WHERE id=?", (now(), now(), job_id)
            )
            c.commit()
            c.close()
            return False

        n = generate_auto_batch(job_id, seeds, AUTO_BATCH)
        return n > 0


def worker(n: int):
    log(f"worker #{n} ready")
    while not STOP.is_set():
        jid = claim_job()
        if jid:
            c = db()
            j = c.execute("SELECT job_type FROM jobs WHERE id=?", (jid,)).fetchone()
            c.close()
            if j and j["job_type"] == "auto":
                process_auto_tick(jid)
                continue
            try:
                process_manual_job(jid)
            except Exception as e:
                log(f"JOB #{jid} FAILED: {type(e).__name__}: {e}")
                c = db()
                c.execute(
                    "UPDATE jobs SET status='failed',error=?,updated_at=? WHERE id=?",
                    (str(e)[:500], now(), jid)
                )
                c.commit()
                c.close()
            continue

        auto_jid = find_running_auto_job()
        if auto_jid:
            did = process_auto_tick(auto_jid)
            if not did:
                time.sleep(0.5)
            continue

        time.sleep(0.5)


# ═══════════════════════════════════════════════════════════════
#  DB READS
# ═══════════════════════════════════════════════════════════════
def get_job(token: str, include_items: bool = True):
    c = db()
    job = c.execute("SELECT * FROM jobs WHERE token=?", (token,)).fetchone()
    if not job:
        c.close()
        return None
    data = dict(job)
    if include_items:
        rows = c.execute(
            "SELECT url,status,http_status,error,latency_ms,checked_at "
            "FROM items WHERE job_id=? ORDER BY id", (job["id"],)
        ).fetchall()
        data["items"] = [dict(r) for r in rows]
    c.close()
    return data


def recent_jobs():
    c = db()
    rows = c.execute(
        "SELECT id,token,filename,total,processed,active,inactive,no_data,"
        "unreachable,too_large,status,job_type,created_at,completed_at,updated_at "
        "FROM jobs ORDER BY id DESC LIMIT 50"
    ).fetchall()
    c.close()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════
#  HTTP HANDLER
# ═══════════════════════════════════════════════════════════════
class Handler(BaseHTTPRequestHandler):
    server_version = "BulkFirebase/2.3"

    def log_message(self, fmt, *args):
        log(fmt % args)

    def send_json(self, obj, status: int = 200, headers=None):
        raw = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        if headers:
            for k, v in headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        path = urlparse(self.path).path

        if path in ("/", "/index.html"):
            fp = BASE / "static" / "index.html"
            if not fp.exists():
                return self.send_json({"ok": False, "error": "index.html missing"}, 404)
            data = fp.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        if path == "/health":
            return self.send_json({
                "ok": True,
                "service": "bulk-firebase-checker",
                "version": "2.3",
                "workers": WORKERS,
                "auto_max_urls": AUTO_MAX_URLS,
                "auto_batch": AUTO_BATCH,
                "auto_expand": AUTO_EXPAND,
                "suffixes": FB_SUFFIXES,
                "telegram": TELEGRAM_ENABLED,
                "verified_count": len(VERIFIED_URLS),
                "file_interval_min": FILE_INTERVAL // 60,
            })

        if path == "/api/public-bulk/jobs":
            return self.send_json({"ok": True, "jobs": recent_jobs()})

        if path == "/api/public-bulk/verified":
            with VERIFIED_LOCK:
                urls = sorted(VERIFIED_URLS)
            return self.send_json({"ok": True, "count": len(urls), "urls": urls})

        if path.startswith("/api/public-bulk/jobs/"):
            tail = path[len("/api/public-bulk/jobs/"):].strip("/")
            token = tail.split("/", 1)[0] if "/" in tail else tail
            job = get_job(token)
            if not job:
                return self.send_json({"ok": False, "error": "Job not found"}, 404)
            return self.send_json({"ok": True, "job": job, "items": job["items"]})

        self.send_json({"ok": False, "error": "Not found"}, 404)

    def _rate_ok(self, ip: str) -> bool:
        with RATE_LOCK:
            ts = [x for x in RATE.get(ip, []) if time.time() - x < RATE_WINDOW]
            if len(ts) >= RATE_LIMIT:
                return False
            ts.append(time.time())
            RATE[ip] = ts
            return True

    def _read_json(self, max_size: int):
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > max_size:
            return None
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_POST(self):
        path = urlparse(self.path).path
        ip = self.client_address[0]

        if path.startswith("/api/public-bulk/jobs/") and path.endswith("/stop"):
            parts = path.strip("/").split("/")
            if len(parts) != 5:
                return self.send_json({"ok": False, "error": "Bad path"}, 400)
            token = parts[3]
            c = db()
            cur = c.execute(
                "UPDATE jobs SET status='stopped',completed_at=?,updated_at=? "
                "WHERE token=? AND status IN ('processing','pending')",
                (now(), now(), token)
            )
            c.commit()
            changed = cur.rowcount
            c.close()
            if not changed:
                return self.send_json({"ok": False, "error": "Job not running"}, 404)
            log(f"JOB token {token[:8]} STOPPED")
            return self.send_json({"ok": True})

        if path == "/api/public-bulk/auto":
            if not self._rate_ok(ip):
                return self.send_json({"ok": False, "error": "Rate limit reached."}, 429)
            try:
                body = self._read_json(50000)
                if body is None:
                    return self.send_json({"ok": False, "error": "Bad request size"}, 413)
            except Exception:
                return self.send_json({"ok": False, "error": "Invalid JSON"}, 400)

            seeds_raw = str(body.get("seeds", "") or "")
            seeds = []
            seen = set()
            for raw in seeds_raw.split(","):
                s = sanitize_seed(raw)
                if s and s not in seen:
                    seen.add(s)
                    seeds.append(s)
                if len(seeds) >= 100:
                    break
            if not seeds:
                return self.send_json({"ok": False, "error": "No valid seeds"}, 400)

            filename = str(body.get("filename") or "auto-mutator")[:180]
            token = secrets.token_urlsafe(24)
            c = db()
            cur = c.execute(
                "INSERT INTO jobs(token,filename,total,status,job_type,seeds) "
                "VALUES(?,?,?,'pending','auto',?)",
                (token, filename, 0, json.dumps(seeds))
            )
            jid = cur.lastrowid
            c.commit()
            c.close()
            log(f"AUTO JOB #{jid} CREATED | {len(seeds)} seeds | {filename}")

            if TELEGRAM_ENABLED:
                tg_notify(
                    f"♻️ <b>Auto Mutator started</b>\n"
                    f"Job #{jid} · {filename}\n"
                    f"Seeds: {len(seeds)}\n"
                    f"Cap: {AUTO_MAX_URLS}"
                )

            return self.send_json({
                "ok": True, "job_id": jid, "token": token,
                "seeds": len(seeds), "max_urls": AUTO_MAX_URLS,
            }, 201)

        if path != "/api/public-bulk/jobs":
            return self.send_json({"ok": False, "error": "Not found"}, 404)

        if not self._rate_ok(ip):
            return self.send_json({"ok": False, "error": "Rate limit reached."}, 429)
        try:
            body = self._read_json(MAX_BYTES + 20000)
            if body is None:
                return self.send_json({"ok": False, "error": "Request too large or empty."}, 413)
        except Exception:
            return self.send_json({"ok": False, "error": "Invalid JSON."}, 400)

        content = str(body.get("content", "") or "")
        if len(content.encode()) > MAX_BYTES:
            return self.send_json(
                {"ok": False, "error": f"TXT exceeds {MAX_BYTES // 1000} KB."}, 413
            )
        urls = parse_urls(content)
        if not urls:
            return self.send_json(
                {"ok": False, "error": "No valid Firebase HTTPS URLs found."}, 400
            )

        filename = str(body.get("filename") or "firebase-urls.txt")[:180]
        token = secrets.token_urlsafe(24)
        c = db()
        cur = c.execute(
            "INSERT INTO jobs(token,filename,total,status,job_type) "
            "VALUES(?,?,?,'pending','manual')",
            (token, filename, len(urls))
        )
        jid = cur.lastrowid
        c.executemany("INSERT INTO items(job_id,url) VALUES(?,?)",
                      [(jid, u) for u in urls])
        c.commit()
        c.close()
        log(f"JOB #{jid} CREATED (manual) | {len(urls)} URLs | {filename}")

        if TELEGRAM_ENABLED:
            tg_notify(
                f"📥 <b>Manual Bulk started</b>\n"
                f"Job #{jid} · {filename}\n"
                f"Total URLs: {len(urls)}"
            )

        return self.send_json({
            "ok": True, "job_id": jid, "token": token, "total": len(urls)
        }, 201)


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    init_db()
    load_verified()

    # Worker threads
    for i in range(WORKERS):
        threading.Thread(target=worker, args=(i + 1,), daemon=True).start()

    # Periodic file sender
    if TELEGRAM_ENABLED:
        threading.Thread(target=periodic_file_sender, daemon=True).start()

    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    log(f"SERVER http://{HOST}:{PORT}")
    log(f"WORKERS {WORKERS} | MAX_URLS {MAX_URLS} | TIMEOUT {TIMEOUT}s")
    log(f"AUTO    cap={AUTO_MAX_URLS} batch={AUTO_BATCH} expand={AUTO_EXPAND}")
    log(f"SUFFIX  {FB_SUFFIXES}")
    log(f"VERIFY  key={FIREBASE_DEFAULT_KEY} (devices must be > 0)")
    log(f"TELEGRAM {'ENABLED ✓' if TELEGRAM_ENABLED else 'DISABLED'}")
    log(f"FILE    every {FILE_INTERVAL // 60} min → {VERIFIED_FILE}")

    if TELEGRAM_ENABLED:
        tg_startup_msg()

    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        STOP.set()
        srv.server_close()


if __name__ == "__main__":
    main()