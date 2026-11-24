# -*- coding: utf-8 -*-
"""
PRIME UNIVERSAL DOWNLOADER — v7.9 PERFORMANCE TUNED (Owner-Only, Interactive Wizard, Forum Topics, Diff Engine)
Upgrades over v7.8:
  - Conservative 1 GB RAM / 1 vCPU performance tuning
  - Pyrogram concurrent transmission setting (4)
  - HTTP connection pool and 2 MiB streaming reads
  - yt-dlp concurrent fragment downloads (4)
Upgrades over v6:
  - Fixed Channel 'can_post_messages' privileges bug
  - Interactive /start Wizard (Upload Batch, Update Old Batch, Single Link, Clean Disk)
  - Update Old Batch (Old TXT vs New TXT URL Diff Engine)
  - Group Forum Topic Auto-Creation + Persistent Topic DB (reuses topics on future updates)
  - Subject-Wise Topic + Auto Chapter Pinning & Chapter-Wise Topic modes
  - Pause / Resume / Skip / Cancel Full Batch live controls
  - Failed Download Fallback Post (sends Index, Title, Topic & Stream URL to destination)
"""
import os, re, sys, time, uuid, glob, shutil, asyncio, sqlite3, threading
import ipaddress, socket, json, random, logging, traceback, html
from urllib.parse import urlparse, unquote, urljoin, quote

import aiohttp
from aiohttp import web
import yt_dlp

from pyrogram import Client, filters, idle, StopPropagation
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from pyrogram.errors import (
    FloodWait, RPCError, ChannelInvalid, ChannelPrivate,
    ChatAdminRequired, PeerIdInvalid, ChatWriteForbidden, UserIsBlocked,
)
from pyrogram.enums import ChatType, ChatMemberStatus, ParseMode
from pyrogram.raw import functions, types

# =========================================================
# CONFIG (SINGLE FILE)
# =========================================================
# Fill these values directly. No .env file or environment variables required.
API_ID     = 39188505                   # Your Telegram API ID
API_HASH   = "b387d01bdf82960593fdc2544704dec8"
BOT_TOKEN  = "8999860182:AAFOPGo4L92iXghrkZUNEp9vIf6K9e4y89A"
OWNER_ID   = 8602814900                 # Owner Telegram user ID

if not API_ID or API_HASH == "PASTE_API_HASH_HERE" or BOT_TOKEN == "PASTE_NEW_BOT_TOKEN_HERE" or not OWNER_ID:
    raise RuntimeError("Fill API_ID, API_HASH, BOT_TOKEN and OWNER_ID in the CONFIG section.")

MAX_FILE_MB          = 2000
MAX_RETRIES          = 3
CONNECT_TIMEOUT      = 30
READ_TIMEOUT         = 180
TOPIC_CREATE_GAP     = 2.5
FLOODWAIT_EXTRA      = 1
# Telegram upload/download transmission workers.
# 8 concurrent parts is a moderate setting for a 1 GB container.
# Actual throughput still depends on the host route and Telegram DC.
UPLOAD_CONCURRENCY = 8
MAX_CONCURRENT_TRANSMISSIONS = UPLOAD_CONCURRENCY
HTTP_CONNECTION_LIMIT = 8
HTTP_PER_HOST_LIMIT = 4
DOWNLOAD_CHUNK_SIZE = 2 * 1024 * 1024  # 2 MiB; fewer Python read/write callbacks

async def telegram_retry(operation, label="Telegram request", attempts=12):
    """Retry Telegram FloodWaits without aborting a batch setup operation."""
    for attempt in range(attempts):
        try:
            return await operation()
        except FloodWait as exc:
            wait_seconds = max(1, int(getattr(exc, "value", 1) or 1)) + FLOODWAIT_EXTRA
            logger.warning("%s hit FloodWait; sleeping %ss (attempt %s/%s)", label, wait_seconds, attempt + 1, attempts)
            await asyncio.sleep(wait_seconds)
        except Exception:
            raise
    raise RuntimeError(f"{label}: FloodWait retry limit ({attempts}) reached")

# Keep this bot's runtime files isolated from every other hosted bot.
BASE_DIR             = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR         = os.path.join(BASE_DIR, "downloads_uploader2")
DB_PATH              = os.path.join(BASE_DIR, "prime_uploader2.db")
SESSION_NAME         = "prime_universal_uploader2"

PROGRESS_INTERVAL    = 2
BULK_STATUS_INTERVAL = 5
CHUNK_SIZE           = DOWNLOAD_CHUNK_SIZE

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# =========================================================
# LOGGING & CLIENT
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("prime")
logging.getLogger("pyrogram").setLevel(logging.WARNING)

app = Client(
    SESSION_NAME,
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir=BASE_DIR,
    max_concurrent_transmissions=MAX_CONCURRENT_TRANSMISSIONS,
)

# STRICT OWNER-ONLY SECURITY GATE
# Silently ignore non-owner updates. Never send warning replies/DMs.
async def owner_only_message_guard(client, message):
    sender = getattr(message, "from_user", None)
    if not sender or sender.id != OWNER_ID:
        if sender:
            logger.info("Ignored non-owner update from user_id=%s", sender.id)
        raise StopPropagation

async def owner_only_callback_guard(client, query):
    sender = getattr(query, "from_user", None)
    if not sender or sender.id != OWNER_ID:
        try:
            await query.answer()
        except Exception:
            pass
        raise StopPropagation

app.add_handler(MessageHandler(owner_only_message_guard, filters.all), group=-1)
app.add_handler(CallbackQueryHandler(owner_only_callback_guard), group=-1)

# =========================================================
# DATABASE
# =========================================================
db = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
db.row_factory = sqlite3.Row
db_lock = threading.RLock()

def init_db():
    with db_lock:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        db.execute("PRAGMA busy_timeout=30000")
        db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, first_seen REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, url TEXT NOT NULL,
            title TEXT, quality TEXT NOT NULL, status TEXT NOT NULL,
            created REAL NOT NULL, started REAL, finished REAL, error TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created);
        CREATE INDEX IF NOT EXISTS idx_jobs_user_created   ON jobs(user_id, created);
        CREATE TABLE IF NOT EXISTS destinations (
            user_id INTEGER PRIMARY KEY, chat_id TEXT NOT NULL,
            chat_type TEXT NOT NULL, title TEXT, updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS bulks (
            bulk_id TEXT PRIMARY KEY, user_id INTEGER NOT NULL,
            status TEXT NOT NULL, total INTEGER NOT NULL DEFAULT 0,
            videos INTEGER NOT NULL DEFAULT 0, pdfs INTEGER NOT NULL DEFAULT 0,
            htmls INTEGER NOT NULL DEFAULT 0, others INTEGER NOT NULL DEFAULT 0,
            destination_chat_id TEXT, destination_type TEXT,
            created REAL NOT NULL, updated REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS bot_settings (
            setting_key TEXT PRIMARY KEY,
            setting_value TEXT NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS forum_topics (
            chat_id TEXT NOT NULL,
            topic_key TEXT NOT NULL,
            topic_title TEXT NOT NULL,
            thread_id INTEGER NOT NULL,
            created_at REAL NOT NULL,
            PRIMARY KEY (chat_id, topic_key)
        );
        """)
        db.commit()

def add_column_if_missing(table, column, ddl):
    with db_lock:
        try:
            cur = db.execute(f"PRAGMA table_info({table})")
            cols = {r[1] for r in cur.fetchall()}
            if column in cols:
                return False
            db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
            db.commit()
            return True
        except sqlite3.OperationalError:
            return False

def run_migrations():
    for col, ddl in [
        ("desired_name",         "TEXT"),
        ("bulk_id",              "TEXT"),
        ("file_index",           "TEXT"),
        ("file_type",            "TEXT"),
        ("topic",                "TEXT"),
        ("chapter",              "TEXT"),
        ("pin_chapter",          "INTEGER DEFAULT 0"),
        ("batch",                "TEXT"),
        ("extracted_by",         "TEXT"),
        ("source_url",           "TEXT"),
        ("destination_chat_id",  "TEXT"),
        ("destination_type",     "TEXT"),
        ("thread_id",            "INTEGER"),
        ("file_size",            "INTEGER"),
        ("downloaded_size",      "INTEGER"),
        ("uploaded_size",        "INTEGER"),
        ("retry_count",          "INTEGER DEFAULT 0"),
        ("download_status",      "TEXT"),
        ("upload_status",        "TEXT"),
        ("error_message",        "TEXT"),
        ("caption",              "TEXT"),
        ("updated_at",           "REAL"),
        ("sent_message_id",      "INTEGER"),
    ]:
        add_column_if_missing("jobs", col, ddl)
    for col, ddl in [
        ("first_message_id", "INTEGER"),
        ("status_message_id", "INTEGER"),
        ("pinned_at", "REAL"),
        ("topic_mode", "TEXT DEFAULT 'none'"),
    ]:
        add_column_if_missing("bulks", col, ddl)

init_db()
run_migrations()

def db_execute(sql, params=(), fetch=False, one=False):
    with db_lock:
        cur = db.execute(sql, params)
        sql_upper = sql.lstrip().upper()
        is_read = sql_upper.startswith(("SELECT", "PRAGMA"))
        if one:
            row = cur.fetchone(); db.commit(); return row
        if fetch or is_read:
            rows = cur.fetchall(); db.commit(); return rows
        db.commit()
        return []

def add_user(uid):
    db_execute("INSERT OR IGNORE INTO users(user_id, first_seen) VALUES(?,?)", (uid, time.time()))

def get_job(jid):
    return db_execute("SELECT * FROM jobs WHERE job_id=?", (jid,), one=True)

def make_job(uid, url, title, quality, bulk_id=None, file_index=None,
             file_type=None, topic="", chapter="", pin_chapter=0, batch="",
             extracted_by="PRIME GHOST", desired_name=None, caption=None,
             destination_chat_id=None, destination_type="dm", thread_id=None):
    jid = uuid.uuid4().hex[:12]
    now = time.time()
    db_execute(
        """INSERT INTO jobs (
            job_id, user_id, url, title, quality, status, created,
            bulk_id, file_index, file_type, topic, chapter, pin_chapter, batch,
            extracted_by, source_url, desired_name, caption, destination_chat_id,
            destination_type, thread_id, download_status, upload_status, updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (jid, uid, url, (title or "")[:500], quality, "QUEUED", now,
         bulk_id, file_index, file_type, topic, chapter, pin_chapter, batch,
         extracted_by, url, desired_name, caption, destination_chat_id,
         destination_type, thread_id, "pending", "pending", now)
    )
    return jid

# =========================================================
# UTILITIES & NORMALIZERS
# =========================================================
def sizefmt(size):
    if size is None or size < 0: return "Unknown"
    size = float(size)
    if size >= 1024 ** 3: return f"{size/1024**3:.2f} GB"
    if size >= 1024 ** 2: return f"{size/1024**2:.1f} MB"
    if size >= 1024:      return f"{size/1024:.1f} KB"
    return f"{size:.0f} B"

def format_time(sec):
    sec = max(0, int(sec))
    h, m, s = sec // 3600, (sec % 3600) // 60, sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

def bar(percent):
    n = max(0, min(10, int(percent / 10)))
    return "█" * n + "░" * (10 - n)

def h(s):
    return html.escape(str(s or ""), quote=False)

def sanitize_filename(name, max_len=200):
    if not name: return "file"
    name = str(name)
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]+', '-', name)
    name = re.sub(r'-{2,}', '-', name)
    name = re.sub(r'\s+', ' ', name).strip(" .-_")
    return (name[:max_len] or "file")

def encode_url(url):
    p = urlparse(url)
    return p._replace(
        path=quote(unquote(p.path), safe="/%:@!$&'()*+,;=-._~"),
        query=quote(unquote(p.query), safe="=&%"),
    ).geturl()

async def safe_edit(message, text, reply_markup=None):
    if message is None: return
    for attempt in range(12):
        try:
            return await message.edit_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        except FloodWait as e:
            delay = int(getattr(e, "value", 0) or 1) + 2
            logger.warning("FloodWait while editing status (%ss); auto-resuming edit", delay)
            await asyncio.sleep(delay)
        except Exception as exc:
            logger.warning("Status edit failed: %s", exc)
            return None
    logger.error("Status edit retry limit reached")
    return None

async def validate_public_url(url):
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("Invalid HTTP/HTTPS URL")
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(
            parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80),
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror:
        raise ValueError("URL host could not be resolved")
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if not ip.is_global:
            raise ValueError("Private/local URLs blocked")

KNOWN_EXTS = {
    ".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v",
    ".mp3", ".m4a", ".wav", ".ogg", ".flac",
    ".pdf", ".zip", ".rar", ".7z", ".tar", ".gz",
    ".html", ".htm", ".txt",
    ".jpg", ".jpeg", ".png", ".gif", ".webp",
    ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".epub",
}

def direct_extension(url):
    ext = os.path.splitext(urlparse(url).path)[1].lower()
    return ext if ext in KNOWN_EXTS else None

def is_direct_media_url(url):
    return direct_extension(url) is not None

def classify_url(url):
    ext = os.path.splitext(urlparse(url).path)[1].lower()
    if ext in (".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v") or "youtube.com" in url or "youtu.be" in url:
        return "video"
    if ext in (".mp3", ".m4a", ".wav", ".ogg", ".flac"): return "audio"
    if ext == ".pdf": return "pdf"
    if ext in (".html", ".htm"): return "html"
    return "other"

def cleanup_job_folder(jid):
    folder = os.path.join(DOWNLOAD_DIR, jid)
    if os.path.exists(folder):
        shutil.rmtree(folder, ignore_errors=True)

def clean_all_downloads():
    """Remove only inactive job folders and orphan temp files. Never touch queued/active jobs."""
    count = 0
    active_statuses = {"QUEUED", "DOWNLOADING", "UPLOADING"}
    for name in os.listdir(DOWNLOAD_DIR):
        path = os.path.join(DOWNLOAD_DIR, name)
        try:
            if not os.path.isdir(path):
                # TXT import temp files are safe only when no longer being written.
                if name.startswith("txt_") or name.startswith("temp_"):
                    os.remove(path); count += 1
                continue
            row = db_execute("SELECT status FROM jobs WHERE job_id=?", (name,), one=True)
            if row and row["status"] in active_statuses:
                continue
            if name in cancel_events:
                continue
            shutil.rmtree(path, ignore_errors=True)
            count += 1
        except Exception as exc:
            logger.warning("Clean disk skipped %s: %s", name, exc)
    return count

# =========================================================
# SMART SUBJECT & CHAPTER EXTRACTOR (FOR SELECTION WAY TXT)
# =========================================================
def normalize_subject_name(raw_topic):
    """Merges 'ARITHMETIC'/'Arithmetic', 'Number System and More'/'Number System & More'."""
    t = re.sub(r'\s+', ' ', (raw_topic or "General").strip())
    low = t.lower().replace(" and ", " & ")
    mapping = {
        "math arithmetic": "Math Arithmetic",
        "maths arithmetic": "Math Arithmetic",
        "arithmetic": "Math Arithmetic",
        "arithmetic (maths)": "Math Arithmetic",
        "math advance": "Math Advance",
        "maths advance": "Math Advance",
        "advance maths": "Math Advance",
        "advance": "Math Advance",
        "english": "English",
        "grammar": "English",
        "english grammar": "English",
        "english vocab": "English",
        "vocab": "English",
        "reasoning": "Reasoning",
        "verbal": "Reasoning",
        "non verbal": "Reasoning",
        "reasoning (verbal)": "Reasoning",
        "reasoning (non-verbal)": "Reasoning",
        "number system & more": "Math Advance",
        "pqrs/ct/rc": "English",
        "misc. grammar practice": "English",
        "mock test": "Mock Tests",
        "geography": "Geography",
        "history": "History",
        "polity": "Polity",
        "economics": "Economics",
        "first watch this video": "Strategy & Intro",
    }
    return mapping.get(low, t.title() if t.isupper() else t)

def extract_chapter_and_is_first_class(title, file_type):
    """Robustly identify lecture number and chapter from Selection Way titles."""
    t = re.sub(r"\s+", " ", str(title or "")).strip()
    is_first = False
    if file_type == "video":
        is_first = bool(re.search(
            r"\b(?:class|lec(?:ture)?|part)\s*[-_.:#]?\s*0*1(?:\b|$)",
            t, re.I
        )) or "watch this first" in t.lower() or bool(re.search(r"\bcomplete\b", t, re.I))

    # Remove known batch/teacher decorations, but retain the actual subject/chapter.
    cleaned = re.sub(r"आरम्भ\s*Batch", "", t, flags=re.I)
    cleaned = re.sub(r"\bby\s+.+?\s+sir\b.*$", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\|\s*by\s+.+$", "", cleaned, flags=re.I)
    parts = [p.strip(" -|:") for p in re.split(r"\s*\|{1,2}\s*", cleaned) if p.strip(" -|:")]
    chapter = ""
    for part in parts:
        if re.fullmatch(r"(?:class|lec(?:ture)?|part)\s*[-_.:#]?\s*0*\d+", part, re.I):
            continue
        if re.search(r"\b(?:class|lec(?:ture)?|part)\s*[-_.:#]?\s*0*\d+\b", part, re.I):
            part = re.sub(r"\b(?:class|lec(?:ture)?|part)\s*[-_.:#]?\s*0*\d+\b.*$", "", part, flags=re.I).strip(" -|:")
        if part and not re.fullmatch(r"(?:english|maths|reasoning|complete)", part, re.I):
            chapter = part
            break
    if not chapter:
        chapter = re.sub(r"[_]+", " ", t)
        chapter = re.sub(r"\b(?:class|lec(?:ture)?|part)[ _-]*0*\d+.*$", "", chapter, flags=re.I).strip(" -|:")
    chapter = re.sub(r"\s+", " ", chapter).strip() or "General"
    return chapter[:100], is_first

# =========================================================
# CAPTIONS + FILENAMES
# =========================================================
LABEL_MAP = {
    "video": "🎬 Class / Lecture",
    "pdf":   "📄 Class PDF",
    "html":  "🌐 HTML Resource",
    "audio": "🎵 Audio Lecture",
    "other": "📁 File",
}
EXT_MAP = {"video": ".mp4", "pdf": ".pdf", "html": ".html", "audio": ".mp3", "other": ""}


PRIME_GHOST_LINK = "https://whatsapp.com/channel/0029VbD0ux1K0IBbhoBNfu44"

# Edit these lines freely. Use 1, 2 or 3 lines; HTML is escaped automatically.
# Example: ["EXTRACTED BY : @INFINITYxPRIME", "JOIN : @YOURCHANNEL"]
EXTRACTED_BY_LINES = [
    "𝐄𝐗𝐓𝐑𝐀𝐂𝐓𝐄𝐃 𝐁𝐘 : @INFINITYxPRIME",
]

def get_extracted_by_lines():
    row = db_execute("SELECT setting_value FROM bot_settings WHERE setting_key='extracted_by'", one=True)
    if row and row["setting_value"]:
        try:
            values = json.loads(row["setting_value"])
            if isinstance(values, list):
                return [str(x).strip() for x in values[:3] if str(x).strip()]
        except Exception:
            pass
    return [str(x).strip() for x in EXTRACTED_BY_LINES[:3] if str(x).strip()]

def build_caption(rec):
    index = html.escape(str(rec.get("index") or "N/A").strip())
    title = html.escape(str(rec.get("title") or "Untitled resource").strip())
    topic = html.escape(str(rec.get("topic") or "General").strip())
    batch = html.escape(str(rec.get("batch") or "Not Available").strip())
    lines = [
        f"🔢 <b>INDEX NO :</b> <code>{index}</code>",
        "",
        f"📌 <b>TITLE :</b> {title}",
        "",
        f"📚 <b>TOPIC :</b> {topic}",
        "",
        f"🎓 <b>COURSE :</b> {batch}",
        "",
        "━━━━━━━━━━━━━━━━━━",
    ]
    footer_lines = get_extracted_by_lines()
    lines.extend(html.escape(x) for x in footer_lines)
    return "\n".join(lines)[:1024]


def build_filename(title, file_type, url):
    url_ext = os.path.splitext(urlparse(url).path)[1].lower()
    ext = url_ext if url_ext in KNOWN_EXTS else EXT_MAP.get(file_type, ".mp4" if file_type == "video" else "")
    safe = sanitize_filename(title)
    if ext and safe.lower().endswith(ext):
        safe = safe[:-len(ext)]
    if safe.upper().endswith("PRIME"):
        safe = safe[:-5].rstrip(" -._")
    safe = safe.rstrip(" -._") or "file"
    return f"{safe[:150]} PRIME{ext}"

# =========================================================
# TXT PARSER
# =========================================================
KEY_ALIASES = {
    "index": "index", "title": "title", "topic": "topic",
    "batch": "batch", "extracted by": "extracted_by", "url": "url",
    "chapter": "chapter", "class number": "class_number", "type": "declared_type",
}

def parse_txt(text):
    records, errors = [], []
    current, line_no = {}, 0

    for raw in text.splitlines():
        line_no += 1
        line = raw.strip()
        if not line:
            if current:
                records.append((line_no, current))
                current = {}
            continue
        if set(line) <= set("=-_*= "):
            continue
        if ":" in line:
            key_raw, _, value = line.partition(":")
            canonical = KEY_ALIASES.get(key_raw.strip().lower())
            if canonical:
                current[canonical] = value.strip()
                continue

        # Fallback single-line format: Title : https://...
        m = re.match(r"^(.+?)\s*:\s*(https?://\S+)$", line)
        if m:
            records.append((line_no, {"title": m.group(1).strip(), "url": m.group(2).strip()}))
            continue

    if current:
        records.append((line_no, current))

    out = []
    for ln, rec in records:
        if not rec.get("url"):
            errors.append((ln, "missing URL")); continue
        if not rec.get("title"):
            errors.append((ln, "missing Title")); continue
        url = rec["url"].strip()
        if not re.match(r"^https?://", url):
            errors.append((ln, f"invalid URL: {url[:80]}")); continue
        ftype = classify_url(url)
        subj = normalize_subject_name(rec.get("topic") or "General")
        chap, is_first = extract_chapter_and_is_first_class(rec["title"].strip(), ftype)
        # Prefer explicit TXT metadata when supplied by the corrected batch file.
        if rec.get("chapter"):
            chap = rec["chapter"].strip()
        if rec.get("class_number"):
            is_first = (ftype == "video" and re.sub(r"\D", "", rec["class_number"]) == "1")
        out.append({
            "index":     (rec.get("index") or "").strip(),
            "title":     rec["title"].strip(),
            "topic":     subj,
            "chapter":   chap,
            "class_number": (rec.get("class_number") or "").strip(),
            "is_first":  is_first,
            "batch":     (rec.get("batch") or "").strip(),
            "url":       url,
            "file_type": ftype,
            "_line":     ln,
        })
    return out, errors

# =========================================================
# DESTINATION & CHANNEL PERMISSION FIX
# =========================================================
def get_user_destination(uid):
    row = db_execute("SELECT * FROM destinations WHERE user_id=?", (uid,), one=True)
    if row:
        return {"chat_id": row["chat_id"], "chat_type": row["chat_type"], "title": row["title"] or "Channel"}
    return {"chat_id": None, "chat_type": "dm", "title": "Personal DM"}

def set_user_destination(uid, chat_id, chat_type, title):
    db_execute(
        "INSERT OR REPLACE INTO destinations(user_id, chat_id, chat_type, title, updated_at) VALUES(?,?,?,?,?)",
        (uid, str(chat_id), chat_type, title, time.time()))

def clear_user_destination(uid):
    db_execute("DELETE FROM destinations WHERE user_id=?", (uid,))

async def validate_channel(client, chat_input):
    try:
        chat = await client.get_chat(chat_input)
    except Exception as e:
        raise ValueError(f"Cannot access group/channel ({type(e).__name__}). Make sure bot is added as Admin.")
    allowed = (ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL)
    if chat.type not in allowed:
        raise ValueError(f"Destination must be a Group, Supergroup or Channel (got {chat.type})")
    try:
        me = await client.get_me()
        member = await client.get_chat_member(chat.id, me.id)
    except Exception as e:
        raise ValueError(f"Bot is not a member of this chat: {type(e).__name__}")

    if chat.type == ChatType.CHANNEL:
        if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            raise ValueError("Bot must be an Administrator in the Channel.")
        # FIXED BUG: Check privileges.can_post_messages properly for Pyrogram v2 / Pyrofork
        priv = getattr(member, "privileges", None)
        can_post = getattr(priv, "can_post_messages", False) if priv else getattr(member, "can_post_messages", True)
        if not can_post and member.status != ChatMemberStatus.OWNER:
            raise ValueError("Bot is admin but lacks 'Post Messages' permission in Channel.")
    elif member.status in (ChatMemberStatus.BANNED, ChatMemberStatus.LEFT):
        raise ValueError("Bot is not an active member of this group.")
    return chat

# =========================================================
# FORUM TOPIC AUTO-CREATION & PERSISTENT DB
# =========================================================
async def get_or_create_forum_topic(chat_id, topic_title):
    """Create/reuse a Telegram Forum Topic. Topics require a supergroup with Topics enabled."""
    chat_id = int(chat_id)
    chat_id_str = str(chat_id)
    topic_title = (topic_title or "General").strip()[:120]
    topic_key = topic_title.lower()[:100]

    await asyncio.sleep(TOPIC_CREATE_GAP)
    chat = await telegram_retry(lambda: app.get_chat(chat_id), f"Get chat {chat_id}")
    if chat.type != ChatType.SUPERGROUP:
        raise RuntimeError(
            "Telegram Topics sirf Supergroup/Forum mein kaam karte hain. "
            "Ye destination normal group ya channel hai."
        )

    me = await telegram_retry(lambda: app.get_me(), "Get bot profile")
    member = await telegram_retry(lambda: app.get_chat_member(chat_id, me.id), f"Check topic permissions in {chat_id}")
    if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
        raise RuntimeError("Subject-wise Topics banane ke liye bot ko Group Admin banao.")
    # Avoid a local privilege-property false negative across Pyrogram forks.
    # Telegram's CreateForumTopic request below is the definitive permission check.

    row = db_execute(
        "SELECT thread_id FROM forum_topics WHERE chat_id=? AND topic_key=?",
        (chat_id_str, topic_key), one=True
    )
    if row:
        return int(row["thread_id"])

    # The topic may have been created manually (or by an older bot version)
    # before its ID was saved locally. Search Telegram's existing forum topics
    # before attempting CreateForumTopic, preventing duplicate topic names.
    # Fresh-host recovery: local SQLite may be empty after migration/redeploy.
    # Telegram's live topic list is therefore checked before creating anything.
    if not hasattr(app, "get_forum_topics"):
        raise RuntimeError(
            "This Pyrogram build cannot list existing forum topics. "
            "Update Pyrofork before uploading; refusing to create a possible duplicate."
        )

    try:
        async for existing in app.get_forum_topics(chat_id, limit=100):
            existing_title = re.sub(r"\\s+", " ", (getattr(existing, "title", None) or "").strip())
            existing_id = getattr(existing, "id", None)
            if existing_id and existing_title.casefold() == topic_title.casefold():
                db_execute(
                    "INSERT OR REPLACE INTO forum_topics(chat_id, topic_key, topic_title, thread_id, created_at) VALUES(?,?,?,?,?)",
                    (chat_id_str, topic_key, topic_title, int(existing_id), time.time())
                )
                logger.info("Reused Telegram topic '%s' (thread_id=%s)", topic_title, existing_id)
                return int(existing_id)
    except Exception as lookup_error:
        logger.exception("Could not verify existing Telegram topics for '%s'", topic_title)
        raise RuntimeError(
            f"Existing topic lookup failed for '{topic_title}'. "
            "Upload stopped to prevent duplicate topic creation: {str(lookup_error)[:180]}"
        ) from lookup_error

    last_error = None
    for attempt in range(8):
        try:
            thread_id = None
            if hasattr(app, "create_forum_topic"):
                try:
                    topic = await telegram_retry(lambda: app.create_forum_topic(chat_id, topic_title), f"Create topic {topic_title}")
                    thread_id = getattr(topic, "id", None)
                except FloodWait:
                    raise
                except Exception as high_level_error:
                    logger.warning("High-level create_forum_topic failed, trying MTProto fallback: %s", high_level_error)

            if not thread_id:
                peer = await app.resolve_peer(chat_id)
                request = functions.channels.CreateForumTopic(
                    channel=peer, title=topic_title, random_id=random.randint(1, 2**62)
                )
                result = await telegram_retry(lambda: app.invoke(request), f"Create topic {topic_title}")
                for update in getattr(result, "updates", []):
                    msg = getattr(update, "message", None)
                    if msg is not None and getattr(msg, "id", None):
                        thread_id = msg.id
                        break
            if not thread_id:
                raise RuntimeError("Telegram ne topic ID return nahi ki. Bot permissions/API response check karo.")

            db_execute(
                "INSERT OR REPLACE INTO forum_topics(chat_id, topic_key, topic_title, thread_id, created_at) VALUES(?,?,?,?,?)",
                (chat_id_str, topic_key, topic_title, int(thread_id), time.time())
            )
            logger.info("Created/reused forum topic '%s' (thread_id=%s) in %s", topic_title, thread_id, chat_id)
            await asyncio.sleep(2.5)  # rate-limit topic setup calls
            return int(thread_id)
        except FloodWait as e:
            wait_for = int(getattr(e, "value", 0) or 1) + 2
            logger.warning("FloodWait creating topic '%s': sleeping %ss, then resuming setup", topic_title, wait_for)
            await asyncio.sleep(wait_for)
            last_error = e
            continue
        except Exception as e:
            logger.exception("Forum topic creation failed: %s", e)
            raise RuntimeError(f"'{topic_title}' topic create nahi hua: {str(e)[:220]}") from e
    raise RuntimeError(f"'{topic_title}' topic create retries exhausted after FloodWait: {last_error}")

# =========================================================
# DOWNLOAD & UPLOAD ENGINES
# =========================================================
class PermanentError(Exception): pass
class TransientError(Exception): pass

async def http_get_with_retry(session, url, **kwargs):
    for attempt in range(MAX_RETRIES):
        try:
            resp = await session.get(url, **kwargs)
            if resp.status in (429, 500, 502, 503, 504):
                resp.release()
                if attempt == MAX_RETRIES - 1:
                    raise TransientError(f"HTTP {resp.status} after {MAX_RETRIES} retries")
                await asyncio.sleep((2 ** attempt) + random.uniform(0, 1))
                continue
            if resp.status >= 400:
                resp.release()
                raise PermanentError(f"HTTP {resp.status}")
            return resp
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES - 1:
                raise TransientError(str(e))
            await asyncio.sleep((2 ** attempt) + random.uniform(0, 1))
    raise TransientError("Max retries reached")

async def direct_download(jid, url, folder, bulk_id=None, on_progress=None):
    url = encode_url(url)
    await validate_public_url(url)
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=CONNECT_TIMEOUT, sock_read=READ_TIMEOUT)
    p = urlparse(url)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": f"{p.scheme}://{p.netloc}/",
    }
    connector = aiohttp.TCPConnector(
        limit=HTTP_CONNECTION_LIMIT,
        limit_per_host=HTTP_PER_HOST_LIMIT,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector, auto_decompress=False) as session:
        current_url = url
        for _ in range(6):
            await validate_public_url(current_url)
            response = await http_get_with_retry(session, current_url, allow_redirects=False)
            if response.status in (301, 302, 303, 307, 308):
                loc = response.headers.get("Location")
                response.release()
                if not loc: raise PermanentError("Redirect without Location")
                current_url = encode_url(urljoin(current_url, loc))
                continue
            break
        else:
            raise PermanentError("Too many redirects")

        async with response:
            filename = os.path.basename(unquote(urlparse(current_url).path)) or "download"
            filename = sanitize_filename(filename)
            if not os.path.splitext(filename)[1]:
                filename += (direct_extension(current_url) or ".bin")

            total = int(response.headers.get("Content-Length") or 0)
            if total > MAX_FILE_MB * 1024 * 1024:
                raise PermanentError(f"File exceeds {MAX_FILE_MB} MB")

            path = os.path.join(folder, f"{jid}_{filename}")
            part_path = path + ".part"
            downloaded, started, last = 0, time.time(), 0
            with open(part_path, "wb") as f:
                async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                    if cancel_events.get(jid) and cancel_events[jid].is_set():
                        raise asyncio.CancelledError("Cancelled/Skipped by user")
                    # Pause check
                    if bulk_id and bulk_id in bulk_states:
                        await bulk_states[bulk_id].pause_event.wait()
                    downloaded += len(chunk)
                    f.write(chunk)
                    now = time.time()
                    if on_progress and now - last >= PROGRESS_INTERVAL:
                        last = now
                        await on_progress(downloaded, total, downloaded / max(now - started, 1))
            if downloaded == 0:
                os.remove(part_path)
                raise PermanentError("Empty file")
            os.replace(part_path, path)
            return path, filename

def ytdlp_download_sync(jid, url, folder, loop, quality_selector, on_progress_async=None, pause_event=None):
    cancel_event = cancel_events[jid]
    last = [0.0]
    def hook(data):
        if cancel_event.is_set():
            raise Exception("Cancelled/Skipped by user")
        if pause_event is not None:
            # Bridge the async pause gate into yt-dlp's worker thread.
            asyncio.run_coroutine_threadsafe(pause_event.wait(), loop).result()
        if data.get("status") == "downloading":
            now = time.time()
            if now - last[0] >= PROGRESS_INTERVAL:
                last[0] = now
                current = data.get("downloaded_bytes") or 0
                total = data.get("total_bytes") or data.get("total_bytes_estimate") or 0
                speed = data.get("speed") or 0
                if on_progress_async:
                    asyncio.run_coroutine_threadsafe(on_progress_async(current, total, speed), loop)

    opts = {
        "format": quality_selector,
        "outtmpl": os.path.join(folder, "%(title).70s_%(id)s.%(ext)s"),
        "merge_output_format": "mp4",
        "progress_hooks": [hook],
        "quiet": True, "no_warnings": True, "noplaylist": True,
        "socket_timeout": 30, "retries": 5, "fragment_retries": 5,
        "concurrent_fragment_downloads": 4,
        "continuedl": True, "overwrites": True,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([url])
    cands = [p for p in glob.glob(os.path.join(folder, "*"))
             if os.path.isfile(p) and not p.endswith((".part", ".ytdl"))]
    if not cands:
        raise RuntimeError("yt-dlp produced no output file")
    return max(cands, key=os.path.getsize)

async def probe_video(path):
    """Extract video metadata and create a JPEG thumbnail when ffmpeg is available.
    Metadata remains valid even if thumbnail generation fails.
    """
    duration = width = height = 0
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-show_entries",
            "format=duration:stream=codec_type,width,height", "-of", "json", path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        out, err = await asyncio.wait_for(proc.communicate(), timeout=30)
        if proc.returncode != 0:
            logger.warning("ffprobe failed for %s: %s", path, err.decode(errors="ignore")[-300:])
            return 0, 0, 0, None

        meta = json.loads(out.decode(errors="replace") or "{}")
        video_stream = next(
            (s for s in meta.get("streams", []) if s.get("codec_type") == "video"),
            None
        )
        if not video_stream:
            return 0, 0, 0, None

        duration = max(0, int(float(meta.get("format", {}).get("duration") or 0)))
        width = int(video_stream.get("width") or 0)
        height = int(video_stream.get("height") or 0)

        thumb = os.path.splitext(path)[0] + "_thumb.jpg"
        seek = max(0, min(10, int(duration * 0.10))) if duration else 0
        proc2 = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-ss", str(seek), "-i", path,
            "-frames:v", "1", "-vf", "scale=320:-2",
            "-q:v", "5", "-f", "image2", thumb,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, thumb_err = await asyncio.wait_for(proc2.communicate(), timeout=45)
        if proc2.returncode != 0 or not os.path.isfile(thumb) or os.path.getsize(thumb) < 1024:
            logger.warning("Thumbnail generation failed for %s: %s",
                           path, thumb_err.decode(errors="ignore")[-300:])
            try:
                if os.path.exists(thumb):
                    os.remove(thumb)
            except OSError:
                pass
            thumb = None
        return duration, width, height, thumb
    except Exception as exc:
        logger.warning("Video probe failed for %s: %s", path, exc)
        return duration, width, height, None

async def send_to_target(target, thread_id, send_func, *args, **kwargs):
    """Send to a topic with FloodWait recovery; resume the same call after Telegram's wait."""
    for attempt in range(12):
        try:
            if thread_id:
                try:
                    return await send_func(target, *args, message_thread_id=int(thread_id), **kwargs)
                except TypeError:
                    return await send_func(target, *args, reply_to_message_id=int(thread_id), **kwargs)
            return await send_func(target, *args, **kwargs)
        except FloodWait as e:
            delay = int(getattr(e, "value", 0) or 1) + 2
            logger.warning("FloodWait on Telegram send (%ss). Auto-resuming same operation.", delay)
            await asyncio.sleep(delay)
    raise RuntimeError("Telegram FloodWait retry limit reached while sending message")

async def upload_file(jid, uid, path, filename, caption, destination_chat_id,
                      thread_id=None, bulk_id=None, on_progress=None):
    target = int(destination_chat_id) if destination_chat_id else uid
    ext = os.path.splitext(filename)[1].lower()
    started, last = time.time(), [0.0]

    async def progress(cur, tot):
        if cancel_events.get(jid) and cancel_events[jid].is_set():
            raise asyncio.CancelledError("Cancelled/Skipped by user")
        if bulk_id and bulk_id in bulk_states:
            await bulk_states[bulk_id].pause_event.wait()
        now = time.time()
        if now - last[0] >= PROGRESS_INTERVAL or cur == tot:
            last[0] = now
            if on_progress:
                await on_progress(cur, tot, cur / max(now - started, 1))

    for attempt in range(MAX_RETRIES + 1):
        try:
            if ext in (".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v"):
                dur, w, hh, thumb = await probe_video(path)
                return await send_to_target(
                    target, thread_id, app.send_video, path,
                    caption=caption, duration=dur, width=w, height=hh,
                    thumb=thumb, file_name=filename, supports_streaming=True, progress=progress
                )
            elif ext in (".mp3", ".m4a", ".wav", ".ogg"):
                return await send_to_target(
                    target, thread_id, app.send_audio, path,
                    caption=caption, file_name=filename, progress=progress
                )
            else:
                return await send_to_target(
                    target, thread_id, app.send_document, path,
                    caption=caption, file_name=filename, progress=progress
                )
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if attempt >= MAX_RETRIES:
                raise
            await asyncio.sleep(2 ** attempt)

# =========================================================
# BULK STATE, PAUSE/RESUME & FALLBACK ENGINE
# =========================================================
job_queue = asyncio.Queue()
worker_task = None
worker_ready = False
cancel_events = {}
status_messages = {}
wizard_sessions = {}  # uid -> interactive state dict

class BulkState:
    def __init__(self, bulk_id, uid, totals, destination_label, topic_mode="none"):
        self.bulk_id = bulk_id
        self.uid = uid
        self.totals = totals
        self.destination_label = destination_label
        self.topic_mode = topic_mode
        self.single_mode = False
        self.status_message = None
        self.channel_status_message = None
        self.topic_status_messages = {}  # thread_id -> editable status message
        self.topic_last_edit = {}
        self.finalized = False
        self.destination_chat_id = None
        self.first_message_pinned = False
        self.current = None
        self.current_jid = None
        self.current_phase = "Preparing batch"
        self.downloaded_bytes = 0
        self.uploaded_bytes = 0
        self.download_speed = 0
        self.upload_speed = 0
        self.started = time.time()
        self.last_edit = 0
        self.last_channel_edit = 0
        self.edit_lock = asyncio.Lock()
        self.cancelled = False
        self.paused = False
        self.pause_event = asyncio.Event()
        self.pause_event.set()  # Not paused initially

bulk_states = {}

def build_bulk_keyboard(bulk_id, is_paused=False):
    pause_btn = (
        InlineKeyboardButton("▶️ RESUME", callback_data=f"bulkresume:{bulk_id}")
        if is_paused else
        InlineKeyboardButton("⏸ PAUSE", callback_data=f"bulkpause:{bulk_id}")
    )
    return InlineKeyboardMarkup([
        [pause_btn, InlineKeyboardButton("⏭ SKIP FILE", callback_data=f"bulkskip:{bulk_id}")],
        [InlineKeyboardButton("🛑 CANCEL FULL BATCH", callback_data=f"bulkcancel:{bulk_id}")]
    ])

async def update_bulk_status(bulk_id, force=False):
    state = bulk_states.get(bulk_id)
    if not state or not state.status_message: return
    now = time.time()
    if not force and now - state.last_edit < BULK_STATUS_INTERVAL: return
    async with state.edit_lock:
        state.last_edit = now
        rows = db_execute(
            "SELECT status, COUNT(*) AS n FROM jobs WHERE bulk_id=? GROUP BY status",
            (bulk_id,), fetch=True) or []
        counts = {r["status"]: r["n"] for r in rows}
        completed   = counts.get("SUCCESS", 0)
        failed      = counts.get("FAILED", 0)
        cancelled   = counts.get("CANCELLED", 0)
        pending     = counts.get("QUEUED", 0)
        downloading = counts.get("DOWNLOADING", 0)
        uploading   = counts.get("UPLOADING", 0)
        total = sum(counts.values()) or 1

        elapsed = time.time() - state.started
        done = completed + failed + cancelled
        remain = pending + downloading + uploading
        eta = (elapsed / max(1, done)) * remain if done > 0 and remain > 0 else 0
        pct = int(done / max(1, total) * 100)

        cur_block = ""
        if state.current:
            c = state.current
            cur_block = (
                "\n━━━━━━━━━━━━━━━━━━\n"
                "📥 <b>CURRENT FILE</b>\n\n"
                f"🔢 Index: <code>{h(c.get('index','N/A'))}</code>\n"
                f"📄 Name: {h(c.get('name','N/A')[:90])}\n"
                f"📚 Topic: <code>{h(c.get('topic','General'))}</code>\n\n"
                "🔗 Source:\n"
                f"<code>{h(c.get('url','')[:180])}</code>\n\n"
                f"📤 Destination: {h(state.destination_label)}\n"
            )

        header = (
            "🛑 <b>BATCH CANCELLED</b>\n" if state.cancelled else
            ("⏸ <b>BATCH PAUSED</b>\n" if state.paused else "👻 <b>PRIME GHOST DOWNLOADER</b>\n")
        )
        text = (
            header
            + "━━━━━━━━━━━━━━━━━━\n\n"
            "📦 <b>BULK DOWNLOAD PROGRESS</b>\n"
            f"🔄 <b>{h(state.current_phase)}</b>\n\n"
            f"{bar(pct)} <code>{pct}%</code>\n\n"
            f"📊 <b>TOTAL FILES:</b> <code>{state.totals.get('total', 0)}</code>\n"
            f"🎬 VIDEOS: <code>{state.totals.get('videos', 0)}</code>\n"
            f"📄 PDF: <code>{state.totals.get('pdfs', 0)}</code>\n"
            f"🌐 HTML: <code>{state.totals.get('htmls', 0)}</code>\n"
            f"📁 OTHER: <code>{state.totals.get('others', 0)}</code>\n\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ COMPLETED: <code>{completed}</code>\n"
            f"❌ FAILED: <code>{failed}</code>\n"
            f"🛑 CANCELLED/SKIPPED: <code>{cancelled}</code>\n"
            f"⏳ PENDING: <code>{pending}</code>\n"
            f"{cur_block}"
            "\n━━━━━━━━━━━━━━━━━━\n\n"
            f"⚡ DOWNLOAD SPEED: <code>{sizefmt(state.download_speed)}/s</code>\n"
            f"⚡ UPLOAD SPEED: <code>{sizefmt(state.upload_speed)}/s</code>\n\n"
            f"📥 DOWNLOADED: <code>{sizefmt(state.downloaded_bytes)}</code>\n"
            f"📤 UPLOADED: <code>{sizefmt(state.uploaded_bytes)}</code>\n\n"
            f"⏱ ELAPSED: <code>{format_time(elapsed)}</code>\n"
            f"⏳ ETA: <code>{format_time(eta) if eta > 0 else '—'}</code>\n\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "👻 <b>POWERED BY PRIME GHOST</b>"
        )
        markup = None if (state.cancelled or remain == 0) else build_bulk_keyboard(bulk_id, state.paused)
        await safe_edit(state.status_message, text, reply_markup=markup)

async def send_failed_fallback_message(row, err_text):
    """When a file fails to download/upload, posts its full details + Stream URL to destination."""
    target = int(row["destination_chat_id"]) if row["destination_chat_id"] else int(row["user_id"])
    thread_id = row["thread_id"]
    fallback_text = (
        "⚠️ <b>FILE DOWNLOAD FAILED — STREAM LINK</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🔢 <b>Index:</b> <code>{h(row['file_index'] or 'N/A')}</code>\n"
        f"📄 <b>Name:</b> {h(row['title'] or 'Untitled')}\n"
        f"📚 <b>Topic:</b> {h(row['topic'] or 'General')}\n"
        f"🎓 <b>Batch:</b> {h(row['batch'] or 'Not Available')}\n\n"
        f"🔗 <b>Direct Stream / Download URL:</b>\n"
        f"{h(row['url'])}\n\n"
        f"❗ <i>Reason: {h(str(err_text)[:140])}</i>\n"
        + "\n".join(html.escape(x) for x in get_extracted_by_lines()) + "\n"
        "━━━━━━━━━━━━━━━━━━"
    )
    try:
        await send_to_target(
            target, thread_id, app.send_message,
            fallback_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )
    except Exception as e:
        logger.warning(f"Failed to send fallback link message: {e}")

async def make_message_link(chat_id, message_id):
    try:
        chat = await app.get_chat(int(chat_id))
        if getattr(chat, "username", None):
            return f"https://t.me/{chat.username}/{message_id}"
    except Exception:
        pass
    raw = str(chat_id)
    internal = raw[4:] if raw.startswith("-100") else raw.lstrip("-")
    return f"https://t.me/c/{internal}/{message_id}"

async def send_batch_topic_index(bulk_id, state):
    rows = db_execute(
        "SELECT file_index,title,topic,chapter,destination_chat_id,sent_message_id FROM jobs "
        "WHERE bulk_id=? AND status='SUCCESS' AND sent_message_id IS NOT NULL ORDER BY created",
        (bulk_id,), fetch=True) or []
    grouped = {}
    for row in rows:
        subject = (row["topic"] or "General").strip()
        chapter = (row["chapter"] or row["title"] or "Resource").strip()
        grouped.setdefault((subject, chapter.lower()), (chapter, row))
    lines = ["📚 <b>𝐁𝐀𝐓𝐂𝐇 𝐈𝐍𝐃𝐄𝐗</b>", "━━━━━━━━━━━━━━━━━━", ""]
    last_subject = None
    for (subject, _), (chapter, row) in grouped.items():
        if subject != last_subject:
            last_subject = subject
            lines += ["", f"🔹 <b>{h(subject.upper())}</b>"]
        link = await make_message_link(row["destination_chat_id"], row["sent_message_id"])
        lines.append(f'　↳ <a href="{link}">{h(str(row["file_index"] or "•") + ". " + chapter[:100])}</a>')
    lines += ["", "━━━━━━━━━━━━━━━━━━",
              f'<a href="{PRIME_GHOST_LINK}"><b>👻 𝐏𝐑𝐈𝐌𝐄 𝐆𝐇𝐎𝐒𝐓</b></a>']
    body = "\n".join(lines)
    target = int(state.destination_chat_id) if state.destination_chat_id else state.uid
    chunks = []
    while len(body) > 3900:
        cut = body.rfind("\n", 0, 3900)
        if cut < 1000: cut = 3900
        chunks.append(body[:cut]); body = body[cut:].lstrip()
    chunks.append(body)
    for chunk in chunks:
        await app.send_message(target, chunk, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

async def update_channel_progress(state, done, total, current_name="", file_fraction=0.0, force=False):
    """Destination status always reports whole-task progress, not just current-file progress."""
    if not state or not state.channel_status_message:
        return
    now = time.time()
    if not force and now - getattr(state, "last_channel_edit", 0) < 2.5:
        return
    state.last_channel_edit = now
    total = max(int(total or 1), 1)
    done = max(0, min(int(done or 0), total))
    file_fraction = max(0.0, min(float(file_fraction or 0.0), 0.999))
    overall_done = min(total, done + file_fraction)
    percent = overall_done * 100.0 / total
    remaining = max(total - done - (1 if file_fraction > 0 else 0), 0)
    text = (
        f"⌛ <b>𝐁𝐀𝐓𝐂𝐇 𝐏𝐑𝐎𝐆𝐑𝐄𝐒𝐒 {percent:.1f}%</b> ⌛\n\n"
        f"📊 Total Tasks: <code>{total}</code>\n"
        f"✅ Completed: <code>{done}</code>\n"
        f"⏳ Remaining: <code>{remaining}</code>\n\n"
        f"🎬 Current File: <b>{h((current_name or 'Preparing batch')[:100])}</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f'<a href="{PRIME_GHOST_LINK}"><b>👻 𝐏𝐑𝐈𝐌𝐄 𝐆𝐇𝐎𝐒𝐓</b></a>'
    )
    try:
        await state.channel_status_message.edit_text(
            text, parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )
    except Exception as e:
        logger.warning("Destination task progress edit failed: %s", e)

async def update_forum_topic_progress(state, thread_id, current_name="", file_fraction=0.0, force=False):
    """Edit the status post inside the currently active forum topic."""
    if not state or not thread_id:
        return
    msg = state.topic_status_messages.get(int(thread_id))
    if not msg:
        return
    now = time.time()
    if not force and now - state.topic_last_edit.get(int(thread_id), 0) < 2.5:
        return
    state.topic_last_edit[int(thread_id)] = now
    counts = db_execute(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN status IN ('SUCCESS','FAILED','CANCELLED') THEN 1 ELSE 0 END) AS done "
        "FROM jobs WHERE bulk_id=? AND thread_id=?",
        (state.bulk_id, int(thread_id)), one=True
    ) or {}
    # sqlite3.Row supports key indexing, not dict.get(). A .get() here
    # used to raise AttributeError from the progress callback and mark every
    # otherwise-valid download as FAILED.
    total = int(counts["total"] or 0)
    done = int(counts["done"] or 0)
    active = max(0, total - done)
    frac = max(0.0, min(float(file_fraction or 0.0), 0.999))
    percent = min(100.0, (done + (frac if active else 0.0)) * 100.0 / max(total, 1))
    body = (
        f"⌛ <b>TOPIC PROGRESS {percent:.1f}%</b>\n\n"
        f"📊 Total Files: <code>{total}</code>\n"
        f"✅ Completed: <code>{done}</code>\n"
        f"⏳ Remaining: <code>{active}</code>\n\n"
        f"🎬 Current: <b>{h((current_name or 'Preparing')[:100])}</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f'<a href="{PRIME_GHOST_LINK}"><b>👻 𝐏𝐑𝐈𝐌𝐄 𝐆𝐇𝐎𝐒𝐓</b></a>'
    )
    try:
        await msg.edit_text(body, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as exc:
        logger.warning("Topic progress edit failed for thread %s: %s", thread_id, exc)

async def process_job(jid):
    row = get_job(jid)
    if not row or row["status"] != "QUEUED":
        return
    bulk_id = row["bulk_id"]
    state = bulk_states.get(bulk_id) if bulk_id else None

    if state and state.cancelled:
        db_execute("UPDATE jobs SET status='CANCELLED', finished=?, updated_at=? WHERE job_id=?",
                   (time.time(), time.time(), jid))
        return

    if state:
        await state.pause_event.wait()
        state.current_jid = jid

    uid = row["user_id"]
    url = row["url"]
    folder = os.path.join(DOWNLOAD_DIR, jid)
    os.makedirs(folder, exist_ok=True)
    cancel_events[jid] = threading.Event()
    job_completed = False
    job_cancelled = False

    thread_id = row["thread_id"]
    filename = row["desired_name"] or build_filename(row["title"] or "file", row["file_type"] or classify_url(url), url)
    # Build the caption when the job actually starts, not only when the batch
    # is queued. This lets /setfooter changes made during a queued batch apply
    # to every file that has not started yet.
    caption = build_caption({
        "index": row["file_index"],
        "title": row["title"],
        "topic": row["topic"],
        "batch": row["batch"],
    })

    try:
        # Resolve/create topics inside the protected job block.
        if bulk_id and state and state.topic_mode in ("subject", "chapter") and row["destination_chat_id"]:
            subject_name = normalize_subject_name(row["topic"] or "General")
            topic_name = subject_name if state.topic_mode == "subject" else f"{subject_name} - {row['chapter']}"
            thread_id = await get_or_create_forum_topic(row["destination_chat_id"], topic_name)
            if not thread_id:
                raise RuntimeError("Forum topic unavailable. Check that this is a forum supergroup and bot has Manage Topics permission.")
            db_execute("UPDATE jobs SET thread_id=? WHERE job_id=?", (thread_id, jid))

        db_execute("UPDATE jobs SET status='DOWNLOADING', started=?, updated_at=? WHERE job_id=?",
                   (time.time(), time.time(), jid))
        if state:
            state.downloaded_bytes = 0
            state.uploaded_bytes = 0
            state.download_speed = 0
            state.upload_speed = 0
            state.current = {"index": row["file_index"], "name": row["title"], "url": url, "topic": row["topic"]}
            state.current_phase = "⬇️ Downloading"
            await update_bulk_status(bulk_id, force=True)

        async def refresh_destination_task_progress(cur, tot, stage="download"):
            if not state or not state.channel_status_message:
                return
            done_row = db_execute(
                "SELECT COUNT(*) AS n FROM jobs WHERE bulk_id=? AND status IN ('SUCCESS','FAILED','CANCELLED')",
                (bulk_id,), one=True
            )
            done_count = done_row["n"] if done_row else 0
            raw_fraction = (cur / tot) if tot and tot > 0 else 0.0
            fraction = (raw_fraction * 0.5) if stage == "download" else (0.5 + raw_fraction * 0.5)
            await update_channel_progress(
                state, done_count, state.totals.get("total", 1),
                row["title"] or "Uploading", file_fraction=fraction
            )
            if thread_id:
                try:
                    await update_forum_topic_progress(state, thread_id, row["title"] or "Uploading", fraction)
                except Exception as status_error:
                    logger.warning("Topic status update ignored for job %s: %s", jid, status_error)

        async def dl_progress(cur, tot, spd):
            if state:
                state.download_speed = spd
                state.downloaded_bytes = cur
                await update_bulk_status(bulk_id)
                await refresh_destination_task_progress(cur, tot, "download")

        async def ul_progress(cur, tot, spd):
            if state:
                state.upload_speed = spd
                state.uploaded_bytes = cur
                await update_bulk_status(bulk_id)
                await refresh_destination_task_progress(cur, tot, "upload")

        final_path = os.path.join(folder, filename)
        # A server restart during UPLOADING leaves the completed local file.
        # Reuse it instead of downloading the same media again.
        if os.path.isfile(final_path) and os.path.getsize(final_path) > 0:
            path = final_path
            logger.info("Reusing completed download for resumed job %s", jid)
        else:
            if is_direct_media_url(url) and "youtube.com" not in url and "youtu.be" not in url:
                path, _ = await direct_download(jid, url, folder, bulk_id=bulk_id, on_progress=dl_progress)
            else:
                loop = asyncio.get_running_loop()
                path = await asyncio.to_thread(
                    ytdlp_download_sync, jid, url, folder, loop,
                    row["quality"] or "bestvideo*+bestaudio/best", dl_progress,
                    state.pause_event if state else None
                )

            if os.path.abspath(final_path) != os.path.abspath(path):
                if os.path.exists(final_path):
                    os.remove(final_path)
                os.replace(path, final_path)
            path = final_path

        db_execute("UPDATE jobs SET status='UPLOADING', updated_at=? WHERE job_id=?", (time.time(), jid))
        if state:
            state.current_phase = "⬆️ Uploading"
            await update_bulk_status(bulk_id, force=True)

        sent_msg = await upload_file(
            jid, uid, path, filename, caption,
            row["destination_chat_id"], thread_id=thread_id,
            bulk_id=bulk_id, on_progress=ul_progress
        )

        # Auto-Pin Logic:
        # 1. If Subject-Wise mode + this is Class 1 of a chapter -> Pin inside topic!
        # 2. Or if normal mode -> Pin first batch message
        if sent_msg and row["destination_chat_id"]:
            should_pin = False
            if state and state.topic_mode == "subject" and int(row["pin_chapter"] or 0) == 1:
                # First video of each detected chapter, pinned in its subject topic.
                should_pin = True
            elif state and state.topic_mode == "chapter" and thread_id:
                # First successful item in each chapter topic (video, PDF or other).
                first_in_topic = db_execute(
                    "SELECT COUNT(*) AS n FROM jobs WHERE bulk_id=? AND thread_id=? AND status='SUCCESS' AND job_id<>?",
                    (bulk_id, thread_id, jid), one=True
                )
                should_pin = not first_in_topic or first_in_topic["n"] == 0
            elif state and state.topic_mode == "none" and not state.first_message_pinned:
                should_pin = True
                state.first_message_pinned = True

            if should_pin:
                try:
                    await app.pin_chat_message(int(row["destination_chat_id"]), sent_msg.id, disable_notification=True)
                except Exception as pe:
                    logger.warning(f"Auto-pin failed: {pe}")

        db_execute("UPDATE jobs SET status='SUCCESS', sent_message_id=?, finished=?, updated_at=? WHERE job_id=?",
                   (getattr(sent_msg, "id", None), time.time(), time.time(), jid))
        job_completed = True

    except asyncio.CancelledError as e:
        job_cancelled = True
        db_execute("UPDATE jobs SET status='CANCELLED', error=?, finished=?, updated_at=? WHERE job_id=?",
                   (str(e)[:300], time.time(), time.time(), jid))
    except Exception as e:
        ev = cancel_events.get(jid)
        if ev and ev.is_set():
            db_execute("UPDATE jobs SET status='CANCELLED', error=?, finished=?, updated_at=? WHERE job_id=?",
                       (str(e)[:300], time.time(), time.time(), jid))
        else:
            db_execute("UPDATE jobs SET status='FAILED', error=?, finished=?, updated_at=? WHERE job_id=?",
                       (str(e)[:500], time.time(), time.time(), jid))
            logger.exception("Job %s FAILED (full traceback follows): %s", jid, e)
            # Send Stream URL fallback message only for genuine failures, not user skips.
            await send_failed_fallback_message(row, e)
    finally:
        cancel_events.pop(jid, None)
        # Keep failed job files for a later retry. Successful/cancelled jobs
        # are safe to clean immediately.
        if job_completed or job_cancelled:
            cleanup_job_folder(jid)
        else:
            logger.warning("Keeping local job files for retry/resume: %s", folder)
        if bulk_id:
            await update_bulk_status(bulk_id, force=True)
            done_row = db_execute("SELECT COUNT(*) AS n FROM jobs WHERE bulk_id=? AND status IN ('SUCCESS','FAILED','CANCELLED')",
                                  (bulk_id,), one=True)
            await update_channel_progress(state, done_row["n"] if done_row else 0,
                                           state.totals["total"], row["title"] or "Uploading", force=True)
            if thread_id:
                await update_forum_topic_progress(state, thread_id, row["title"] or "Completed", 0.0, force=True)
            remaining = db_execute(
                "SELECT COUNT(*) AS n FROM jobs WHERE bulk_id=? AND status IN ('QUEUED','DOWNLOADING','UPLOADING')",
                (bulk_id,), one=True
            )
            if remaining and remaining["n"] == 0:
                final_status = "CANCELLED" if state and state.cancelled else "COMPLETED"
                db_execute("UPDATE bulks SET status=?, updated=? WHERE bulk_id=?",
                           (final_status, time.time(), bulk_id))
                if state and not state.finalized:
                    state.finalized = True
                    if final_status == "COMPLETED" and not getattr(state, "single_mode", False):
                        try:
                            await send_batch_topic_index(bulk_id, state)
                        except Exception as ix:
                            logger.warning(f"Batch index message failed: {ix}")
                    if state.channel_status_message:
                        try:
                            await state.channel_status_message.delete()
                        except Exception:
                            pass
                        try:
                            state.channel_status_message = await app.send_message(
                                int(state.destination_chat_id),
                                f"⌛ <b>𝐀𝐔𝐓𝐎𝐌𝐀𝐓𝐄𝐃 𝐓𝐀𝐒𝐊 100.00%</b> ⌛\n\n"
                                f" ┠ 📊 Total Links = <code>{state.totals['total']}</code>\n"
                                f" ┠ ⚡️ Currently On = <code>{state.totals['total']}</code>\n"
                                " ┠ ⏳ Remaining Links = <code>0</code>\n\n"
                                "⚡ <b>𝐀𝐔𝐓𝐎 𝐔𝐏𝐃𝐀𝐓𝐈𝐍𝐆 COMPLETE</b> ⚡\n"
                                "🎬 Name » <b>Batch Completed</b>\n\n"
                                f'<a href="{PRIME_GHOST_LINK}"><b>👻 𝐏𝐑𝐈𝐌𝐄 𝐆𝐇𝐎𝐒𝐓</b></a>',
                                parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                        except Exception as pe:
                            logger.warning(f"Final channel status post failed: {pe}")

async def queue_worker():
    while True:
        jid = await job_queue.get()
        try:
            await process_job(jid)
        except Exception as e:
            logger.error(f"Worker error on {jid}: {e}")
        finally:
            job_queue.task_done()

async def ensure_worker():
    global worker_task, worker_ready
    if worker_ready: return
    worker_ready = True
    db_execute("UPDATE jobs SET status='QUEUED' WHERE status IN ('DOWNLOADING','UPLOADING')")
    rows = db_execute("SELECT job_id FROM jobs WHERE status='QUEUED' ORDER BY created", fetch=True) or []
    for r in rows:
        await job_queue.put(r["job_id"])
    worker_task = asyncio.create_task(queue_worker())

# =========================================================
# INTERACTIVE /START MENU & WIZARD FLOW
# =========================================================
def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📦 Upload Batch (.txt)", callback_data="wiz:newbatch"),
         InlineKeyboardButton("🔄 Update Old Batch", callback_data="wiz:updbatch")],
        [InlineKeyboardButton("🔗 Single Link Upload", callback_data="wiz:single"),
         InlineKeyboardButton("🧹 Clean Disk", callback_data="menu:cleandisk")],
        [InlineKeyboardButton("📊 Active Status", callback_data="menu:status"),
         InlineKeyboardButton("📤 Saved Destination", callback_data="menu:dest")]
    ])

@app.on_message(filters.command("start") & filters.private)
async def start_handler(client, message):
    uid = message.from_user.id
    add_user(uid)
    await ensure_worker()
    wizard_sessions.pop(uid, None)
    dest = get_user_destination(uid)
    await message.reply_text(
        "👻 <b>PRIME GHOST DOWNLOADER v7 PRO</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "👑 <b>Owner-Only Control Panel</b>\n\n"
        "• <b>Upload Batch:</b> Full `.txt` batch with Topic/Chapter auto-setup\n"
        "• <b>Update Old Batch:</b> Give Old `.txt` + New `.txt` → uploads only new classes!\n"
        "• <b>Failed Fallback:</b> Dead links auto-post their Stream URL with Index & Title\n\n"
        f"📤 <b>Default Destination:</b> <code>{h(dest['title'])}</code>",
        reply_markup=main_menu_keyboard(),
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.private & filters.document)
async def document_wizard_handler(client, message):
    uid = message.from_user.id
    await ensure_worker()
    doc = message.document
    sess = wizard_sessions.get(uid)
    if not sess or sess.get("step") not in ("wait_new_txt", "wait_old_txt"):
        return await message.reply_text("⚠️ Pehle /start dabao aur Upload Batch ya Update Old Batch button select karo. Random files accept nahi hongi.")
    if not (doc.file_name or "").lower().endswith(".txt"):
        return await message.reply_text("❌ Sirf `.txt` file bhej bhai.")

    temp_path = os.path.join(DOWNLOAD_DIR, f"txt_{uid}_{uuid.uuid4().hex[:8]}.txt")
    try:
        await client.download_media(message, file_name=temp_path)
        with open(temp_path, "rb") as f:
            text_content = f.read().decode("utf-8-sig", errors="replace")
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

    records, errors = parse_txt(text_content)
    if not records:
        return await message.reply_text("❌ Is `.txt` mein koi valid records nahi mile.")

    sess = wizard_sessions.get(uid)
    if not sess:
        return await message.reply_text("⚠️ Upload session nahi mila. /start se Upload Batch button select karo.")

    # Step 1 of Update Old Batch: Receive OLD txt
    if sess.get("mode") == "updbatch" and sess.get("step") == "wait_old_txt":
        old_urls = {r["url"].strip() for r in records}
        sess["old_urls"] = old_urls
        sess["old_count"] = len(old_urls)
        sess["step"] = "wait_new_txt"
        wizard_sessions[uid] = sess
        return await message.reply_text(
            f"✅ <b>Old Batch TXT Saved!</b>\n"
            f"📦 Old Links Found: <code>{len(old_urls)}</code>\n\n"
            "👉 <b>Ab apni NEW (Updated) `.txt` file bhej:</b>",
            parse_mode=ParseMode.HTML
        )

    # Deduplicate within new TXT & filter out old URLs if in Update Mode
    old_urls = sess.get("old_urls", set())
    seen = set()
    final_records = []
    dup_count = 0
    old_skipped = 0

    for i, r in enumerate(records, 1):
        if not r.get("index"):
            r["index"] = f"{i:03d}"
        u = r["url"].strip()
        if u in seen:
            dup_count += 1
            continue
        seen.add(u)
        if u in old_urls:
            old_skipped += 1
            continue
        final_records.append(r)

    if not final_records:
        wizard_sessions.pop(uid, None)
        return await message.reply_text(
            "⚠️ <b>Koi nayi class nahi bachi upload karne ke liye!</b>\n\n"
            f"📄 Total in TXT: <code>{len(records)}</code>\n"
            f"⏭ Old Batch Skipped: <code>{old_skipped}</code>\n"
            f"♻️ Duplicates: <code>{dup_count}</code>",
            parse_mode=ParseMode.HTML
        )

    # Keep the parsed TXT in memory and ask which index range to upload.
    # The user can answer with /dl all, /dl 1 99, or shorthand /66 999.
    sess["pending_records"] = final_records
    sess["old_skipped"] = old_skipped
    sess["dup_count"] = dup_count
    sess["step"] = "wait_range"
    wizard_sessions[uid] = sess

    await message.reply_text(
        "📄 <b>TXT FILE ANALYSED</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📦 Total valid files: <code>{len(final_records)}</code>\n"
        f"⏭ Old batch links skipped: <code>{old_skipped}</code>\n"
        f"♻️ Duplicate links skipped: <code>{dup_count}</code>\n\n"
        "Ab batao kis index range ko upload karna hai:\n\n"
        "• <code>/dl all</code>  → poori TXT\n"
        "• <code>/dl 1 99</code> → index 1 se 99\n"
        "• <code>/66 999</code>  → index 66 se 999\n\n"
        "Range TXT ke <b>Index</b> ke basis par select hogi. Upload abhi start nahi hua.",
        parse_mode=ParseMode.HTML
    )

async def prepare_selected_range(message, uid, start_index=None, end_index=None):
    sess = wizard_sessions.get(uid)
    if not sess or sess.get("step") != "wait_range":
        return await message.reply_text("Pehle .txt file bhejo. Range selection abhi active nahi hai.")

    records = sess.get("pending_records") or []
    if not records:
        wizard_sessions.pop(uid, None)
        return await message.reply_text("❌ Pending TXT records nahi mile. TXT dobara bhejo.")

    if start_index is None and end_index is None:
        selected = list(records)
        range_label = "ALL"
    else:
        if start_index is None or end_index is None or start_index < 1 or end_index < start_index:
            return await message.reply_text("❌ Invalid range. Example: <code>/dl 1 99</code>", parse_mode=ParseMode.HTML)

        selected = []
        for position, record in enumerate(records, 1):
            raw_index = str(record.get("index") or "").strip()
            match = re.search(r"\d+", raw_index)
            # If TXT index is missing/non-numeric, use its parsed line order.
            index_value = int(match.group()) if match else position
            if start_index <= index_value <= end_index:
                selected.append(record)
        range_label = f"{start_index}-{end_index}"

    if not selected:
        return await message.reply_text(
            f"❌ Range <code>{range_label}</code> mein koi file nahi mili. TXT ke Index values check karo.",
            parse_mode=ParseMode.HTML
        )

    # Calculate counts and chapter first-class pin flags only for selected records.
    counts = {"video": 0, "pdf": 0, "html": 0, "other": 0}
    subjects = set()
    seen_chapters_per_subj = set()
    for record in selected:
        ftype = record.get("file_type", "other")
        counts[ftype if ftype in counts else "other"] += 1
        subjects.add(record.get("topic") or "General")
        chapter_key = ((record.get("topic") or "General"), (record.get("chapter") or "").lower())
        if ftype == "video" and chapter_key not in seen_chapters_per_subj:
            seen_chapters_per_subj.add(chapter_key)
            record["pin_chapter"] = 1
        else:
            record["pin_chapter"] = 0

    sess["records"] = selected
    sess["counts"] = counts
    sess["subjects"] = sorted(subjects)
    sess["selected_range"] = range_label
    sess["step"] = "choose_dest"
    wizard_sessions[uid] = sess

    saved_dest = get_user_destination(uid)
    kb_rows = [
        [InlineKeyboardButton("📩 Send in Personal DM", callback_data="wdest:dm")],
        [InlineKeyboardButton("📢 Enter Group / Channel ID", callback_data="wdest:askid")]
    ]
    if saved_dest["chat_id"]:
        kb_rows.insert(0, [InlineKeyboardButton(
            f"✅ Use Saved: {saved_dest['title'][:28]}", callback_data="wdest:saved"
        )])
    kb_rows.append([InlineKeyboardButton("❌ Cancel", callback_data="wiz:cancel")])

    await message.reply_text(
        "📊 <b>SELECTED RANGE REPORT</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🔢 <b>Selected index:</b> <code>{range_label}</code>\n"
        f"📦 <b>Files to upload:</b> <code>{len(selected)}</code>\n"
        f"🎬 Videos: <code>{counts['video']}</code>\n"
        f"📄 PDFs: <code>{counts['pdf']}</code>\n"
        f"🌐 HTML: <code>{counts['html']}</code>\n"
        f"📁 Others: <code>{counts['other']}</code>\n"
        f"🗂 Subjects: <code>{h(', '.join(subjects)[:700])}</code>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "👇 <b>Ab destination select karo:</b>",
        reply_markup=InlineKeyboardMarkup(kb_rows),
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.command("dl") & filters.private)
async def download_range_command(client, message):
    parts = (message.text or "").split()
    if len(parts) == 2 and parts[1].lower() == "all":
        return await prepare_selected_range(message, message.from_user.id)
    if len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
        return await prepare_selected_range(
            message, message.from_user.id, int(parts[1]), int(parts[2])
        )
    return await message.reply_text(
        "Usage:\n<code>/dl all</code>\n<code>/dl 1 99</code>",
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.regex(r"^/\d+(?:@[A-Za-z0-9_]+)?\s+\d+\s*$") & filters.private)
async def shorthand_download_range_command(client, message):
    parts = (message.text or "").split()
    if len(parts) != 2:
        return
    first = re.match(r"^/(\d+)", parts[0])
    if not first or not parts[1].isdigit():
        return
    return await prepare_selected_range(
        message, message.from_user.id, int(first.group(1)), int(parts[1])
    )

async def prompt_topic_mode(message_or_query, uid):
    sess = wizard_sessions.get(uid)
    if not sess: return
    dest = sess["dest"]
    # Do not trust Chat.is_forum alone: some Pyrogram/Pyrofork builds return
    # an incomplete Chat object even when Telegram has already created topics.
    # The actual CreateForumTopic API call is the authoritative test.
    if dest["chat_type"] in ("dm", "channel"):
        sess["topic_mode"] = "none"
        return await prompt_final_confirm(message_or_query, uid)

    # Show topic modes for every saved group. If it is not a forum, or the bot
    # lacks Manage Topics, get_or_create_forum_topic will return Telegram's
    # actual RPC error instead of falsely claiming Topics are disabled here.

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📚 Subject-Wise Topics + Chapter Auto-Pin", callback_data="wtopic:subject")],
        [InlineKeyboardButton("📖 Chapter-Wise Topics (Separate)", callback_data="wtopic:chapter")],
        [InlineKeyboardButton("📋 Normal Line-by-Line (No Topics)", callback_data="wtopic:none")],
        [InlineKeyboardButton("❌ Cancel", callback_data="wiz:cancel")]
    ])
    text = (
        f"✅ <b>Destination Set:</b> <code>{h(dest['title'])}</code>\n\n"
        "🗂 <b>Group Upload Mode Chuno:</b>\n"
        "• <b>Subject-Wise + Chapter Pin:</b> Bot har Subject (Arithmetic, Grammar, etc.) ka Topic banayega aur har naye Chapter ki 1st Class ko auto-pin karega.\n"
        "• <b>Chapter-Wise:</b> Har chapter ka alag topic banega.\n"
        "• <b>Normal Line-by-Line:</b> Bina topic ke seedha line se jayega."
    )
    if hasattr(message_or_query, "edit_text"):
        await message_or_query.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        await message_or_query.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)

async def prompt_final_confirm(message_or_query, uid):
    sess = wizard_sessions.get(uid)
    if not sess: return
    dest = sess["dest"]
    counts = sess["counts"]
    mode_labels = {
        "subject": "📚 Subject-Wise Topics + Chapter Auto-Pin",
        "chapter": "📖 Chapter-Wise Topics",
        "none":    "📋 Line-by-Line (Normal)"
    }
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ CONFIRM & START UPLOAD", callback_data="wstart:go")],
        [InlineKeyboardButton("❌ Cancel", callback_data="wiz:cancel")]
    ])
    text = (
        "🚀 <b>READY TO START BATCH</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📤 <b>Destination:</b> <code>{h(dest['title'])}</code> (<code>{dest['chat_id'] or 'DM'}</code>)\n"
        f"🗂 <b>Upload Mode:</b> {mode_labels.get(sess.get('topic_mode','none'))}\n"
        f"📦 <b>Total Files:</b> <code>{len(sess['records'])}</code> "
        f"(🎬 {counts['video']} | 📄 {counts['pdf']})\n"
        f"🔢 <b>TXT Index Range:</b> <code>{h(sess.get('selected_range', 'ALL'))}</code>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Confirm karte hi bot upload shuru kar dega:"
    )
    if hasattr(message_or_query, "edit_text"):
        await message_or_query.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        await message_or_query.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)

@app.on_message(
    filters.private & filters.text &
    ~filters.command(["start", "help", "status", "queue", "cancel", "retry",
                      "setchannel", "setgroup", "setdestination", "channel", "unsetchannel", "setdm", "single", "cleandisk", "ping", "health", "dl",
                      "setfooter", "footer", "clearfooter"]) &
    ~filters.regex(r"^/\d+(?:@[A-Za-z0-9_]+)?\s+\d+\s*$")
)
async def text_and_wizard_input_handler(client, message):
    uid = message.from_user.id
    await ensure_worker()
    raw = message.text.strip()
    # Slash commands (including /dl and /66 999) belong to their own handlers.
    if raw.startswith("/"):
        return
    sess = wizard_sessions.get(uid)

    # If wizard is waiting for Channel/Group ID
    if sess and sess.get("step") == "wait_dest_id":
        chat_input = raw if raw.startswith("@") else (int(raw) if raw.lstrip("-").isdigit() else raw)
        try:
            chat = await validate_channel(client, chat_input)
        except ValueError as e:
            return await message.reply_text(
                f"❌ <b>Validation Failed:</b>\n{h(str(e))}\n\nDobara sahi Group/Channel ID bhej:",
                parse_mode=ParseMode.HTML
            )
        title = chat.title or chat.username or str(chat.id)
        dest_type = "channel" if chat.type == ChatType.CHANNEL else "group"
        set_user_destination(uid, chat.id, dest_type, title)
        sess["dest"] = {"chat_id": str(chat.id), "chat_type": dest_type, "title": title, "is_forum": bool(getattr(chat, "is_forum", False))}
        sess["step"] = "choose_topic"
        return await prompt_topic_mode(message, uid)

    # A URL is accepted only after the user explicitly presses the Single Link button
    # or sends /single. Arbitrary chat text must never create a job.
    if not sess or sess.get("step") != "wait_single_link":
        return await message.reply_text("⚠️ Pehle /start menu se Upload Batch ya Single Link button select karo. Random text/link accept nahi hoga.")
    if not raw.startswith(("http://", "https://")):
        return await message.reply_text("❌ Single Link mode active hai. Valid HTTP/HTTPS URL bhejo, ya /start se cancel karo.")
    wizard_sessions.pop(uid, None)

    url = encode_url(raw)
    dest = get_user_destination(uid)
    ftype = classify_url(url)
    title = os.path.basename(unquote(urlparse(url).path)) or "Single Media"
    filename = build_filename(title, ftype, url)

    bulk_id = uuid.uuid4().hex[:12]
    counts = {
        "total": 1,
        "videos": 1 if ftype == "video" else 0,
        "pdfs": 1 if ftype == "pdf" else 0,
        "htmls": 1 if ftype == "html" else 0,
        "others": 1 if ftype not in ("video", "pdf", "html") else 0,
    }
    db_execute(
        "INSERT INTO bulks(bulk_id, user_id, status, total, videos, pdfs, htmls, others, "
        "destination_chat_id, destination_type, topic_mode, created, updated) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (bulk_id, uid, "RUNNING", 1, counts["videos"], counts["pdfs"],
         counts["htmls"], counts["others"], dest["chat_id"], dest["chat_type"],
         "none", time.time(), time.time())
    )
    state = BulkState(bulk_id, uid, counts, dest["title"], topic_mode="none")
    state.single_mode = True
    state.destination_chat_id = dest["chat_id"]
    bulk_states[bulk_id] = state

    status_msg = await message.reply_text(
        "👻 <b>PRIME GHOST | SINGLE FILE</b>\\n"
        "━━━━━━━━━━━━━━━━━━\\n"
        "🔄 <b>QUEUED</b>\\n"
        f"📄 <b>File:</b> {h(title[:120])}\\n"
        f"📤 <b>Destination:</b> {h(dest['title'])}\\n\\n"
        "⏳ Waiting for worker...",
        parse_mode=ParseMode.HTML
    )
    state.status_message = status_msg

    caption = build_caption({
        "index": "1", "title": title, "topic": "General",
        "batch": "Single Link"
    })
    jid = make_job(
        uid, url, title, "direct" if is_direct_media_url(url) else "bestvideo*+bestaudio/best",
        bulk_id=bulk_id, file_index="1", file_type=ftype, topic="General",
        batch="Single Link", desired_name=filename, caption=caption,
        destination_chat_id=dest["chat_id"], destination_type=dest["chat_type"]
    )
    await update_bulk_status(bulk_id, force=True)
    await job_queue.put(jid)

# =========================================================
# CALLBACK HANDLERS (WIZARD + LIVE CONTROLS)
# =========================================================
@app.on_callback_query()
async def cb_handler(client, query):
    uid = query.from_user.id
    data = query.data or ""

    if data == "wiz:newbatch":
        wizard_sessions[uid] = {"mode": "newbatch", "step": "wait_new_txt"}
        await query.answer()
        return await query.message.edit_text(
            "📦 <b>NEW BATCH UPLOAD</b>\n\n👉 Apni `.txt` file yahan bhej:",
            parse_mode=ParseMode.HTML
        )

    if data == "wiz:updbatch":
        wizard_sessions[uid] = {"mode": "updbatch", "step": "wait_old_txt"}
        await query.answer()
        return await query.message.edit_text(
            "🔄 <b>UPDATE OLD BATCH (DIFF ENGINE)</b>\n\n"
            "1️⃣ Sabse pehle apni <b>OLD `.txt`</b> file bhej (jo pehle upload ho chuki hai):",
            parse_mode=ParseMode.HTML
        )

    if data == "wiz:single":
        wizard_sessions[uid] = {"mode": "single", "step": "wait_single_link"}
        await query.answer()
        return await query.message.edit_text(
            "🔗 <b>SINGLE UPLOAD</b>\n\n👉 Seedha apna Video ya PDF link paste kar:",
            parse_mode=ParseMode.HTML
        )

    if data == "wiz:cancel":
        wizard_sessions.pop(uid, None)
        await query.answer("Cancelled")
        return await query.message.edit_text("❌ Setup cancelled. Use /start to begin again.")

    if data.startswith("wdest:"):
        sess = wizard_sessions.get(uid)
        if not sess:
            return await query.answer("Session expired. Send /start", show_alert=True)
        choice = data.split(":", 1)[1]
        if choice == "dm":
            sess["dest"] = {"chat_id": None, "chat_type": "dm", "title": "Personal DM"}
            sess["topic_mode"] = "none"
            await query.answer()
            return await prompt_final_confirm(query.message, uid)
        elif choice == "saved":
            sess["dest"] = get_user_destination(uid)
            if sess["dest"].get("chat_id"):
                try:
                    saved_chat = await client.get_chat(sess["dest"]["chat_id"])
                    sess["dest"]["is_forum"] = bool(getattr(saved_chat, "is_forum", False))
                except Exception:
                    sess["dest"]["is_forum"] = False
            await query.answer()
            return await prompt_topic_mode(query.message, uid)
        elif choice == "askid":
            sess["step"] = "wait_dest_id"
            await query.answer()
            return await query.message.edit_text(
                "📢 <b>Apne Group ya Channel ka ID bhej</b>\n"
                "Example: <code>-1004406917280</code> ya <code>@mychannel</code>\n\n"
                "<i>(Note: Group mein Topic-wise dalna hai toh Group setting mein 'Topics' ON hona chahiye aur Bot ko 'Manage Topics' + 'Pin Messages' permission honi chahiye)</i>",
                parse_mode=ParseMode.HTML
            )

    if data.startswith("wtopic:"):
        sess = wizard_sessions.get(uid)
        if not sess:
            return await query.answer("Session expired.", show_alert=True)
        sess["topic_mode"] = data.split(":", 1)[1]
        await query.answer()
        return await prompt_final_confirm(query.message, uid)

    if data == "wstart:go":
        sess = wizard_sessions.pop(uid, None)
        if not sess or not sess.get("records"):
            return await query.answer("Nothing to start.", show_alert=True)
        await query.answer("Starting batch!")

        final = sess["records"]
        counts = sess["counts"]
        dest = sess["dest"]
        topic_mode = sess.get("topic_mode", "none")

        bulk_id = uuid.uuid4().hex[:12]
        db_execute(
            "INSERT INTO bulks(bulk_id, user_id, status, total, videos, pdfs, htmls, others, "
            "destination_chat_id, destination_type, topic_mode, created, updated) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (bulk_id, uid, "RUNNING", len(final), counts["video"], counts["pdf"],
             counts["html"], counts["other"], dest["chat_id"], dest["chat_type"],
             topic_mode, time.time(), time.time())
        )

        state = BulkState(bulk_id, uid, {
            "total": len(final), "videos": counts["video"], "pdfs": counts["pdf"],
            "htmls": counts["html"], "others": counts["other"]
        }, dest["title"], topic_mode=topic_mode)
        state.destination_chat_id = dest["chat_id"]
        state.status_message = query.message
        bulk_states[bulk_id] = state
        if dest.get("chat_id") is not None and dest.get("chat_type") != "dm":
            try:
                state.channel_status_message = await telegram_retry(lambda: send_to_target(
                    int(dest["chat_id"]), None, app.send_message,
                    f"⌛ <b>𝐀𝐔𝐓𝐎𝐌𝐀𝐓𝐄𝐃 𝐓𝐀𝐒𝐊 0.00%</b> ⌛\n\n"
                    f"📊 Total Tasks: <code>{len(final)}</code>\n"
                    f"✅ Completed: <code>0</code>\n"
                    f"⏳ Remaining: <code>{len(final)}</code>\n\n"
                    "⚡ <b>WHOLE BATCH STATUS</b> ⚡\n"
                    f"🎬 Destination: <b>{h(dest.get('title') or 'Batch Upload')}</b>\n\n"
                    f'<a href="{PRIME_GHOST_LINK}"><b>👻 𝐏𝐑𝐈𝐌𝐄 𝐆𝐇𝐎𝐒𝐓</b></a>',
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True), "Post batch status")
            except Exception as pe:
                logger.warning(f"Could not create channel progress message: {pe}")

        # IMPORTANT: Create every required forum topic BEFORE queueing the first upload.
        # Then post one editable status message as the latest/root status inside each topic.
        topic_threads = {}
        if dest.get("chat_id") and topic_mode in ("subject", "chapter"):
            try:
                for r in final:
                    subject = normalize_subject_name(r.get("topic") or "General")
                    topic_title = subject if topic_mode == "subject" else f"{subject} - {r.get('chapter') or r.get('title') or 'Chapter'}"
                    key = topic_title.lower().strip()
                    if key not in topic_threads:
                        topic_threads[key] = await get_or_create_forum_topic(dest["chat_id"], topic_title)
                # Create the editable status post in every topic before upload begins.
                for key, thread_id in topic_threads.items():
                    topic_title = next((
                        (normalize_subject_name(r.get("topic") or "General") if topic_mode == "subject" else
                         f"{normalize_subject_name(r.get('topic') or 'General')} - {r.get('chapter') or r.get('title') or 'Chapter'}")
                        for r in final
                        if (normalize_subject_name(r.get("topic") or "General") if topic_mode == "subject" else
                            f"{normalize_subject_name(r.get('topic') or 'General')} - {r.get('chapter') or r.get('title') or 'Chapter'}").lower().strip() == key
                    ), key)
                    topic_total = sum(1 for r in final if (
                        (normalize_subject_name(r.get("topic") or "General") if topic_mode == "subject" else
                         f"{normalize_subject_name(r.get('topic') or 'General')} - {r.get('chapter') or r.get('title') or 'Chapter'}").lower().strip() == key
                    ))
                    status_msg = await telegram_retry(lambda: send_to_target(
                        int(dest["chat_id"]), thread_id, app.send_message,
                        f"⌛ <b>TOPIC PROGRESS 0.0%</b>\n\n📊 Total Files: <code>{topic_total}</code>\n"
                        f"✅ Completed: <code>0</code>\n⏳ Remaining: <code>{topic_total}</code>\n\n"
                        f"🗂 Subject: <b>{h(topic_title)}</b>\n🎬 Current: <b>Waiting to start</b>\n"
                        "━━━━━━━━━━━━━━━━━━\n"
                        f'<a href="{PRIME_GHOST_LINK}"><b>👻 𝐏𝐑𝐈𝐌𝐄 𝐆𝐇𝐎𝐒𝐓</b></a>',
                        parse_mode=ParseMode.HTML, disable_web_page_preview=True
                    ), f"Post status in topic {topic_title}")
                    state.topic_status_messages[int(thread_id)] = status_msg
                    await asyncio.sleep(TOPIC_CREATE_GAP)
            except Exception as topic_error:
                db_execute("UPDATE bulks SET status='FAILED', updated=? WHERE bulk_id=?", (time.time(), bulk_id))
                await query.message.reply_text(
                    f"❌ <b>Batch start nahi hua.</b>\nTopic setup fail hua: <code>{h(str(topic_error)[:500])}</code>\n"
                    "Koi file upload queue nahi ki gayi.", parse_mode=ParseMode.HTML
                )
                state.current_phase = "❌ Topic setup paused; no jobs queued"
                state.status_message = query.message
                bulk_states[bulk_id] = state
                await update_bulk_status(bulk_id, force=True)
                return

        queued_job_ids = []
        for r in final:
            caption = build_caption(r)
            filename = build_filename(r["title"], r["file_type"], r["url"])
            jid = make_job(
                uid, r["url"], r["title"],
                "direct" if is_direct_media_url(r["url"]) else "bestvideo*+bestaudio/best",
                bulk_id=bulk_id, file_index=r["index"], file_type=r["file_type"],
                topic=r["topic"], chapter=r["chapter"], pin_chapter=r.get("pin_chapter", 0),
                batch=r["batch"], desired_name=filename, caption=caption,
                destination_chat_id=dest["chat_id"], destination_type=dest["chat_type"]
            )
            if dest.get("chat_id") and topic_mode in ("subject", "chapter"):
                subject = normalize_subject_name(r.get("topic") or "General")
                topic_title = subject if topic_mode == "subject" else f"{subject} - {r.get('chapter') or r.get('title') or 'Chapter'}"
                thread_id = topic_threads.get(topic_title.lower().strip())
                if thread_id:
                    db_execute("UPDATE jobs SET thread_id=? WHERE job_id=?", (int(thread_id), jid))
            queued_job_ids.append(jid)

        # All jobs and topic mappings are persisted before the worker receives any job.
        for queued_jid in queued_job_ids:
            await job_queue.put(queued_jid)

        await update_bulk_status(bulk_id, force=True)
        return

    # LIVE BATCH CONTROLS: PAUSE / RESUME / SKIP / CANCEL
    if data.startswith("bulkpause:"):
        bulk_id = data.split(":", 1)[1]
        state = bulk_states.get(bulk_id)
        if not state: return await query.answer("Batch not active.", show_alert=True)
        state.paused = True
        state.pause_event.clear()
        state.current_phase = "⏸ Paused by Owner"
        await query.answer("⏸ Batch Paused!")
        return await update_bulk_status(bulk_id, force=True)

    if data.startswith("bulkresume:"):
        bulk_id = data.split(":", 1)[1]
        state = bulk_states.get(bulk_id)
        if not state: return await query.answer("Batch not active.", show_alert=True)
        state.paused = False
        state.pause_event.set()
        state.current_phase = "▶️ Resumed"
        await query.answer("▶️ Batch Resumed!")
        return await update_bulk_status(bulk_id, force=True)

    if data.startswith("bulkskip:"):
        bulk_id = data.split(":", 1)[1]
        state = bulk_states.get(bulk_id)
        if not state or not state.current_jid:
            return await query.answer("No active file to skip.", show_alert=True)
        ev = cancel_events.get(state.current_jid)
        if ev: ev.set()
        await query.answer("⏭ Skipping current file...")
        return

    if data.startswith("bulkcancel:"):
        bulk_id = data.split(":", 1)[1]
        state = bulk_states.get(bulk_id)
        if state:
            state.cancelled = True
            state.pause_event.set()
            state.current_phase = "🛑 Batch Cancelled"
        db_execute("UPDATE jobs SET status='CANCELLED', finished=? WHERE bulk_id=? AND status='QUEUED'",
                   (time.time(), bulk_id))
        if state and state.current_jid and cancel_events.get(state.current_jid):
            cancel_events[state.current_jid].set()
        await query.answer("🛑 Batch cancellation requested. Active file will stop and clean itself.", show_alert=True)
        return await update_bulk_status(bulk_id, force=True)

    if data == "menu:cleandisk":
        n = clean_all_downloads()
        return await query.answer(f"🧹 Cleaned {n} temporary files/folders from disk!", show_alert=True)

    if data == "menu:dest":
        dest = get_user_destination(uid)
        return await query.answer(f"Saved: {dest['title']} ({dest['chat_id'] or 'DM'})", show_alert=True)

    if data == "menu:status":
        rows = db_execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE status IN ('QUEUED','DOWNLOADING','UPLOADING')",
            one=True
        )
        return await query.answer(f"Active/Pending Jobs in Queue: {rows['n'] if rows else 0}", show_alert=True)

@app.on_message(filters.command(["setchannel", "setgroup", "setdestination"]) & filters.private)
async def setchannel_cmd(client, message):
    uid = message.from_user.id
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: <code>/setchannel -1001234567890</code>", parse_mode=ParseMode.HTML)
    arg = parts[1].strip()
    chat_input = arg if arg.startswith("@") else (int(arg) if arg.lstrip("-").isdigit() else arg)
    try:
        chat = await validate_channel(client, chat_input)
    except ValueError as e:
        return await message.reply_text(f"❌ <b>Validation failed</b>\n\n{h(str(e))}", parse_mode=ParseMode.HTML)
    title = chat.title or chat.username or str(chat.id)
    dest_type = "channel" if chat.type == ChatType.CHANNEL else "group"
    set_user_destination(uid, chat.id, dest_type, title)
    await message.reply_text(
        f"✅ <b>Destination set to {dest_type.title()}</b>\n"
        f"📛 Name: <b>{h(title)}</b>\n"
        f"🆔 ID: <code>{chat.id}</code>",
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.command(["help"]) & filters.private)
async def help_cmd(client, message):
    await message.reply_text(
        "👻 <b>PRIME GHOST COMMANDS</b>\n━━━━━━━━━━━━━━━━━━\n"
        "/start - Main menu\n"
        "/status - Active batch/job status\n"
        "/queue - Pending and running jobs\n"
        "/setchannel ID - Set group/channel\n"
        "/unsetchannel - Remove saved group/channel\n"
        "/setdm - Send future single links to your DM\n"
        "/single - Start single-link mode (then paste link)\n"
        "/setfooter - Set 1-3 custom caption footer lines\n"
        "/footer - Show current caption footer\n"
        "/clearfooter - Restore default footer\n"
        "/cleandisk - Clean temporary files\n"
        "/ping - Bot health\n\n"
        "Single link: /single ke baad link paste karo. Destination DM chahiye to pehle /setdm.",
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.command("setfooter") & filters.private)
async def setfooter_cmd(client, message):
    raw = message.text or ""
    # Supports both: /setfooter TEXT and /setfooter followed by 1-3 lines.
    payload = raw.partition(" ")[2].strip()
    if "\n" in raw:
        payload = raw.split("\n", 1)[1].strip()
    if not payload:
        return await message.reply_text(
            "Footer set karne ke liye command ke baad 1-3 lines likho.\n\n"
            "Example: <code>/setfooter 𝐄𝐗𝐓𝐑𝐀𝐂𝐓𝐄𝐃 𝐁𝐘 : @INFINITYxPRIME</code>\n"
            "Multiple lines bhi allowed hain.", parse_mode=ParseMode.HTML)
    values = [line.strip() for line in payload.splitlines() if line.strip()]
    if len(values) > 3:
        return await message.reply_text("❌ Maximum 3 footer lines allowed.")
    db_execute(
        "INSERT OR REPLACE INTO bot_settings(setting_key,setting_value,updated_at) VALUES('extracted_by',?,?)",
        (json.dumps(values, ensure_ascii=False), time.time())
    )
    preview = "\n".join(html.escape(x) for x in values)
    await message.reply_text("✅ <b>Caption footer updated:</b>\n\n" + preview, parse_mode=ParseMode.HTML)

@app.on_message(filters.command("footer") & filters.private)
async def footer_cmd(client, message):
    values = get_extracted_by_lines()
    await message.reply_text("📝 <b>Current caption footer:</b>\n\n" + "\n".join(html.escape(x) for x in values), parse_mode=ParseMode.HTML)

@app.on_message(filters.command("clearfooter") & filters.private)
async def clearfooter_cmd(client, message):
    db_execute("DELETE FROM bot_settings WHERE setting_key='extracted_by'")
    await message.reply_text("✅ Custom footer cleared. Default footer restored.")

@app.on_message(filters.command("status") & filters.private)
async def status_cmd(client, message):
    rows = db_execute("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status", fetch=True) or []
    counts = {r["status"]: r["n"] for r in rows}
    active = counts.get("DOWNLOADING", 0) + counts.get("UPLOADING", 0)
    queued = counts.get("QUEUED", 0)
    dest = get_user_destination(message.from_user.id)
    await message.reply_text(
        "📊 <b>BOT STATUS</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ Active: <code>{active}</code>\n"
        f"⏳ Queued: <code>{queued}</code>\n"
        f"✅ Done: <code>{counts.get('SUCCESS', 0)}</code>\n"
        f"❌ Failed: <code>{counts.get('FAILED', 0)}</code>\n"
        f"📤 Destination: <b>{h(dest['title'])}</b>",
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.command("queue") & filters.private)
async def queue_cmd(client, message):
    rows = db_execute(
        "SELECT job_id, title, status FROM jobs WHERE status IN ('QUEUED','DOWNLOADING','UPLOADING') ORDER BY created LIMIT 15",
        fetch=True
    ) or []
    if not rows:
        return await message.reply_text("✅ Queue empty hai.")
    lines = [f"{i}. <b>{h((r['title'] or 'Untitled')[:65])}</b> · <code>{r['status']}</code>" for i, r in enumerate(rows, 1)]
    await message.reply_text("📋 <b>ACTIVE QUEUE</b>\n━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines), parse_mode=ParseMode.HTML)

@app.on_message(filters.command("channel") & filters.private)
async def channel_cmd(client, message):
    dest = get_user_destination(message.from_user.id)
    await message.reply_text(
        f"📤 <b>Current destination:</b> {h(dest['title'])}\n"
        f"🆔 <code>{dest['chat_id'] or 'Personal DM'}</code>",
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.command("unsetchannel") & filters.private)
async def unsetchannel_cmd(client, message):
    clear_user_destination(message.from_user.id)
    await message.reply_text("✅ Saved destination removed. Future uploads default to Personal DM.")

@app.on_message(filters.command("setdm") & filters.private)
async def setdm_cmd(client, message):
    clear_user_destination(message.from_user.id)
    wizard_sessions.pop(message.from_user.id, None)
    await message.reply_text("✅ Default destination set to Personal DM. Ab single link paste karoge to bot yahin reply mein upload karega.")

@app.on_message(filters.command("single") & filters.private)
async def single_cmd(client, message):
    wizard_sessions[message.from_user.id] = {"mode": "single", "step": "wait_single_link"}
    await message.reply_text("🔗 <b>Single-link mode ready.</b> Ab ek HTTP/HTTPS Video/PDF link bhejo. Random messages accept nahi honge. DM destination ke liye /setdm use karo.", parse_mode=ParseMode.HTML)

@app.on_message(filters.command("cleandisk") & filters.private)
async def cleandisk_cmd(client, message):
    n = clean_all_downloads()
    await message.reply_text(f"🧹 Cleaned <code>{n}</code> temporary items from disk.", parse_mode=ParseMode.HTML)

@app.on_message(filters.command(["ping", "health"]) & filters.private)
async def health_handler(client, message):
    await message.reply_text(
        "✅ <b>PRIME GHOST DOWNLOADER ONLINE</b>\n"
        f"🤖 Bot: <code>{h((await client.get_me()).username or 'unknown')}</code>\n"
        f"👑 Owner ID: <code>{OWNER_ID}</code>\n"
        f"💾 Database: <code>{h(os.path.basename(DB_PATH))}</code>",
        parse_mode=ParseMode.HTML,
    )


# =========================================================
# RENDER WEB HEALTH CHECK
# Render Free Web Services can spin down after ~15 minutes
# without inbound HTTP traffic. Expose /health so an external
# uptime monitor can ping this service every 10-14 minutes.
# =========================================================
BOT_READY = False

async def render_health_handler(request):
    if not BOT_READY or not app.is_connected:
        return web.json_response(
            {"status": "starting", "bot": "not_ready"},
            status=503,
        )
    return web.json_response(
        {
            "status": "ok",
            "service": "PRIME GHOST DOWNLOADER",
            "bot": "online",
            "uptime_seconds": int(time.time() - BOT_STARTED_AT),
        },
        status=200,
    )

async def start_render_health_server():
    port = int(os.environ.get("PORT", "10000"))
    web_app = web.Application()
    web_app.router.add_get("/", render_health_handler)
    web_app.router.add_get("/health", render_health_handler)
    runner = web.AppRunner(web_app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("Render health endpoint listening on 0.0.0.0:%s (/health)", port)
    return runner

BOT_STARTED_AT = time.time()


async def main():
    global BOT_READY
    health_runner = None
    try:
        # Render requires a web process bound to its assigned PORT.
        health_runner = await start_render_health_server()
        await app.start()
        BOT_READY = True
        # Publish the full command menu in Telegram when the user types '/'.
        try:
            await app.set_bot_commands([
                BotCommand("start", "Open main menu"),
                BotCommand("help", "Show all commands"),
                BotCommand("status", "Show active batch and progress"),
                BotCommand("queue", "Show pending and running jobs"),
                BotCommand("setchannel", "Set group/channel destination"),
                BotCommand("channel", "Show current destination"),
                BotCommand("unsetchannel", "Remove saved destination"),
                BotCommand("setdm", "Set destination back to personal DM"),
                BotCommand("single", "Single link upload to current destination"),
                BotCommand("setfooter", "Set custom caption footer (1-3 lines)"),
                BotCommand("footer", "Show current caption footer"),
                BotCommand("clearfooter", "Restore default caption footer"),
                BotCommand("cleandisk", "Delete temporary download files"),
                BotCommand("ping", "Check bot health"),
                BotCommand("dl", "Select TXT upload range"),
            ])
        except Exception as command_error:
            logger.warning("Could not publish Telegram command menu: %s", command_error)
        me = await app.get_me()
        logger.info(
            "PRIME GHOST DOWNLOADER v7.8 SQLITE-ROW-PROGRESS-FIX STARTED | "
            "bot=@%s id=%s | owner_id=%s | db=%s | session=%s",
            me.username or "no_username", me.id, OWNER_ID, DB_PATH, SESSION_NAME
        )
        await ensure_worker()
        await idle()
    finally:
        BOT_READY = False
        if health_runner is not None:
            try:
                await health_runner.cleanup()
            except Exception as health_error:
                logger.warning("Render health server cleanup failed: %s", health_error)
        if worker_task and not worker_task.done():
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        if app.is_connected:
            await app.stop()

if __name__ == "__main__":
    app.run(main())
