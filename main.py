#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Telegram Bulk Forward Controller
Railway / Python 3.11+

Architecture:
- BOT_TOKEN: only for the control panel.
- Telethon User Account: performs the actual forwarding.
- Telegram MTProto messages.forwardMessages: one request per batch.
- No media download/upload.
- Only message IDs for the current batch are kept in memory.
- Batch size is discovered from Telegram's config forwarded_count_max when available.
- SQLite stores task state; message content/media are never stored.

Environment:
  BOT_TOKEN   required
  OWNER_ID    required
  API_ID      required
  API_HASH    required
  SESSION_STRING optional (login from panel if absent)
  DB_PATH     optional, default publisher.db
  MAX_TASKS   optional, default 1
  MAX_REPETITIONS optional, default 100
  BATCH_SIZE  optional override; otherwise Telegram config forwarded_count_max

Start:
  python main.py
"""

import asyncio
import base64
import logging
import os
import sqlite3
import time
from contextlib import suppress
from datetime import datetime, timezone
from typing import Optional

from telethon import TelegramClient, events, functions, types, utils, errors
from telethon.sessions import StringSession
from telethon.tl.types import InputPeerChannel, InputPeerChat, InputPeerUser

try:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
    from telegram.ext import (
        Application,
        CallbackQueryHandler,
        CommandHandler,
        ContextTypes,
        MessageHandler,
        filters,
    )
except ImportError as exc:
    raise RuntimeError("python-telegram-bot is required") from exc


# -------------------- Configuration --------------------

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or "0")
API_ID = int(os.getenv("API_ID", "0") or "0")
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()

DB_PATH = os.getenv("DB_PATH", "publisher.db")
MAX_TASKS = max(1, int(os.getenv("MAX_TASKS", "1")))
MAX_REPETITIONS = max(1, int(os.getenv("MAX_REPETITIONS", "100")))

# If set, this is an explicit override. Otherwise Telegram's config value is used.
BATCH_SIZE_OVERRIDE = int(os.getenv("BATCH_SIZE", "0") or "0")

# A conservative fallback only if Telegram config cannot be read.
# This is NOT claimed to be Telegram's hard limit.
BATCH_FALLBACK = 100

if not BOT_TOKEN or not OWNER_ID or not API_ID or not API_HASH:
    raise RuntimeError("Missing BOT_TOKEN / OWNER_ID / API_ID / API_HASH")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# -------------------- SQLite --------------------

DB = sqlite3.connect(DB_PATH, check_same_thread=False)
DB.row_factory = sqlite3.Row
DB.execute("PRAGMA journal_mode=WAL")
DB.execute("PRAGMA synchronous=NORMAL")

DB.executescript(
    """
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_chat_id INTEGER NOT NULL,
        target_chat_id INTEGER NOT NULL,
        start_message_id INTEGER NOT NULL,
        end_message_id INTEGER NOT NULL,
        message_count INTEGER NOT NULL,
        repeat_count INTEGER NOT NULL,
        current_repeat INTEGER NOT NULL DEFAULT 1,
        last_message_id INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'pending',
        batch_size INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        error TEXT
    );
    """
)
DB.commit()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def setting_get(key: str, default: str = "") -> str:
    row = DB.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def setting_set(key: str, value: str) -> None:
    DB.execute(
        "INSERT INTO settings(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    DB.commit()


def save_task(task_id: int, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = now_iso()
    cols = ", ".join(f"{k}=?" for k in fields)
    vals = list(fields.values()) + [task_id]
    DB.execute(f"UPDATE tasks SET {cols} WHERE id=?", vals)
    DB.commit()


def get_task(task_id: int):
    return DB.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()


def get_running_task():
    return DB.execute(
        "SELECT * FROM tasks WHERE status IN ('pending','running','waiting') "
        "ORDER BY id LIMIT 1"
    ).fetchone()


# -------------------- Runtime state --------------------

client = TelegramClient(
    StringSession(SESSION_STRING),
    API_ID,
    API_HASH,
    device_model="Bulk Forward Controller",
    system_version="1.0",
    app_version="1.0",
    lang_code="ar",
    # This client only ever issues requests (forwardMessages, get_messages,
    # get_entity, ...); it never reacts to incoming updates. Disabling
    # update reception removes a steady stream of background processing
    # Telethon would otherwise do on every update, freeing the loop for
    # the actual forwarding work.
    receive_updates=False,
)

bot_app: Optional[Application] = None
worker_task: Optional[asyncio.Task] = None
stop_event = asyncio.Event()
control_lock = asyncio.Lock()

# Values selected through the bot panel.
selected_source: Optional[dict] = None
selected_target: Optional[dict] = None
selected_repeat: int = 1

# Telegram's runtime config.
telegram_forward_limit = BATCH_FALLBACK

# Login state. The phone/code/password values are kept only in RAM while logging in.
login_state = {}


# -------------------- Telegram / peer helpers --------------------

async def discover_forward_limit() -> int:
    """
    Telegram publishes forwarded_count_max in the client config.
    It is the authoritative runtime value to use when available.
    """
    if BATCH_SIZE_OVERRIDE > 0:
        return max(1, BATCH_SIZE_OVERRIDE)

    try:
        cfg = await client(functions.help.GetConfigRequest())
        value = getattr(cfg, "forwarded_count_max", None)
        if isinstance(value, int) and value > 0:
            return value
    except Exception as exc:
        logging.warning("Could not read forwarded_count_max: %s", exc)

    return BATCH_FALLBACK


async def resolve_peer(value: str):
    value = value.strip()
    if not value:
        raise ValueError("القيمة فارغة")

    # Telegram integer IDs may be entered directly.
    if re_integer(value):
        entity = await client.get_entity(int(value))
    else:
        entity = await client.get_entity(value)

    return entity


def re_integer(value: str) -> bool:
    if value.startswith("-"):
        return value[1:].isdigit()
    return value.isdigit()


async def ensure_access(entity):
    # get_input_entity verifies that Telethon can resolve the peer.
    return await client.get_input_entity(entity)


async def can_post(entity) -> bool:
    """
    For channels/supergroups, verify that the user can send messages.
    For ordinary private/group chats, successful entity resolution is enough.
    """
    if isinstance(entity, types.Channel):
        if entity.broadcast:
            try:
                perms = await client.get_permissions(entity, "me")
                return bool(getattr(perms, "post_messages", False))
            except Exception:
                return bool(getattr(entity, "creator", False))
        try:
            perms = await client.get_permissions(entity, "me")
            return bool(getattr(perms, "send_messages", True))
        except Exception:
            return True
    return True


async def peer_display(entity) -> str:
    username = getattr(entity, "username", None)
    title = getattr(entity, "title", None)
    if username:
        return f"@{username}"
    if title:
        return title
    return str(getattr(entity, "id", ""))


# -------------------- Message range discovery --------------------

async def get_source_range(entity):
    """
    Determine the first/last message IDs without loading the whole history.
    Only two message objects are requested.
    """
    # These three lookups are independent reads, so they are issued
    # concurrently instead of sequentially - this cuts the wall-clock time
    # of this call to roughly one round trip instead of three.
    newest, oldest, total = await asyncio.gather(
        client.get_messages(entity, limit=1),
        client.get_messages(entity, limit=1, reverse=True),
        client.get_messages(entity, limit=0),
    )

    if not newest or not oldest:
        raise ValueError("كروب التخزين لا يحتوي على رسائل")

    first_id = oldest[0].id
    last_id = newest[0].id
    if first_id > last_id:
        first_id, last_id = last_id, first_id

    # Count is returned by Telegram for get_messages(limit=0).
    count = int(getattr(total, "total", 0) or 0)

    return first_id, last_id, count


async def iter_message_id_batches(entity, start_id: int, end_id: int,
                                  batch_size: int, after_id: int = 0):
    """
    Yields only integer message IDs for the current batch.

    Telethon fetches the metadata necessary to enumerate the history, but
    this function never downloads media bytes. At most one current batch of
    Message objects is held before converting it to IDs.
    """
    cursor = max(start_id - 1, after_id)

    while cursor < end_id:
        ids = []
        async for msg in client.iter_messages(
            entity,
            min_id=cursor,
            max_id=end_id + 1,
            limit=batch_size,
            reverse=True,
        ):
            if msg.id <= cursor or msg.id > end_id:
                continue
            ids.append(msg.id)
            if len(ids) >= batch_size:
                break

        if not ids:
            break

        yield ids
        cursor = ids[-1]

        if len(ids) < batch_size:
            # There are no more messages in the requested range.
            break


# -------------------- Bulk MTProto forwarding --------------------

async def forward_batch(source, target, ids, drop_author=True):
    """
    One MTProto messages.forwardMessages request.

    No download_media/send_file path is used.
    drop_author=True removes the forwarded-author header where Telegram
    permits it.
    """
    if not ids:
        return

    random_ids = [utils.generate_random_long() for _ in ids]

    # Raw MTProto is used deliberately so send_as can be supplied.
    #
    # send_as=target asks Telegram to post using the target channel identity
    # when the logged-in account has that permission. If Telegram rejects
    # send_as, we surface the RPC error rather than silently changing behavior.
    request = functions.messages.ForwardMessagesRequest(
        from_peer=source,
        id=ids,
        random_id=random_ids,
        to_peer=target,
        drop_author=drop_author,
        send_as=target if isinstance(target, (types.InputPeerChannel, types.InputChannel)) else None,
    )
    await client(request)


# -------------------- Worker --------------------

async def worker(task_id: int):
    global telegram_forward_limit

    async with control_lock:
        row = get_task(task_id)
        if not row:
            return

        source = await client.get_input_entity(row["source_chat_id"])
        target = await client.get_input_entity(row["target_chat_id"])

        batch_size = int(row["batch_size"]) or telegram_forward_limit
        repeat_count = int(row["repeat_count"])

        save_task(task_id, status="running")

        try:
            for repeat in range(int(row["current_repeat"]), repeat_count + 1):
                if stop_event.is_set():
                    save_task(task_id, status="stopped", current_repeat=repeat)
                    return

                # On a fresh repeat start from the beginning.
                after_id = int(row["last_message_id"]) if (
                    repeat == int(row["current_repeat"])
                ) else 0

                save_task(
                    task_id,
                    current_repeat=repeat,
                    last_message_id=after_id,
                    status="running",
                )

                while True:
                    if stop_event.is_set():
                        save_task(
                            task_id,
                            status="stopped",
                            current_repeat=repeat,
                            last_message_id=after_id,
                        )
                        return

                    ids = []
                    async for batch in iter_message_id_batches(
                        source,
                        int(row["start_message_id"]),
                        int(row["end_message_id"]),
                        batch_size,
                        after_id=after_id,
                    ):
                        ids = batch
                        break

                    if not ids:
                        break

                    try:
                        await forward_batch(source, target, ids, drop_author=True)
                    except errors.FloodWaitError as exc:
                        wait_for = int(exc.seconds)
                        save_task(
                            task_id,
                            status="waiting",
                            current_repeat=repeat,
                            last_message_id=after_id,
                            error=f"FloodWait: {wait_for}s",
                        )
                        await notify_owner(
                            f"انتظار FloodWait: {wait_for} ثانية\n"
                            f"الدورة: {repeat}/{repeat_count}"
                        )

                        # Respect Telegram's server-provided wait. No bypass.
                        try:
                            await asyncio.wait_for(
                                stop_event.wait(), timeout=wait_for
                            )
                            save_task(task_id, status="stopped")
                            return
                        except asyncio.TimeoutError:
                            pass

                        save_task(
                            task_id,
                            status="running",
                            error="",
                        )
                        continue

                    except errors.RPCError as exc:
                        save_task(
                            task_id,
                            status="failed",
                            error=f"{type(exc).__name__}: {exc}",
                        )
                        await notify_owner(
                            f"فشلت العملية.\n"
                            f"الدورة: {repeat}/{repeat_count}\n"
                            f"الخطأ: {type(exc).__name__}: {exc}"
                        )
                        return

                    after_id = ids[-1]
                    save_task(
                        task_id,
                        status="running",
                        current_repeat=repeat,
                        last_message_id=after_id,
                        error="",
                    )

                # Repeat completed.
                save_task(
                    task_id,
                    current_repeat=repeat + 1,
                    last_message_id=0,
                    status="running",
                )

            save_task(
                task_id,
                current_repeat=repeat_count,
                last_message_id=0,
                status="completed",
                error="",
            )
            await notify_owner(
                f"اكتمل النشر الجماعي.\n"
                f"الدورات: {repeat_count}\n"
                f"حجم الدفعة: {batch_size}"
            )

        except asyncio.CancelledError:
            # Cancellation is only used during process shutdown.
            save_task(task_id, status="stopped")
            raise
        except Exception as exc:
            logging.exception("Worker failed")
            save_task(
                task_id,
                status="failed",
                error=f"{type(exc).__name__}: {exc}",
            )
            await notify_owner(f"توقفت المهمة بسبب خطأ: {type(exc).__name__}: {exc}")


# -------------------- Bot UI --------------------

def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID


def main_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("كروب التخزين", callback_data="source"),
                InlineKeyboardButton("تحديد الكل", callback_data="select_all"),
            ],
            [
                InlineKeyboardButton("القناة الهدف", callback_data="target"),
                InlineKeyboardButton("عدد مرات النشر", callback_data="repeat"),
            ],
            [
                InlineKeyboardButton("بدء النشر", callback_data="start"),
                InlineKeyboardButton("إيقاف النشر", callback_data="stop"),
            ],
            [
                InlineKeyboardButton("حالة المهمة", callback_data="status"),
                InlineKeyboardButton("استئناف", callback_data="resume"),
                InlineKeyboardButton("تسجيل الدخول", callback_data="login"),
            ],
        ]
    )


def status_text() -> str:
    task = get_running_task()
    if not task:
        return "لا توجد مهمة قيد التنفيذ."

    return (
        f"المهمة: #{task['id']}\n"
        f"الحالة: {task['status']}\n"
        f"الرسائل: {task['message_count']}\n"
        f"الدورات: {task['current_repeat']}/{task['repeat_count']}\n"
        f"آخر رسالة معالجة: {task['last_message_id']}\n"
        f"حجم الدفعة: {task['batch_size']}"
        + (f"\nالخطأ: {task['error']}" if task["error"] else "")
    )


async def notify_owner(text: str):
    if bot_app:
        with suppress(Exception):
            await bot_app.bot.send_message(OWNER_ID, text)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_owner(update.effective_user.id):
        return
    await update.message.reply_text(
        "لوحة التحكم\n\n"
        f"حجم دفعة MTProto الحالي: {telegram_forward_limit}",
        reply_markup=main_keyboard(),
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global selected_source, selected_target, selected_repeat

    query = update.callback_query
    if not query or not is_owner(query.from_user.id):
        return

    await query.answer()
    action = query.data

    if action == "source":
        context.user_data["awaiting"] = "source"
        await query.message.reply_text(
            "أرسل ID كروب التخزين أو @username."
        )
        return

    if action == "target":
        context.user_data["awaiting"] = "target"
        await query.message.reply_text(
            "أرسل @username أو ID للقناة/الكروب الهدف."
        )
        return

    if action == "repeat":
        context.user_data["awaiting"] = "repeat"
        await query.message.reply_text(
            f"أرسل عدد مرات النشر، من 1 إلى {MAX_REPETITIONS}."
        )
        return

    if action == "select_all":
        if not selected_source:
            await query.message.reply_text("حدد كروب التخزين أولًا.")
            return

        try:
            first_id, last_id, count = await get_source_range(selected_source["entity"])
            selected_source.update(
                first_id=first_id,
                last_id=last_id,
                count=count,
            )
            await query.message.reply_text(
                f"تم تحديد نطاق الرسائل.\n"
                f"العدد: {count}\n"
                f"من ID: {first_id}\n"
                f"إلى ID: {last_id}\n\n"
                "لم يتم تنزيل الوسائط أو تخزين محتوى الرسائل."
            )
        except Exception as exc:
            await query.message.reply_text(f"تعذر تحديد الرسائل: {exc}")
        return

    if action == "start":
        await start_job(query.message)
        return

    if action == "resume":
        await resume_job(query.message)
        return

    if action == "stop":
        stop_event.set()
        task = get_running_task()
        if task:
            await query.message.reply_text(
                "تم طلب الإيقاف. ستتوقف المهمة بعد انتهاء طلب MTProto الحالي."
            )
        else:
            await query.message.reply_text("لا توجد مهمة قيد التنفيذ.")
        return

    if action == "status":
        await query.message.reply_text(status_text(), reply_markup=main_keyboard())
        return

    if action == "login":
        await query.message.reply_text(
            "إذا كانت SESSION_STRING موجودة، فالحساب متصل بالفعل.\n"
            "لإنشاء جلسة جديدة أرسل /login."
        )
        return


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global selected_source, selected_target, selected_repeat

    if not update.effective_user or not is_owner(update.effective_user.id):
        return

    awaiting = context.user_data.get("awaiting")
    if not awaiting:
        return

    text = (update.message.text or "").strip()
    context.user_data.pop("awaiting", None)

    if awaiting == "repeat":
        if not text.isdigit() or not (1 <= int(text) <= MAX_REPETITIONS):
            await update.message.reply_text(
                f"القيمة يجب أن تكون بين 1 و{MAX_REPETITIONS}."
            )
            return
        selected_repeat = int(text)
        await update.message.reply_text(
            f"عدد مرات النشر: {selected_repeat}",
            reply_markup=main_keyboard(),
        )
        return

    if awaiting in ("source", "target"):
        try:
            entity = await resolve_peer(text)
            await ensure_access(entity)

            if awaiting == "source":
                selected_source = {"entity": entity, "chat_id": int(entity.id)}
                await update.message.reply_text(
                    f"تم تعيين كروب التخزين: {await peer_display(entity)}\n"
                    "اضغط «تحديد الكل» لتحديد نطاق الرسائل.",
                    reply_markup=main_keyboard(),
                )
            else:
                if not await can_post(entity):
                    await update.message.reply_text(
                        "الحساب لا يملك صلاحية النشر في هذا الهدف."
                    )
                    return

                selected_target = {"entity": entity, "chat_id": int(entity.id)}
                await update.message.reply_text(
                    f"تم تعيين الهدف: {await peer_display(entity)}",
                    reply_markup=main_keyboard(),
                )
        except Exception as exc:
            await update.message.reply_text(f"تعذر الوصول إلى الهدف: {exc}")


async def resume_job(message):
    global worker_task

    if get_running_task():
        await message.reply_text("هناك مهمة قيد التنفيذ.")
        return

    row = DB.execute(
        "SELECT * FROM tasks WHERE status='stopped' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        await message.reply_text("لا توجد مهمة متوقفة قابلة للاستئناف.")
        return

    # If the last batch completed a repeat, move to the next repeat.
    current_repeat = int(row["current_repeat"])
    last_message_id = int(row["last_message_id"])
    if current_repeat > int(row["repeat_count"]):
        await message.reply_text("هذه المهمة مكتملة بالفعل.")
        return

    save_task(row["id"], status="pending", current_repeat=current_repeat,
              last_message_id=last_message_id, error="")
    stop_event.clear()
    worker_task = asyncio.create_task(worker(row["id"]))
    await message.reply_text(
        f"تم استئناف المهمة #{row['id']}.\n"
        f"الدورة: {current_repeat}/{row['repeat_count']}\n"
        f"آخر رسالة معالجة: {last_message_id}"
    )


async def start_job(message):
    global worker_task

    if get_running_task():
        await message.reply_text("هناك مهمة قيد التنفيذ.")
        return

    if not selected_source or "first_id" not in selected_source:
        await message.reply_text("حدد كروب التخزين ثم اضغط «تحديد الكل».")
        return

    if not selected_target:
        await message.reply_text("حدد القناة/الكروب الهدف أولًا.")
        return

    if selected_source["chat_id"] == selected_target["chat_id"]:
        await message.reply_text("المصدر والهدف متطابقان.")
        return

    count = int(selected_source["count"])
    repeat = int(selected_repeat)

    DB.execute(
        """
        INSERT INTO tasks(
            source_chat_id,target_chat_id,start_message_id,end_message_id,
            message_count,repeat_count,current_repeat,last_message_id,
            status,batch_size,created_at,updated_at,error
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            selected_source["chat_id"],
            selected_target["chat_id"],
            selected_source["first_id"],
            selected_source["last_id"],
            count,
            repeat,
            1,
            0,
            "pending",
            telegram_forward_limit,
            now_iso(),
            now_iso(),
            "",
        ),
    )
    DB.commit()

    task_id = DB.execute("SELECT last_insert_rowid()").fetchone()[0]
    stop_event.clear()

    worker_task = asyncio.create_task(worker(task_id))

    await message.reply_text(
        f"بدأت المهمة #{task_id}.\n"
        f"الرسائل: {count}\n"
        f"الدورات: {repeat}\n"
        f"حجم الدفعة: {telegram_forward_limit}\n\n"
        "التنفيذ يستخدم MTProto bulk forward ولا ينزل الوسائط."
    )


# -------------------- Login --------------------

async def login_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_owner(update.effective_user.id):
        return

    if await client.is_user_authorized():
        await update.message.reply_text("جلسة المستخدم متصلة وصالحة.")
        return

    context.user_data["login_step"] = "phone"
    await update.message.reply_text(
        "أرسل رقم الهاتف بصيغة دولية.\n"
        "سيتم الاحتفاظ بالبيانات مؤقتًا أثناء تسجيل الدخول فقط."
    )


async def login_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_owner(update.effective_user.id):
        return

    step = context.user_data.get("login_step")
    if not step:
        return

    text = (update.message.text or "").strip()

    try:
        if step == "phone":
            sent = await client.send_code_request(text)
            context.user_data["phone"] = text
            context.user_data["phone_code_hash"] = sent.phone_code_hash
            context.user_data["login_step"] = "code"
            await update.message.reply_text("أرسل رمز تسجيل الدخول.")
            return

        if step == "code":
            phone = context.user_data["phone"]
            phone_code_hash = context.user_data["phone_code_hash"]
            try:
                await client.sign_in(phone, text, phone_code_hash=phone_code_hash)
            except errors.SessionPasswordNeededError:
                context.user_data["login_step"] = "password"
                await update.message.reply_text("أرسل كلمة مرور التحقق بخطوتين.")
                return

            await finish_login(update, context)
            return

        if step == "password":
            await client.sign_in(password=text)
            await finish_login(update, context)
            return

    except errors.PhoneCodeInvalidError:
        await update.message.reply_text("رمز تسجيل الدخول غير صحيح.")
    except errors.PhoneCodeExpiredError:
        context.user_data.clear()
        await update.message.reply_text("انتهت صلاحية الرمز. ابدأ /login من جديد.")
    except Exception as exc:
        context.user_data.clear()
        await update.message.reply_text(f"فشل تسجيل الدخول: {type(exc).__name__}: {exc}")


async def finish_login(update, context):
    me = await client.get_me()
    session = client.session.save()
    setting_set("session_string", session)
    context.user_data.clear()

    await update.message.reply_text(
        "تم تسجيل الدخول بنجاح.\n"
        f"الحساب: {getattr(me, 'first_name', '')}\n\n"
        "تم حفظ الجلسة في SQLite. للإنتاج يفضل وضعها أيضًا في SESSION_STRING."
    )


# -------------------- Recovery / startup --------------------

async def recover_after_restart():
    """
    A task that was running when Railway restarted cannot know whether the
    last request reached Telegram. Therefore it is marked stopped rather than
    blindly replaying the last batch and risking duplicates.
    """
    DB.execute(
        "UPDATE tasks SET status='stopped', error=? "
        "WHERE status IN ('running','waiting')",
        ("تم إيقاف المهمة بعد إعادة تشغيل البرنامج؛ راجع الموضع واستأنف يدويًا.",),
    )
    DB.commit()


async def telegram_startup():
    await client.connect()

    if not await client.is_user_authorized():
        saved = setting_get("session_string", "")
        if saved:
            # Rebuild the single client with the saved StringSession.
            await client.disconnect()
            client.session = StringSession(saved)
            await client.connect()

    global telegram_forward_limit
    telegram_forward_limit = await discover_forward_limit()

    if await client.is_user_authorized():
        me = await client.get_me()
        logging.info(
            "User session connected: id=%s username=%s batch_limit=%s",
            me.id,
            getattr(me, "username", None),
            telegram_forward_limit,
        )
    else:
        logging.warning("User session is not authorized yet.")

    await recover_after_restart()


async def post_init(application: Application):
    global bot_app
    bot_app = application
    await telegram_startup()


async def post_shutdown(application: Application):
    global worker_task
    if worker_task and not worker_task.done():
        stop_event.set()
        with suppress(Exception):
            await asyncio.wait_for(worker_task, timeout=10)
    await client.disconnect()
    DB.close()


def build_app():
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("login", login_cmd))
    application.add_handler(CallbackQueryHandler(button_handler))

    # Login input has priority; normal panel input uses awaiting.
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            combined_text_handler,
        )
    )
    return application


async def combined_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_owner(update.effective_user.id):
        return

    if context.user_data.get("login_step"):
        await login_message_handler(update, context)
    else:
        await message_handler(update, context)


def main():
    # uvloop is a drop-in, faster event loop implementation for asyncio.
    # It's optional: if it isn't installed, the standard asyncio loop is
    # used and nothing else about the program changes.
    try:
        import uvloop

        uvloop.install()
        logging.info("uvloop event loop enabled.")
    except ImportError:
        logging.info("uvloop not installed; using the default asyncio event loop.")

    app = build_app()
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        close_loop=False,
    )


if __name__ == "__main__":
    main()
