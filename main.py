import os
import re
import pandas as pd
import asyncio
from fastapi import FastAPI, Request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, constants
from telegram import Chat
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters
)
import logging
from datetime import datetime, timezone, timedelta
from uuid import uuid4

# ✅ تخزين كل اقتراحات المستخدمين
suggestion_records = {}

# ✅ إعداد السجلات
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

# ✅ إعداد التوكن
API_TOKEN = os.getenv("TELEGRAM_TOKEN")

# ✅ تهيئة المتغيرات
df_admins = df_replies = df_branches = df_maintenance = df_parts = df_manual = df_independent = pd.DataFrame()
ALL_USERS = set()
user_sessions = {}

# ✅ تحميل بيانات Excel
try:
    excel_data = pd.read_excel("bot_data.xlsx", sheet_name=None)

    df_admins = excel_data["managers"]
    df_replies = excel_data["suggestion_replies"]
    df_branches = excel_data["branches"]
    df_maintenance = excel_data["maintenance"]
    df_parts = excel_data["parts"]
    df_manual = excel_data["manual"]
    df_independent = excel_data["independent"]

    if "all_users_log" in excel_data:
        df_users = excel_data["all_users_log"]
        ALL_USERS = set(df_users["user_id"].dropna().astype(int).tolist())
    else:
        df_users = pd.DataFrame(columns=["user_id"])

    AUTHORIZED_USERS = df_admins["manager_id"].dropna().astype(int).tolist()
    SUGGESTION_REPLIES = dict(zip(df_replies["key"], df_replies["reply"]))
    initial_branches = df_branches.to_dict(orient="records")

except Exception as e:
    logging.error(f"[DATA LOAD ERROR] ⚠️ خطأ في قراءة bot_data.xlsx: {e}")
    AUTHORIZED_USERS = []
    SUGGESTION_REPLIES = {}
    initial_branches = []

# ✅ تهيئة تطبيق FastAPI وتطبيق التلغرام
app = FastAPI()
application = Application.builder().token(API_TOKEN).build()
application.bot_data["branches"] = initial_branches

# ✅ 1. تعريف دالة تنظيف الجلسات
async def cleanup_old_sessions(context: ContextTypes.DEFAULT_TYPE, max_age_minutes: int = 15):
    """🧹 يحذف الجلسات القديمة من user_sessions لتقليل الضغط"""
    now = datetime.now(timezone.utc)
    removed = 0

    for user_id in list(user_sessions):
        original_count = len(user_sessions[user_id])
        user_sessions[user_id] = [
            msg for msg in user_sessions[user_id]
            if (now - msg["timestamp"]).total_seconds() < max_age_minutes * 60
        ]
        if not user_sessions[user_id]:
            del user_sessions[user_id]
            removed += original_count

    logging.info(f"[CLEANUP] 🧹 تم تنظيف {removed} رسالة من الجلسات القديمة.")

def register_message(user_id, message_id, chat_id=None, context=None, skip_delete=False):
    if user_id not in user_sessions:
        user_sessions[user_id] = []

    user_sessions[user_id].append({
        "message_id": message_id,
        "chat_id": chat_id or user_id,
        "timestamp": datetime.now(timezone.utc)
    })

    # ✅ لا تقم بالحذف إذا skip_delete=True
    if not skip_delete and context and hasattr(context, "job_queue") and context.job_queue:
        try:
            context.job_queue.run_once(
                schedule_delete_message,
                timedelta(minutes=5),
                data={
                    "user_id": user_id,
                    "message_id": message_id,
                    "chat_id": chat_id or user_id
                }
            )
        except Exception as e:
            logging.warning(f"[JOB ERROR] فشل في جدولة الحذف التلقائي للرسالة {message_id}: {e}")

async def schedule_delete_message(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    chat_id = job_data.get("chat_id")
    message_id = job_data.get("message_id")
    user_id = job_data.get("user_id")

    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logging.info(f"[DELETE] 🗑️ تم حذف الرسالة رقم {message_id} للمستخدم {user_id}")
    except Exception as e:
        logging.warning(f"⚠️ الرسالة {message_id} للمستخدم {user_id} ربما حُذفت مسبقًا أو غير موجودة.")

async def log_event(update: Update, message: str, level="info"):
    user = update.effective_user
    chat = update.effective_chat
    timestamp = datetime.now(timezone.utc) + timedelta(hours=3)

    log_msg = (
        f"{timestamp:%Y-%m-%d %H:%M:%S} | "
        f"📩 من: [{user.full_name}] | "
        f"🆔 المستخدم: {user.id} | "
        f"📣 المحادثة: {chat.id} | "
        f"📝 {message}"
    )

    if level == "error":
        logging.error(log_msg)
    else:
        logging.info(log_msg)
        
# ✅ دالة البدء async
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.bot_data.get("maintenance_mode"):
        user_name = update.effective_user.full_name
        msg = await update.message.reply_text(
            f"🛠️ عزيزي {user_name}\n\nبرنامج GO قيد التحديث والصيانة حالياً.\n🔄 الرجاء المحاولة لاحقاً."
        )
        context.job_queue.run_once(
            lambda c: c.bot.delete_message(chat_id=msg.chat_id, message_id=msg.message_id),
            when=60
        )
        return

    user = update.effective_user
    chat = update.effective_chat
    user_id = user.id
    chat_id = chat.id
    user_name = user.full_name

    if update.message:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
        except:
            pass

    if chat.type == "private" and not context.user_data.get(user_id, {}).get("session_valid") and user_id not in AUTHORIZED_USERS:
        text = update.message.text.strip().lower() if update.message else ""
        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")
        user_block = f"`🧑‍🏫 عزيزي {user_name}`"
        delete_block = f"`⏳ سيتم حذف هذا التنبيه تلقائيًا خلال 5 دقائق ({delete_time} / 🇸🇦)`"

        if text in ["/start", "start", "go", "/go"] and "start=go" not in text:
            alert_message = (
                "📣 يسعدنا اهتمامك بخدمات برنامج GO!\n\n"
                "❌ لا يمكنك بدء الخدمة مباشرة من الخاص.\n"
                "🔐 لضمان الخصوصية، نرجو الانضمام إلى المجموعة وكتابة  go هناك.\n\n"
                "[👥 اضغط هنا للانضمام إلى مجموعة CHERY](https://t.me/CHERYKSA_group)"
            )
        else:
            alert_message = (
                "🚫 عذرًا، لا يمكن الدخول من الخاص بهذه الطريقة.\n"
                "🔐 هذه الخدمة مخصصة فقط لمن بدأ الجلسة من المجموعة بنفسه.\n"
                "✳️ نرجو العودة إلى المجموعة وكتابة كلمة go يدويًا لإعادة التفعيل."
            )

        msg = await update.message.reply_text(
            f"{user_block}\n\n{alert_message}\n\n{delete_block}",
            parse_mode=constants.ParseMode.MARKDOWN,
            disable_web_page_preview=True
        )
        register_message(user_id, msg.message_id, chat_id, context)
        return

    context.user_data.setdefault(user_id, {})
    context.user_data[user_id]["manual_sent"] = False

    global ALL_USERS
    if user_id not in ALL_USERS:
        ALL_USERS.add(user_id)
        try:
            df_users = pd.DataFrame(sorted(ALL_USERS), columns=["user_id"])
            with pd.ExcelWriter("bot_data.xlsx", engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                df_users.to_excel(writer, sheet_name="all_users_log", index=False)
        except Exception as e:
            logging.error(f"[SAVE USERS] فشل حفظ المستخدمين في Excel: {e}")

    try:
        stats_df = pd.read_excel("bot_data.xlsx", sheet_name="bot_stats")
    except:
        stats_df = pd.DataFrame(columns=["key", "value"])

    if "total_go_uses" in stats_df["key"].values:
        stats_df.loc[stats_df["key"] == "total_go_uses", "value"] += 1
    else:
        stats_df = pd.concat([stats_df, pd.DataFrame([{"key": "total_go_uses", "value": 1}])], ignore_index=True)

    try:
        with pd.ExcelWriter("bot_data.xlsx", engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            stats_df.to_excel(writer, sheet_name="bot_stats", index=False)
    except Exception as e:
        logging.error(f"[SAVE STATS] فشل حفظ عدد /go إلى Excel: {e}")

    group_title = context.user_data[user_id].get("group_title", "غير معروف")
    group_id = context.user_data[user_id].get("group_id", user_id)
    previous_user_name = context.user_data[user_id].get("user_name", user_name)

    if chat_id > 0 and user_id in context.bot_data:
        bot_data = context.bot_data[user_id]
        context.user_data[user_id].update(bot_data)
        del context.bot_data[user_id]

        group_title = bot_data.get("group_title", "غير معروف")
        group_id = bot_data.get("group_id", user_id)
        previous_user_name = bot_data.get("user_name", user_name)

    context.user_data[user_id].update({
        "action": None,
        "compose_text": None,
        "compose_media": None,
        "compose_mode": None,
        "group_title": group_title,
        "group_id": group_id,
        "user_name": previous_user_name,
        "final_group_name": group_title,
        "final_group_id": group_id
    })

    await log_event(update, "بدأ المستخدم التفاعل مع /go")

    if chat_id < 0:
        context.bot_data[user_id] = {
            "group_title": update.effective_chat.title or "غير معروف",
            "group_id": chat_id,
            "user_name": user_name
        }

        photo_path = "GO-CHERY.JPG"
        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(seconds=90)).strftime("%I:%M %p")

        user_block = f"`🧑‍💼 مرحباً {user_name}`"
        program_description = (
            "🤖 *نظام الاستعلامات الذكي لعملاء شيري برو*\n"
            "🔧 صيانة دورية • قطع غيار • دليل المالك • مراكز خدمة ومتاجر\n"
            "🛠️ والمزيد من الخدمات المتكاملة بين يديك."
        )
        delete_block = f"`⏳ سيتم حذف هذا المنشور خلال 90 ثانية ({delete_time} / 🇸🇦)`"

        full_caption = (
           f"{user_block}\n\n"
           f"{program_description}\n\n"
           "💡 اضغط الزر أدناه لبدء خدمتك في الخاص:\n\n"
           f"{delete_block}"
        )

        bot_username = context.bot.username
        link = f"https://t.me/{bot_username}?start=go"
        keyboard = [[InlineKeyboardButton("🚀  انطلق  مع  برنامج  GO", url=link)]]

        try:
            msg = await context.bot.send_photo(
                chat_id=chat_id,
                photo=open(photo_path, "rb"),
                caption=full_caption,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=constants.ParseMode.MARKDOWN
            )
            register_message(user_id, msg.message_id, chat_id, context)
            if context and hasattr(context, "job_queue") and context.job_queue:
                context.job_queue.run_once(
                    schedule_delete_message,
                    timedelta(seconds=90),
                    data={"user_id": user_id, "message_id": msg.message_id, "chat_id": chat_id}
                )
        except Exception as e:
            logging.error(f"فشل في إرسال الترحيب بالصورة: {e}")
        return

    context.user_data[user_id].pop("suggestion_used", None)

    keyboard = [
        [InlineKeyboardButton("🔧 استعلام  قطع الغيار", callback_data=f"parts_{user_id}")],
        [InlineKeyboardButton("🚗 استعلام  الصيانة الدورية", callback_data=f"maintenance_{user_id}")],
        [InlineKeyboardButton("📘 عرض دليل المالك CHERY", callback_data=f"manual_{user_id}")],
        [InlineKeyboardButton("🛠️ المتاجر ومراكر الخدمة", callback_data=f"service_{user_id}")],
        [InlineKeyboardButton("✉️ تقديم اقتراح أو ملاحظة", callback_data=f"suggestion_{user_id}")]
    ]

    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")

    msg1 = await update.message.reply_text(
        f"`🧑‍💼 مرحباً {user_name}`\n\n"
        "🤖 لقد وصلت إلى *برنامج GO / CHERY* التفاعلي.\n"
        "💡 يمكنك الآن بدء رحلتك الذكية معنا في خدمات الصيانة وقطع الغيار والمزيد في مكان واحد.\n\n"
        f"`⏳ سيتم حذف هذه الرسالة تلقائيًا خلال 5 دقائق ({delete_time} / 🇸🇦)`",
        parse_mode=constants.ParseMode.MARKDOWN
    )

    msg2 = await update.message.reply_text(
        "اختار الخدمة المطلوبة 🛠️ :",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

    register_message(user_id, msg1.message_id, chat_id, context)
    register_message(user_id, msg2.message_id, chat_id, context)

    context.user_data[user_id]["session_valid"] = False

async def handle_go_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    user_id = user.id
    user_name = user.full_name
    chat_id = chat.id

    if chat.type != "private":
        context.bot_data[user_id] = {
            "group_title": chat.title or "غير معروف",
            "group_id": chat.id,
            "user_name": user.full_name
        }
        logging.info(f"[GO من المجموعة] سجلنا بيانات المجموعة {chat.title} / {chat.id} للمستخدم {user.full_name}")

        # ✅ إنشاء جلسة مؤقتة صالحة لمرة واحدة فقط
        context.user_data[user_id] = context.user_data.get(user_id, {})
        context.user_data[user_id]["session_valid"] = True

    # ✅ رفض الدخول في الخاص إن لم يكن هناك جلسة صالحة أو كان متطفلًا
    if chat.type == "private" and (
        not context.user_data.get(user_id, {}).get("session_valid")
    ) and user_id not in AUTHORIZED_USERS:
        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")

        user_block = f"`🧑‍🏫 عزيزي {user_name}`"
        alert_message = (
            "📣 يسعدنا اهتمامك بخدمات برنامج GO!\n\n"
            "❌ لا يمكنك بدء الخدمة مباشرة من الخاص.\n"
            "🔐 الرجاء الدخول إلى المجموعة وكتابة go بنفسك.\n\n"
            "[👥 اضغط هنا للانضمام إلى مجموعة CHERY](https://t.me/CHERYKSA_group)"
        )
        delete_block = f"`⏳ سيتم حذف هذا التنبيه تلقائيًا خلال 5 دقائق ({delete_time} / 🇸🇦)`"

        msg = await update.message.reply_text(
            f"{user_block}\n\n{alert_message}\n\n{delete_block}",
            parse_mode=constants.ParseMode.MARKDOWN,
            disable_web_page_preview=True
        )
        register_message(user_id, msg.message_id, chat_id, context)
        return

    # ✅ تابع تنفيذ start
    await start(update, context)

async def start_suggestion_session(user_id, context):
    from uuid import uuid4
    suggestion_id = uuid4().hex

    context.user_data.setdefault(user_id, {})

    # ✅ استرداد من user_data فقط (يفترض أن start() تعامل مع bot_data بالفعل)
    group_name = context.user_data[user_id].get("group_title", "غير معروف")
    group_id = context.user_data[user_id].get("group_id", "غير معروف")
    user_name = context.user_data[user_id].get("user_name", "—")

    # ✅ فقط كاحتياط: محاولة استرداد من bot_data إذا فقدت المعلومات (حالات نادرة)
    if (group_name in ["غير معروف", None] or group_id in ["غير معروف", None, user_id]) and user_id in context.bot_data:
        fallback = context.bot_data[user_id]
        group_name = fallback.get("group_title", group_name)
        group_id = fallback.get("group_id", group_id)
        user_name = fallback.get("user_name", user_name)
        del context.bot_data[user_id]

    # ✅ سجل الاقتراح
    suggestion_records.setdefault(user_id, {})
    suggestion_records[user_id][suggestion_id] = {
        "text": None,
        "media": None,
        "submitted": False,
        "admin_messages": {},
        "group_name": group_name,
        "group_id": group_id,
        "user_name": user_name
    }

    context.user_data[user_id]["active_suggestion_id"] = suggestion_id
    return suggestion_id

### ✅ الدالة المعدلة: handle_message (فقط جزء الاقتراح)
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global df_admins
    message = update.message
    user = update.effective_user
    admin_id = user.id
    chat = update.effective_chat
    user_id = user.id
    chat_id = chat.id
    user_name = user.full_name

    # ✅ تعريف action بشكل آمن
    action = context.user_data.get(admin_id, {}).get("action")

    # ✅ حذف مشرف
    if action == "awaiting_admin_removal":
        try:
            global df_admins  # ← استخدم النسخة المحمّلة مسبقًا

            target_id = int(message.text.strip())

            if target_id == 1543083749:
                await message.reply_text("🚫 لا يمكن حذف المدير الأعلى.")
                return

            if target_id not in df_admins["manager_id"].astype(int).values:
                await message.reply_text("❌ هذا المشرف غير موجود.")
                return

            # حذف من الذاكرة
            df_admins = df_admins[df_admins["manager_id"].astype(int) != target_id]

            # تحديث القائمة الحية
            if target_id in AUTHORIZED_USERS:
                AUTHORIZED_USERS.remove(target_id)

            # حفظ التغييرات
            with pd.ExcelWriter("bot_data.xlsx", engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                df_admins.to_excel(writer, sheet_name="managers", index=False)

            await message.reply_text(f"🗑️ تم حذف المشرف بنجاح: `{target_id}`", parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            await message.reply_text(f"❌ فشل أثناء حذف المشرف:\n{e}")

        context.user_data[user_id]["action"] = None
        return

    # ✅ إضافة مشرف من لوحة التحكم
    if context.user_data.get(admin_id, {}).get("action") == "awaiting_new_admin_id":
        try:
            text = message.text.strip()
            if not text.isdigit():
                await message.reply_text("❌ يجب إدخال رقم ID رقمي صالح.")
                return

            new_admin_id = int(text)
            if new_admin_id in AUTHORIZED_USERS:
                await message.reply_text("ℹ️ هذا المشرف موجود مسبقًا.")
                return

            # تحديث القائمة الحية
            AUTHORIZED_USERS.append(new_admin_id)
            df_admins = pd.concat([df_admins, pd.DataFrame([{"manager_id": new_admin_id}])], ignore_index=True)

            # حفظ إلى Excel
            with pd.ExcelWriter("bot_data.xlsx", engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                df_admins.to_excel(writer, sheet_name="managers", index=False)

            await message.reply_text(f"✅ تم إضافة المشرف: `{new_admin_id}`", parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            await message.reply_text(f"❌ فشل أثناء حفظ الملف:\n{e}")

        context.user_data[admin_id]["action"] = None
        return
        
    # ✅ تحديد المستخدم الفعلي المرتبط بالرد
    user_id = context.user_data.get(admin_id, {}).get("custom_reply_for", admin_id)
    mode = context.user_data.get(user_id, {}).get("action") or context.user_data.get(admin_id, {}).get("compose_mode")

    # ✅ منطق إضافة مشرف جديد (من قائمة التحكم)
    if context.user_data.get(admin_id, {}).get("action") == "awaiting_new_admin_id":
        text = message.text.strip()
        if not text.isdigit():
            await message.reply_text("❌ يجب إدخال رقم ID رقمي صالح.")
            return

        new_admin_id = int(text)
        if new_admin_id in AUTHORIZED_USERS:
            await message.reply_text("ℹ️ هذا المشرف موجود مسبقًا.")
            return

        AUTHORIZED_USERS.append(new_admin_id)

        try:
            df_admins = pd.read_excel("bot_data.xlsx", sheet_name="managers")
            df_admins = pd.concat([df_admins, pd.DataFrame([{"manager_id": new_admin_id}])], ignore_index=True)
            with pd.ExcelWriter("bot_data.xlsx", engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                df_admins.to_excel(writer, sheet_name="managers", index=False)
            await message.reply_text(f"✅ تم إضافة المشرف: `{new_admin_id}`", parse_mode=constants.ParseMode.MARKDOWN)
        except Exception as e:
            await message.reply_text(f"❌ فشل أثناء حفظ الملف:\n{e}")

        context.user_data[admin_id]["action"] = None
        return

    # ✅ معالجة الاقتراح أو الرد المخصص
    if mode in ["suggestion", "custom_reply"]:
        context.user_data.setdefault(user_id, {})

        suggestion_id = context.user_data[user_id].get("active_suggestion_id")
        if not suggestion_id:
            suggestion_id = await start_suggestion_session(user_id, context)

        record = suggestion_records[user_id][suggestion_id]

        # 🧹 تصفير الحقول
        if not context.user_data[admin_id].get("compose_text") and not context.user_data[admin_id].get("compose_media"):
            record["text"] = ""
            record["media"] = None

        group_name = chat.title if chat.type in ['group', 'supergroup', 'channel'] else "خاص"
        group_id = chat.id

        if group_name == "خاص" or group_id == user_id:
            fallback = context.user_data.get(user_id, {}) or context.bot_data.get(user_id, {})
            group_name = fallback.get("group_title", "غير معروف")
            group_id = fallback.get("group_id", user_id)

        record["group_name"] = group_name
        record["group_id"] = group_id
        context.user_data[admin_id]["compose_mode"] = mode

        # ✅ تسجيل النص
        if message.text:
            context.user_data[admin_id]["compose_text"] = message.text.strip()
            record["text"] = message.text.strip()

        # ✅ تسجيل الوسائط
        elif message.photo or message.video or message.document or message.voice:
            if message.photo:
                file_id = message.photo[-1].file_id
                media_type = "photo"
            elif message.video:
                file_id = message.video.file_id
                media_type = "video"
            elif message.document:
                file_id = message.document.file_id
                media_type = "document"
            elif message.voice:
                file_id = message.voice.file_id
                media_type = "voice"
            context.user_data[admin_id]["compose_media"] = {"type": media_type, "file_id": file_id}
            record["media"] = {"type": media_type, "file_id": file_id}

        # ✅ أزرار التحكم
        if mode == "suggestion":
            buttons = [
                [InlineKeyboardButton("📤 إرسال", callback_data="send_suggestion")],
                [InlineKeyboardButton("❌ إلغاء", callback_data="cancel_suggestion")]
            ]
        elif mode == "custom_reply":
            buttons = [
                [InlineKeyboardButton("📤 إرسال الرد", callback_data="submit_admin_reply")],
                [InlineKeyboardButton("❌ إلغاء", callback_data="cancel_custom_reply")]
            ]

        has_text = context.user_data[admin_id].get("compose_text")
        has_media = context.user_data[admin_id].get("compose_media")

        if has_text and has_media:
            await message.reply_text("✅ تم حفظ النص والوسائط. يمكنك الإرسال الآن:", reply_markup=InlineKeyboardMarkup(buttons))
        elif has_text:
            await message.reply_text("📎 تم حفظ النص. يمكنك الآن إدخال وسائط أو الإرسال:", reply_markup=InlineKeyboardMarkup(buttons))
        elif has_media:
            await message.reply_text("🖼️ تم حفظ الوسائط. يمكنك الآن إدخال نص أو الإرسال:", reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await message.reply_text("⚠️ لم يتم تسجيل أي محتوى. الرجاء إدخال نص أو وسائط.")
        return

# ✅ استعلام قطع غيار
    if context.user_data.get(user_id, {}).get("action") == "parts" and message.text:
        part_name = message.text.strip().lower()

    # 🔁 منع إعادة اختيار الفئة - فقط نسمح بمحاولات بحث متعددة
        context.user_data[user_id].setdefault("search_attempts", 0)
        context.user_data[user_id]["search_attempts"] += 1

        if context.user_data[user_id]["search_attempts"] > 3:
            msg = await message.reply_text("🚫 لقد استهلكت جميع محاولات البحث خلال الجلسة.\n🔁 ابدأ من جديد باستخدام go من المجموعة")
            register_message(user_id, msg.message_id, chat.id, context)
            context.user_data[user_id].clear()
            return

        selected_car = context.user_data[user_id].get("selected_car")

        if not selected_car:
            msg = await message.reply_text("❗ لم يتم اختيار فئة السيارة.")
            register_message(user_id, msg.message_id, chat.id, context)
            return

    # تصفية الصفوف الخاصة بفئة السيارة
        filtered_df = df_parts[df_parts["Station No"] == selected_car]

    # الأعمدة التي نريد البحث فيها — حسب بنية ملف PARTS
        columns_to_search = ["Station Name", "Part No"]

    # البحث داخل الأعمدة المحددة
        matches = filtered_df[
            filtered_df[columns_to_search].apply(lambda x: x.str.contains(part_name, case=False, na=False)).any(axis=1)
        ]

        if matches.empty:
            msg = await message.reply_text("❌ لم يتم العثور على نتائج ضمن فئة السيارة أو الادخال خاطئ.")
            register_message(user_id, msg.message_id, chat.id, context)
            return

        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")
        footer = f"\n\n<code>⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 5 دقائق ({delete_time} / 🇸🇦)</code>"

        user_name = update.effective_user.full_name
        remaining = 3 - context.user_data[user_id]["search_attempts"]

        for i, row in matches.iterrows():
            part_name_value = row.get("Station Name", "غير معروف")
            part_number_value = row.get("Part No", "غير معروف")

            text = f"""<code>🧑‍💼 استعلام خاص بـ {user_name}</code>

🚗 <b>الفئة:</b> {selected_car}
🔹 <b>اسم القطعة:</b> {part_name_value}
🔹 <b>رقم القطعة:</b> {part_number_value}

📌 تبقّى لك: ({remaining} من 3) محاولات""" + footer

        keyboard = []
        if pd.notna(row.get("Image")):
            keyboard.append([InlineKeyboardButton("عرض الصورة 📸", callback_data=f"part_image_{i}_{user_id}")])
        msg = await message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode=ParseMode.HTML
        )
        register_message(user_id, msg.message_id, chat.id, context)

    await log_event(update, f"✅ بحث دقيق ضمن {selected_car}: {part_name}")
    register_message(user_id, message.message_id, chat.id, context)
    return

async def handle_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    mode = context.user_data.get(user_id, {}).get("compose_mode")

    if mode == "suggestion":
        suggestion_records.pop(user_id, None)
        context.user_data[user_id].clear()
        await query.edit_message_text("❌ تم إلغاء الاقتراح.")
    else:
        await query.answer("🚫 لا توجد عملية نشطة لإلغائها.", show_alert=True)

    # ✅ حذف الرسالة التي تحتوي الزر (سواء في الوضعين)
    try:
        await query.message.delete()
    except:
        pass
        
async def show_manual_car_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data.split("_")
    user_id = int(data[1])

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go",
            show_alert=True
        )
        return

    await log_event(update, "📘 فتح قائمة دليل المالك")

    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")

    try:
        # ✅ يتم أخذ السيارات من Excel بنفس الترتيب الموجود في الشيت
        car_names = df_manual["car_name"].dropna().drop_duplicates().tolist()
    except Exception as e:
        await log_event(update, f"❌ فشل في تحميل قائمة السيارات من Excel: {e}", level="error")
        msg = await query.message.reply_text("📂 تعذر تحميل قائمة دليل المالك حالياً.")
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        return

    keyboard = [
        [InlineKeyboardButton(car, callback_data=f"manualcar_{car.replace(' ', '_')}_{user_id}")]
        for car in car_names
    ]

    text = (
        "📘 اختر فئة السيارة للاطلاع على دليل المالك:\n\n"
        f"`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 5 دقائق ({delete_time} / 🇸🇦)`"
    )

    try:
        msg = await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=constants.ParseMode.MARKDOWN
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        context.user_data[user_id]['manual_msg_id'] = msg.message_id
        context.user_data[user_id]['last_message_id'] = msg.message_id
    except Exception as e:
        await log_event(update, f"❌ فشل في إرسال قائمة دليل المالك: {e}", level="error")

async def handle_manualcar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split("_")
    user_id_from_callback = int(parts[-1])
    car_name = " ".join(parts[1:-1])

    if query.from_user.id != user_id_from_callback:
        requester = await context.bot.get_chat(user_id_from_callback)
        await query.answer(f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go", show_alert=True)
        return

        if context.user_data.get(user_id_from_callback, {}).get("manual_sent"):
            await query.answer("❌ لا يمكنك اختيار دليل آخر في نفس الاستعلام، يرجى استخدام الأمر /go من جديد.", show_alert=True)
            return

        context.user_data[user_id_from_callback]["manual_sent"] = True

    try:
        old_msg_id = context.user_data.get(user_id_from_callback, {}).get("manual_msg_id")
        if old_msg_id:
            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=old_msg_id)
    except:
        pass

    match = df_manual[df_manual["car_name"].str.strip() == car_name.strip()]
    if match.empty:
        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")
        caption = (
            f"`🧑‍💼 استعلام خاص بـ {query.from_user.full_name}`\n\n"
            f"📘 نعتذر، دليل المالك لهذه السيارة غير متوفر حالياً.\n"
            f"📂 سيتم رفع الملف قريباً بعد التحديث.\n\n"
            f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 5 دقائق ({delete_time} / 🇸🇦)`"
        )
        msg = await query.message.reply_text(caption, parse_mode=constants.ParseMode.MARKDOWN)
        register_message(user_id_from_callback, msg.message_id, query.message.chat_id, context)
        await log_event(update, f"📂 لا توجد بيانات لـ {car_name}", level="error")
        return

    image_url = match["cover_image"].values[0]
    index = match.index[0]
    user_name = query.from_user.full_name
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")

    if pd.isna(image_url) or image_url.strip() == "":
        caption = (
            f"`🧑‍💼 استعلام خاص بـ {user_name}`\n\n"
            f"📘 نعتذر، دليل المالك لهذه السيارة غير متوفر حالياً.\n"
            f"📂 سيتم رفع الملف قريباً بعد التحديث.\n\n"
            f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 5 دقائق ({delete_time} / 🇸🇦)`"
        )
        msg = await query.message.reply_text(caption, parse_mode=constants.ParseMode.MARKDOWN)
        register_message(user_id_from_callback, msg.message_id, query.message.chat_id, context)
        await log_event(update, f"📂 لا يوجد غلاف لـ {car_name}", level="error")
        return

    caption = (
        f"`🧑‍💼 استعلام خاص بـ {user_name}`\n\n"
        f"📜 دليل المالك للسيارة ({car_name})\n\n"
        f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 5 دقائق ({delete_time} / 🇸🇦)`"
    )

    keyboard = [[InlineKeyboardButton("📘 استعراض دليل المالك", callback_data=f"openpdf_{index}_{user_id_from_callback}")]]
    
    try:
        msg = await context.bot.send_photo(
            chat_id=query.message.chat_id,
            photo=image_url,
            caption=caption,
            parse_mode=constants.ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        register_message(user_id_from_callback, msg.message_id, query.message.chat_id, context)
        context.user_data[user_id_from_callback]["manual_msg_id"] = msg.message_id
        await log_event(update, f"✅ تم عرض غلاف دليل {car_name}")
    except Exception as e:
        await log_event(update, f"❌ خطأ أثناء إرسال الغلاف لـ {car_name}: {e}", level="error")
        msg = await query.message.reply_text("📂 فشل في إرسال الغلاف. يرجى المحاولة لاحقاً.")
        register_message(user_id_from_callback, msg.message_id, query.message.chat_id, context)

    context.user_data[user_id_from_callback].pop("manual_viewed", None)

async def select_car_for_parts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data.split("_")
    user_id = int(data[-1])

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go", show_alert=True)
        return

    car = " ".join(data[1:-1])
    
    context.user_data.setdefault(user_id, {})
    context.user_data[user_id]["selected_car"] = car
    context.user_data[user_id]["action"] = "parts"
    context.user_data[user_id]["search_attempts"] = 0  # إعادة تعيين العداد عند كل اختيار فئة

    # التصنيفات الرئيسية للقطع الاستهلاكية
    part_categories = {
        "🧴 الزيوت": "زيت",
        "🌀 الفلاتر": "فلتر",
        "🔋 البطاريات": "بطارية",
        "🔌 البواجي": "بواجي",
        "🧼 منتجات مساعدة": "منتج",
        "⚙️ السيور": "سير",
        "🛞 الفحمات": "فحمات",
        "💧 سوائل النقل": "سائل ناقل",
    }

    keyboard = [
        [InlineKeyboardButton(name, callback_data=f"catpart_{keyword}_{user_id}")]
        for name, keyword in part_categories.items()
    ]

    msg = await query.edit_message_text(
        f"🔧 اختر تصنيف القطع لفئة: {car}",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    register_message(user_id, msg.message_id, query.message.chat_id, context)
    await log_event(update, f"اختار فئة قطع الغيار: {car}")

    await query.answer()  # تأكيد استقبال callback query

async def handle_manualdfcar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split("_")
    index = int(parts[1])
    user_id = int(parts[2])

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go",
            show_alert=True
        )
        return

    # ✅ السماح بالدخول مرة جديدة إذا عاد المستخدم من /go
    if context.user_data.get(user_id, {}).get("manual_sent"):
        await query.answer(
            f"❌ عزيزي {query.from_user.full_name}، لا يمكنك فتح هذا الاستعلام مرتين بنفس الجلسة. الرجاء استخدام /go مره اخرى.",
            show_alert=True
        )
        return

    try:
        row = df_manual.iloc[index]
        car_name = row["car_name"]
        file_id = row["pdf_file_id"]
    except:
        await query.answer("❌ تعذر تحميل الملف – غير متوفر أو بيانات غير صالحة.", show_alert=True)
        return

    user_name = query.from_user.full_name
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")

    # ✅ إذا لم يوجد PDF، نعرض رسالة تنبيه محترمة ومنسقة
    if pd.isna(file_id) or str(file_id).strip() == "":
        caption = (
            f"`🧑‍💼 استعلام خاص بـ {user_name}`\n\n"
            f"📘 نعتذر، دليل المالك لهذه السيارة غير متوفر حالياً.\n"
            f"📂 سيتم رفع الملف قريباً بعد التحديث.\n\n"
            f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 5 دقائق ({delete_time} / 🇸🇦)`"
        )
        try:
            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
        except:
            pass
        msg = await query.message.reply_text(caption, parse_mode=constants.ParseMode.MARKDOWN)
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, f"📂 لا يوجد ملف PDF لـ {car_name}", level="error")
        return

    caption = (
        f"`🧑‍💼 استعلام خاص بـ {user_name}`\n\n"
        f"📘 دليل المالك للسيارة: {car_name}\n\n"
        f"`⏳ سيتم حذف هذا الملف تلقائيًا خلال 5 دقائق ({delete_time} / 🇸🇦)`"
    )

    try:
        await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
    except:
        pass

    try:
        msg = await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=file_id,
            caption=caption,
            parse_mode=constants.ParseMode.MARKDOWN
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        context.user_data[user_id]["manual_sent"] = True
        await log_event(update, f"📘 تم إرسال ملف دليل {car_name}")
    except Exception as e:
        await log_event(update, f"❌ فشل في إرسال دليل PDF لـ {car_name}: {e}", level="error")
        await query.message.reply_text("📂 تعذر إرسال الملف. حاول لاحقاً.")

async def send_part_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split("_")
    index, user_id = int(parts[2]), int(parts[3])

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go",
            show_alert=True
        )
        return

    if context.user_data.get(user_id, {}).get(f"image_opened_{index}"):
        await query.answer(f"❌ عزيزي {query.from_user.full_name}، لا يمكنك فتح هذا الاستعلام مرتين بنفس الجلسة. الرجاء استخدام /go مره اخرى.", show_alert=True)
        return

    context.user_data.setdefault(user_id, {})[f"image_opened_{index}"] = True
    row = df_parts.iloc[index]

    user_name = query.from_user.full_name
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")
    header = f"`🧑‍💻 استعلام خاص بـ {user_name}`\n"
    footer = f"\n`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 5 دقائق ({delete_time} / 🇸🇦)`"

    caption = f"{header}*الاسم:* {row['Station Name']}\n*الرقم:* {row['Part No']}{footer}"

    msg = await context.bot.send_photo(chat_id=query.message.chat_id, photo=row["Image"], caption=caption, parse_mode=constants.ParseMode.MARKDOWN)
    register_message(user_id, msg.message_id, query.message.chat_id, context)
    
async def car_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data.split("_")
    user_id = int(data[-1])

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go",
            show_alert=True
        )
        return

    car = "_".join(data[1:-1]).replace("_", " ")
    context.user_data[user_id]["car_type"] = car

    kms = df_maintenance[df_maintenance["car_type"] == car]["km_service"].unique().tolist()
    keyboard = [[InlineKeyboardButton(f"{km}", callback_data=f"km_{km}_{user_id}")] for km in kms]

    msg = await query.edit_message_text("اختر مسافة km الصيانة 🧾 :", reply_markup=InlineKeyboardMarkup(keyboard))
    register_message(user_id, msg.message_id, query.message.chat_id, context)
    
    await log_event(update, f"اختار {car} من قائمة السيارات")

async def km_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data.split("_")
    user_id = int(data[-1])

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go",
            show_alert=True
        )
        return

    if context.user_data.get(user_id, {}).get("km_used"):
        await query.answer(f"❌ عزيزي {query.from_user.full_name}، لا يمكنك فتح هذا الاستعلام مرتين بنفس الجلسة. الرجاء استخدام /go من جديد.", show_alert=True)
        return

    context.user_data[user_id]["km_used"] = True
    context.user_data[user_id]["km"] = data[1]
    car = context.user_data[user_id]["car_type"]
    results = df_maintenance[(df_maintenance["car_type"] == car) & (df_maintenance["km_service"] == data[1])]

    user_name = query.from_user.full_name
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")
    header = f"`🧑‍💻 استعلام خاص بـ {user_name}`\n\n"
    footer = f"\n\n`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 5 دقائق ({delete_time} / 🇸🇦)`"

    for i, row in results.iterrows():
        text = f"""🚗 *نوع السيارة:* {car}
📏 *المسافة:* {data[1]}
🛠️ *الإجراءات:* _{row['maintenance_action']}_"""
        text = header + text + footer

        keyboard = [
            [InlineKeyboardButton("عرض تكلفة الصيانة 💰", callback_data=f"cost_{i}_{user_id}")],
            [InlineKeyboardButton("عرض ملف الصيانة 📂", callback_data=f"brochure_{i}_{user_id}")]
        ]
        msg = await query.message.reply_text(
            text, 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode=constants.ParseMode.MARKDOWN
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)

    await log_event(update, f"اختار {car} على مسافة {data[1]} كم")

    try:
        await asyncio.sleep(1)
        await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
    except:
        pass

    # ✅ تفريغ الجلسة بعد انتهاء الاستخدام
    context.user_data[user_id] = {}

async def send_cost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    index, user_id = int(query.data.split("_")[1]), int(query.data.split("_")[2])

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص ب‏ {requester.first_name} {requester.last_name} - استخدم الأمر /go", 
            show_alert=True
        )
        return

    if context.user_data.get(user_id, {}).get("cost_used"):
        await query.answer("❌ لا يمكنك فتح هذا الاستعلام مرتين بنفس الجلسة.", show_alert=True)
        return

    result = df_maintenance.iloc[index]
    car_type = result['car_type']
    km_service = result['km_service']
    cost = result['cost_in_riyals']

    user_name = query.from_user.full_name
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")

    caption = (
        f"`🧑‍💻 استعلام خاص بـ {user_name}`\n"
        f"`📅 آخر تحديث للأسعار: شهر أبريل / 2025`\n"
        f"🚗 نوع السيارة: {car_type}\n"
        f"📏 المسافة: {km_service} كم\n"
        f"💰 تكلفة الصيانة: {cost} ريال\n"
        f"🏢 الشركة: سنابل الحديثة\n"
        f"📞 للحجز اتصل: 8002440228\n\n"
        f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا بعد 5 دقائق ({delete_time} / 🇸🇦)`"
    )

    # حذف زر التكلفة فقط
    try:
        keyboard = query.message.reply_markup.inline_keyboard
        updated_keyboard = [row for row in keyboard if not any("cost_" in button.callback_data for button in row)]
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(updated_keyboard))
    except:
        pass

    msg = await context.bot.send_message(chat_id=query.message.chat_id, text=caption, parse_mode=constants.ParseMode.MARKDOWN)
    register_message(user_id, msg.message_id, query.message.chat_id, context)
    context.user_data[user_id]["cost_used"] = True

    await log_event(update, f"عرض تكلفة الصيانة للسيارة {car_type} عند {km_service} كم")
   # ✅ إنهاء الجلسة بعد الإرسال
    context.user_data[user_id] = {}

async def send_brochure(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    index, user_id = int(query.data.split("_")[1]), int(query.data.split("_")[2])

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go", 
            show_alert=True
        )
        return

    if context.user_data.get(user_id, {}).get("brochure_used"):
        await query.answer(
            f"❌ عزيزي {query.from_user.full_name}، لا يمكنك فتح هذا الاستعلام مرتين بنفس الجلسة. الرجاء استخدام /go مره اخرى.", 
            show_alert=True
        )
        return

    result = df_maintenance.iloc[index]
    user_name = query.from_user.full_name
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")
    header = f"`🧑‍💻 استعلام خاص بـ {user_name}`\n"
    footer = f"\n`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 5 دقائق ({delete_time} / 🇸🇦)`"

    caption = f"{header}*نوع السيارة 🚗:* {result['car_type']}\n*المسافة 📏:* {result['km_service']}{footer}"

    try:
        msg = await context.bot.send_photo(
            chat_id=query.message.chat_id, 
            photo=result["brochure_display"], 
            caption=caption, 
            parse_mode=constants.ParseMode.MARKDOWN
        )
    except:
        msg = await query.message.reply_text("📂 الملف قيد التحديث حاليا سيكون متاح لاحقا.")

    register_message(user_id, msg.message_id, query.message.chat_id, context)
    context.user_data[user_id]["brochure_used"] = True

    # حذف زر العرض فقط
    try:
        keyboard = query.message.reply_markup.inline_keyboard
        updated_keyboard = [row for row in keyboard if not any("brochure_" in button.callback_data for button in row)]
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(updated_keyboard))
    except:
        pass

    await log_event(update, f"📄 عرض ملف صيانة لـ {result['car_type']} عند {result['km_service']} كم")
    # ✅ إنهاء الجلسة بعد الإرسال
    context.user_data[user_id] = {}

async def handle_service_centers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    context.user_data.setdefault(user_id, {})["service_used"] = True

    try:
        await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
    except:
        pass

    # ✅ إرسال الفيديو وتسجيله
    video_path = "مراكز خدمة شيري.MP4"
    if os.path.exists(video_path):
        with open(video_path, "rb") as video_file:
            user_name = query.from_user.full_name
            now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
            delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")
            caption = (
                f"`🧑‍💻 استعلام خاص بـ {user_name}`\n\n"
                f"🗺️  مراكز الخدمة CHERY\n\n"
                f"`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 5 دقائق ({delete_time} / 🇸🇦)`"
            )
            msg1 = await context.bot.send_video(
                chat_id=query.message.chat_id,
                video=video_file,
                caption=caption,
                parse_mode=constants.ParseMode.MARKDOWN
            )
            context.user_data[user_id]["map_msg_id"] = msg1.message_id
            register_message(user_id, msg1.message_id, query.message.chat_id, context)

    # ✅ زرّين في رسالة واحدة
    keyboard = [
        [InlineKeyboardButton("📍 مواقع فروع شركة شيري", callback_data=f"branches_{user_id}")],
        [InlineKeyboardButton("🔧 المتاجر ومراكز الصيانة المستقلة", callback_data=f"independent_{user_id}")]
    ]
    msg2 = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text="🛠️ الرجاء اختيار أحد الخيارات التالية:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    register_message(user_id, msg2.message_id, query.message.chat_id, context)

    await log_event(update, "عرض مراكز الخدمة الرسمية للمستخدم")
    
async def handle_branch_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data.split("_")
    user_id = int(data[1])

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go",
            show_alert=True,
        )
        return

    # 🧹 حذف فيديو المواقع السابق
    map_msg_id = context.user_data.get(user_id, {}).get("map_msg_id")
    if map_msg_id:
        try:
            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=map_msg_id)
        except:
            pass
        context.user_data[user_id]["map_msg_id"] = None

    # 🧹 حذف زر "📍 مواقع فروع شركة شيري" فقط
    try:
        keyboard = query.message.reply_markup.inline_keyboard
        updated_keyboard = [row for row in keyboard if not any("branches_" in button.callback_data for button in row)]
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(updated_keyboard))
    except:
        pass

    user_name = query.from_user.full_name
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")

    header = f"`🧑‍💼 استعلام خاص بـ {user_name}`"
    middle = "🚨 مواقع مراكز الصيانة شيري CHERY"
    footer = f"\n\n`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 5 دقائق ({delete_time} / 🇸🇦)`"

    branches = context.bot_data.get("branches", [])
    keyboard = []

    for branch in branches:
        city = str(branch.get("city", "")).strip()
        name = str(branch.get("branch_name", "")).strip()
        url = str(branch.get("url", "")).strip()

        if not city:
            continue

        label = f"📍 {city} / {name}" if name else f"📍 {city}"
        if url and url.startswith("http"):
            keyboard.append([InlineKeyboardButton(label, url=url)])
        else:
            keyboard.append([InlineKeyboardButton(label, callback_data=f"not_ready_{user_id}")])

    if not keyboard:
        await query.answer("❌ لا يوجد فروع صالحة للعرض حالياً.", show_alert=True)
        return

    msg = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f"{header}\n{middle}:{footer}",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=constants.ParseMode.MARKDOWN
    )

    register_message(user_id, msg.message_id, query.message.chat_id, context)
    await log_event(update, "عرض قائمة فروع مراكز شيري الرسمية")

async def handle_independent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = int(query.data.split("_")[1])

    # 🧹 حذف فيديو المواقع السابق
    map_msg_id = context.user_data.get(user_id, {}).get("map_msg_id")
    if map_msg_id:
        try:
            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=map_msg_id)
        except:
            pass
        context.user_data[user_id]["map_msg_id"] = None

    # 🧹 حذف زر "🔧 المتاجر ومراكز الصيانة المستقلة" فقط
    try:
        keyboard = query.message.reply_markup.inline_keyboard
        updated_keyboard = [row for row in keyboard if not any("independent_" in button.callback_data for button in row)]
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(updated_keyboard))
    except:
        pass

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go",
            show_alert=True
        )
        return

    context.user_data.setdefault(user_id, {})["independent_used"] = True

    image_path = "شروط-الصيانة.jpg"
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")

    if os.path.exists(image_path):
        with open(image_path, "rb") as image_file:
            caption = (
                f"`🧑‍💻 استعلام خاص بـ {query.from_user.full_name}`\n\n"
                f"📋 شروط الصيانة للمراكز المستقلة:\n\n"
                f"يمكنك إجراء الصيانة الدورية لدى المراكز المستقلة وفقًا لتعليمات الشركة الصانعة مع مراعاة تدوين البيانات كاملة بالفاتورة كما هو واضح في توجيه وزارة التجارة والاستثمار أعلاه\n\n"
                f"`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 5 دقائق ({delete_time} / 🇸🇦)`"
            )
            msg1 = await context.bot.send_photo(
                chat_id=query.message.chat_id,
                photo=image_file,
                caption=caption,
                parse_mode=constants.ParseMode.MARKDOWN
            )
            register_message(user_id, msg1.message_id, query.message.chat_id, context)

    cities = df_independent["city"].dropna().unique().tolist()
    city_buttons = [[InlineKeyboardButton(city, callback_data=f"setcity_{city}_{user_id}")] for city in cities]

    msg2 = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text="🌍 اختر المدينة لعرض المراكز والمتاجر مباشرة:",
        reply_markup=InlineKeyboardMarkup(city_buttons),
        parse_mode=constants.ParseMode.MARKDOWN,
    )
    register_message(user_id, msg2.message_id, query.message.chat_id, context)
    await log_event(update, "عرض شروط وخيارات المراكز المستقلة")
    context.user_data[user_id] = {}

async def set_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split("_")
    city = parts[1]
    user_id = int(parts[2])

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go",
            show_alert=True
        )
        return

    # 🔴 إزالة قفل تكرار المدينة
    # if context.user_data.get(user_id, {}).get("city_selected"):

    context.user_data.setdefault(user_id, {})["city"] = city

    try:
        await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
    except:
        pass

    keyboard = [
        [InlineKeyboardButton("✅ قائمة المراكز المعتمدة", callback_data=f"show_centers_{user_id}")],
        [InlineKeyboardButton("🛒 قائمة متاجر قطع الغيار", callback_data=f"show_stores_{user_id}")]
    ]

    msg = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f"`🧑‍💻 استعلام خاص بـ {query.from_user.full_name}`\n\n🔍 اختر نوع الخدمة بعد اختيار المدينة ({city}):",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=constants.ParseMode.MARKDOWN
    )

    register_message(user_id, msg.message_id, query.message.chat_id, context)
    await log_event(update, f"اختار مدينة: {city}")

async def _send_independent_results(update: Update, context: ContextTypes.DEFAULT_TYPE, filter_type: str):
    query = update.callback_query
    user_id = query.from_user.id
    city = context.user_data.get(user_id, {}).get("city")

    if not city:
        await query.answer("❌ لم يتم تحديد المدينة. استخدم /go لإعادة التحديد.", show_alert=True)
        return

    results = df_independent[
        (df_independent["city"] == city) & (df_independent["type"].str.contains(filter_type))
    ]

    if results.empty:
        msg = await query.message.reply_text(f"🚫 لا توجد بيانات {filter_type} حالياً في {city}.")
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, f"🚫 لا توجد نتائج {filter_type} في {city}", level="error")
        return

    user_name = query.from_user.full_name
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")

    for _, row in results.iterrows():
        name = row.get("name", "بدون اسم")
        phone = row.get("phone", "غير متوفر")
        activity_type = row.get("type", "غير محدد")
        city_name = row.get("city", "غير معروفة")
        location_url = row.get("location_url", "❌ لا يوجد رابط")
        image_url = row.get("image_url") if pd.notna(row.get("image_url", None)) else None

        caption = (
            f"`🧑‍💼 استعلام خاص بـ {user_name}`\n\n"
            f"🏪 *الاسم:* {name}\n"
            f"📞 الهاتف: {phone}\n"
            f"🏙️ المدينة: {city_name}\n"
            f"⚙️ النشاط: {activity_type}\n"
            f"🌐 [رابط الموقع]({location_url})\n\n"
            f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 5 دقائق ({delete_time} / 🇸🇦)`"
        )

        try:
            if image_url:
                msg = await context.bot.send_photo(
                    chat_id=query.message.chat_id,
                    photo=image_url,
                    caption=caption,
                    parse_mode=constants.ParseMode.MARKDOWN
                )
            else:
                msg = await query.message.reply_text(caption, parse_mode=constants.ParseMode.MARKDOWN)
        except:
            msg = await query.message.reply_text(caption, parse_mode=constants.ParseMode.MARKDOWN)

        register_message(user_id, msg.message_id, query.message.chat_id, context)

    await log_event(update, f"📜 عرض قائمة {filter_type} في {city}")

async def show_center_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = int(query.data.split("_")[2])

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go",
            show_alert=True
        )
        return

    if context.user_data.get(user_id, {}).get("centers_used"):
        await query.answer("❌ لا يمكنك تكرار هذا الاستعلام في نفس الجلسة، استخدم /go من جديد.", show_alert=True)
        return

    context.user_data.setdefault(user_id, {})["centers_used"] = True

    # حذف زر "المراكز المعتمدة" فقط
    try:
        keyboard = query.message.reply_markup.inline_keyboard
        updated_keyboard = [row for row in keyboard if not any("show_centers_" in btn.callback_data for btn in row)]
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(updated_keyboard))
    except:
        pass

    await _send_independent_results(update, context, filter_type="مركز")
    await log_event(update, f"📜 عرض قائمة المراكز المعتمدة في {context.user_data[user_id].get('city', 'غير معروفة')}")

async def show_store_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = int(query.data.split("_")[2])

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go",
            show_alert=True
        )
        return

    if context.user_data.get(user_id, {}).get("stores_used"):
        await query.answer("❌ لا يمكنك تكرار هذا الاستعلام في نفس الجلسة، استخدم /go من جديد.", show_alert=True)
        return

    context.user_data.setdefault(user_id, {})["stores_used"] = True

    # حذف زر "المتاجر" فقط
    try:
        keyboard = query.message.reply_markup.inline_keyboard
        updated_keyboard = [row for row in keyboard if not any("show_stores_" in btn.callback_data for btn in row)]
        await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(updated_keyboard))
    except:
        pass

    await _send_independent_results(update, context, filter_type="متجر")
    await log_event(update, f"📜 عرض قائمة المتاجر في {context.user_data[user_id].get('city', 'غير معروفة')}")
    
### 🟢 تحديث دالة button لتسجيل معلومات المجموعة بشكل صحيح:
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data.split("_")

    if query.data.startswith("catpart_"):
        # تعامل خاص مع أزرار التصنيفات
        _, keyword, user_id_str = data
        user_id = int(user_id_str)
        action = "catpart"
    else:
        # باقي الأنواع الأخرى مثل parts_1543 أو suggestion_123
        action, user_id_str = data[0], data[1]
        user_id = int(user_id_str)

    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go",
            show_alert=True
        )
        return

    chat = query.message.chat
    context.user_data.setdefault(user_id, {})
    context.user_data[user_id]["group_title"] = chat.title or "خاص"
    context.user_data[user_id]["group_id"] = chat.id

    if action == "parts":
        keyboard = [
            [InlineKeyboardButton("🔍 استعلام قطع غيار استهلاكية", callback_data=f"consumable_{user_id}")],
            [InlineKeyboardButton("🌐 استعلام قطع غيار عام", callback_data=f"external_{user_id}")]
        ]
        msg = await query.edit_message_text(
            "يرجى اختيار نوع الاستعلام عن قطع الغيار:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, "فتح قائمة قطع الغيار")
        return

    elif action == "external":
        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")
        keyboard = [[InlineKeyboardButton("🔗 فتح موقع الاستعلام", url="https://www.cheryksa.com/ar/spareparts")]]
        msg = await query.edit_message_text(
            f"🌐 تم تجهيز الرابط، اضغط الزر بالأسفل للانتقال إلى موقع استعلام قطع غيار شيري الرسمي:\n\n"
            f"`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 5 دقائق ({delete_time} / 🇸🇦)`",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=constants.ParseMode.MARKDOWN
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, "تم فتح رابط قطع الغيار الخارجي")
        return

    elif action == "consumable":
        car_categories = df_parts["Station No"].dropna().unique().tolist()
        keyboard = [[InlineKeyboardButton(car, callback_data=f"carpart_{car.replace(' ', '_')}_{user_id}")] for car in car_categories]
        context.user_data[user_id]["reselect_count"] = 0
        try:
            msg = await query.edit_message_text(
                "🚗 اختر فئة السيارة لاستعلام القطع:",
                reply_markup=InlineKeyboardMarkup(keyboard)
           )
            register_message(user_id, msg.message_id, query.message.chat_id, context)
        except telegram.error.BadRequest as e:
             if "Message is not modified" not in str(e):
                 raise  # فقط تجاهل الخطأ هذا، والباقي اظهره

        await log_event(update, "اختيار فئة السيارة لقطع الغيار")
        return

    elif action == "catpart":
        keyword = data[1].strip().lower()
        user_id = int(data[2])
        selected_car = context.user_data[user_id].get("selected_car")

        if not selected_car:
            await query.answer("❌ يرجى اختيار فئة السيارة أولاً.", show_alert=True)
            return

        filtered_df = df_parts[df_parts["Station No"] == selected_car]

    # توليد كلمات بحث متعددة بصيغ الجمع والمفرد (مبسطة)
        search_variants = [
            keyword,
            keyword + "ات" if not keyword.endswith("ات") else keyword[:-2],
            keyword.replace("ي", "ى") if "ي" in keyword else keyword,
            keyword.replace("ى", "ي") if "ى" in keyword else keyword,
            keyword  # تكرار للكلمة بدون تغيير (لضمان)
    ]
    # اجعلها فريدة
        search_variants = list(set(search_variants))

    # بناء pattern regex للبحث بجميع الكلمات في نفس الوقت (case insensitive)
        pattern = "|".join(re.escape(term) for term in search_variants)

    # البحث في العمود "Station Name"
        matches = filtered_df[
            filtered_df["Station Name"]
            .astype(str)
            .str.lower()
            .str.contains(pattern, regex=True, na=False)
    ]

        if matches.empty:
            await query.answer("❌ لا توجد نتائج ضمن هذا التصنيف.", show_alert=True)
            return

        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=5)).strftime("%I:%M %p")
        footer = f"\n\n<code>⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 5 دقائق ({delete_time} / 🇸🇦)</code>"

        user_name = query.from_user.full_name

        for i, row in matches.iterrows():
            part_name_value = row.get("Station Name", "غير معروف")
            part_number_value = row.get("Part No", "غير معروف")

            text = (
                f"<code>🧑‍💼 استعلام خاص بـ {user_name}</code>\n\n"
                f"🚗 <b>الفئة:</b> {selected_car}\n"
                f"🔹 <b>اسم القطعة:</b> {part_name_value}\n"
                f"🔹 <b>رقم القطعة:</b> {part_number_value}\n\n"
                f"📌 تم العثور على نتائج بناءً على التصنيف"
                + footer
            )

            keyboard = []
            if pd.notna(row.get("Image")):
                keyboard.append([InlineKeyboardButton("عرض الصورة 📸", callback_data=f"part_image_{i}_{user_id}")])

            msg = await query.message.reply_text(
                text, reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None, parse_mode=ParseMode.HTML
            )
            register_message(user_id, msg.message_id, query.message.chat_id, context)

        await log_event(update, f"✅ استعلام تصنيفي: {keyword} ضمن {selected_car}")
        return

    elif action == "maintenance":
        context.user_data[user_id]["action"] = "maintenance"
        cars = df_maintenance["car_type"].dropna().unique().tolist()
        keyboard = [[InlineKeyboardButton(car, callback_data=f"car_{car.replace(' ', '_')}_{user_id}")] for car in cars]
        msg = await query.edit_message_text(
            "اختر فئة السيارة 🚗 :",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, "فتح قائمة صيانة دورية")
        return

    elif action == "suggestion":
        context.user_data[user_id]["action"] = "suggestion"
        msg = await query.edit_message_text("✉️ يرجى كتابة اقتراحك أو ملاحظتك أدناه:")
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, "بدأ المستخدم إرسال اقتراح أو ملاحظة")

        # ✅ بدء جلسة جديدة إن لم تكن موجودة
        if "active_suggestion_id" not in context.user_data[user_id]:
            suggestion_id = await start_suggestion_session(user_id, context)
        else:
            suggestion_id = context.user_data[user_id]["active_suggestion_id"]

        # ✅ حفظ معلومات المجموعة داخل الجلسة الصحيحة
        suggestion_records[user_id][suggestion_id]["group_name"] = chat.title if chat.title else "خاص"
        suggestion_records[user_id][suggestion_id]["group_id"] = chat.id
        suggestion_records[user_id][suggestion_id]["user_name"] = update.effective_user.full_name
        return

### ✅ الدالة المعدلة: handle_suggestion
async def handle_suggestion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    user_id = user.id

    # ✅ السماح بجلسة جديدة إذا عاد من المجموعة
    if (
        user_id in suggestion_records and
        suggestion_records[user_id].get("submitted") and
        not context.user_data.get(user_id, {}).get("from_group")
    ):
        await update.message.reply_text("⚠️ لا يمكنك إرسال اقتراح جديد قبل الانتهاء من الاقتراح الحالي.")
        return

    # ✅ حفظ السياق إن جاء من مجموعة
    if chat.type != "private":
        context.user_data[user_id] = {"from_group": True}
    else:
        context.user_data[user_id] = {}

    # ✅ تأكيد تسجيل اسم المجموعة ورقمها داخل user_data لضمان استخدامها لاحقًا
    context.user_data.setdefault(user_id, {})
    if chat.type != "private":
        context.user_data[user_id]["group_title"] = chat.title or "غير معروف"
        context.user_data[user_id]["group_id"] = chat.id
    else:
        context.user_data[user_id]["group_title"] = "خاص"
        context.user_data[user_id]["group_id"] = "غير معروف"

    # ✅ إنشاء سجل جديد دائمًا
    suggestion_records[user_id] = {
        "text": None,
        "media": None,
        "admin_messages": {},
        "group_name": chat.title if chat.type != "private" else "خاص",
        "group_id": chat.id if chat.type != "private" else "غير معروف",
        "replied_by": None,
        "caption": None
    }

    await update.message.reply_text("✉️ يرجى كتابة اقتراحك أو إرسال صورة أو فيديو أو ملف كملاحظة.")
    await log_event(update, "بدأ المستخدم إرسال اقتراح أو ملاحظة")

async def handle_suggestion_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data_parts = query.data.split("_")

    if len(data_parts) < 3 or not data_parts[1].isdigit():
        await query.answer("❌ لا يمكن معالجة الطلب، البيانات غير مكتملة.", show_alert=True)
        return

    user_id = int(data_parts[1])
    suggestion_id = data_parts[2]
    admin_id = query.from_user.id
    admin_name = query.from_user.full_name

    if admin_id not in AUTHORIZED_USERS:
        await query.answer("❌ غير مصرح لك بالرد.", show_alert=True)
        return

    record = suggestion_records.get(user_id, {}).get(suggestion_id)
    if not record:
        await query.answer("❌ لا يوجد سجل لهذا الاقتراح.", show_alert=True)
        return

    if record.get("replied_by") and record.get("caption"):
        await query.answer(
            f"🟥 تم الرد على هذا الاقتراح مسبقًا من قبل: {record['replied_by']}",
            show_alert=True
        )
        return

    record["reply_opened_by"] = admin_name
    record["user_name"] = record.get("user_name", query.from_user.full_name)

    # ✅ تصحيح بيانات المجموعة إذا كانت ناقصة أو غير صحيحة
    if record.get("group_name") in ["خاص", None] or record.get("group_id") == user_id:
        user_ctx = context.user_data.get(user_id, {})
        record["group_name"] = user_ctx.get("group_title") or user_ctx.get("final_group_name", "غير معروف")
        record["group_id"] = user_ctx.get("group_id") or user_ctx.get("final_group_id", "غير معروف")

    keyboard = [
        [InlineKeyboardButton(text, callback_data=f"sendreply_{key}_{user_id}_{suggestion_id}")]
        for key, text in SUGGESTION_REPLIES.items()
    ]
    keyboard.append([InlineKeyboardButton("✍️ كتابة رد مخصص", callback_data=f"customreply_{user_id}_{suggestion_id}")])

    msg = await context.bot.send_message(
        chat_id=admin_id,
        text=(
            "✉️ اختر نوع الرد المناسب لإرساله للمستخدم\n\n"
            f"👤 <b>اسم المستخدم:</b> {record.get('user_name')}\n"
            f"🆔 <b>رقم المستخدم:</b> <code>{user_id}</code>\n"
            f"🏘️ <b>المجموعة:</b> {record.get('group_name')}\n"
            f"🔢 <b>رقم المجموعة:</b> <code>{record.get('group_id')}</code>"
        ),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )

    # حذف القائمة القديمة إن وجدت
    if "reply_menu_chat" in record and "reply_menu_id" in record:
        try:
            await context.bot.delete_message(record["reply_menu_chat"], record["reply_menu_id"])
        except:
            pass

    record["reply_menu_id"] = msg.message_id
    record["reply_menu_chat"] = msg.chat_id

### ✅ الدالة المعدلة: send_suggestion
async def send_suggestion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    suggestion_id = context.user_data.get(user_id, {}).get("active_suggestion_id")
    if not suggestion_id:
        await query.answer("⚠️ لا توجد جلسة اقتراح نشطة.", show_alert=True)
        return

    record = suggestion_records.get(user_id, {}).get(suggestion_id)
    if not record:
        await query.answer("⚠️ لا يوجد اقتراح محفوظ.", show_alert=True)
        return

    text = record.get("text", "")
    media = record.get("media")

    if not text and not media:
        await query.answer("⚠️ لا يمكن إرسال اقتراح فارغ.", show_alert=True)
        return

    record.pop("replied_by", None)
    record.pop("caption", None)

    user_name = query.from_user.full_name
    record["user_name"] = user_name

    # ✅ استخدام القيم النهائية المضمونة من user_data
    user_context = context.user_data.get(user_id, {})
    group_name = user_context.get("final_group_name", "غير معروف")
    group_id = user_context.get("final_group_id", "غير معروف")
    record["group_name"] = group_name
    record["group_id"] = group_id

    logging.info(f"[تأكيد المجموعة] المستخدم: {user_id} | المجموعة: {group_name} | ID: {group_id}")

    header = (
        f"👤 الاسم: {user_name}\n"
        f"🆔 رقم المستخدم: <code>{user_id}</code>\n"
        f"🏘️ المجموعة: {group_name}\n"
        f"🔢 رقم المجموعة: <code>{group_id}</code>\n"
        "╰─────────╯"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 الرد على الاقتراح", callback_data=f"reply_{user_id}_{suggestion_id}")]
    ])

    record["admin_messages"] = {}

    for admin_id in AUTHORIZED_USERS:
        try:
            sent = None
            full_caption = header
            if media:
                mtype = media["type"]
                fid = media["file_id"]
                if text:
                    full_caption += f"\n\n📝 <b>الاقتراح:</b>\n{text}"
                if mtype == "photo":
                    sent = await context.bot.send_photo(admin_id, fid, caption=full_caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                elif mtype == "video":
                    sent = await context.bot.send_video(admin_id, fid, caption=full_caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                elif mtype == "document":
                    sent = await context.bot.send_document(admin_id, fid, caption=full_caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                elif mtype == "voice":
                    sent = await context.bot.send_voice(admin_id, fid, caption=full_caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            else:
                suggestion_block = f"\n\n📝 <b>الاقتراح:</b>\n<code>{text}</code>" if text else ""
                full_caption += suggestion_block
                sent = await context.bot.send_message(admin_id, text=full_caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)

            if sent:
                record["admin_messages"][admin_id] = sent.message_id

        except Exception as e:
            logging.error(f"[اقتراح] فشل في إرسال الاقتراح للمشرف {admin_id}: {e}")

    record["submitted"] = True
    record["timestamp"] = datetime.now()

    try:
        await query.message.delete()
    except:
        pass

    await context.bot.send_message(
        chat_id=user_id,
        text="🎉 شكرًا لمساهمتك معنا!\n\n✅ تم إرسال اقتراحك بنجاح إلى فريق GO.\n\n📌 لمراجعته وتنفيذه اذا امكن .",
        parse_mode=ParseMode.MARKDOWN
    )

    context.user_data.pop(user_id, None)
    
async def handle_send_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data_parts = query.data.split("_")

    if len(data_parts) < 4:
        await query.answer("❌ تنسيق البيانات غير صحيح.", show_alert=True)
        return

    reply_key = data_parts[1]
    user_id = int(data_parts[2])
    suggestion_id = data_parts[3]
    admin_id = query.from_user.id
    admin_name = query.from_user.full_name

    record = suggestion_records.get(user_id, {}).get(suggestion_id)
    if not record:
        await query.answer("❌ لا يوجد سجل لهذا الاقتراح.", show_alert=True)
        return

    if record.get("replied_by"):
        await query.answer("🟥 تم الرد مسبقًا على هذا الاقتراح.", show_alert=True)
        return

    # 🔁 تصحيح بيانات المجموعة حتى لو كانت الوسائط فقط
    if record.get("group_name") in ["خاص", None] or record.get("group_id") in [None, user_id]:
        record["group_name"] = context.user_data.get(user_id, {}).get("group_title", "غير معروف")
        record["group_id"] = context.user_data.get(user_id, {}).get("group_id", "غير معروف")

    group_name = record.get("group_name", "غير معروف")
    group_id = record.get("group_id", "غير معروف")
    user_name = record.get("user_name", "—")
    original_text = record.get("text") or "❓ لا يوجد اقتراح محفوظ."
    reply_text = SUGGESTION_REPLIES.get(reply_key, "📌 تم الرد على اقتراحك.")
    has_media = record.get("media")

    # ✅ رسالة المستخدم
    user_caption = (
        f"\u200F📣 *رد من برنامج GO:*\n\n"
        f"\u200F📝 *اقتراحك:*\n"
        f"```{original_text.strip()}```\n\n"
        f"\u200F💬 *رد المشرف:*\n"
        f"```{reply_text.strip()}```\n\n"
        f"\u200F🤖 *شكرًا لمساهمتك معنا.*"
    )

    # ✅ رسالة المشرفين
    admin_caption = (
        f"\u200F📣 *رد من برنامج GO:*\n\n"
        f"\u200F👤 `{user_name}`\n"
        f"\u200F🆔 {user_id}\n"
        f"\u200F🏘️ \u202B{group_name}\u202C\n"
        f"\u200F🔢 `{group_id}`\n"
        + (f"\u200F📎 يحتوي على وسائط\n" if has_media else "") + "\n"
        f"\u200F📝 *الاقتراح:*\n```{original_text.strip()}```\n\n"
        f"\u200F💬 *رد المشرف:*\n```{reply_text.strip()}```\n\n"
        f"\u200F✅ تم الرد من قبل: `{admin_name}`"
    )

    try:
        media = record.get("media")
        if media:
            mtype = media["type"]
            fid = media["file_id"]
            if mtype == "photo":
                await context.bot.send_photo(user_id, fid, caption=user_caption, parse_mode=ParseMode.MARKDOWN)
            elif mtype == "video":
                await context.bot.send_video(user_id, fid, caption=user_caption, parse_mode=ParseMode.MARKDOWN)
            elif mtype == "document":
                await context.bot.send_document(user_id, fid, caption=user_caption, parse_mode=ParseMode.MARKDOWN)
            elif mtype == "voice":
                await context.bot.send_voice(user_id, fid, caption=user_caption, parse_mode=ParseMode.MARKDOWN)
        else:
            with open("GO-now.jpg", "rb") as image:
                await context.bot.send_photo(user_id, image, caption=user_caption, parse_mode=ParseMode.MARKDOWN)

        record["replied_by"] = admin_name
        record["caption"] = user_caption

        try:
            await query.message.delete()
        except:
            pass

        # حذف قائمة الرد
        if "reply_menu_chat" in record and "reply_menu_id" in record:
            for aid in AUTHORIZED_USERS:
                try:
                    await context.bot.delete_message(record["reply_menu_chat"], record["reply_menu_id"])
                except:
                    pass
            record.pop("reply_menu_chat", None)
            record.pop("reply_menu_id", None)

        # إرسال نسخة للمشرفين
        for aid in AUTHORIZED_USERS:
            try:
                if media:
                    mtype = media["type"]
                    fid = media["file_id"]
                    if mtype == "photo":
                        await context.bot.send_photo(aid, fid, caption=admin_caption, parse_mode=ParseMode.MARKDOWN)
                    elif mtype == "video":
                        await context.bot.send_video(aid, fid, caption=admin_caption, parse_mode=ParseMode.MARKDOWN)
                    elif mtype == "document":
                        await context.bot.send_document(aid, fid, caption=admin_caption, parse_mode=ParseMode.MARKDOWN)
                    elif mtype == "voice":
                        await context.bot.send_voice(aid, fid, caption=admin_caption, parse_mode=ParseMode.MARKDOWN)
                else:
                    with open("GO-now.jpg", "rb") as image:
                        await context.bot.send_photo(aid, image, caption=admin_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logging.warning(f"[رد جاهز - إشعار مشرف {aid}] فشل: {e}")

        context.user_data.pop(admin_id, None)

    except Exception as e:
        logging.error(f"[رد جاهز] فشل في إرسال الرد للمستخدم {user_id}: {e}")

async def handle_custom_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    admin_id = query.from_user.id

    if not data.startswith("customreply_"):
        await query.answer("🚫 بيانات غير صالحة.", show_alert=True)
        return

    try:
        parts = data.split("_")
        user_id = int(parts[1])
        suggestion_id = parts[2]
    except Exception:
        await query.answer("🚫 فشل في استخراج بيانات الاقتراح.", show_alert=True)
        return

    record = suggestion_records.get(user_id, {}).get(suggestion_id)
    if not record:
        await query.answer("❌ لا يوجد سجل لهذا الاقتراح.", show_alert=True)
        return

    # ✅ تصحيح معلومات المجموعة إن كانت ناقصة
    if record.get("group_name") in ["خاص", None] or record.get("group_id") in [None, user_id]:
        record["group_name"] = context.user_data.get(user_id, {}).get("group_title", "غير معروف")
        record["group_id"] = context.user_data.get(user_id, {}).get("group_id", "غير معروف")

    # 📌 تفعيل وضع الإدخال اليدوي
    context.user_data.setdefault(admin_id, {})
    context.user_data[admin_id]["compose_mode"] = "custom_reply"
    context.user_data[admin_id]["custom_reply_for"] = user_id
    context.user_data[admin_id]["active_suggestion_id"] = suggestion_id

    msg = await query.message.reply_text(
        f"✍️ أرسل الآن الرد المخصص ليتم إرساله للمستخدم `{user_id}`:",
        parse_mode=ParseMode.MARKDOWN
    )

    # ✅ تسجيل الرسالة للحذف التلقائي إن أردت
    register_message(admin_id, msg.message_id, query.message.chat_id, context)

### ✅ الدالة المعدلة: submit_admin_reply
async def submit_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    admin_id = query.from_user.id
    user_id = context.user_data.get(admin_id, {}).get("custom_reply_for")
    suggestion_id = context.user_data.get(admin_id, {}).get("active_suggestion_id")

    if not user_id or not suggestion_id:
        await query.answer("❌ لا توجد جلسة رد نشطة.", show_alert=True)
        return

    record = suggestion_records.get(user_id, {}).get(suggestion_id)
    if not record:
        await query.answer("❌ لا يوجد سجل لهذا الاقتراح.", show_alert=True)
        return

    if record.get("replied_by"):
        await query.answer("🟥 تم الرد مسبقًا على هذا الاقتراح.", show_alert=True)
        return

    text = context.user_data[admin_id].get("compose_text")

    # ✅ معالجة الوسائط من المشرف أو من المستخدم
    media = context.user_data[admin_id].get("compose_media")
    if not media and record.get("media"):
        media = record["media"]
    elif media:
        record["media"] = media  # حفظ وسائط المشرف داخل السجل

    if not text and not media:
        await query.answer("⚠️ لا يمكن إرسال رد فارغ.", show_alert=True)
        return

    user_name = record.get("user_name", "—")
    original_text = record.get("text", "❓ لا يوجد اقتراح محفوظ.")
    admin_name = update.effective_user.full_name
    has_media = bool(media)

    # ⛑️ تصحيح بيانات المجموعة
    if record.get("group_name") in ["خاص", None] or record.get("group_id") == user_id:
        record["group_name"] = context.user_data.get(user_id, {}).get("group_title", "غير معروف")
        record["group_id"] = context.user_data.get(user_id, {}).get("group_id", "غير معروف")

    group_name = record.get("group_name", "غير معروف")
    group_id = record.get("group_id", "غير معروف")

    user_caption = (
        f"\u200F📣 *رد من برنامج GO:*\n\n"
        f"\u200F📝 *اقتراحك:*\n```{original_text.strip()}```\n\n"
        f"\u200F💬 *رد المشرف:*\n```{text.strip()}```\n\n"
        f"\u200F🤖 *شكرًا لمساهمتك معنا.*"
    )

    admin_caption = (
        f"\u200F📣 *رد من برنامج GO:*\n\n"
        f"\u200F👤 `{user_name}`\n"
        f"\u200F🆔 {user_id}\n"
        f"\u200F🏘️ \u202B{group_name}\u202C\n"
        f"\u200F🔢 `{group_id}`\n"
        + (f"\u200F📎 يحتوي على وسائط\n" if has_media else "") + "\n"
        f"\u200F📝 *الاقتراح:*\n```{original_text.strip()}```\n\n"
        f"\u200F💬 *رد المشرف:*\n```{text.strip()}```\n\n"
        f"\u200F✅ تم الرد من قبل: `{admin_name}`"
    )

    try:
        # ✅ إرسال الرد للمستخدم
        if media:
            mtype = media["type"]
            fid = media["file_id"]
            if mtype == "photo":
                await context.bot.send_photo(user_id, fid, caption=user_caption, parse_mode=ParseMode.MARKDOWN)
            elif mtype == "video":
                await context.bot.send_video(user_id, fid, caption=user_caption, parse_mode=ParseMode.MARKDOWN)
            elif mtype == "document":
                await context.bot.send_document(user_id, fid, caption=user_caption, parse_mode=ParseMode.MARKDOWN)
            elif mtype == "voice":
                await context.bot.send_voice(user_id, fid, caption=user_caption, parse_mode=ParseMode.MARKDOWN)
        else:
            with open("GO-now.jpg", "rb") as image:
                await context.bot.send_photo(user_id, image, caption=user_caption, parse_mode=ParseMode.MARKDOWN)

        record["replied_by"] = admin_name
        record["caption"] = user_caption

        try:
            await query.message.delete()
        except:
            pass

        if "reply_menu_chat" in record and "reply_menu_id" in record:
            for aid in AUTHORIZED_USERS:
                try:
                    await context.bot.delete_message(record["reply_menu_chat"], record["reply_menu_id"])
                except:
                    pass
            record.pop("reply_menu_chat", None)
            record.pop("reply_menu_id", None)

        for aid in AUTHORIZED_USERS:
            try:
                if media:
                    mtype = media["type"]
                    fid = media["file_id"]
                    if mtype == "photo":
                        await context.bot.send_photo(aid, fid, caption=admin_caption, parse_mode=ParseMode.MARKDOWN)
                    elif mtype == "video":
                        await context.bot.send_video(aid, fid, caption=admin_caption, parse_mode=ParseMode.MARKDOWN)
                    elif mtype == "document":
                        await context.bot.send_document(aid, fid, caption=admin_caption, parse_mode=ParseMode.MARKDOWN)
                    elif mtype == "voice":
                        await context.bot.send_voice(aid, fid, caption=admin_caption, parse_mode=ParseMode.MARKDOWN)
                else:
                    with open("GO-now.jpg", "rb") as image:
                        await context.bot.send_photo(aid, image, caption=admin_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logging.warning(f"[رد مخصص - إشعار مشرف {aid}] فشل: {e}")

        context.user_data.pop(admin_id, None)

    except Exception as e:
        logging.error(f"[رد مخصص] فشل في إرسال الرد للمستخدم {user_id}: {e}")

# ✅ لوحة التحكم الإدارية
async def handle_control_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # 🧠 سجل محاولة الدخول
    await log_event(update, "🛠️ المستخدم طلب الدخول إلى لوحة التحكم")

    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("🚫 غير مصرح لك بالدخول إلى لوحة التحكم.")
        return

    keyboard = [
        [InlineKeyboardButton("👤 المشرفون", callback_data="admins_menu")],
        [InlineKeyboardButton("📊 الإحصائيات", callback_data="show_stats")],
        [InlineKeyboardButton("🧹 تنظيف الجلسات", callback_data="clear_sessions")],
        [InlineKeyboardButton("♻️ إعادة تحميل الإعدادات", callback_data="reload_settings")],
        [InlineKeyboardButton("🚧 تفعيل وضع الصيانة", callback_data="ctrl_maintenance_on")],
        [InlineKeyboardButton("✅ إنهاء وضع الصيانة", callback_data="ctrl_maintenance_off")],
        [InlineKeyboardButton("🧨 تدمير البيانات", callback_data="self_destruct")],
        [InlineKeyboardButton("🔁 إعادة تشغيل الجلسة", callback_data="restart_session")],
        [InlineKeyboardButton("🚪 خروج", callback_data="exit_control")]
    ]

    await update.message.reply_text(
        "🎛️ *لوحة التحكم الخاصة بالمشرفين*\n\nيرجى اختيار الإجراء المطلوب:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

# ✅ معالجة الضغط على أزرار الصيانة
async def handle_control_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    global df_admins

    if user_id not in AUTHORIZED_USERS:
        await query.answer("🚫 لا تملك صلاحية التحكم.", show_alert=True)
        return

    if query.data == "control_back":
        await query.message.edit_text(
            "🛠️ *لوحة التحكم:*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👤 المشرفون", callback_data="admins_menu")],
                [InlineKeyboardButton("📊 الإحصائيات", callback_data="show_stats")],
                [InlineKeyboardButton("🧹 تنظيف الجلسات", callback_data="clear_sessions")],
                [InlineKeyboardButton("♻️ إعادة تحميل الإعدادات", callback_data="reload_settings")],
                [InlineKeyboardButton("🚧 تفعيل وضع الصيانة", callback_data="ctrl_maintenance_on")],
                [InlineKeyboardButton("✅ إنهاء وضع الصيانة", callback_data="ctrl_maintenance_off")],
                [InlineKeyboardButton("🧨 تدمير البيانات", callback_data="self_destruct")],
                [InlineKeyboardButton("🔁 إعادة تشغيل الجلسة", callback_data="restart_session")],
                [InlineKeyboardButton("🚪 خروج", callback_data="exit_control")]
            ]),
            parse_mode=constants.ParseMode.MARKDOWN
        )
        return

    if query.data == "exit_control":
        await query.message.delete()
        return

    if query.data == "ctrl_maintenance_on":
        context.bot_data["maintenance_mode"] = True
        await query.answer("🚧 تم تفعيل وضع الصيانة.", show_alert=True)
        await query.message.edit_text("🚧 تم تفعيل وضع الصيانة. البوت الآن غير متاح للمستخدمين.")
        return

    if query.data == "ctrl_maintenance_off":
        context.bot_data["maintenance_mode"] = False
        await query.answer("✅ تم إنهاء وضع الصيانة.", show_alert=True)
        await query.message.edit_text("✅ تم إنهاء وضع الصيانة. البوت الآن يعمل بشكل طبيعي.")
        return

    if query.data == "self_destruct":
        if user_id == 1543083749:
            await query.answer("💣 لاتملك هذي الصلاحية  (تدمير البيانات).", show_alert=True)
        else:
            await query.answer("🚫 أنت لا تملك الصلاحية لتنفيذ هذا الإجراء.", show_alert=True)
        return

    if query.data == "admins_menu":
        await query.message.edit_text(
            "👤 *إدارة المشرفين: اختر الإجراء المطلوب*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📑 عرض المشرفين", callback_data="list_admins")],
                [InlineKeyboardButton("➕ إضافة مشرف", callback_data="add_admin")],
                [InlineKeyboardButton("🗑️ حذف مشرف", callback_data="delete_admin")],
                [InlineKeyboardButton("⬅️ عودة", callback_data="control_back")]
            ]),
            parse_mode=constants.ParseMode.MARKDOWN
        )
        return

    if query.data == "list_admins":
        try:
            rows = []
            for i, row in df_admins.iterrows():
                id_ = int(row["manager_id"])
                try:
                    user = await context.bot.get_chat(id_)
                    name = user.full_name
                except:
                    name = "❓ غير معروف"
                rows.append(f"{i+1}. {name}\n🆔 `{id_}`")
            await query.message.edit_text(
                "📑 *قائمة المشرفين:*\n\n" + "\n\n".join(rows),
                parse_mode=constants.ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ عودة", callback_data="admins_menu")]])
            )
        except Exception as e:
            await query.message.reply_text(f"❌ فشل في تحميل القائمة: {e}")
        return

    if query.data == "add_admin":
        context.user_data[user_id] = {"action": "awaiting_new_admin_id"}
        await query.message.reply_text("✏️ أرسل الآن رقم ID الخاص بالمشرف الجديد.")
        return

    if query.data == "delete_admin":
        context.user_data[user_id] = {"action": "awaiting_admin_removal"}
        await query.message.reply_text("🗑️ أرسل رقم ID للمشرف الذي ترغب بحذفه نهائيًا.")
        return

    if query.data == "clear_sessions":
        removed_count = cleanup_old_sessions(context)
        await query.answer("🧼 تم تنفيذ التنظيف", show_alert=False)
        await query.message.edit_text(
            f"🧹 تم تنظيف الجلسات المؤقتة.\n📌 عدد الرسائل المحذوفة: {removed_count}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ عودة", callback_data="control_back")]])
        )
        return

    if query.data == "show_stats":
        try:
            total_users = len(ALL_USERS)
            current_active = len(user_sessions)

            try:
                stats_df = pd.read_excel("bot_data.xlsx", sheet_name="bot_stats")
                if "total_go_uses" in stats_df["key"].values:
                    total_go = stats_df.loc[stats_df["key"] == "total_go_uses", "value"].values[0]
                else:
                    total_go = 0
            except:
                total_go = 0

            await query.message.edit_text(
                f"📈 *عدد العمليات الكلي:* `{total_users}`\n"
                f"👥 *المتفاعلين حاليًا:* `{current_active}`\n"
                f"🚀 *مرات استخدام /go:* `{total_go}`",
                parse_mode=constants.ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ عودة", callback_data="control_back")]
                ])
            )
        except Exception as e:
            await query.message.edit_text(
                f"❌ فشل أثناء عرض الإحصائيات:\n{e}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ عودة", callback_data="control_back")]
                ])
            )
        return

    if query.data == "reload_settings":
        try:
            df_admins = pd.read_excel("bot_data.xlsx", sheet_name="managers")
            AUTHORIZED_USERS.clear()
            for _, row in df_admins.iterrows():
                AUTHORIZED_USERS.append(int(row["manager_id"]))
            await query.message.edit_text("✅ تم إعادة تحميل ملف الإعدادات وتحديث البيانات.",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ عودة", callback_data="control_back")]]))
        except Exception as e:
            await query.message.edit_text(f"❌ حدث خطأ أثناء تحميل الإعدادات:\n{e}",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ عودة", callback_data="control_back")]]))
        return

    if query.data == "restart_session":
        context.user_data.clear()
        context.bot_data.clear()
        await query.answer("🔁 تم إعادة تشغيل الجلسة بنجاح.", show_alert=True)
        await query.message.edit_text("♻️ تم تفريغ جميع بيانات الجلسة.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ عودة", callback_data="control_back")]]))
        return

async def handle_add_admin_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message = update.message

    if context.user_data.get(user_id, {}).get("action") != "awaiting_new_admin_id":
        return  # تجاهل الرسائل خارج السياق

    new_admin_id_text = message.text.strip()
    if not new_admin_id_text.isdigit():
        await message.reply_text("❌ يجب إدخال رقم ID رقمي صالح.")
        return

    new_admin_id = int(new_admin_id_text)

    global df_admins  # ✅ استخدم النسخة المحملة في الذاكرة

    if new_admin_id in AUTHORIZED_USERS:
        await message.reply_text("ℹ️ هذا المشرف موجود مسبقًا.")
        return

    # ✅ إضافة إلى القائمة الحالية
    AUTHORIZED_USERS.append(new_admin_id)
    df_admins = pd.concat([df_admins, pd.DataFrame([{"manager_id": new_admin_id}])], ignore_index=True)

    # ✅ حفظ التغييرات في الملف Excel
    try:
        with pd.ExcelWriter("bot_data.xlsx", engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df_admins.to_excel(writer, sheet_name="managers", index=False)

        await message.reply_text(f"✅ تم إضافة المشرف بنجاح: `{new_admin_id}`", parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        await message.reply_text(f"❌ حدث خطأ أثناء حفظ التغييرات:\n{e}")

    # 🧼 مسح الحالة
    context.user_data[user_id]["action"] = None

# 🟢 أوامر البداية (start/go)
application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("go", start))
application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"(?i)^go$"), handle_go_text))
application.add_handler(CommandHandler("go25s", handle_control_panel))
application.add_handler(CallbackQueryHandler(
    handle_control_buttons,
    pattern="^(ctrl_maintenance_on|ctrl_maintenance_off|reload_settings|add_admin|list_admins|clear_sessions|show_stats|self_destruct|exit_control|control_back|admins_menu|restart_session|delete_admin)$"
))

# 🟢 استجابات المستخدم العامة (اقتراحات، صيانة، قطع غيار... إلخ)
application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))

# 🟢 خدمات الصيانة وقطع الغيار
application.add_handler(CallbackQueryHandler(car_choice, pattern=r"^car_.*_\d+$"))
application.add_handler(CallbackQueryHandler(km_choice, pattern=r"^km_.*_\d+$"))
application.add_handler(CallbackQueryHandler(send_cost, pattern=r"^cost_\d+_\d+$"))
application.add_handler(CallbackQueryHandler(send_brochure, pattern=r"^brochure_\d+_\d+$"))
application.add_handler(CallbackQueryHandler(send_part_image, pattern=r"^part_image_\d+_\d+$"))
application.add_handler(CallbackQueryHandler(button, pattern=r"^catpart_.*_\d+$"))

# 🟢 القائمة الرئيسية: صيانة - قطع غيار - دليل - مراكز - اقتراح
application.add_handler(CallbackQueryHandler(button, pattern=r"^(parts|maintenance|consumable|external|suggestion)_\d+$"))
application.add_handler(CallbackQueryHandler(select_car_for_parts, pattern="^carpart_"))

# 🟢 مراكز الخدمة الرسمية والمستقلة
application.add_handler(CallbackQueryHandler(handle_service_centers, pattern=r"^service_\d+$"))
application.add_handler(CallbackQueryHandler(handle_branch_list, pattern=r"^branches_\d+$"))
application.add_handler(CallbackQueryHandler(handle_independent, pattern=r"^independent_\d+$"))
application.add_handler(CallbackQueryHandler(show_center_list, pattern=r"^show_centers_\d+$"))
application.add_handler(CallbackQueryHandler(show_store_list, pattern=r"^show_stores_\d+$"))
application.add_handler(CallbackQueryHandler(set_city, pattern=r"^setcity_.*_\d+$"))

# 🟢 دليل المالك
application.add_handler(CallbackQueryHandler(show_manual_car_list, pattern="^manual_"))
application.add_handler(CallbackQueryHandler(handle_manualcar, pattern=r"^manualcar_.*_\d+$"))
application.add_handler(CallbackQueryHandler(handle_manualdfcar, pattern="^openpdf_"))

# 🟢 نظام الاقتراحات (نسخة مصححة ومتوافقة مع الهيكل الجديد)
application.add_handler(CallbackQueryHandler(send_suggestion, pattern=r"^send_suggestion$"))
application.add_handler(CallbackQueryHandler(handle_suggestion_reply, pattern=r"^reply_\d+_.+$"))
application.add_handler(CallbackQueryHandler(handle_send_reply, pattern=r"^sendreply_[a-zA-Z0-9]+_\d+_.+$"))
application.add_handler(CallbackQueryHandler(handle_custom_reply, pattern=r"^customreply_\d+_.+$"))
application.add_handler(CallbackQueryHandler(submit_admin_reply, pattern=r"^submit_admin_reply$"))

# 🟢 زر "إلغاء" لأي إجراء
application.add_handler(CallbackQueryHandler(handle_cancel, pattern=r"^cancel_"))

# 🟠 أزرار غير نشطة
application.add_handler(
    CallbackQueryHandler(
        lambda u, c: asyncio.create_task(u.callback_query.answer("🚫 هذا الزر غير نشط حالياً.")),
        pattern="^disabled$"
    )
)

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"message": "Bot is alive"}

@app.post("/webhook")
async def webhook_handler(request: Request):
    json_data = await request.json()
    update = Update.de_json(json_data, application.bot)
    await application.update_queue.put(update)
    return {"ok": True}

@app.on_event("startup")
async def on_startup():
    import requests

    # 🔄 تحديث Webhook مرة واحدة عند تشغيل التطبيق (اختياري لكن مفيد)
    webhook_url = os.getenv("RENDER_EXTERNAL_URL") or "https://chery-go.onrender.com/webhook"
    response = requests.get(f"https://api.telegram.org/bot{API_TOKEN}/setWebhook?url={webhook_url}")
    print(f"🔗 Webhook set: {response.status_code}")

    await application.initialize()
    await application.start()

    # ✅ تفعيل JobQueue (تنظيف الجلسات القديمة فقط)
    if application.job_queue:
        application.job_queue.run_repeating(cleanup_old_sessions, interval=60 * 60)
        print("✅ JobQueue تم تشغيلها")
    else:
        print("⚠️ job_queue غير مفعلة أو غير جاهزة")

# ✅ اختياري للتشغيل المحلي (ليس مطلوبًا في Render)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
