import os
import html
import asyncio
import logging
import pandas as pd
from uuid import uuid4
from datetime import datetime, timezone, timedelta, time
from pathlib import Path
import shutil
from typing import Optional
from fastapi import FastAPI, Request
import telegram.ext._jobqueue as tg_jobqueue
from telegram.error import BadRequest
import types
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand, constants, Chat, CallbackQuery
)
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder, Application, CommandHandler,
    CallbackQueryHandler, MessageHandler, ContextTypes, filters
)

# -----------------------------------------------------------
# 1) سجلات GO للاقتراحات والنقاشات
# -----------------------------------------------------------

suggestion_records = {}  # جميع اقتراحات المستخدمين

team_threads: dict[int, dict] = {}  # نقاشات فريق GO الداخلية
TEAM_THREAD_COUNTER = 0
# عدّاد استخدام GO في الذاكرة فقط (بدون كتابة مباشرة على Excel)
GLOBAL_GO_COUNTER = 0

# -----------------------------------------------------------
# 2) نظام السجلات
# -----------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

# -----------------------------------------------------------
# 3) تصحيح set_application داخل JobQueue لإزالة weakref
# -----------------------------------------------------------

def _patched_set_application(self, application):
    """استبدال weakref بـ lambda للحفاظ على التطبيق دائماً."""
    self._application = lambda: application

tg_jobqueue.JobQueue.set_application = _patched_set_application

# -----------------------------------------------------------
# 4) إعداد التوكن
# -----------------------------------------------------------

API_TOKEN = os.getenv("TELEGRAM_TOKEN")

# -----------------------------------------------------------
# 5) تعريف initial_branches لتفادي NameError
# -----------------------------------------------------------

initial_branches = {
    "سنابل الحديثة": [],
    "إكسيد": [],
    "جيتور": [],
    "أومودا": [],
    "BYD": [],
    "سواسيت": [],
    "جايكو": [],
}

# -----------------------------------------------------------
# 6) تهيئة FastAPI + Telegram Application
# -----------------------------------------------------------

app = FastAPI()
application = Application.builder().token(API_TOKEN).updater(None).build()

# 🔒 قفل واحد لعمليات الكتابة على ملف Excel لمنع التعارض والتلف
EXCEL_LOCK = asyncio.Lock()

# 📁 مجلد النسخ الاحتياطي لملف الإكسل
BACKUP_DIR = Path("backups")
try:
    BACKUP_DIR.mkdir(exist_ok=True)
except Exception as e:
    logging.error(f"[BACKUP] ❌ فشل إنشاء مجلد النسخ الاحتياطي: {e}")

async def create_excel_backup(reason: str = "manual", context: Optional[ContextTypes.DEFAULT_TYPE] = None, notify_chat_id: Optional[int] = None):
    """إنشاء نسخة احتياطية من ملف bot_data.xlsx داخل مجلد backups"""
    src = Path("bot_data.xlsx")
    if not src.exists():
        logging.warning("[BACKUP] ⚠️ ملف bot_data.xlsx غير موجود – لا يمكن إنشاء نسخة احتياطية.")
        if context and notify_chat_id:
            try:
                await context.bot.send_message(
                    chat_id=notify_chat_id,
                    text="⚠️ لا يوجد ملف بيانات bot_data.xlsx حالياً، لم يتم إنشاء نسخة احتياطية."
                )
            except Exception:
                pass
        return

    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    ts = now_saudi.strftime("%Y%m%d_%H%M%S")
    backup_name = f"bot_data_{ts}_{reason}.xlsx"
    backup_path = BACKUP_DIR / backup_name

    try:
        loop = asyncio.get_running_loop()
        # نضمن عدم تعارض أي عملية كتابة أخرى على نفس الملف
        async with EXCEL_LOCK:
            await loop.run_in_executor(None, shutil.copy2, src, backup_path)

        logging.info(f"[BACKUP] ✅ تم إنشاء نسخة احتياطية: {backup_path}")
        # إشعار الشخص الذي طلب النسخ (مثل المشرف في لوحة التحكم)
        if context and notify_chat_id:
            try:
                await context.bot.send_message(
                    chat_id=notify_chat_id,
                    text="✅ تم إنشاء نسخة احتياطية لبيانات النظام بنجاح."
                )
            except Exception:
                pass

        # إرسال النسخة الاحتياطية إلى قناة/قروب النسخ الاحتياطي إن وُجد TG_BACKUP_CHAT_ID
        if context:
            try:
                backup_chat_env = os.getenv("TG_BACKUP_CHAT_ID")
                backup_chat_id = int(backup_chat_env) if backup_chat_env else None
            except Exception:
                backup_chat_id = None

            if backup_chat_id:
                try:
                    with open(backup_path, "rb") as doc:
                        await context.bot.send_document(
                            chat_id=backup_chat_id,
                            document=doc,
                            caption=f"📦 نسخة احتياطية ({reason}) من بيانات نظام GO"
                        )
                except Exception as e2:
                    logging.error(f"[BACKUP] ❌ فشل إرسال النسخة الاحتياطية إلى قناة النسخ: {e2}")
    except Exception as e:
        logging.error(f"[BACKUP] ❌ فشل إنشاء النسخة الاحتياطية: {e}")
        if context and notify_chat_id:
            try:
                await context.bot.send_message(
                    chat_id=notify_chat_id,
                    text="❌ حدث خطأ أثناء إنشاء النسخة الاحتياطية."
                )
            except Exception:
                pass

async def daily_backup_job(context: ContextTypes.DEFAULT_TYPE):
    """نسخ احتياطي يومي تلقائي لملف الإكسل"""
    try:
        # نمرر context حتى يتمكن من الإرسال إلى قناة النسخ الاحتياطي إن وُجد TG_BACKUP_CHAT_ID
        await create_excel_backup(reason="daily", context=context, notify_chat_id=None)
    except Exception as e:
        logging.error(f"[BACKUP] ❌ خطأ أثناء تنفيذ النسخ الاحتياطي اليومي: {e}")



# إصلاح الخطأ: تعريف initial_branches قبل استخدامها
application.bot_data["branches"] = initial_branches

# -----------------------------------------------------------
# 7) قواعد البيانات – DataFrames فارغة حتى يتم التحميل لاحقاً
# -----------------------------------------------------------

df_admins = pd.DataFrame()
df_replies = pd.DataFrame()
df_branches = pd.DataFrame()
df_maintenance = pd.DataFrame()
df_parts = pd.DataFrame()
df_manual = pd.DataFrame()
df_independent = pd.DataFrame()
df_faults = pd.DataFrame()

# -----------------------------------------------------------
# 8) متغيرات عامة للنظام
# -----------------------------------------------------------

ALL_USERS = set()
user_sessions = {}

# إحصائيات ثابتة (تعويض سنتين تشغيل)
BASE_STATS = {
    "users": 6074,
    "groups": 10,
    "go_uses": 21695,
}

# تعويض تقييمات سنتين تشغيل (إحصائيات فقط، لا تُكتب في الإكسل)
BASE_RATINGS = {
    "count": 1762,   # 👈 عدّل هذا الرقم: عدد المقيمين الافتراضي القديم
    "avg": 4.8,     # 👈 متوسط التقييم القديم (من 1 إلى 4)
}

# قائمة السيارات لخدمة قطع الغيار الاستهلاكية
unique_cars = []

# رسالة النماذج الغير جاهزة
PLACEHOLDER_TEXT = "هذا الطراز قيد التجهيز من قبل فريق GO"

# -----------------------------------------------------------
# 9) دليل تواصل الوكلاء
# -----------------------------------------------------------

BRAND_CONTACTS = {
    # مفتاح الوكيل الرئيسي
    "سنابل الحديثة": {
        "company": "سنابل الحديثة",
        "phone": "8002440228",
    },

    # إكسيد – نفس الوكيل (يجب إضافتها)
    "EXEED": {
        "company": "سنابل الحديثة",
        "phone": "8002440228",
    },
    "إكسيد": {
        "company": "سنابل الحديثة",
        "phone": "8002440228",
    },

    # جيتور
    "جيتور": {
        "company": "التوريدات الوطنية للسيارات",
        "phone": "920051222",
    },
    "JETOUR": {
        "company": "التوريدات الوطنية للسيارات",
        "phone": "920051222",
    },

    # باقي البراندات
    "BYD": {"company": "", "phone": ""},
    "جايكو": {"company": "", "phone": ""},
    "أومودا": {"company": "", "phone": ""},
    "سواسيت": {"company": "", "phone": ""},
}

# 🆕 ربط أسماء البراندات (زي ما تجي من الإكسل) بمفتاح الوكيل في BRAND_CONTACTS
DEALER_FOR_BRAND = {
    # شيري – مستقر
    "CHERY": "سنابل الحديثة",

    # إكسيد – كل الصيغ المحتملة
    "EXEED": "سنابل الحديثة",
    "EXCEED": "سنابل الحديثة",
    "EXEED LX": "سنابل الحديثة",
    "EXCEED LX": "سنابل الحديثة",
    "EXEED-LX": "سنابل الحديثة",
    "EXCEED-LX": "سنابل الحديثة",
    "EXEED TXL": "سنابل الحديثة",
    "EXCEED TXL": "سنابل الحديثة",
    "EXEED-TXL": "سنابل الحديثة",
    "EXCEED-TXL": "سنابل الحديثة",
    "إكسيد": "سنابل الحديثة",
    "اكسيد": "سنابل الحديثة",

    # جيتور – ثابت
    "JETOUR": "جيتور",
    "جيتور": "جيتور",

    # fallback:
    "EXEED LX 2024": "سنابل الحديثة",
    "EXEED LX 2023": "سنابل الحديثة",
    "EXEED TXL 2024": "سنابل الحديثة",
}

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
    # شيت الاعطال الشائعة اختياري
    df_faults = excel_data.get("faults", pd.DataFrame())

    # ✅ استخراج قائمة السيارات الفريدة لقطع الغيار الاستهلاكية من شيت parts
    try:
        unique_cars = sorted(
            df_parts["Station No"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
    except Exception as e2:
        logging.error(f"[DATA] فشل في بناء قائمة unique_cars من شيت parts: {e2}")
        unique_cars = []

    if "all_users_log" in excel_data:
        df_users = excel_data["all_users_log"]
        ALL_USERS = set(df_users["user_id"].dropna().astype(int).tolist())
    else:
        df_users = pd.DataFrame(columns=["user_id"])

    AUTHORIZED_USERS = df_admins["manager_id"].dropna().astype(int).tolist()
    SUGGESTION_REPLIES = dict(zip(df_replies["key"], df_replies["reply"]))

    # ✅ تحويل شيت الفروع إلى list[dict]
    initial_branches = df_branches.to_dict(orient="records")

    # ✅ هنا نغذي bot_data بالبيانات فعلياً
    application.bot_data["branches"] = initial_branches

except Exception as e:
    logging.error(f"[DATA LOAD ERROR] ⚠️ خطأ في قراءة bot_data.xlsx: {e}")
    AUTHORIZED_USERS = []
    SUGGESTION_REPLIES = {}
    initial_branches = []
    unique_cars = []

    # ✅ حتى في حالة الخطأ نخليها قيمة معروفة (قائمة فاضية)
    application.bot_data["branches"] = initial_branches

# ✅ group_logs: شيت تجميعي لكل المجموعات/القنوات
try:
    df_group_logs = excel_data.get('group_logs', None)
    if df_group_logs is None:
        df_group_logs = pd.DataFrame(columns=['chat_id','title','type','last_seen_utc'])
except Exception:
    df_group_logs = pd.DataFrame(columns=['chat_id','title','type','last_seen_utc'])

# ================================
#  🔄 جوب دوري لحفظ البيانات المتراكمة في ملف Excel
# ================================

async def show_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    صفحة إحصائيات GO + فتح التقييم في نفس الشاشة (HTML مسموح من تيليجرام)
    """
    query = update.callback_query
    data = query.data or ""
    user = query.from_user

    # استخراج user_id من الكول باك لو متوفر
    user_id = user.id
    if data.startswith("rate_"):
        try:
            user_id = int(data.split("_", 1)[1])
        except Exception:
            pass

    user_name_raw = user.full_name or "الصديق"
    user_name_safe = html.escape(user_name_raw)

    # === المستخدمون ===
    try:
        real_users = len(ALL_USERS)
    except Exception:
        real_users = 0
    total_users = BASE_STATS["users"] + real_users

    # === قراءة كل الشيتات مرة واحدة (إن أمكن) ===
    excel_all = {}
    try:
        excel_all = pd.read_excel("bot_data.xlsx", sheet_name=None)
    except Exception:
        excel_all = {}

    # === المجموعات ===
    try:
        df_groups = excel_all.get("group_logs", pd.DataFrame())
        real_groups = df_groups["chat_id"].nunique() if not df_groups.empty else 0
    except Exception:
        real_groups = 0
    total_groups = BASE_STATS["groups"] + real_groups

        # === مرات استخدام GO (من الذاكرة فقط) ===
    try:
        real_go = int(GLOBAL_GO_COUNTER)
    except Exception:
        real_go = 0

    total_go = BASE_STATS["go_uses"] + real_go

    # === التقييمات (مع BASE_RATINGS) ===
    rating_info = "⭐ لا توجد تقييمات مسجلة حاليًا"
    try:
        df_ratings = excel_all.get("ratings", pd.DataFrame())

        real_count = 0
        real_avg = 0.0
        if not df_ratings.empty and "rating" in df_ratings.columns:
            real_count = len(df_ratings)
            real_avg = float(df_ratings["rating"].mean())

        base_count = int(BASE_RATINGS.get("count", 0) or 0)
        base_avg = float(BASE_RATINGS.get("avg", 0) or 0.0)

        total_ratings_display = base_count + real_count

        if total_ratings_display > 0:
            if base_count == 0 and real_count > 0:
                combined_avg = round(real_avg, 2)
            elif base_count > 0 and real_count == 0:
                combined_avg = round(base_avg, 2)
            else:
                combined_avg = round(
                    (base_count * base_avg + real_count * real_avg)
                    / total_ratings_display,
                    2,
                )

            stars = "⭐" * int(round(combined_avg))

            rating_info = (
                "⭐ التقييمات:\n"
                f"عدد المقيمين: <a href=\"tg://user?id=0\">{total_ratings_display}</a>\n"
                f"متوسط التقييم: <a href=\"tg://user?id=0\">{combined_avg}</a> {stars}"
            )
    except Exception:
        pass

    # === الوقت ===
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    refresh_time = (now_saudi + timedelta(minutes=12)).strftime("%I:%M %p")

    # === بناء نص الإحصائيات (HTML مسموح) ===
    text = (
        "<b>📊 لوحة إحصائيات نظام الصيانة GO</b>\n"
        f"👤 <i>المستخدم:</i> <code><i>{user_name_safe}</i></code>\n\n"
        "<b>📌 الملخص العام</b>\n"
        f"👥 عدد مستخدمين نظام GO: <a href=\"tg://user?id=0\">{total_users}</a>\n"
        f"🏡 عدد القروبات المرتبطة بنظام GO: <a href=\"tg://user?id=0\">{total_groups}</a>\n"
        f"🚀 إجمالي مرات استخدام نظام GO: <a href=\"tg://user?id=0\">{total_go}</a>\n\n"
        f"{rating_info}\n\n"
        "⏳ <code><i>تُحدَّث هذه الأرقام تلقائيًا مع نشاط الاعضاء.</i></code>\n"
        f"<code>{refresh_time} / 🇸🇦</code>\n\n"
        "🔹 <i>فريق GO يشكرك على ثقتك ودعمك المستمر.</i>"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("😞 غير راضٍ", callback_data=f"ratingval_1_{user_id}")],
        [InlineKeyboardButton("😐 مقبول", callback_data=f"ratingval_2_{user_id}")],
        [InlineKeyboardButton("😊 جيد", callback_data=f"ratingval_3_{user_id}")],
        [InlineKeyboardButton("😍 ممتاز", callback_data=f"ratingval_4_{user_id}")],
        [InlineKeyboardButton("⬅️ الرجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
    ])

    try:
        await query.message.edit_text(
            text=text,
            reply_markup=keyboard,
            parse_mode=constants.ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except BadRequest as e:
        # نتجاهل فقط حالة "Message is not modified"
        if "Message is not modified" in str(e):
            return
        raise

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

# ================================================================
#  ⚙️ عدادات الإحصائيات: تحديث الذاكرة + حفظ فعلي في Excel
#  - group_logs      → للإحصائيات + الإرسال الجماعي
#  - ALL_USERS       → للإحصائيات + النسخ الاحتياطي
#  - total_go_uses   → عداد استخدام GO في bot_stats
# ================================================================

# 📌 تحديث group_logs: تعديل الداتا في الذاكرة + حفظ مباشر في Excel
async def update_group_logs_async(chat):
    """
    تحديث سجل المجموعات:
    - يحدث df_group_logs في الذاكرة
    - ثم يكتب الشيت group_logs في bot_data.xlsx
    """
    global df_group_logs
    try:
        if chat.type not in ("group", "supergroup", "channel"):
            return

        chat_id = int(chat.id)
        title = chat.title or "غير معروف"
        chat_type = chat.type
        now_utc = datetime.now(timezone.utc).isoformat()

        # تأكد من وجود الداتا فريم
        if df_group_logs is None:
            df_group_logs = pd.DataFrame(columns=["chat_id", "title", "type", "last_seen_utc"])

        for col in ["chat_id", "title", "type", "last_seen_utc"]:
            if col not in df_group_logs.columns:
                df_group_logs[col] = None

        # تحديث أو إضافة السطر
        mask = df_group_logs["chat_id"].astype(str) == str(chat_id)
        if mask.any():
            df_group_logs.loc[mask, "title"] = title
            df_group_logs.loc[mask, "type"] = chat_type
            df_group_logs.loc[mask, "last_seen_utc"] = now_utc
        else:
            df_group_logs.loc[len(df_group_logs)] = {
                "chat_id": chat_id,
                "title": title,
                "type": chat_type,
                "last_seen_utc": now_utc,
            }

        # حفظ الشيت في Excel (group_logs) عشان الإحصائيات والإرسال الجماعي
        try:
            # قفل واحد لكل عمليات الكتابة على bot_data.xlsx لمنع التعارض
            async with EXCEL_LOCK:
                with pd.ExcelWriter(
                    "bot_data.xlsx",
                    engine="openpyxl",
                    mode="a",
                    if_sheet_exists="replace",
                ) as writer:
                    df_group_logs.to_excel(writer, sheet_name="group_logs", index=False)

            logging.info(f"[GROUP_LOGS] ✅ تم تحديث group_logs للقروب {title} ({chat_id}) في Excel")
        except Exception as e:
            logging.error(f"[GROUP_LOGS] ❌ فشل حفظ group_logs في Excel: {e}")

    except Exception as e:
        logging.error(f"[GROUP_LOGS] ❌ فشل تحديث group_logs: {e}")


# 📌 حفظ ALL_USERS في Excel — يُستخدم في الإحصائيات والنسخ الاحتياطي
def _update_all_users_log_sync():
    """
    حفظ ALL_USERS في شيت all_users_log داخل bot_data.xlsx
    """
    global ALL_USERS
    try:
        df_users = pd.DataFrame(sorted(ALL_USERS), columns=["user_id"])

        with pd.ExcelWriter(
            "bot_data.xlsx",
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace",
        ) as writer:
            df_users.to_excel(writer, sheet_name="all_users_log", index=False)

        logging.info(f"[SAVE USERS] ✅ تم حفظ {len(ALL_USERS)} مستخدم في all_users_log")
    except Exception as e:
        logging.error(f"[SAVE USERS] ❌ فشل حفظ all_users_log في Excel: {e}")


async def update_all_users_log_async():
    """
    غلاف async لحفظ المستخدمين:
    - يشغل _update_all_users_log_sync في ثريد مستقل
    - حتى ما يبطّئ /go ولا start
    """
    try:
        loop = asyncio.get_running_loop()
        # استخدام قفل واحد لكل عمليات الكتابة على bot_data.xlsx
        async with EXCEL_LOCK:
            await loop.run_in_executor(None, _update_all_users_log_sync)
    except Exception as e:
        logging.error(f"[SAVE USERS] ❌ خطأ في تشغيل حفظ all_users_log في الخلفية: {e}")


# 📌 عدّاد استخدام GO — كتابة مباشرة في bot_stats (المصدر اللي تقرأ منه شاشة الإحصائيات)
GLOBAL_GO_COUNTER = 0  # يبقى لو حبيت تستخدمه لاحقاً، لكن الإحصائيات تعتمد على Excel

def _update_go_stats_sync():
    """
    عدّاد استخدام GO في الذاكرة فقط
    ما يقرأ ولا يكتب على bot_data.xlsx نهائياً
    """
    global GLOBAL_GO_COUNTER
    GLOBAL_GO_COUNTER += 1
    logging.info(f"[GO STATS] buffered go usage (now {GLOBAL_GO_COUNTER})")


async def update_go_stats_async():
    """غلاف async بسيط لزيادة العدّاد في الذاكرة"""
    try:
        _update_go_stats_sync()
    except Exception as e:
        logging.error(f"[GO STATS] فشل تحديث عداد GO في الذاكرة: {e}")

# ================================================================
#  ⚙️ health_log أيضًا يبقى في الذاكرة — الكتابة بالجوب لاحقًا
# ================================================================
HEALTH_BUFFER = []

def _write_health_log_sync():
    global HEALTH_BUFFER, ALL_USERS, GLOBAL_GO_COUNTER
    try:
        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        HEALTH_BUFFER.append({
            "timestamp": now_saudi.isoformat(timespec="seconds"),
            "total_users": len(ALL_USERS),
            "total_go_uses": GLOBAL_GO_COUNTER,
        })
        logging.info(f"[HEALTH] buffered heartbeat")
    except Exception as e:
        logging.error(f"[HEALTH] فشل كتابة health_log في الذاكرة: {e}")


async def health_log_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        _write_health_log_sync()
    except Exception as e:
        logging.error(f"[HEALTH LOG] خطأ أثناء تحديث health_log في الذاكرة: {e}")

import requests  # تأكد هذا موجود فوق مع الاستيرادات لو مو مضاف

# 🔁 جوب بسيط يطلب عنوان الخدمة لإبقاء Render مستيقظ
async def keepalive_ping(context: ContextTypes.DEFAULT_TYPE):
    try:
        base_url = os.getenv("RENDER_EXTERNAL_URL") or "https://chery-go-8a2z.onrender.com"

        # لو أحد كتبها بدون بروتوكول
        if not base_url.startswith("http"):
            base_url = "https://" + base_url.lstrip("/")

        # نستخدم ثريد منفصل عشان ما نحجز event loop
        await asyncio.to_thread(
            requests.get,
            base_url,
            timeout=5,
        )
        logging.info(f"[KEEPALIVE] ✅ Ping {base_url}")
    except Exception as e:
        logging.error(f"[KEEPALIVE] ❌ فشل Ping الخدمة: {e}")

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
                timedelta(minutes=15),
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

async def reset_manual_search_state(context: ContextTypes.DEFAULT_TYPE):
    """تصـفير عداد البحث اليدوي (search_attempts) بعد 15 دقيقة من آخر استعلام"""
    job_data = getattr(context, "job", None).data if getattr(context, "job", None) else {}
    user_id = job_data.get("user_id")
    if user_id is None:
        return

    try:
        # user_data على مستوى التطبيق (أكثر أماناً داخل الجوب)
        user_data = context.application.user_data.get(user_id, {})
    except Exception:
        # احتياطاً
        user_data = context.user_data.get(user_id, {})

    if not isinstance(user_data, dict):
        return

    # حذف عداد البحث اليدوي
    user_data.pop("search_attempts", None)

    # إذا ما زالت الحركة parts نلغيها (جلسة بحث يدوي انتهت)
    if user_data.get("action") == "parts":
        user_data.pop("action", None)

    logging.info(f"[CLEANUP] ✅ تصفير عداد البحث اليدوي للمستخدم {user_id}")

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

    # 👇 هذا يضمن ظهور الرسالة في Runtime Logs حتى لو إعدادات اللوق تغيّرت
    print(log_msg)

def get_part_price(row: pd.Series) -> Optional[str]:
    """
    ترجع السعر كنص من الصف اذا كان العمود موجود وغير فارغ
    ندعم عدة أسماء أعمدة محتملة بما فيها Approx Price
    """
    candidate_cols = ["Approx Price", "Price", "price", "السعر", "التكلفة", "Cost", "cost"]
    for col in candidate_cols:
        if col in row:
            value = str(row[col]).strip()
            if value and value.lower() != "nan":
                return value
    return None

def make_back_button(target: str, user_id: int) -> InlineKeyboardButton:
    """
    يبني زر رجوع موحد
    target مثال: main / parts_menu / maintenance_menu / manual_menu ...
    """
    return InlineKeyboardButton("🔙 رجوع", callback_data=f"back:{target}:{user_id}")


async def handle_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أزرار الرجوع الموحدة من نوع back:target:user_id"""
    query = update.callback_query
    raw = query.data or ""
    parts = raw.split(":")

    if len(parts) < 3:
        await query.answer("❌ زر رجوع غير معروف.", show_alert=True)
        return

    _, target, user_id_str = parts

    try:
        user_id = int(user_id_str)
    except ValueError:
        await query.answer("❌ خطأ في بيانات زر الرجوع.", show_alert=True)
        return

    # تجهيز كيبورد القائمة الرئيسية بشكل آمن
    kb = build_main_menu_keyboard(user_id)
    if isinstance(kb, InlineKeyboardMarkup):
        main_menu_markup = kb
    else:
        main_menu_markup = InlineKeyboardMarkup(kb)

    if target == "main":
        text_main = "فضلا اختار الخدمة المطلوبة 🛠️ :"
        try:
            # لو الرسالة نص نعدلها، لو صورة / ملف نرسل رسالة جديدة
            if query.message and query.message.text:
                msg = await query.edit_message_text(text_main, reply_markup=main_menu_markup)
            else:
                msg = await query.message.reply_text(text_main, reply_markup=main_menu_markup)
        except Exception:
            msg = await query.message.reply_text(text_main, reply_markup=main_menu_markup)

        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, "⬅️ رجوع الى القائمة الرئيسية (نظام back:main)")
        return

    # باقي الأهداف لاحقاً
    await query.answer("هذا زر رجوع لم يتم تفعيله بعد.", show_alert=True)

def build_main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("🔧 استعلامات قطع الغيار", callback_data=f"parts_{user_id}")],
        [InlineKeyboardButton("🚗 استعلامات الصيانة الدورية", callback_data=f"maintenance_{user_id}")],
        [InlineKeyboardButton("📘 استعراض دليل المالك", callback_data=f"manual_{user_id}")],
        [InlineKeyboardButton("🛠️ المتاجر ومراكز الخدمة", callback_data=f"service_{user_id}")],
        [InlineKeyboardButton("🔧 الأعطال الشائعة وحلولها", callback_data=f"faults_{user_id}")],
        [InlineKeyboardButton("✉️ مركز الدعم الفني والاستفسارات", callback_data=f"suggestion_{user_id}")],
        # ✅ زر واحد فقط: إحصائيات + تقييم
        [InlineKeyboardButton("📊 إحصائيات GO والتقييم", callback_data=f"rate_{user_id}")]
    ]

    # ✅ مميزات إضافية للمشرفين فقط
    if user_id in AUTHORIZED_USERS:
        # زر إرسال توصية فنية
        keyboard.insert(
            -1,
            [InlineKeyboardButton("📡 إرسال توصية فنية", callback_data="send_reco")]
        )
        # زر نقاشات فريق GO
        keyboard.insert(
            -1,
            [InlineKeyboardButton("🟦 دعوة فريق GO للنقاش", callback_data=f"team_main_{user_id}")]
        )

    return InlineKeyboardMarkup(keyboard)
       
# ✅ دالة البدء async
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.bot_data.get("maintenance_mode"):
        user_name = update.effective_user.full_name
        with open("GO-SS.PNG", "rb") as photo:
            msg = await update.message.reply_photo(
                photo=photo,
                caption=(
                    f"🛠️ مرحبا {user_name}\n\n"
                    "برنامج <b>GO</b> قيد التحديث والصيانة حالياً.\n"
                    "🔄 الرجاء المحاولة لاحقاً."
                ),
                parse_mode="HTML"
            )
        context.job_queue.run_once(
            lambda c: c.bot.delete_message(chat_id=msg.chat_id, message_id=msg.message_id),
            when=30
        )
        return

    user = update.effective_user
    chat = update.effective_chat
    user_id = user.id
    chat_id = chat.id
    user_name = user.full_name

    # حذف رسالة /start أو go الأصلية حتى لا تتكرر
    if update.message:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
        except:
            pass

    # ✅ منع المتطفلين من الدخول من الخاص مباشرة بدون جلسة من المجموعة
    if chat.type == "private" and not context.user_data.get(user_id, {}).get("session_valid") and user_id not in AUTHORIZED_USERS:
        text = update.message.text.strip().lower() if update.message else ""
        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")
        user_block = f"🧑‍🏫 مرحبا {user_name}"
        delete_block = f"⏳ سيتم حذف هذا التنبيه تلقائيًا خلال 15 دقيقة ({delete_time} / 🇸🇦)"

        if text in ["/start", "start", "go", "/go"] and "start=go" not in text:
            alert_message = (
               "📣 يسعدنا اهتمامك بخدمات *نظام الصيانة GO*!\n\n"
               "❌ لا يمكنك بدء الخدمة مباشرة من الخاص.\n"
               "🔐 حفاظًا على الخصوصية، يرجى العودة إلى مجموعتك أو الانضمام إلى المجموعة أدناه وكتابة الأمر (go) هناك.\n\n"
               "[👥 اضغط هنا للانضمام إلى المجموعة ](https://t.me/CHERYKSA_group)"
            )
        else:
            alert_message = (
                "🚫 عذرًا، لا يمكنك بدء الخدمة بهذه الطريقة.\n"
                "🔐 زر الانطلاق يستعمل لمره واحدة وهو مخصص فقط لمن بدأ الجلسة من المجموعة بنفسه.\n"
                "✳️ يرجى العودة إلى المجموعة وكتابة الأمر (go) يدويًا لبدء الخدمة."
            )

        msg = await update.message.reply_text(
            f"{user_block}\n\n{alert_message}\n\n{delete_block}",
            parse_mode=constants.ParseMode.MARKDOWN,
            disable_web_page_preview=True
        )
        register_message(user_id, msg.message_id, chat_id, context)
        return

    # تنظيف مفاتيح image_opened_ لمنع التعارض في فتح الصور القديمة
    keys_to_remove = [key for key in context.user_data.get(user_id, {}) if key.startswith("image_opened_")]
    for key in keys_to_remove:
        del context.user_data[user_id][key]

    context.user_data.setdefault(user_id, {})
    context.user_data[user_id]["manual_sent"] = False

    # ✅ تسجيل المستخدم في all_users_log (تحديث في الخلفية عند أول استخدام)
    global ALL_USERS
    if user_id not in ALL_USERS:
        ALL_USERS.add(user_id)
        try:
            asyncio.create_task(update_all_users_log_async())
        except Exception as e:
            logging.error(f"[SAVE USERS] فشل جدولة حفظ all_users_log في الخلفية: {e}")

    # ✅ تحديث عداد استخدام go في الخلفية (بدون تعطيل رسالة الترحيب والقوائم)
    try:
        asyncio.create_task(update_go_stats_async())
    except Exception as e:
        logging.error(f"[SAVE STATS] فشل جدولة تحديث /go في الخلفية: {e}")

    # ✅ استرجاع بيانات المجموعة المحفوظة للمستخدم
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

    # ✅ إذا النداء من مجموعة: نرسل بانر الترحيب ونخرج
    if chat_id < 0:
        context.bot_data[user_id] = {
            "group_title": update.effective_chat.title or "غير معروف",
            "group_id": chat_id,
            "user_name": user_name
        }

        photo_path = "GO-CHERY.PNG"

        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(seconds=90)).strftime("%I:%M %p")

        user_block = f"`🧑‍💼 مرحباً {user_name}`"

        program_description = (
            "**🚀 انطلق الآن مع النسخة المطوّرة من نظام GO**\n"
            "`التجربة الأذكى لخدمة ملاك شيري / إكسيد / جايكو / أومودا / سوايست / جيتور / BYD.`\n\n"
            "**⚙️ خدمات تفاعلية**\n"
            "`صيانة دورية • قطع غيار • دليل المالك • مراكز خدمة ومتاجر معتمدة.`\n\n"
            "**🔧 الأعطال الشائعة وحلولها**\n"
            "`معلومات موثوقة وخطوات تساعدك على فهم المشكلة قبل زيارة الصيانة.`\n\n"
            "**🛠️ مركز الدعم الفني**\n"
            "`استقبال استفساراتك ودعم فني مباشر من فريق GO.`\n\n"
        )

        delete_block = f"`⏳ سيتم حذف هذا المنشور خلال 90 ثانية ({delete_time} / 🇸🇦)`"

        full_caption = (
            f"{user_block}\n\n"
            f"{program_description}"
            "💡 اضغط الزر بالأسفل للانتقال إلى خدمة GO:\n"
            f"{delete_block}"
        )

        bot_username = context.bot.username
        link = f"https://t.me/{bot_username}?start=go"
        keyboard = [[InlineKeyboardButton("🚀 ابدأ الخدمة الآن عبر GO", url=link)]]

        try:
            if os.path.exists(photo_path):
                with open(photo_path, "rb") as photo:
                    msg = await context.bot.send_photo(
                        chat_id=chat_id,
                        photo=photo,
                        caption=full_caption,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode=constants.ParseMode.MARKDOWN
                    )
            else:
                msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=full_caption,
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
            logging.error(f"[GO GROUP] فشل إرسال الترحيب بالصورة: {e}")

        return  # ← هذا return يُنهي فرع المجموعة فقط

    # ------------------------------------------------------------------------
    # من هنا الخاص
    # ------------------------------------------------------------------------

    context.user_data[user_id].pop("suggestion_used", None)
    context.user_data[user_id].pop("search_attempts", None)

    keyboard = build_main_menu_keyboard(user_id)

    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

    msg1 = await update.message.reply_text(
        f"`🧑‍💼 مرحباً {user_name}`\n\n"
        "🚀 *يسعدنا وصولك داخل نظام GO للاستعلام الفني والخدمات المساندة.*\n"
        "يوفّر لك GO بيئة موحدة للحصول على معلومات دقيقة حول صيانة سيارتك، وحلول الأعطال، ودليل الاستخدام، مع دعم فني مباشر عند الحاجة.\n\n"
        "💡 *تم نقلك الآن لبداية جلسة استعلام تفاعلية… وستظهر لك في الأسفل قائمة الخدمات المتاحة داخل GO لتبدأ منها الاستعلام المناسب.*\n\n"
        f"`⏳ سيتم حذف هذه الرسالة خلال 10 دقائق ({delete_time} / 🇸🇦)`",
        parse_mode=constants.ParseMode.MARKDOWN
    )

    msg2 = await update.message.reply_text(
        "فضلاً اختر الخدمة المطلوبة 🛠️ :",
        reply_markup=keyboard
    )

    # 🧽 تنظيف مفاتيح الجلسة القديمة
    for key in list(context.user_data[user_id].keys()):
        if key.startswith("image_opened_") or key.endswith("_used") or key.endswith("_sent"):
            context.user_data[user_id].pop(key, None)

    register_message(user_id, msg1.message_id, chat_id, context)
    register_message(user_id, msg2.message_id, chat_id, context)

    for key in list(context.user_data[user_id].keys()):
        if key.startswith("cat_used_"):
            context.user_data[user_id].pop(key, None)

    context.user_data[user_id]["session_valid"] = False

async def handle_go_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    user_id = user.id
    user_name = user.full_name
    chat_id = chat.id

    # 🧾 لو جت من مجموعة: جهّز الجلسة ثم أرسل الترحيب أولاً، وبعدها حدّث group_logs في الخلفية
    if chat.type != "private":
        # حفظ بيانات القروب للمستخدم عشان نستخدمها لما ينتقل للخاص
        context.bot_data[user_id] = {
            "group_title": chat.title or "غير معروف",
            "group_id": chat.id,
            "user_name": user_name
        }

        # إنشاء جلسة مؤقتة صالحة لمرة واحدة فقط
        context.user_data[user_id] = context.user_data.get(user_id, {})
        context.user_data[user_id]["session_valid"] = True

        # تنظيف مفاتيح الصور القديمة
        keys_to_remove = [key for key in context.user_data[user_id] if key.startswith("image_opened_")]
        for key in keys_to_remove:
            del context.user_data[user_id][key]

        # ✅ أرسل بانر GO / زر الانطلاق بسرعة
        await start(update, context)

        # ✅ بعد إرسال الرسالة للمجموعة، حدّث group_logs في الخلفية بدون ما تأخر الترحيب
        try:
            asyncio.create_task(update_group_logs_async(chat))
        except Exception as e:
            logging.warning(f"[GROUP_LOGS] فشل جدولة تحديث group_logs للقروب {chat.id}: {e}")

        logging.info(f"[GO من المجموعة] سجلنا بيانات المجموعة {chat.title} / {chat.id} للمستخدم {user.full_name}")
        return

    # ✅ من هنا: التعامل في الخاص
    if chat.type == "private" and (
        not context.user_data.get(user_id, {}).get("session_valid")
    ) and user_id not in AUTHORIZED_USERS:
        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

        user_block = f"🧑‍🏫 مرحبا {user_name}"
        alert_message = (
            "📣 يسعدنا اهتمامك بخدمات *نظام الصيانة GO*!\n\n"
            "❌ لا يمكنك بدء الخدمة مباشرة من الخاص.\n"
            "🔐 حفاظًا على الخصوصية، يرجى العودة إلى مجموعتك أو الانضمام إلى المجموعة أدناه وكتابة الأمر (go) هناك.\n\n"
            "[👥 اضغط هنا للانضمام إلى مجموعة ](https://t.me/CHERYKSA_group)"
        )
        delete_block = f"⏳ سيتم حذف هذا التنبيه تلقائيًا خلال 10 دقيقة ({delete_time} / 🇸🇦)"

        msg = await update.message.reply_text(
            f"{user_block}\n\n{alert_message}\n\n{delete_block}",
            parse_mode=constants.ParseMode.MARKDOWN,
            disable_web_page_preview=True
        )
        register_message(user_id, msg.message_id, chat_id, context)
        return

    # ✅ في الخاص مع جلسة صالحة أو مشرف → نترك دالة start تكمل نفس منطق الترحيب والقائمة
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

def _next_team_thread_id() -> int:
    """توليد رقم تسلسلي لكل نقاش داخلي لفريق GO"""
    global TEAM_THREAD_COUNTER
    TEAM_THREAD_COUNTER += 1
    return TEAM_THREAD_COUNTER


async def handle_team_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """استقبال رسالة داخلية من مشرف ضمن نقاش فريق GO"""
    message = update.message
    admin = update.effective_user
    admin_id = admin.id

    text = (message.text or "").strip()
    if not text:
        await message.reply_text("⚠️ الرسالة الداخلية يجب أن تكون نصية.")
        return

    state = context.user_data.get(admin_id, {})
    thread_id = state.get("team_thread_id")
    if not thread_id or thread_id not in team_threads:
        await message.reply_text("⚠️ لا توجد جلسة نقاش داخلي نشطة.")
        state["team_mode"] = False
        state.pop("team_thread_id", None)
        return

    thread = team_threads[thread_id]
    thread.setdefault("messages", [])
    thread["messages"].append(
        {
            "from": admin_id,
            "name": admin.full_name,
            "text": text,
            "at": datetime.now(timezone.utc).isoformat()
        }
    )

    reply_count = thread.get("reply_count", 0) + 1
    thread["reply_count"] = reply_count

    ctx = thread.get("context", {}) or {}

    header_lines = [
        f"🧵 نقاش فريق GO رقم #{thread_id}",
        f"🔁 رد رقم {reply_count} من: {admin.full_name} ({admin_id})",
    ]

    if thread.get("type") == "suggestion":
        member_name = ctx.get("user_name", "غير معروف")
        member_id = ctx.get("user_id", "غير معروف")
        group_name = ctx.get("group_name", "غير معروف")
        group_id = ctx.get("group_id", "غير معروف")
        suggestion_id = ctx.get("suggestion_id", "")

        header_lines.append("")
        header_lines.append(f"👤 العضو: {member_name} ({member_id})")
        header_lines.append(f"🏘️ المجموعة: {group_name} ({group_id})")
        if suggestion_id:
            header_lines.append(f"🆔 رقم الاستفسار: {suggestion_id}")

        original_text = (ctx.get("text") or "").strip()
        if original_text:
            header_lines.append("")
            header_lines.append("📝 نص استفسار العضو:")
            header_lines.append(f"```{original_text}```")

    header = "\n".join(header_lines)
    body = f"{header}\n\n💬 مداخلة المشرف:\n```{text}```"

    # إيقاف وضع الكتابة لهذا المشرف
    state["team_mode"] = False
    state.pop("team_thread_id", None)

    # إرسال الرسالة لكل المشرفين
    for aid in AUTHORIZED_USERS:
        try:
            buttons = [
                [InlineKeyboardButton("✉️ رد على هذا النقاش", callback_data=f"team_reply_{thread_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(buttons)
            await context.bot.send_message(
                chat_id=aid,
                text=body,
                parse_mode=constants.ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
        except Exception as e:
            logging.warning(f"[TEAM_THREAD] فشل إرسال رد النقاش للمشرف {aid}: {e}")

# =========================== توصيات فنية عامة للمجموعات ===========================

async def start_recommendation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """بدء وضع كتابة توصية فنية من مشرف"""
    query = update.callback_query
    admin_id = query.from_user.id

    if admin_id not in AUTHORIZED_USERS:
        await query.answer("هذه الميزة متاحة لمشرفي نظام GO فقط.", show_alert=True)
        return

    context.user_data.setdefault(admin_id, {})
    context.user_data[admin_id]["reco_mode"] = "awaiting_reco"
    context.user_data[admin_id]["reco_text"] = None
    context.user_data[admin_id]["reco_media"] = None

    await query.answer()

    await query.message.reply_text(
        "📡 *إرسال توصية فنية لجميع المجموعات*\n\n"
        "✏️ أرسل الآن نص التوصية التي ترغب بنشرها في جميع المجموعات التي يعمل فيها GO كمشرف.\n"
        "📎 يمكنك إرفاق *وسيط واحد فقط* (صورة أو مستند أو فيديو أو رسالة صوتية) مع التوصية.\n\n"
        "ℹ️ بعد الإرسال ستظهر لك *معاينة* قبل البث النهائي.",
        parse_mode=ParseMode.MARKDOWN,
    )


async def handle_recommendation_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """استقبال نص/وسائط التوصية من المشرف وتجهيز المعاينة"""
    admin_id = update.effective_user.id
    if admin_id not in AUTHORIZED_USERS:
        return

    ud = context.user_data.setdefault(admin_id, {})
    if ud.get("reco_mode") != "awaiting_reco":
        return  # ليس في وضع التوصية

    message = update.message

    # نص التوصية: إما text أو caption للوسائط
    text = (message.text or message.caption or "").strip()

    # التقاط وسيط واحد اختياري
    media = None
    if message.photo:
        media = {"type": "photo", "file_id": message.photo[-1].file_id}
    elif message.document:
        media = {"type": "document", "file_id": message.document.file_id}
    elif message.video:
        media = {"type": "video", "file_id": message.video.file_id}
    elif message.voice:
        media = {"type": "voice", "file_id": message.voice.file_id}

    if not text and not media:
        await message.reply_text("⚠️ لا يمكن حفظ توصية فارغة اكتب نص التوصية أو أرفق وسائط معها.")
        return

    ud["reco_text"] = text
    ud["reco_media"] = media

    admin_name = update.effective_user.full_name

    preview_caption = (
        "📡 *معاينة التوصية الفنية قبل الإرسال*\n\n"
        f"👤 *الناشر:* `{admin_name}`\n\n"
        "📄 *نص التوصية:*\n"
        f"```{text or 'بدون نص صريح (الوسائط فقط) '}```\n\n"
        "✅ إذا كانت مناسبة اضغط «بث التوصية الآن» أو أرسل رسالة جديدة لتعديل النص قبل الإرسال."
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📡 بث التوصية الآن", callback_data="reco_broadcast")],
        [InlineKeyboardButton("❌ إلغاء التوصية", callback_data="reco_cancel")],
    ])

    # إرسال المعاينة بنفس الوسيط إن وجد
    if media:
        mtype = media["type"]
        fid = media["file_id"]
        if mtype == "photo":
            await message.reply_photo(fid, caption=preview_caption, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
        elif mtype == "video":
            await message.reply_video(fid, caption=preview_caption, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
        elif mtype == "document":
            await message.reply_document(fid, caption=preview_caption, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
        elif mtype == "voice":
            await message.reply_voice(fid, caption=preview_caption, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
    else:
        await message.reply_text(preview_caption, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)


async def broadcast_recommendation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """بث التوصية على جميع المجموعات + إشعار جميع المشرفين"""
    query = update.callback_query
    admin_id = query.from_user.id
    admin_name = query.from_user.full_name

    if admin_id not in AUTHORIZED_USERS:
        await query.answer("هذه الميزة متاحة لمشرفي نظام GO فقط.", show_alert=True)
        return

    ud = context.user_data.setdefault(admin_id, {})
    text = ud.get("reco_text")
    media = ud.get("reco_media")

    if not text and not media:
        await query.answer("لا توجد توصية جاهزة للبث. يرجى إرسال التوصية أولاً.", show_alert=True)
        return

    await query.answer("📡 جاري بث التوصية على المجموعات...", show_alert=False)

    targets = collect_target_chat_ids(context)
    sent = failed = skipped = 0

    for chat_id in targets:
        try:
            member = await context.bot.get_chat_member(chat_id, context.bot.id)
            if member.status not in ("administrator", "creator"):
                skipped += 1
                continue

            if media:
                mtype = media["type"]
                fid = media["file_id"]
                caption = text or ""
                if mtype == "photo":
                    await context.bot.send_photo(chat_id, fid, caption=caption)
                elif mtype == "video":
                    await context.bot.send_video(chat_id, fid, caption=caption)
                elif mtype == "document":
                    await context.bot.send_document(chat_id, fid, caption=caption)
                elif mtype == "voice":
                    await context.bot.send_voice(chat_id, fid, caption=caption)
            else:
                await context.bot.send_message(chat_id, text)

            sent += 1
        except Exception as e:
            logging.warning(f"[RECO BROADCAST] فشل إرسال التوصية إلى {chat_id}: {e}")
            failed += 1

    # ملخص للمشرف الناشر
    summary = (
        "📡 تمت عملية بث التوصية الفنية.\n\n"
        f"✅ تم الإرسال إلى: {sent} مجموعة\n"
        f"⏭️ تم التخطي في: {skipped} مجموعة (البوت ليس مشرفاً)\n"
        f"⚠️ فشل الإرسال في: {failed} مجموعة"
    )
    try:
        await query.message.reply_text(summary)
    except Exception:
        pass

    # إشعار جميع المشرفين (بدون أرقام تعريفية)
    group_title = ud.get("group_title", "—")

    admin_notification_caption = (
        "📡 تمت عملية بث توصية فنية جديدة.\n\n"
        f"👤 الناشر: {admin_name}\n"
        f"👥 المجموعة التابعة له: {group_title}\n\n"
        "📄 نص التوصية:\n"
        f"{text or '— التوصية بدون نص (وسائط فقط) —'}"
    )

    for aid in AUTHORIZED_USERS:
        try:
            if media:
                mtype = media["type"]
                fid = media["file_id"]
                if mtype == "photo":
                    await context.bot.send_photo(aid, fid, caption=admin_notification_caption)
                elif mtype == "video":
                    await context.bot.send_video(aid, fid, caption=admin_notification_caption)
                elif mtype == "document":
                    await context.bot.send_document(aid, fid, caption=admin_notification_caption)
                elif mtype == "voice":
                    await context.bot.send_voice(aid, fid, caption=admin_notification_caption)
            else:
                await context.bot.send_message(aid, admin_notification_caption)
        except Exception as e:
            logging.warning(f"[RECO NOTIFY ADMIN] فشل إشعار المشرف {aid}: {e}")

    # تنظيف الحالة
    ud["reco_mode"] = None
    ud["reco_text"] = None
    ud["reco_media"] = None


async def cancel_recommendation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """إلغاء وضع التوصية والرجوع للقائمة الرئيسية"""
    query = update.callback_query
    admin_id = query.from_user.id
    ud = context.user_data.setdefault(admin_id, {})

    # تصفير حالة التوصية
    ud["reco_mode"] = None
    ud["reco_text"] = None
    ud["reco_media"] = None

    await query.answer("تم إلغاء التوصية.", show_alert=False)

    # إخفاء رسالة التوصية / المعاينة من الشات
    try:
        await query.message.delete()
    except Exception:
        pass

    # رجوع للقائمة الرئيسية في الخاص
    try:
        keyboard = build_main_menu_keyboard(admin_id)
        msg = await context.bot.send_message(
            chat_id=admin_id,
            text="✅ تم إلغاء التوصية الفنية.\nفضلاً اختر الخدمة المطلوبة 🛠️ :",
            reply_markup=keyboard
        )
        register_message(admin_id, msg.message_id, admin_id, context)
    except Exception as e:
        logging.warning(f"[RECO CANCEL] فشل إرسال القائمة الرئيسية بعد الإلغاء للمشرف {admin_id}: {e}")

### ✅ الدالة المعدلة: handle_message (فقط جزء الاقتراح)
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global df_admins
    message = update.message
    user = update.effective_user
    admin_id = user.id
    chat = update.effective_chat
    chat_id = chat.id
    user_id = user.id
    user_name = user.full_name

    # 🔒 ضمان وجود قواميس للمستخدم/المشرف قبل الكتابة عليها
    context.user_data.setdefault(admin_id, {})
    context.user_data.setdefault(user_id, {})

    # 📨 إذا كان المشرف في وضع نقاش داخلي لفريق GO نوجّه الرسالة هناك
    if context.user_data[admin_id].get("team_mode"):
        await handle_team_message(update, context)
        return

    action = context.user_data.get(user_id, {}).get("action")

    # ✅ حذف مشرف
    if action == "awaiting_admin_removal":
        try:
            target_id = int(message.text.strip())
            if target_id == 1543083749:
                await message.reply_text("🚫 لا يمكن حذف المدير الأعلى.")
                return
            if target_id not in df_admins["manager_id"].astype(int).values:
                await message.reply_text("❌ هذا المعرف غير موجود في قائمة المشرفين.")
                return

            df_admins = df_admins[df_admins["manager_id"].astype(int) != target_id]
            if target_id in AUTHORIZED_USERS:
                AUTHORIZED_USERS.remove(target_id)

            # قفل الكتابة على ملف الإكسل قبل تعديل شيت managers
            async with EXCEL_LOCK:
                with pd.ExcelWriter("bot_data.xlsx", engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    df_admins.to_excel(writer, sheet_name="managers", index=False)

            await message.reply_text(f"🗑️ تم حذف المشرف بنجاح:\n<code>{target_id}</code>", parse_mode="HTML")
        except Exception as e:
            await message.reply_text(f"❌ حدث خطأ أثناء حذف المشرف:\n<code>{e}</code>", parse_mode="HTML")
        context.user_data[admin_id]["action"] = None
        return

    # ✅ إضافة مشرف
    if action == "awaiting_new_admin_id":
        try:
            text = message.text.strip()
            if not text.isdigit():
                await message.reply_text("❌ يجب إدخال رقم ID رقمي صالح.")
                return
            new_admin_id = int(text)
            if new_admin_id in AUTHORIZED_USERS:
                await message.reply_text("ℹ️ هذا المشرف موجود مسبقًا.")
                return

            AUTHORIZED_USERS.append(new_admin_id)
            df_admins = pd.concat([df_admins, pd.DataFrame([{"manager_id": new_admin_id}])], ignore_index=True)
            # قفل الكتابة على ملف الإكسل قبل تعديل شيت managers
            async with EXCEL_LOCK:
                with pd.ExcelWriter("bot_data.xlsx", engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    df_admins.to_excel(writer, sheet_name="managers", index=False)

            await message.reply_text(f"✅ تم إضافة المشرف:\n<code>{new_admin_id}</code>", parse_mode="HTML")
        except Exception as e:
            await message.reply_text(f"❌ فشل أثناء حفظ الملف:\n<code>{e}</code>", parse_mode="HTML")
        context.user_data[admin_id]["action"] = None
        return

    # 🛰️ وضع كتابة توصية فنية (للمشرفين فقط)
    if admin_id in AUTHORIZED_USERS:
        reco_mode = context.user_data.get(admin_id, {}).get("reco_mode")
        if reco_mode == "awaiting_reco":
            await handle_recommendation_message(update, context)
            return

    # ✅ حالات الاقتراح والرد المخصص
    actual_user_id = context.user_data.get(admin_id, {}).get("custom_reply_for", admin_id)
    mode = context.user_data.get(actual_user_id, {}).get("action") or context.user_data.get(admin_id, {}).get("compose_mode")

    if mode in ["suggestion", "custom_reply"]:
        context.user_data.setdefault(actual_user_id, {})
        suggestion_id = context.user_data[actual_user_id].get("active_suggestion_id")
        if not suggestion_id:
            suggestion_id = await start_suggestion_session(actual_user_id, context)

        record = suggestion_records[actual_user_id][suggestion_id]

        if not context.user_data[admin_id].get("compose_text") and not context.user_data[admin_id].get("compose_media"):
            if mode == "suggestion":
                record["text"] = ""
                record["media"] = None
            elif mode == "custom_reply":
                record["reply_text"] = ""
                record["reply_media"] = None

        group_name = chat.title if chat.type in ["group", "supergroup"] else "خاص"
        group_id = chat.id
        if group_name == "خاص" or group_id == actual_user_id:
            fallback = context.user_data.get(actual_user_id, {}) or context.bot_data.get(actual_user_id, {})
            group_name = fallback.get("group_title", "غير معروف")
            group_id = fallback.get("group_id", actual_user_id)

        record["group_name"] = group_name
        record["group_id"] = group_id
        context.user_data[admin_id]["compose_mode"] = mode

        if message.text:
            context.user_data[admin_id]["compose_text"] = message.text.strip()
            if mode == "suggestion":
                record["text"] = message.text.strip()
            elif mode == "custom_reply":
                record["reply_text"] = message.text.strip()

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
            if mode == "suggestion":
                record["media"] = {"type": media_type, "file_id": file_id}
            elif mode == "custom_reply":
                record["reply_media"] = {"type": media_type, "file_id": file_id}

        if mode == "suggestion":
            buttons = [
                [InlineKeyboardButton("📤 إرسال", callback_data="send_suggestion")],
                [InlineKeyboardButton("❌ إلغاء", callback_data="cancel_suggestion")]
            ]
        else:
            buttons = [
                [InlineKeyboardButton("📤 إرسال الرد", callback_data="submit_admin_reply")],
                [InlineKeyboardButton("❌ إلغاء", callback_data="cancel_custom_reply")]
            ]

        has_text = context.user_data[admin_id].get("compose_text")
        has_media = context.user_data[admin_id].get("compose_media")

        if has_text and has_media:
            await message.reply_text("✅ تم حفظ النص والوسائط. يمكنك الإرسال الآن:", reply_markup=InlineKeyboardMarkup(buttons))
        elif has_text:
            await message.reply_text("📎 لقد قمت بادخال النص بنجاج . يمكنك الآن إدخال وسائط أو الإرسال:", reply_markup=InlineKeyboardMarkup(buttons))
        elif has_media:
            await message.reply_text("🖼️ لقد قمت بادخال الوسائط بنجاح . يمكنك الآن إدخال نص أو الإرسال:", reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await message.reply_text("⚠️ لم يتم تسجيل أي محتوى. الرجاء إدخال نص أو وسائط.")
        return

    # ✅ استعلام قطع الغيار بالنص
    if (
        context.user_data.get(user_id, {}).get("action") == "parts"
        and message.text
        and chat.type == "private"
        and context.user_data.get(user_id, {}).get("session_valid")
    ):
        # ✅ تسجيل رسالة المستخدم نفسها ليتم حذفها بعد 15 دقيقة
        register_message(user_id, message.message_id, chat.id, context)

        part_name = message.text.strip().lower()
        MAX_ATTEMPTS = 8
        current_attempts = context.user_data[user_id].get("search_attempts", 0)

        # ✅ تجاوز الحد الأقصى للمحاولات
        if current_attempts >= MAX_ATTEMPTS:
            msg = await message.reply_text(
                "🚫 لقد استهلكت جميع استعلامات البحث اليدوي (8 استعلامات).\n🔁 ابدأ من جديد باستخدام (go) من المجموعة."
            )
            register_message(user_id, msg.message_id, chat.id, context)
            context.user_data[user_id].clear()
            return

        # ✅ تحديث عداد المحاولات
        context.user_data[user_id]["search_attempts"] = current_attempts + 1
        remaining = MAX_ATTEMPTS - current_attempts - 1

        # ✅ رسالة توضح رقم الاستعلام المتبقي + جدولتها للحذف
        if remaining > 0:
            info_msg = await message.reply_text(
                f"🔁 تم تسجيل الاستعلام رقم {current_attempts + 1}.\nتبقى لك {remaining} من أصل {MAX_ATTEMPTS} استعلامات."
            )
            register_message(user_id, info_msg.message_id, chat.id, context)
        else:
            info_msg = await message.reply_text("⚠️ تبقى آخر استعلام مسموح لك خلال هذي الجلسة.")
            register_message(user_id, info_msg.message_id, chat.id, context)

        # ✅ جدولة تصفير عداد البحث اليدوي بعد 15 دقيقة من آخر استعلام
        if context.job_queue:
            try:
                context.job_queue.run_once(
                    reset_manual_search_state,
                    when=timedelta(minutes=15),
                    data={"user_id": user_id}
                )
            except Exception as e:
                logging.warning(f"[JOB ERROR] فشل في جدولة تصفير عداد البحث اليدوي للمستخدم {user_id}: {e}")

        selected_car = context.user_data[user_id].get("selected_car")
        if not selected_car:
            msg = await message.reply_text("❗ لم يتم اختيار فئة السيارة.")
            register_message(user_id, msg.message_id, chat.id, context)
            return

        filtered_df = df_parts[df_parts["Station No"] == selected_car]
        columns_to_search = ["Station Name", "Part No"]
        matches = filtered_df[
            filtered_df[columns_to_search].apply(
                lambda x: x.str.contains(part_name, case=False, na=False)
            ).any(axis=1)
        ]

        if matches.empty:
            msg = await message.reply_text("❌ لم يتم العثور على نتائج او الادخال خاطي.")
            register_message(user_id, msg.message_id, chat.id, context)
            return

        user_name = message.from_user.full_name
        user_name_safe = html.escape(user_name)
        selected_car_safe = html.escape(selected_car)
        part_name_safe = html.escape(part_name)
        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

        header = (
            "🧑‍💼 استعلام خاص بـ: "
            f"<i>{user_name_safe}</i>\n"
            "🚗 فئة السيارة: "
            f"<i>{selected_car_safe}</i>\n\n"
        )

        results_header = (
            f"<b>📌 نتائج البحث عن:</b> <code>{part_name_safe}</code>\n"
        )

        lines = []
        for idx, (_, row) in enumerate(matches.iterrows(), start=1):
            station = html.escape(str(row.get("Station Name", "غير معروف")))
            part_no = html.escape(str(row.get("Part No", "غير معروف")))
            price = get_part_price(row)

            line_parts = [
                f"{idx}️⃣ <b>{station}</b>",
                f"   <code>رقم القطعة: {part_no}</code>",
            ]

            if price:
                price_disp = html.escape(str(price)).strip()
                if "ريال" not in price_disp and "SAR" not in price_disp.upper():
                    price_disp = f"{price_disp} ريال"
                line_parts.append(f"   <code>السعر التقريبي: {price_disp}</code>")

            lines.append("\n".join(line_parts))

        body = "\n\n".join(lines)

        # 💡 ملاحظة بدون span
        note_line = (
            "\n\n<i>💡 يمكن عرض صور قطع الغيار بشكل أوضح من خلال التصنيفات داخل خدمة قطع الغيار.</i>"
        )

        footer = (
            f"\n\n<code>⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 15 دقيقة "
            f"({delete_time} / 🇸🇦)</code>"
        )

        text = header + results_header + body + note_line + footer

        keyboard_rows = []
        keyboard_rows.append(
            [InlineKeyboardButton("🗂 عرض القطع المصنفة", callback_data=f"consumable_{user_id}")]
        )

        parts_brand = context.user_data[user_id].get("parts_brand")
        if parts_brand:
            safe_brand = parts_brand.replace(" ", "_")
            keyboard_rows.append(
                [InlineKeyboardButton("⬅️ رجوع لاختيار سيارة", callback_data=f"pbrand_{safe_brand}_{user_id}")]
            )

        keyboard_rows.append(
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
        )

        msg = await message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard_rows),
        )
        register_message(user_id, msg.message_id, chat.id, context)
        return

async def handle_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    mode = context.user_data.get(user_id, {}).get("compose_mode")

    if mode == "suggestion":
        suggestion_records.pop(user_id, None)
        context.user_data[user_id].clear()
        await query.edit_message_text("❌ تم إلغاء الاستفسار/الملاحظة.")
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

    await log_event(update, "📘 فتح قائمة دليل المالك")

    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

    # نحاول أولاً استخدام البراندات
    try:
        manual_df = df_manual
    except Exception as e:
        await log_event(update, f"❌ فشل في تحميل بيانات دليل المالك من Excel: {e}", level="error")
        msg = await query.message.reply_text("📂 تعذر تحميل بيانات دليل المالك حالياً.")
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        return

    brands = []
    if not manual_df.empty and "brand" in manual_df.columns:
        brands = (
            manual_df["brand"]
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )
        brands = [b for b in brands if b]

    # ✅ في حال وجود براندات → نعرض قائمة البراندات
    if brands:
        keyboard = []
        for brand in brands:
            safe_brand = brand.replace(" ", "_")
            keyboard.append(
                [InlineKeyboardButton(brand, callback_data=f"mnlbrand_{safe_brand}_{user_id}")]
            )

        keyboard.append(
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back:main:{user_id}")]
        )

        text = (
            "📘 اختر العلامة التجارية أولاً للاطلاع على دليل المالك:\n\n"
            f"`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
        )

        markup = InlineKeyboardMarkup(keyboard)

        try:
            # لو الرسالة نص نعدلها، لو صورة (غلاف) نرسل رسالة جديدة
            if getattr(query.message, "text", None):
                msg = await query.message.edit_text(
                    text,
                    reply_markup=markup,
                    parse_mode=constants.ParseMode.MARKDOWN
                )
            else:
                msg = await query.message.reply_text(
                    text,
                    reply_markup=markup,
                    parse_mode=constants.ParseMode.MARKDOWN
                )
        except Exception as e:
            await log_event(update, f"❌ فشل في إرسال قائمة براندات دليل المالك: {e}", level="error")
            msg = await query.message.reply_text(
                text,
                reply_markup=markup,
                parse_mode=constants.ParseMode.MARKDOWN
            )

        register_message(user_id, msg.message_id, query.message.chat_id, context)
        context.user_data.setdefault(user_id, {})
        context.user_data[user_id]["manual_msg_id"] = msg.message_id
        context.user_data[user_id]["last_message_id"] = msg.message_id
        return

    # 🔁 في حال عدم وجود عمود brand → نرجع للسلوك القديم (قائمة سيارات مباشرة)
    try:
        car_names = manual_df["car_name"].dropna().drop_duplicates().tolist()
    except Exception as e:
        await log_event(update, f"❌ فشل في تحميل قائمة السيارات من Excel: {e}", level="error")
        msg = await query.message.reply_text("📂 تعذر تحميل قائمة دليل المالك حالياً.")
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        return

    keyboard = [
        [InlineKeyboardButton(car, callback_data=f"manualcar_{car.replace(' ', '_')}_{user_id}")]
        for car in car_names
    ]

    keyboard.append(
        [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back:main:{user_id}")]
    )

    text = (
        "📘 اختر فئة السيارة للاطلاع على دليل المالك:\n\n"
        f"`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
    )

    markup = InlineKeyboardMarkup(keyboard)

    try:
        if getattr(query.message, "text", None):
            msg = await query.message.edit_text(
                text,
                reply_markup=markup,
                parse_mode=constants.ParseMode.MARKDOWN
            )
        else:
            msg = await query.message.reply_text(
                text,
                reply_markup=markup,
                parse_mode=constants.ParseMode.MARKDOWN
            )
    except Exception as e:
        await log_event(update, f"❌ فشل في إرسال قائمة دليل المالك: {e}", level="error")
        msg = await query.message.reply_text(
            text,
            reply_markup=markup,
            parse_mode=constants.ParseMode.MARKDOWN
        )

    register_message(user_id, msg.message_id, query.message.chat_id, context)
    context.user_data.setdefault(user_id, {})
    context.user_data[user_id]["manual_msg_id"] = msg.message_id
    context.user_data[user_id]["last_message_id"] = msg.message_id

async def manual_brand_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    استقبال ضغط زر براند دليل المالك:
    mnlbrand_<BRAND>_<USER_ID>
    """
    query = update.callback_query
    data = query.data.split("_")
    user_id = int(data[-1])

    # البراند قد يحتوي مسافات → نجمع ما بين mnlbrand و user_id
    brand = "_".join(data[1:-1]).replace("_", " ").strip()

    context.user_data.setdefault(user_id, {})
    context.user_data[user_id]["manual_brand"] = brand

    await log_event(update, f"📘 اختيار براند دليل المالك: {brand}")

    try:
        manual_df = df_manual
    except NameError:
        await query.answer("⚠️ بيانات دليل المالك غير متاحة حالياً.", show_alert=True)
        return

    subset = manual_df.copy()
    if "brand" in subset.columns:
        subset = subset[subset["brand"].astype(str).str.strip() == brand]

    car_names = (
        subset.get("car_name", pd.Series(dtype=str))
        .dropna()
        .astype(str)
        .str.strip()
        .drop_duplicates()
        .tolist()
    )

    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

    # 🔁 براند بدون سيارات (تحضيري فقط) → Placeholder
    if not car_names:
        text = (
            f"`🧑‍💻 استعلام خاص بـ {query.from_user.full_name}`\n\n"
            f"🏷 البراند المختار: {brand}\n\n"
            f"📌 {PLACEHOLDER_TEXT}\n\n"
            f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
        )

        keyboard = [
            [InlineKeyboardButton("⬅️ رجوع لاختيار براند آخر", callback_data=f"manual_{user_id}")],
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back:main:{user_id}")],
        ]

        msg = await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=constants.ParseMode.MARKDOWN,
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, f"براند دليل المالك بدون سيارات فعلية: {brand}")
        return

    # ✅ لدينا سيارات لهذا البراند → نعرضها
    keyboard = [
        [
            InlineKeyboardButton(
                car,
                callback_data=f"manualcar_{car.replace(' ', '_')}_{user_id}",
            )
        ]
        for car in car_names
    ]

    # أزرار الرجوع
    keyboard.append(
        [InlineKeyboardButton("⬅️ رجوع لاختيار براند آخر", callback_data=f"manual_{user_id}")]
    )
    keyboard.append(
        [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back:main:{user_id}")]
    )

    text = (
        f"📘 البراند: {brand}\n\n"
        "🚗 اختر فئة السيارة للاطلاع على دليل المالك:\n\n"
        f"`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
    )

    try:
        msg = await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=constants.ParseMode.MARKDOWN,
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        context.user_data[user_id]["manual_msg_id"] = msg.message_id
        context.user_data[user_id]["last_message_id"] = msg.message_id
    except Exception as e:
        await log_event(
            update,
            f"❌ فشل في إرسال قائمة سيارات دليل المالك للبراند {brand}: {e}",
            level="error",
        )


async def handle_manualcar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split("_")
    user_id_from_callback = int(parts[-1])
    car_name = " ".join(parts[1:-1])
    user_name = query.from_user.full_name

    try:
        old_msg_id = context.user_data.get(user_id_from_callback, {}).get("manual_msg_id")
        if old_msg_id:
            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=old_msg_id)
    except:
        pass

    # ✅ نستخدم البراند المخزن إن وجد لتصفية شيت manual
    df = df_manual.copy()
    brand = context.user_data.get(user_id_from_callback, {}).get("manual_brand")
    if brand and "brand" in df.columns:
        df = df[df["brand"].astype(str).str.strip() == str(brand).strip()]

    match = df[df["car_name"].astype(str).str.strip() == car_name.strip()]

    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

    if match.empty:
        caption = get_manual_not_available_message(user_name, car_name, delete_time)
        msg = await query.message.reply_text(caption, parse_mode=constants.ParseMode.MARKDOWN)
        register_message(user_id_from_callback, msg.message_id, query.message.chat_id, context)
        await log_event(update, f"📂 لا توجد بيانات لـ {car_name}", level="error")
        return

    image_url = match["cover_image"].values[0]
    index = match.index[0]

    if pd.isna(image_url) or str(image_url).strip() == "":
        caption = get_manual_not_available_message(user_name, car_name, delete_time)
        msg = await query.message.reply_text(caption, parse_mode=constants.ParseMode.MARKDOWN)
        register_message(user_id_from_callback, msg.message_id, query.message.chat_id, context)
        await log_event(update, f"📂 لا يوجد غلاف لـ {car_name}", level="error")
        return

    caption = get_manual_caption(user_name, car_name)

    # ✅ أزرار: استعراض الدليل + اختيار سيارة اخرى + رجوع للقائمة الرئيسية
    keyboard = [
        [InlineKeyboardButton("📘 استعراض دليل المالك", callback_data=f"openpdf_{index}_{user_id_from_callback}")],
        [InlineKeyboardButton("⬅️ اختيار سيارة اخرى", callback_data=f"manual_{user_id_from_callback}")],
        [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back:main:{user_id_from_callback}")]
    ]

    try:
        msg = await query.message.reply_photo(
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

async def handle_manualdfcar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split("_")
    index = int(parts[1])
    user_id = int(parts[2])

    try:
        row = df_manual.iloc[index]
        car_name = row["car_name"]
        file_id = row["pdf_file_id"]
    except Exception:
        await query.answer("❌ تعذر تحميل الملف – غير متوفر أو بيانات غير صالحة.", show_alert=True)
        return

    user_name = query.from_user.full_name
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

    # لا يوجد ملف PDF متوفر
    if pd.isna(file_id) or str(file_id).strip() == "":
        caption = get_manual_not_available_message(user_name, car_name, delete_time)

        try:
            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
        except:
            pass

        back_keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⬅️ اختيار سيارة اخرى", callback_data=f"manual_{user_id}")],
                [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back:main:{user_id}")],
            ]
        )

        msg = await query.message.reply_text(
            caption,
            parse_mode=constants.ParseMode.MARKDOWN,
            reply_markup=back_keyboard
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, f"📂 لا يوجد ملف PDF لـ {car_name}", level="error")
        return

    # يوجد ملف PDF
    caption = get_manual_caption(user_name, car_name)

    # نحاول حذف الرسالة السابقة (الغلاف مثلاً) قبل إرسال الملف
    try:
        await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
    except:
        pass

    # 🔙 أزرار مع ملف الـ PDF:
    # 1) اختيار سيارة اخرى
    # 2) رجوع للقائمة الرئيسية
    back_keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⬅️ اختيار سيارة اخرى", callback_data=f"manual_{user_id}")],
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back:main:{user_id}")],
        ]
    )

    try:
        msg = await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=file_id,
            caption=caption,
            parse_mode=constants.ParseMode.MARKDOWN,
            reply_markup=back_keyboard
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        context.user_data[user_id]["manual_sent"] = True
        await log_event(update, f"📘 تم إرسال ملف دليل {car_name}")
    except Exception as e:
        await log_event(update, f"❌ فشل في إرسال دليل PDF لـ {car_name}: {e}", level="error")
        await query.message.reply_text("📂 تعذر إرسال الملف. حاول لاحقاً.")

def get_manual_not_available_message(user_name: str, car_name: str, delete_time: str) -> str:
    return (
        f"`🧑‍💻 استعلام خاص بـ {user_name}`\n\n"
        f"📘 نعتذر، دليل المالك للسيارة ({car_name}) غير متوفر حالياً.\n"
        f"📂 سيتم رفع الملف قريباً بالتحديث القادم.\n\n"
        f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
    )

def get_manual_caption(user_name: str, car_name: str) -> str:
    return (
        f"`🧑‍💼 استعلام خاص بـ {user_name}`\n\n"
        f"📜 دليل المالك للسيارة ({car_name})\n\n"
    )

async def select_car_for_parts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data.split("_")
    user_id = int(data[-1])
    car = " ".join(data[1:-1])

    context.user_data.setdefault(user_id, {})
    context.user_data[user_id]["selected_car"] = car
    context.user_data[user_id]["action"] = "parts"
    context.user_data[user_id]["session_valid"] = True  # ✅ تفعيل الجلسة اليدوية

    if "search_attempts" not in context.user_data[user_id]:
        context.user_data[user_id]["search_attempts"] = 0

    # التصنيفات الرئيسية للقطع الاستهلاكية
    part_categories = {
        "🧴 الزيوت": "زيت",
        "🌀 الفلاتر": "فلتر",
        "🔌 البواجي": "بواجي",
        "⚙️ السيور": "سير",
        "🛞 الاقمشة فحمات": "فحمات",
        "💧 السوائل ": "سائل ",
        "🔋 البطاريات": "بطارية",
        "🧼 منتجات مساعدة": "منتج",
    }

    keyboard = [
        [InlineKeyboardButton(name, callback_data=f"catpart_{keyword}_{user_id}")]
        for name, keyword in part_categories.items()
    ]

    # 🔙 زر رجوع لاختيار سيارة أخرى من نفس البراند (إن وجد براند)
    parts_brand = context.user_data[user_id].get("parts_brand")
    if parts_brand:
        safe_brand = parts_brand.replace(" ", "_")
        keyboard.append(
            [InlineKeyboardButton("⬅️ رجوع لاختيار سيارة اخرى", callback_data=f"pbrand_{safe_brand}_{user_id}")]
        )

    # 🔙 زر رجوع للقائمة الرئيسية
    keyboard.append(
        [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
    )

    # ✅ تنسيق الرد النهائي بصيغة احترافية
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")
    user_name = query.from_user.full_name

    text = (
        f"`🧑‍💼 استعلام خاص بـ {user_name}`\n\n"
        f"🚗 الفئة المختارة: {car}\n\n"
        "اختر تصنيف القطعة التي تريد استعلامها:\n"
        "مثال: فلاتر – زيوت – بواجي – سيور – فحمات – سوائل – بطاريات – منتجات مساعدة.\n\n"
        f"`⏳ سيتم حذف هذه الجلسة تلقائياً خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
    )

    # ⬅️ مهم: لو الرسالة الأصلية صورة، edit_message_text سيفشل → نستخدم reply_text
    try:
        msg = await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=constants.ParseMode.MARKDOWN,
        )
    except Exception:
        msg = await query.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=constants.ParseMode.MARKDOWN,
        )

    register_message(user_id, msg.message_id, query.message.chat_id, context)
    await log_event(update, f"عرض تصنيفات القطع للفئة: {car}")
    
async def send_part_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split("_")
    index, user_id = int(parts[2]), int(parts[3])
        
    context.user_data.setdefault(user_id, {})[f"image_opened_{index}"] = True
    row = df_parts.iloc[index]

    user_name = query.from_user.full_name
    user_data = context.user_data.setdefault(user_id, {})
    selected_car = user_data.get("selected_car", "غير معروف")

    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

    station = html.escape(str(row['Station Name'])) if pd.notna(row['Station Name']) else "غير معروف"
    part_no = html.escape(str(row['Part No'])) if pd.notna(row['Part No']) else "غير متوفر"

    caption = (
        f"`🧑‍💻 استعلام خاص بـ: {user_name}`\n"
        f"`🚗 الفئة: {selected_car}`\n\n"
        f"القطعة: {station}\n"
        f"رقم القطعة: {part_no}\n\n"
    )

    # ✅ تحديد ما إذا كانت هذه هي آخر صورة في نفس تصنيف القطع
    reply_markup = None
    last_index = user_data.get("last_image_index_for_cat")

    # إذا هذه هي آخر صورة (أو لم يتم تخزين رقم آخر صورة) نضيف أزرار الرجوع
    if last_index is None or last_index == index:
        buttons = []

        # زر رجوع لقائمة تصنيفات القطع لنفس الفئة
        if selected_car not in (None, "", "غير معروف"):
            safe_car = str(selected_car).replace(" ", "_")
            buttons.append([
                InlineKeyboardButton(
                    "🗂 رجوع لقائمة تصنيفات القطع",
                    callback_data=f"showparts_{safe_car}_{user_id}"
                )
            ])

        # زر الرجوع للقائمة الرئيسية
        buttons.append([
            InlineKeyboardButton(
                "⬅️ رجوع للقائمة الرئيسية",
                callback_data=f"back_main_{user_id}"
            )
        ])

        reply_markup = InlineKeyboardMarkup(buttons)

    msg = await context.bot.send_photo(
        chat_id=query.message.chat_id,
        photo=row["Image"],
        caption=caption,
        parse_mode=constants.ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

    register_message(user_id, msg.message_id, query.message.chat_id, context)
    
async def car_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data.split("_")
    user_id = int(data[-1])

    # اسم السيارة من الكول باك
    car = "_".join(data[1:-1]).replace("_", " ")

    # حفظ نوع السيارة في جلسة المستخدم
    user_data = context.user_data.setdefault(user_id, {})
    user_data["car_type"] = car

    # جلب مسافات الصيانة لهذه السيارة
    kms = (
        df_maintenance[df_maintenance["car_type"] == car]["km_service"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    keyboard = [
        [InlineKeyboardButton(f"{km}", callback_data=f"km_{km}_{user_id}")]
        for km in kms
    ]

    # (اختياري) رجوع لقائمة سيارات نفس البراند إن كان محفوظاً
    brand = user_data.get("brand")
    if brand:
        safe_brand = str(brand).replace(" ", "_")
        keyboard.append(
            [InlineKeyboardButton("⬅️ رجوع لقائمة السيارات", callback_data=f"mbrand_{safe_brand}_{user_id}")]
        )

    # زر رجوع للقائمة الرئيسية
    keyboard.append(
        [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
    )

    # النص مع اسم السيارة في الأعلى
    text = f"🚗 {car}\nاختر مسافة km الصيانة 🧾 :"

    # 🔁 لو الرسالة الأصلية نص → نعدلها، لو كانت ملف/صورة → نرسل رسالة جديدة
    try:
        if getattr(query.message, "text", None):
            msg = await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        else:
            raise Exception("message has no text")
    except Exception:
        msg = await query.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    register_message(user_id, msg.message_id, query.message.chat_id, context)
    await log_event(update, f"اختار {car} من قائمة السيارات")


async def km_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data.split("_")

    # شكل الكول باك: km_<km>_<user_id>
    if len(data) < 3:
        await query.answer("❌ استعلام غير صالح.", show_alert=True)
        return

    km_value = data[1]
    try:
        user_id = int(data[2])
    except ValueError:
        await query.answer("❌ استعلام غير صالح.", show_alert=True)
        return

    # 🔐 حماية الاستعلام ليبقى خاص بصاحبه
    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص ب‏ {requester.first_name} {requester.last_name} - استخدم الأمر go",
            show_alert=True
        )
        return

    user_data = context.user_data.setdefault(user_id, {})
    car = user_data.get("car_type")
    if not car:
        await query.answer("⚠️ لا توجد سيارة محددة لهذه الجلسة.", show_alert=True)
        return

    # 🔎 اختيار الصفوف المطابقة لنوع السيارة والمسافة
    results = df_maintenance[
        (df_maintenance["car_type"] == car) &
        (df_maintenance["km_service"].astype(str) == str(km_value))
    ]

    if results.empty:
        await query.answer("⚠️ لا توجد بيانات صيانة لهذا الطراز عند هذه المسافة.", show_alert=True)
        return

    user_name = query.from_user.full_name
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")
    header = f"`🧑‍💻 استعلام خاص بـ {user_name}`\n\n"

    for i, row in results.iterrows():
        maintenance_action = str(row.get("maintenance_action", "")).strip()

        # 🧩 حالة الطراز قيد التجهيز
        if PLACEHOLDER_TEXT in maintenance_action:
            text = (
                f"{header}"
                f"🚗 *نوع السيارة:* {car}\n"
                f"📏 *المسافة:* {km_value} كم\n\n"
                f"📌 {PLACEHOLDER_TEXT}\n\n"
                f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
            )
        else:
            # ✳️ الحالة العادية: عرض الإجراءات الفعلية من الإكسل
            text = (
                f"{header}"
                f"🚗 *نوع السيارة:* {car}\n"
                f"📏 *المسافة:* {km_value}\n"
                f"🛠️ *الإجراءات:* _{maintenance_action}_\n\n"
                f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
            )

        safe_car = str(car).replace(" ", "_")

        keyboard = [
            [InlineKeyboardButton("عرض تكلفة الصيانة 💰", callback_data=f"cost_{i}_{user_id}")],
            [InlineKeyboardButton("عرض ملف الصيانة 📂", callback_data=f"brochure_{i}_{user_id}")],
            # رجوع لقائمة مسافات الصيانة لنفس السيارة
            [InlineKeyboardButton("⬅️ رجوع لقائمة مسافات الصيانة", callback_data=f"car_{safe_car}_{user_id}")],
            # رجوع للقائمة الرئيسية
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
        ]


        msg = await query.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=constants.ParseMode.MARKDOWN
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)

    await log_event(update, f"اختار {car} على مسافة {km_value} كم")

    # محاولة حذف رسالة اختيار الـ KM بعد الإرسال
    try:
        await asyncio.sleep(1)
        await context.bot.delete_message(
            chat_id=query.message.chat_id,
            message_id=query.message.message_id
        )
    except:
        pass

    # ✅ تفريغ الجلسة بعد انتهاء الاستخدام
    # context.user_data[user_id] = {}

async def send_cost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    index, user_id = int(query.data.split("_")[1]), int(query.data.split("_")[2])

    # 🔐 حماية الاستعلام
    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص ب‏ {requester.first_name} {requester.last_name} - استخدم الأمر go",
            show_alert=True
        )
        return

    result = df_maintenance.iloc[index]
    car_type = result["car_type"]
    km_service = result["km_service"]
    cost = result["cost_in_riyals"]
    maintenance_action = str(result.get("maintenance_action", "")).strip()

    # 🏷 قراءة البراند من شيت الصيانة كما هو
    brand_raw = str(result.get("brand", "")).strip()

    # 🧩 ربط البراند بوكيله:
    if brand_raw:
        br_low = brand_raw.lower()

        # ✅ تطبيع كل صيغ إكسيد → EXEED
        if ("exeed" in br_low) or ("exceed" in br_low) or ("إكسيد" in brand_raw) or ("اكسيد" in brand_raw):
            norm_brand = "EXEED"
        # ✅ شيري
        elif ("chery" in br_low) or ("شيري" in brand_raw):
            norm_brand = "CHERY"
        # ✅ جيتور
        elif ("jetour" in br_low) or ("جيتور" in brand_raw):
            norm_brand = "JETOUR"
        else:
            # أي براند آخر نستخدمه كما هو
            norm_brand = brand_raw

        dealer_key = DEALER_FOR_BRAND.get(norm_brand, norm_brand)
    else:
        dealer_key = "سنابل الحديثة"

    # جلب بيانات الشركة والرقم من القاموس
    contact_info = BRAND_CONTACTS.get(dealer_key, {})
    company_name = contact_info.get("company", "")
    company_phone = contact_info.get("phone", "")

    user_name = query.from_user.full_name
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

    # 🧩 إذا كان هذا الطراز قيد التجهيز → لا نعرض أرقام أسعار
    if PLACEHOLDER_TEXT in maintenance_action:
        caption = (
            f"`🧑‍💻 استعلام خاص بـ {user_name}`\n"
            f"🚗 نوع السيارة: {car_type}\n"
            f"📏 المسافة: {km_service} كم\n\n"
            f"📌 {PLACEHOLDER_TEXT}\n\n"
            f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
        )
    else:
        # ✳️ الحالة العادية: عرض تكلفة الصيانة
        caption = (
            f"`🧑‍💻 استعلام خاص بـ {user_name}`\n"
            f"`📅 آخر تحديث للأسعار: شهر اكتوبر / 2025`\n"
            f"🚗 نوع السيارة: {car_type}\n"
            f"📏 المسافة: {km_service} كم\n"
            f"💰 تكلفة الصيانة: {cost} ريال\n"
            f"🏢 الشركة: {company_name}\n"
            f"📞 للحجز اتصل: {company_phone}\n\n"
            f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
        )

    # حذف زرّي "عرض التكلفة" و "عرض ملف الصيانة" من الرسالة الأصلية
    try:
        keyboard = query.message.reply_markup.inline_keyboard
        updated_keyboard = [
            row for row in keyboard
            if not any(
                (btn.callback_data and ("cost_" in btn.callback_data or "brochure_" in btn.callback_data))
                for btn in row
            )
        ]
        await query.message.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(updated_keyboard) if updated_keyboard else None
        )
    except:
        pass

    safe_car = str(car_type).replace(" ", "_")

    # 🔙 أزرار الرسالة الجديدة لتكلفة الصيانة:
    back_keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📄 عرض ملف الصيانة", callback_data=f"brochure_{index}_{user_id}")],
            [InlineKeyboardButton("⬅️ رجوع لقائمة مسافات الصيانة", callback_data=f"car_{safe_car}_{user_id}")],
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")],
        ]
    )

    msg = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=caption,
        parse_mode=constants.ParseMode.MARKDOWN,
        reply_markup=back_keyboard
    )
    register_message(user_id, msg.message_id, query.message.chat_id, context)

    await log_event(update, f"عرض تكلفة الصيانة للسيارة {car_type} عند {km_service} كم")

    # ✅ لا نمسح الجلسة بالكامل حتى يبقى زر "اختيار سيارة" يعمل بعد الرجوع
    user_data = context.user_data.get(user_id, {})
    if isinstance(user_data, dict):
        # فقط نمسح القيم المؤقتة لو حاب في المستقبل
        for k in ["km_value", "maintenance_results"]:
            user_data.pop(k, None)
            
async def maintenance_brand_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    استقبال ضغط زر براند الصيانة:
    mbrand_<BRAND>_<USER_ID>
    """
    query = update.callback_query
    data = query.data.split("_")
    user_id = int(data[-1])

    # قد يكون البراند فيه مسافات، نجمع ما بين mbrand و user_id
    brand = "_".join(data[1:-1]).replace("_", " ").strip()

    context.user_data.setdefault(user_id, {})
    context.user_data[user_id]["brand"] = brand

    if "brand" not in df_maintenance.columns:
        await query.answer("⚠️ بيانات البراند غير متوفرة حالياً.", show_alert=True)
        return

    # استخراج السيارات لهذا البراند من شيت الصيانة
    cars = (
        df_maintenance[
            df_maintenance["brand"].astype(str).str.strip() == brand
        ]["car_type"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    # لو ما في أي سيارة (يعني البراند كله مجرد صفوف تحضيرية)
    if not cars:
        text = (
            f"`🧑‍💻 استعلام خاص بـ {query.from_user.full_name}`\n\n"
            f"🚗 البراند المختار: {brand}\n\n"
            f"📌 {PLACEHOLDER_TEXT}\n\n"
            "`⏳ سيتم إضافة تفاصيل الصيانة لهذا البراند في التحديثات القادمة من فريق GO.`"
        )

        keyboard = [
            [InlineKeyboardButton("⬅️ رجوع لاختيار براند آخر", callback_data=f"maintenance_{user_id}")],
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")],
        ]

        msg = await query.edit_message_text(
            text,
            parse_mode=constants.ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, f"براند بدون سيارات فعلياً في الصيانة: {brand}")
        return

    # ✅ لدينا سيارات لهذا البراند → نعرض القائمة
    keyboard = [
        [
            InlineKeyboardButton(
                car,
                callback_data=f"car_{car.replace(' ', '_')}_{user_id}"
            )
        ]
        for car in cars
    ]
    # زر رجوع لاختيار براند آخر
    keyboard.append(
        [InlineKeyboardButton("⬅️ رجوع لاختيار براند آخر", callback_data=f"maintenance_{user_id}")]
    )
    # زر رجوع للقائمة الرئيسية
    keyboard.append(
        [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
    )

    msg = await query.edit_message_text(
        f"🚗 اختر فئة السيارة ضمن {brand}:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    register_message(user_id, msg.message_id, query.message.chat_id, context)
    await log_event(update, f"عرض سيارات الصيانة للبراند: {brand}")


async def parts_brand_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """استقبال ضغط زر براند قطع الغيار:
    pbrand_<BRAND>_<USER_ID>
    """
    query = update.callback_query
    data = query.data.split("_")
    # آخر جزء هو user_id
    try:
        user_id = int(data[-1])
    except ValueError:
        await query.answer("❌ خطأ في بيانات المستخدم.", show_alert=True)
        return

    # قد يكون اسم البراند يحتوي على مسافات → نجمع ما بين pbrand و user_id
    brand = "_".join(data[1:-1]).replace("_", " ").strip()

    context.user_data.setdefault(user_id, {})
    context.user_data[user_id]["parts_brand"] = brand

    # نحاول قراءة شيت القطع
    try:
        parts_df = df_parts
    except NameError:
        await query.answer("❌ بيانات القطع غير متاحة حالياً.", show_alert=True)
        return

    # تصفية السيارات الخاصة بهذا البراند
    subset = parts_df.copy()
    if "brand" in subset.columns:
        subset = subset[subset["brand"].astype(str).str.strip() == brand]

    cars = (
        subset.get("Station No", pd.Series(dtype=str))
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )
    cars = [c for c in cars if c]

    # لا توجد سيارات لهذا البراند → نعرض Placeholder
    if not cars:
        text = (
            f"🏷 البراند: {brand}\n\n"
            f"🚫 لا توجد حالياً بيانات جاهزة للقطع الاستهلاكية لهذا البراند.\n\n"
            f"📌 {PLACEHOLDER_TEXT}"
        )
        keyboard = [
            [InlineKeyboardButton("⬅️ رجوع لاختيار براند آخر", callback_data=f"consumable_{user_id}")],
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")],
        ]
        msg = await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, f"براند قطع غيار بدون سيارات فعلية: {brand}")
        return

    # لدينا سيارات لهذا البراند → نعرضها
    keyboard = [
        [
            InlineKeyboardButton(
                car,
                callback_data=f"showparts_{car.replace(' ', '_')}_{user_id}"
            )
        ]
        for car in cars
    ]
    # أزرار الرجوع
    keyboard.append([InlineKeyboardButton("⬅️ رجوع لاختيار براند آخر", callback_data=f"consumable_{user_id}")])
    keyboard.append([InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")])

    msg = await query.edit_message_text(
        f"🏷 البراند: {brand}\n\n"
        f"🚗 اختر فئة السيارة لعرض القطع الاستهلاكية:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    register_message(user_id, msg.message_id, query.message.chat_id, context)
    await log_event(update, f"عرض سيارات قطع الغيار للبراند: {brand}")

async def send_brochure(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    index, user_id = int(query.data.split("_")[1]), int(query.data.split("_")[2])

    # 🔐 حماية الاستعلام ليبقى خاص بصاحبه
    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا الاستعلام خاص ب‏ {requester.first_name} {requester.last_name} - استخدم الأمر /go",
            show_alert=True
        )
        return

    result = df_maintenance.iloc[index]
    user_name = query.from_user.full_name
    car_type = result["car_type"]
    km_service = result["km_service"]
    maintenance_action = str(result.get("maintenance_action", "")).strip()

    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

    header = f"`🧑‍💻 استعلام خاص بـ {user_name}`\n"

    safe_car = str(car_type).replace(" ", "_")

    # 🔙 أزرار الرسالة الجديدة لملف الصيانة:
    # 1) عرض تكلفة الصيانة
    # 2) رجوع لقائمة المسافات
    # 3) رجوع للقائمة الرئيسية
    back_keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("💰 عرض تكلفة الصيانة", callback_data=f"cost_{index}_{user_id}")],
            [InlineKeyboardButton("⬅️ رجوع لقائمة مسافات الصيانة", callback_data=f"car_{safe_car}_{user_id}")],
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")],
        ]
    )

    # 🧩 إذا كان الطراز قيد التجهيز → لا نحاول إرسال صورة
    if PLACEHOLDER_TEXT in maintenance_action:
        caption = (
            f"{header}"
            f"*نوع السيارة 🚗:* {car_type}\n"
            f"*المسافة 📏:* {km_service}\n\n"
            f"📌 {PLACEHOLDER_TEXT}\n\n"
            f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
        )

        msg = await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=caption,
            parse_mode=constants.ParseMode.MARKDOWN,
            reply_markup=back_keyboard,
        )
    else:
        # ✳️ الحالة العادية: إرسال صورة البروشور من العمود brochure_display
        caption = (
            f"{header}"
            f"*نوع السيارة 🚗:* {car_type}\n"
            f"*المسافة 📏:* {km_service}\n\n"
            f"`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
        )

        try:
            msg = await context.bot.send_photo(
                chat_id=query.message.chat_id,
                photo=result["brochure_display"],
                caption=caption,
                parse_mode=constants.ParseMode.MARKDOWN,
                reply_markup=back_keyboard,
            )
        except Exception:
            # لو ما فيه صورة أو في خطأ
            msg = await query.message.reply_text(
                "📂 الملف قيد التحديث حاليا سيكون متاح لاحقا.",
                reply_markup=back_keyboard,
            )

    register_message(user_id, msg.message_id, query.message.chat_id, context)

    # حذف زرّي "عرض ملف الصيانة" و "عرض التكلفة" من الرسالة الأصلية (حتى لا يتكرروا فوق)
    try:
        keyboard = query.message.reply_markup.inline_keyboard
        updated_keyboard = [
            row for row in keyboard
            if not any(
                (btn.callback_data and ("brochure_" in btn.callback_data or "cost_" in btn.callback_data))
                for btn in row
            )
        ]
        await query.message.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(updated_keyboard) if updated_keyboard else None
        )
    except:
        pass

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
            delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")
            caption = (
                f"`🧑‍💻 استعلام خاص بـ {user_name}`\n\n"
                f"🗺️  مراكز الخدمة CHERY\n\n"
                f"`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
            )
            msg1 = await context.bot.send_video(
                chat_id=query.message.chat_id,
                video=video_file,
                caption=caption,
                parse_mode=constants.ParseMode.MARKDOWN
            )
            context.user_data[user_id]["map_msg_id"] = msg1.message_id
            register_message(user_id, msg1.message_id, query.message.chat_id, context)

    # ✅ زرّين + زر رجوع في رسالة واحدة
    keyboard = [
        [InlineKeyboardButton("📍 مواقع فروع شركة شيري", callback_data=f"branches_{user_id}")],
        [InlineKeyboardButton("🔧 المتاجر ومراكز الصيانة المستقلة", callback_data=f"independent_{user_id}")],
        [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back:main:{user_id}")]
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

    # 🧹 حذف فيديو المواقع السابق إن وجد
    map_msg_id = context.user_data.get(user_id, {}).get("map_msg_id")
    if map_msg_id:
        try:
            await context.bot.delete_message(
                chat_id=query.message.chat_id,
                message_id=map_msg_id
            )
        except:
            pass
        context.user_data[user_id]["map_msg_id"] = None

    # 🧹 حذف زرّي "📍 مواقع الفروع" و"🔧 المتاجر المستقلة" من الرسالة السابقة
    try:
        old_keyboard = query.message.reply_markup.inline_keyboard
        new_keyboard = [
            row for row in old_keyboard
            if not any(
                btn.callback_data
                and ("branches_" in btn.callback_data or "independent_" in btn.callback_data)
                for btn in row
            )
        ]
        await query.message.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(new_keyboard) if new_keyboard else None
        )
    except:
        pass

    user_name = query.from_user.full_name
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

    header = f"`🧑‍💼 استعلام خاص بـ {user_name}`"
    middle = "🚨 مواقع مراكز الصيانة شيري CHERY"
    footer = f"\n\n`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 15 دقيقة ({delete_time} / 🇸🇦)`"

    # ==========================================================
    # 🛑 حماية مهمة: branches قد تكون dict وليس list → تسبب خطأ
    # ==========================================================

    raw_branches = context.bot_data.get("branches", [])

    branches: list = []

    if isinstance(raw_branches, list):
        branches = raw_branches

    elif isinstance(raw_branches, dict):
        # إذا رفعنا البيانات على شكل dict من الإكسل
        # نجمع كل العناصر داخلها
        for v in raw_branches.values():
            if isinstance(v, list):
                branches.extend(v)
            elif isinstance(v, dict):
                branches.append(v)

    # الآن branches مضمونة أنها قائمة من dicts

    keyboard_rows: list[list[InlineKeyboardButton]] = []

    for branch in branches:
        if not isinstance(branch, dict):
            continue  # حماية إضافية

        city = str(branch.get("city", "")).strip()
        name = str(branch.get("branch_name", "")).strip()
        url = str(branch.get("url", "")).strip()

        if not city:
            continue

        label = f"📍 {city} / {name}" if name else f"📍 {city}"

        if url and url.startswith("http"):
            keyboard_rows.append([InlineKeyboardButton(label, url=url)])
        else:
            keyboard_rows.append([InlineKeyboardButton(label, callback_data=f"not_ready_{user_id}")])

    if not keyboard_rows:
        await query.answer("❌ لا يوجد فروع صالحة للعرض حالياً.", show_alert=True)
        return

    # زر المراكز المستقلة
    keyboard_rows.append(
        [InlineKeyboardButton("🔧 المتاجر ومراكز الصيانة المستقلة", callback_data=f"independent_{user_id}")]
    )

    # زر الرجوع
    keyboard_rows.append(
        [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back:main:{user_id}")]
    )

    msg = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f"{header}\n{middle}:{footer}",
        parse_mode=constants.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
    )

    register_message(user_id, msg.message_id, query.message.chat_id, context)
    await log_event(update, "عرض قائمة فروع مراكز شيري الرسمية")

async def handle_independent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = int(query.data.split("_")[1])

    # 🧹 حذف فيديو المواقع السابق إن وجد
    map_msg_id = context.user_data.get(user_id, {}).get("map_msg_id")
    if map_msg_id:
        try:
            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=map_msg_id)
        except:
            pass
        context.user_data[user_id]["map_msg_id"] = None

    # 🧹 حذف زرّي "🔧 المتاجر والمراكز المستقلة" و "📍 مواقع فروع شركة شيري" من الرسالة القديمة
    try:
        keyboard = query.message.reply_markup.inline_keyboard
        updated_keyboard = [
            row for row in keyboard
            if not any(
                btn.callback_data
                and ("independent_" in btn.callback_data or "branches_" in btn.callback_data)
                for btn in row
            )
        ]
        await query.message.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(updated_keyboard) if updated_keyboard else None
        )
    except:
        pass

    context.user_data.setdefault(user_id, {})["independent_used"] = True

    image_path = "شروط-الصيانة.jpg"
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

    # 🖼 إرسال صورة شروط الصيانة إن وجدت
    if os.path.exists(image_path):
        with open(image_path, "rb") as image_file:
            caption = (
                f"`🧑‍💻 استعلام خاص بـ {query.from_user.full_name}`\n\n"
                f"📋 شروط الصيانة للمراكز المستقلة:\n\n"
                f"يمكنك إجراء الصيانة الدورية لدى المراكز المستقلة مع الحفاظ على الضمان متى ما التزمت "
                f"بقطع الغيار والزيوت المطابقة لتعليمات الشركة الصانعة، وتم تدوين بيانات السيارة والفاتورة "
                f"بشكل صحيح وواضح.\n\n"
                f"`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
            )
            msg1 = await context.bot.send_photo(
                chat_id=query.message.chat_id,
                photo=image_file,
                caption=caption,
                parse_mode=constants.ParseMode.MARKDOWN
            )
            register_message(user_id, msg1.message_id, query.message.chat_id, context)

    # 🌍 قائمة المدن من شيت المراكز المستقلة
    cities = df_independent["city"].dropna().unique().tolist()
    city_buttons = [
        [InlineKeyboardButton(city, callback_data=f"setcity_{city}_{user_id}")]
        for city in cities
    ]

    # ✅ إضافة زر "مواقع فروع شركة شيري" أسفل المدن
    city_buttons.append(
        [InlineKeyboardButton("📍 مواقع فروع شركة شيري", callback_data=f"branches_{user_id}")]
    )

    # ✅ زر رجوع للقائمة الرئيسية أسفل المدن
    city_buttons.append(
        [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back:main:{user_id}")]
    )

    msg2 = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text="🌍 اختر المدينة لعرض المراكز والمتاجر مباشرة:",
        reply_markup=InlineKeyboardMarkup(city_buttons),
        parse_mode=constants.ParseMode.MARKDOWN,
    )
    register_message(user_id, msg2.message_id, query.message.chat_id, context)
    await log_event(update, "عرض قائمة المدن للمراكز والمتاجر المستقلة")


async def set_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split("_")
    city = parts[1]
    user_id = int(parts[2])

    # 🔴 إزالة قفل تكرار المدينة (معطل)
    # if context.user_data.get(user_id, {}).get("city_selected"):

    context.user_data.setdefault(user_id, {})["city"] = city

    try:
        await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
    except:
        pass

    keyboard = [
        [InlineKeyboardButton("✅ قائمة المراكز المعتمدة", callback_data=f"show_centers_{user_id}")],
        [InlineKeyboardButton("🛒 قائمة متاجر قطع الغيار", callback_data=f"show_stores_{user_id}")],
        [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back:main:{user_id}")]
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
    """
    عرض نتائج المراكز / المتاجر المستقلة مع صورة المتجر (إن وجدت) + رابط الموقع من ملف Excel.
    يعتمد على شيت independent بالأعمدة:
    name, phone, type, image_url, location_url, city
    """
    query = update.callback_query
    user_id = query.from_user.id
    city = context.user_data.get(user_id, {}).get("city")

    if not city:
        await query.answer("❌ لم يتم تحديد المدينة. استخدم /go لإعادة التحديد.", show_alert=True)
        return

    # فلترة حسب المدينة ونوع السجل (مثلاً: 'مركز' أو 'متجر')
    try:
        results = df_independent[
            (df_independent["city"] == city) &
            (df_independent["type"].astype(str).str.contains(filter_type))
        ]
    except Exception as e:
        logging.error(f"[INDEPENDENT] خطأ أثناء فلترة البيانات: {e}")
        await query.answer("❌ حدث خطأ أثناء قراءة بيانات المراكز المستقلة.", show_alert=True)
        return

    if results.empty:
        msg = await query.message.reply_text(f"🚫 لا توجد بيانات {filter_type} حالياً في {city}.")
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, f"🚫 لا توجد نتائج {filter_type} في {city}", level="error")
        return

    user_name = query.from_user.full_name
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

    for _, row in results.iterrows():
        name = row.get("name", "بدون اسم")
        phone = row.get("phone", "غير متوفر")
        result_type = row.get("type", "")
        image_url = row.get("image_url", "")
        location_url = row.get("location_url", "")

        # 📝 نص الوصف
        text = (
            f"`🧑‍💻 استعلام خاص بـ {user_name}`\n"
            f"`🏙️ المدينة: {city}`\n\n"
            f"🏪 الاسم: {name}\n"
            f"📞 الهاتف: {phone}\n"
        )

        # 🌐 رابط الموقع إن وجد
        if isinstance(location_url, str) and location_url.strip():
            text += f"🌐 رابط الموقع:\n{location_url.strip()}\n"

        text += (
            f"\n`⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 15 دقيقة ({delete_time} / 🇸🇦)`"
        )

        # 🖼 إذا عندنا رابط صورة صالح نرسلها كصورة + كابشن، غير كذا نرسل نص فقط
        try:
            if isinstance(image_url, str) and image_url.strip().lower().startswith("http"):
                msg = await context.bot.send_photo(
                    chat_id=query.message.chat_id,
                    photo=image_url.strip(),
                    caption=text,
                    parse_mode=constants.ParseMode.MARKDOWN,
                )
            else:
                msg = await query.message.reply_text(
                    text,
                    parse_mode=constants.ParseMode.MARKDOWN
                )
            register_message(user_id, msg.message_id, query.message.chat_id, context)
        except Exception as e:
            logging.warning(f"[INDEPENDENT] فشل إرسال نتيجة مع الصورة لـ {name}: {e}")
            try:
                # fallback: إرسال نص فقط لو الصورة فشلت
                msg = await query.message.reply_text(
                    text,
                    parse_mode=constants.ParseMode.MARKDOWN
                )
                register_message(user_id, msg.message_id, query.message.chat_id, context)
            except Exception as e2:
                logging.error(f"[INDEPENDENT] فشل إرسال نتيجة نصية لـ {name}: {e2}")

    await log_event(update, f"✅ عرض نتائج {filter_type} في {city}")

async def show_center_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = int(query.data.split("_")[2])

    # 🧹 إزالة أزرار اختيار نوع الخدمة من الرسالة القديمة (المراكز + المتاجر)
    try:
        keyboard = query.message.reply_markup.inline_keyboard
        updated_keyboard = [
            row for row in keyboard
            if not any(
                btn.callback_data
                and ("show_centers_" in btn.callback_data or "show_stores_" in btn.callback_data)
                for btn in row
            )
        ]
        await query.message.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(updated_keyboard) if updated_keyboard else None
        )
    except:
        pass

    # 📋 عرض قائمة المراكز المعتمدة
    await _send_independent_results(update, context, filter_type="مركز")

    # 🔁 بعد عرض النتائج: زر "متاجر" + "رجوع"
    back_keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🛒 قائمة متاجر قطع الغيار", callback_data=f"show_stores_{user_id}")],
            [InlineKeyboardButton("🏙️ اختيار مدينة أخرى", callback_data=f"independent_{user_id}")],
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back:main:{user_id}")],
        ]
    )

    back_msg = await query.message.reply_text(
        "يمكنك الآن استعراض متاجر قطع الغيار أو العودة للقائمة الرئيسية:",
        reply_markup=back_keyboard,
    )
    register_message(user_id, back_msg.message_id, query.message.chat_id, context)

    await log_event(
        update,
        f"📜 عرض قائمة المراكز المعتمدة في {context.user_data[user_id].get('city', 'غير معروفة')}"
    )

async def show_store_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = int(query.data.split("_")[2])

    # 🧹 إزالة أزرار اختيار نوع الخدمة من الرسالة القديمة (المراكز + المتاجر)
    try:
        keyboard = query.message.reply_markup.inline_keyboard
        updated_keyboard = [
            row for row in keyboard
            if not any(
                btn.callback_data
                and ("show_centers_" in btn.callback_data or "show_stores_" in btn.callback_data)
                for btn in row
            )
        ]
        await query.message.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(updated_keyboard) if updated_keyboard else None
        )
    except:
        pass

    # 📋 عرض قائمة المتاجر
    await _send_independent_results(update, context, filter_type="متجر")

    # 🔁 بعد عرض النتائج: زر "مراكز" + "رجوع"
    back_keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ قائمة المراكز المعتمدة", callback_data=f"show_centers_{user_id}")],
            [InlineKeyboardButton("🏙️ اختيار مدينة أخرى", callback_data=f"independent_{user_id}")],
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back:main:{user_id}")],
        ]
    )

    back_msg = await query.message.reply_text(
        "يمكنك الآن استعراض المراكز المعتمدة أو العودة للقائمة الرئيسية:",
        reply_markup=back_keyboard,
    )
    register_message(user_id, back_msg.message_id, query.message.chat_id, context)

    await log_event(
        update,
        f"📜 عرض قائمة المتاجر في {context.user_data[user_id].get('city', 'غير معروفة')}"
    )

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    raw_data = query.data or ""

    # 🔙 زر رجوع للقائمة الرئيسية back_main_USERID
    if raw_data.startswith("back_main_"):
        try:
            user_id = int(raw_data.split("_")[2])
        except Exception:
            await query.answer("❌ خطأ في زر الرجوع", show_alert=True)
            return

        keyboard = build_main_menu_keyboard(user_id)

        # نحاول اولاً تعديل الرسالة اذا كانت نص عادي
        msg = None
        try:
            if getattr(query.message, "text", None):
                msg = await query.edit_message_text(
                    "اختر الخدمة المطلوبة:",
                    reply_markup=keyboard
                )
            else:
                # رسالة فيها ملف او كابتشن → نرسل رسالة جديدة
                raise Exception("message has no text")
        except Exception:
            msg = await query.message.reply_text(
                "اختر الخدمة المطلوبة:",
                reply_markup=keyboard
            )

        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, "⬅️ رجوع الى القائمة الرئيسية")
        return

    # ✅ ازرار الرجوع الموحدة back:target:user_id
    if raw_data.startswith("back:"):
        await handle_back(update, context)
        return

    # ✅ معالجة خاصة لزر showparts_ (لأن الاسم فيه مسافات تتحول إلى _)
    if raw_data.startswith("showparts_"):
        try:
            data = raw_data[len("showparts_"):]
            last_underscore = data.rfind("_")
            selected_car = data[:last_underscore].replace("_", " ").strip()
            user_id = int(data[last_underscore + 1:])

            context.user_data.setdefault(user_id, {})
            context.user_data[user_id]["selected_car"] = selected_car

            await select_car_for_parts(update, context)
        except Exception as e:
            logging.error(f"🔴 Error in showparts callback: {e}")
            await query.answer("❌ حدث خطأ أثناء معالجة التصنيف.", show_alert=True)
        return

    # من هنا يكمل كود الأزرار العام
    data = raw_data.split("_")

    # ✅ تحضير action و user_id مع حالات خاصة catpart_ و faultcat_
    action = None
    user_id: Optional[int] = None

    if raw_data.startswith("catpart_"):
        # شكل الداتا: catpart_keyword_userid
        if len(data) < 3:
            await query.answer("⚠️ بيانات غير صالحة، يرجى المحاولة مجددًا.", show_alert=True)
            return
        _, keyword, user_id_str = data
        action = "catpart"
        try:
            user_id = int(user_id_str)
        except ValueError:
            logging.error(f"🔴 فشل في تحليل user_id في catpart: {user_id_str}")
            await query.answer("⚠️ خطأ في البيانات، يرجى المحاولة مجددًا.", show_alert=True)
            return

    elif raw_data.startswith("faultcat_"):
        # شكل الداتا: faultcat_idx_userid
        if len(data) < 3:
            await query.answer("❌ بيانات غير صالحة لهذا الاختيار.", show_alert=True)
            return
        action = "faultcat"
        try:
            user_id = int(data[2])
        except ValueError:
            await query.answer("❌ خطأ في بيانات المستخدم.", show_alert=True)
            return

    else:
        # باقي الأنواع الأخرى مثل parts_123 أو suggestion_123 أو faults_123 أو maintenance_123 ...
        if len(data) < 2:
            await query.answer("⚠️ زر غير مفهوم، يرجى المحاولة مجددًا.", show_alert=True)
            return
        action, user_id_str = data[0], data[1]
        try:
            user_id = int(user_id_str)
        except ValueError:
            logging.error(f"🔴 فشل في تحليل user_id: {user_id_str}")
            await query.answer("⚠️ خطأ في البيانات، يرجى المحاولة مجددًا.", show_alert=True)
            return

    chat = query.message.chat
    context.user_data.setdefault(user_id, {})
    context.user_data[user_id]["group_title"] = chat.title or "خاص"
    context.user_data[user_id]["group_id"] = chat.id

    # ================== 🔧 خدمة الأعطال الشائعة ==================
    if action == "faults":
        try:
            faults_df = df_faults
        except NameError:
            faults_df = pd.DataFrame()

        # لا يوجد شيت او فارغ
        if faults_df is None or faults_df.empty or "category" not in faults_df.columns:
            text = (
                "🔧 الأعطال الشائعة وحلولها\n\n"
                "هذه الخدمة تحت التحديث حالياً أو لم يتم إضافة بيانات في ملف Excel بعد.\n\n"
                "عند تجهيز قاعدة بيانات الأعطال سوف تظهر لك قائمة بالأنظمة والأعراض والحلول بإذن الله."
            )
            keyboard = [
                [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
            ]
            msg = await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            register_message(user_id, msg.message_id, query.message.chat_id, context)
            await log_event(update, "محاولة فتح خدمة الاعطال الشائعة بدون بيانات")
            return

        # تجهيز قائمة الانظمة / التصنيفات
        categories = (
            faults_df["category"]
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )

        if not categories:
            text = (
                "🔧 الأعطال الشائعة وحلولها\n\n"
                "لم يتم العثور على أي تصنيفات للأعطال في ملف Excel.\n"
                "فضلاً قم بإضافة بيانات في شيت faults."
            )
            keyboard = [
                [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
            ]
            msg = await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            register_message(user_id, msg.message_id, query.message.chat_id, context)
            return

        # حفظ التصنيفات في user_data مع الفهرس
        context.user_data[user_id]["fault_categories"] = categories

        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

        keyboard = []
        for idx, cat in enumerate(categories):
            keyboard.append(
                [InlineKeyboardButton(cat, callback_data=f"faultcat_{idx}_{user_id}")]
            )

        # زر رجوع
        keyboard.append(
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
        )

        text = (
            "🔧 الأعطال الشائعة وحلولها\n\n"
            "اختر النظام أو التصنيف الذي ترغب عرض الأعطال الشائعة الخاصة به:\n\n"
            "`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 15 دقيقة "
            f"({delete_time} / 🇸🇦)`"
        )

        msg = await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=constants.ParseMode.MARKDOWN
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, "فتح قائمة الاعطال الشائعة الرئيسية")
        return

    elif action == "faultcat":
        # عرض اعطال تصنيف معين
        if len(data) < 3:
            await query.answer("❌ بيانات غير صالحة لهذا الاختيار.", show_alert=True)
            return

        idx = int(data[1])

        user_store = context.user_data.get(user_id, {})
        categories = user_store.get("fault_categories", [])

        if not categories or idx < 0 or idx >= len(categories):
            await query.answer("❌ لم يتم العثور على هذا التصنيف. حاول من جديد عبر القائمة الرئيسية.", show_alert=True)
            return

        selected_category = categories[idx]

        try:
            faults_df = df_faults
        except NameError:
            faults_df = pd.DataFrame()

        if faults_df is None or faults_df.empty:
            await query.answer("❌ لا توجد بيانات أعطال حالياً.", show_alert=True)
            return

        # تصفية الاعطال حسب التصنيف
        subset = faults_df[
            faults_df["category"].astype(str).str.strip() == str(selected_category).strip()
        ]

        if subset.empty:
            msg = await query.message.reply_text(
                f"🚫 لا توجد أعطال مسجلة حالياً تحت التصنيف:\n• {selected_category}"
            )
            register_message(user_id, msg.message_id, query.message.chat_id, context)
            await log_event(update, f"لا توجد اعطال لتصنيف {selected_category}")
            return

        user_name = query.from_user.full_name
        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

        for _, row in subset.iterrows():
            car_type = row.get("car_type", "")
            symptom = row.get("symptom", "")
            cause = row.get("cause", "")
            solution = row.get("solution", "")

            text = (
                f"`🧑‍💻 استعلام خاص بـ {user_name}`\n"
                f"`🔧 النظام / التصنيف: {selected_category}`\n"
            )

            if str(car_type).strip():
                text += f"`🚗 نوع السيارة (إن وجد): {car_type}`\n"

            text += "\n"

            if str(symptom).strip():
                text += f"🔹 العَرَض:\n{symptom}\n\n"
            if str(cause).strip():
                text += f"🔹 السبب المحتمل:\n{cause}\n\n"
            if str(solution).strip():
                text += f"🔹 الحل المقترح:\n{solution}\n\n"

            text += (
                f"`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 15 دقيقة "
                f"({delete_time} / 🇸🇦)`"
            )

            msg = await query.message.reply_text(
                text,
                parse_mode=constants.ParseMode.MARKDOWN
            )
            register_message(user_id, msg.message_id, query.message.chat_id, context)

        # رسالة ختامية فيها أزرار رجوع:
        # 1) العودة لقائمة الأعطال
        # 2) رجوع للقائمة الرئيسية
        back_keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⬅️ العودة لقائمة الأعطال", callback_data=f"faults_{user_id}")],
                [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")],
            ]
        )
        back_msg = await query.message.reply_text(
            "يمكنك العودة إلى قائمة الأعطال أو الرجوع إلى القائمة الرئيسية:",
            reply_markup=back_keyboard
        )
        register_message(user_id, back_msg.message_id, query.message.chat_id, context)

        await log_event(update, f"عرض اعطال التصنيف: {selected_category}")
        return

    # ================== الصيانة الدورية بنظام البراندات ==================
    if action == "maintenance":
        # نحدد أن المستخدم داخل مسار الصيانة
        context.user_data.setdefault(user_id, {})
        context.user_data[user_id]["action"] = "maintenance"

        # نحاول نقرأ البراندات من شيت الصيانة
        if "brand" in df_maintenance.columns:
            brands = (
                df_maintenance["brand"]
                .dropna()
                .astype(str)
                .str.strip()
                .unique()
                .tolist()
            )
            brands = [b for b in brands if b]  # حذف الفراغات إن وجدت
        else:
            brands = []

        # لو مافي عمود brand لأي سبب نرجع للسلوك القديم (قائمة سيارات واحدة)
        if not brands:
            cars = (
                df_maintenance["car_type"]
                .dropna()
                .astype(str)
                .str.strip()
                .unique()
                .tolist()
            )

            keyboard = [
                [
                    InlineKeyboardButton(
                        car,
                        callback_data=f"car_{car.replace(' ', '_')}_{user_id}"
                    )
                ]
                for car in cars
            ]
            keyboard.append(
                [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
            )

            msg = await query.edit_message_text(
                "🚗 اختر فئة السيارة للصيانة الدورية:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            register_message(user_id, msg.message_id, query.message.chat_id, context)
            await log_event(update, "فتح قائمة الصيانة الدورية (بدون براندات)")
            return

        # ✅ هنا السلوك الجديد: عرض براندات أولاً
        keyboard = []
        for brand in brands:
            safe_brand = brand.replace(" ", "_")
            keyboard.append(
                [
                    InlineKeyboardButton(
                        brand,
                        callback_data=f"mbrand_{safe_brand}_{user_id}"
                    )
                ]
            )

        # زر رجوع للقائمة الرئيسية
        keyboard.append(
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
        )

        msg = await query.edit_message_text(
            "🏷 اختر العلامة التجارية أولاً ثم سيتم عرض فئات السيارات:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, "فتح قائمة الصيانة الدورية حسب البراند")
        return


    if action == "parts":
        keyboard = [
            # استعلام القطع الاستهلاكية (يبقى كما هو)
            [InlineKeyboardButton(
                "🧩 استعلام قطع الغيار الاستهلاكية",
                callback_data=f"consumable_{user_id}"
            )],
            # استعلام قطع غيار عام → يفتح موقع شيري مباشرة كرابط
            [InlineKeyboardButton(
                "🧩 استعلام قطع غيار عام (موقع شيري الرسمي)",
                url="https://www.cheryksa.com/ar/spareparts"
            )],
            # زر الرجوع للقائمة الرئيسية
            [InlineKeyboardButton(
                "⬅️ رجوع للقائمة الرئيسية",
                callback_data=f"back_main_{user_id}"
            )],
        ]

        msg = await query.edit_message_text(
            "اختر نوع استعلام قطع الغيار ⚙️ :",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, "اختار استعلام قطع الغيار")
        return

    elif action in ("external", "extparts"):
        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")
        keyboard = [[InlineKeyboardButton("🔗 فتح موقع الاستعلام", url="https://www.cheryksa.com/ar/spareparts")]]
        msg = await query.edit_message_text(
            "🌐 تم تجهيز الرابط، اضغط الزر بالأسفل للانتقال إلى موقع استعلام قطع غيار شيري الرسمي:\n\n"
            f"`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 15 دقيقة ({delete_time} / 🇸🇦)`",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=constants.ParseMode.MARKDOWN
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, "تم فتح رابط قطع الغيار الخارجي (extparts)")
        return


    elif action in ("external", "extparts"):
        # دعم الاسم القديم external والجديد extparts لنفس الوظيفة
        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")
        keyboard = [[InlineKeyboardButton("🔗 فتح موقع الاستعلام", url="https://www.cheryksa.com/ar/spareparts")]]
        msg = await query.edit_message_text(
            "🌐 تم تجهيز الرابط، اضغط الزر بالأسفل للانتقال إلى موقع استعلام قطع غيار شيري الرسمي:\n\n"
            f"`⏳ سيتم حذف هذا الاستعلام تلقائياً خلال 15 دقيقة ({delete_time} / 🇸🇦)`",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=constants.ParseMode.MARKDOWN
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, "تم فتح رابط قطع الغيار الخارجي")
        return

    elif action == "consumable":
        # أولاً نحاول عرض البراندات من شيت parts
        try:
            parts_df = df_parts
        except NameError:
            parts_df = pd.DataFrame()

        brands = []
        if not parts_df.empty and "brand" in parts_df.columns:
            brands = (
                parts_df["brand"]
                .dropna()
                .astype(str)
                .str.strip()
                .unique()
                .tolist()
            )
            brands = [b for b in brands if b]

        # في حال توفر البراندات → نعرض قائمة البراندات أولاً
        if brands:
            keyboard = []
            for brand in brands:
                safe_brand = brand.replace(" ", "_")
                keyboard.append(
                    [InlineKeyboardButton(brand, callback_data=f"pbrand_{safe_brand}_{user_id}")]
                )

            keyboard.append(
                [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
            )

            msg = await query.edit_message_text(
                "🏷 اختر العلامة التجارية أولاً لعرض فئات السيارات للقطع الاستهلاكية:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            register_message(user_id, msg.message_id, query.message.chat_id, context)
            await log_event(update, "فتح قائمة البراندات للقطع الاستهلاكية (parts)")
            return

        # في حال عدم توفر عمود brand نعود للسلوك القديم (قائمة سيارات واحدة)
        keyboard = []

        for car in unique_cars:
            callback_data = f"showparts_{car.replace(' ', '_')}_{user_id}"
            keyboard.append([InlineKeyboardButton(car, callback_data=callback_data)])

        # زر رجوع في اسفل القائمة
        keyboard.append([InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")])

        if not unique_cars:
            await query.edit_message_text("❌ لا توجد سيارات متاحة في قاعدة البيانات.")
            await log_event(update, "❌ لا توجد سيارات متاحة في قاعدة البيانات (consumable)")
            return

        msg = await query.edit_message_text("🚗 اختر فئة السيارة المطلوبة:", reply_markup=InlineKeyboardMarkup(keyboard))
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, "عرض قائمة السيارات للقطع الاستهلاكية (بدون براندات)")
        return

    elif action == "catpart":
        keyword = data[1]
        user_id = int(data[2])
        selected_car = context.user_data[user_id].get("selected_car")

        if not selected_car:
            await query.answer("❌ يرجى اختيار فئة السيارة أولاً.", show_alert=True)
            return

        filtered_df = df_parts[df_parts["Station No"] == selected_car]
        matches = filtered_df[
            filtered_df["Station Name"]
            .astype(str)
            .str.strip()
            .str.contains(f"^{keyword}|\\s{keyword}", case=False, na=False)
        ]

        if matches.empty:
            await query.answer("❌ لم يتم توفير بيانات لهذا التصنيف بعد.\nهذا الطراز قيد الإعداد من فريق GO.", show_alert=True)
            return

    # 📌 ➤ إضافة بسيطة فقط: حفظ آخر صورة في هذا التصنيف
        last_image_index = None
        for idx, row in matches.iterrows():
            if pd.notna(row.get("Image")):
                last_image_index = idx

        context.user_data.setdefault(user_id, {})
        context.user_data[user_id]["last_image_index_for_cat"] = last_image_index
    # 📌 انتهى التعديل الوحيد هنا

        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")
        footer = f"\n<code>⏳ سيتم حذف هذا الاستعلام تلقائيًا خلال 15 دقيقة ({delete_time} / 🇸🇦)</code>"

        user_name = query.from_user.full_name

    # 🔹 رسائل القطع داخل التصنيف
        for i, row in matches.iterrows():
            part_name_value = row.get("Station Name", "غير معروف")
            part_number_value = row.get("Part No", "غير معروف")
            price = get_part_price(row)  # 💰 استخراج السعر إن وجد

            text = (
                f"<code>🧑‍💼 استعلام خاص بـ {user_name}</code>\n"
                f"<code>🚗 الفئة: {selected_car}</code>\n\n"
                f"🔹 <b>اسم القطعة:</b> {part_name_value}\n"
                f"🔹 <b>رقم القطعة:</b> {part_number_value}\n"
            )

            if price:
                price_display = price
                if "ريال" not in price and "SAR" not in price.upper():
                    price_display = f"{price} ريال"
                text += f"🔹 <b>السعر التقريبي:</b> {price_display}\n"

            text += f"\n<code>📌 تم العثور على نتائج بناءً على التصنيف</code>{footer}"

            keyboard = []
            if pd.notna(row.get("Image")):
                keyboard.append(
                    [InlineKeyboardButton("عرض الصورة 📸", callback_data=f"part_image_{i}_{user_id}")]
                )

            msg = await query.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
                parse_mode=ParseMode.HTML
            )
            register_message(user_id, msg.message_id, query.message.chat_id, context)

    # 🔹 رسالة ختامية فيها أزرار رجوع
        safe_car = selected_car.replace(" ", "_")
        back_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🗂 رجوع لقائمة تصنيفات القطع", callback_data=f"showparts_{safe_car}_{user_id}")],
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
        ])

        back_msg = await query.message.reply_text(
            "يمكنك الرجوع لقائمة تصنيفات القطع لنفس الفئة أو العودة للقائمة الرئيسية:",
            reply_markup=back_keyboard
        )
        register_message(user_id, back_msg.message_id, query.message.chat_id, context)

        await log_event(update, f"✅ استعلام تصنيفي: {keyword} ضمن {selected_car}")
        return

    elif action == "suggestion":
        context.user_data[user_id]["action"] = "suggestion"

        user_name = query.from_user.full_name
        now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
        delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

        user_block = (
            f"🧑‍💼 *استفسار دعم فني خاص بـ* "
            f"<code><i>{user_name}</i></code>\n"
        )

        prompt_block = (
            "💬 *أهلاً بك في مركز الدعم الفني لنظام GO.*\n\n"

            "✉️ يرجى كتابة استفسارك أو ملاحظتك.\n"
            "   <i>يمكنك إرفاق ملف واحد فقط (صورة – مستند – مقطع صوتي).</i>\n\n"

            "⚠️ *لخدمتك بدقة أعلى:* \n"
            "   <code><i>فضلاً أضف فئة السيارة – الموديل – سنة الصنع داخل الاستفسار.</i></code>\n\n"

            "📎 إذا رغبت بإرسال عدة ملفات، يُفضّل إرسال كل ملف في استفسار مستقل.\n\n"

            f"⏳ <i>سيتم حفظ هذه الجلسة مؤقتاً لمتابعة رد فريق GO ({delete_time} / 🇸🇦)</i>"
        )

        text = f"{user_block}\n\n{prompt_block}"

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 إرسال الاستفسار إلى فريق GO", callback_data="send_suggestion")],
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]
        ])

        msg = await query.edit_message_text(
            text,
            reply_markup=keyboard,
            parse_mode=constants.ParseMode.MARKDOWN
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)
        await log_event(update, "بدأ المستخدم إرسال استفسار أو ملاحظة عبر مركز الدعم الفني")

        if "active_suggestion_id" not in context.user_data[user_id]:
            suggestion_id = await start_suggestion_session(user_id, context)
        else:
            suggestion_id = context.user_data[user_id]["active_suggestion_id"]

        suggestion_records[user_id][suggestion_id]["group_name"] = chat.title if chat.title else "خاص"
        suggestion_records[user_id][suggestion_id]["group_id"] = chat.id
        suggestion_records[user_id][suggestion_id]["user_name"] = update.effective_user.full_name
        return

async def start_team_general_thread(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """زر: team_main_USERID من القائمة الرئيسية"""
    query = update.callback_query
    data = (query.data or "").split("_")

    if len(data) != 3:
        await query.answer("❌ بيانات غير صالحة.", show_alert=True)
        return

    try:
        admin_id_from_cb = int(data[2])
    except ValueError:
        await query.answer("❌ خطأ في رقم المستخدم.", show_alert=True)
        return

    admin = query.from_user
    admin_id = admin.id

    if admin_id != admin_id_from_cb or admin_id not in AUTHORIZED_USERS:
        await query.answer("❌ غير مصرح لك باستخدام هذا الزر.", show_alert=True)
        return

    thread_id = _next_team_thread_id()
    team_threads[thread_id] = {
        "type": "general",
        "created_by": admin_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "context": {
            "source": "main_menu",
            "chat_id": query.message.chat.id,
            "chat_title": getattr(query.message.chat, "title", "خاص"),
        },
        "reply_count": 0,
    }

    state = context.user_data.setdefault(admin_id, {})
    state["team_mode"] = True
    state["team_thread_id"] = thread_id

    await query.answer()
    await context.bot.send_message(
        chat_id=admin_id,
        text=(
            f"🧵 تم فتح نقاش داخلي جديد لفريق GO رقم #{thread_id}.\n\n"
            "✍️ اكتب رسالتك الأولى الآن، وسيتم إرسالها لبقية المشرفين."
        ),
    )
async def start_team_opinion_thread(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """زر: team_opinion_userId_suggestionId من إشعارات الرد"""
    query = update.callback_query
    data = (query.data or "").split("_")

    if len(data) < 3:
        await query.answer("❌ بيانات غير صالحة.", show_alert=True)
        return

    admin = query.from_user
    admin_id = admin.id
    if admin_id not in AUTHORIZED_USERS:
        await query.answer("❌ غير مصرح لك باستخدام هذا الزر.", show_alert=True)
        return

    try:
        user_id = int(data[2])
    except ValueError:
        await query.answer("❌ رقم مستخدم غير صحيح.", show_alert=True)
        return

    # suggestion_id هو بقية السلسلة (عادة uuid بدون _، لكن للاحتياط)
    suggestion_id = "_".join(data[3:]) if len(data) > 3 else ""
    record = suggestion_records.get(user_id, {}).get(suggestion_id)
    if not record:
        await query.answer("⚠️ لا يوجد سجل لهذا الاستفسار.", show_alert=True)
        return

    thread_id = _next_team_thread_id()
    team_threads[thread_id] = {
        "type": "suggestion",
        "created_by": admin_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "context": {
            "user_id": user_id,
            "user_name": record.get("user_name"),
            "group_name": record.get("group_name"),
            "group_id": record.get("group_id"),
            "suggestion_id": suggestion_id,
            "text": record.get("text"),
        },
        "reply_count": 0,
    }

    state = context.user_data.setdefault(admin_id, {})
    state["team_mode"] = True
    state["team_thread_id"] = thread_id

    await query.answer()
    await context.bot.send_message(
        chat_id=admin_id,
        text=(
            f"🧵 تم فتح نقاش داخلي حول استفسار العضو {record.get('user_name','')} "
            f"(نقاش #{thread_id}).\n\n"
            "✍️ اكتب رأيك أو ملاحظتك الآن، وسيتم إرسالها لبقية المشرفين."
        ),
    )
async def team_reply_existing_thread(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """زر: team_reply_threadId من رسالة نقاش سابقة"""
    query = update.callback_query
    data = (query.data or "").split("_")

    if len(data) != 3:
        await query.answer("❌ بيانات غير صالحة.", show_alert=True)
        return

    try:
        thread_id = int(data[2])
    except ValueError:
        await query.answer("❌ رقم نقاش غير صحيح.", show_alert=True)
        return

    admin = query.from_user
    admin_id = admin.id
    if admin_id not in AUTHORIZED_USERS:
        await query.answer("❌ غير مصرح لك باستخدام هذا الزر.", show_alert=True)
        return

    if thread_id not in team_threads:
        await query.answer("⚠️ هذا النقاش لم يعد موجوداً.", show_alert=True)
        return

    state = context.user_data.setdefault(admin_id, {})
    state["team_mode"] = True
    state["team_thread_id"] = thread_id

    await query.answer()
    await context.bot.send_message(
        chat_id=admin_id,
        text=(
            f"🧵 نقاش فريق GO #{thread_id}\n\n"
            "✍️ اكتب ردك الآن ليتم إرساله لبقية المشرفين ضمن هذا النقاش."
        ),
    )

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
        await update.message.reply_text("⚠️ لا يمكنك إرسال استفسار جديد قبل الانتهاء من الحالي.")
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

    # ✅ رسالة ترحيب أوضح من مركز الدعم + تنسيق اسم المستخدم ونص الحذف بخط نحيف رمادي
    now_saudi = datetime.now(timezone.utc) + timedelta(hours=3)
    delete_time = (now_saudi + timedelta(minutes=15)).strftime("%I:%M %p")

    user_block = (
        f"🧑‍💼 *استفسار دعم فني خاص بـ* "
        f"<code><i>{user_name}</i></code>\n"
    )

    prompt_block = (
        "💬 *أهلاً بك في مركز الدعم الفني لنظام GO.*\n\n"

        "✉️ يرجى كتابة استفسارك أو ملاحظتك.\n"
        "   <i>يمكنك إرفاق ملف واحد فقط (صورة – مستند – مقطع صوتي).</i>\n\n"

        "⚠️ *لخدمتك بدقة أعلى:* \n"
        "   <code><i>فضلاً أضف فئة السيارة – الموديل – سنة الصنع داخل الاستفسار.</i></code>\n\n"

        "📎 إذا رغبت بإرسال عدة ملفات، يُفضّل إرسال كل ملف في استفسار مستقل.\n\n"

        f"⏳ <i>سيتم حفظ هذه الجلسة مؤقتاً لمتابعة رد فريق GO ({delete_time} / 🇸🇦)</i>"
    )

    text = f"{user_block}\n\n{prompt_block}\n\n{delete_block}"

    msg = await update.message.reply_text(
        text,
        parse_mode=constants.ParseMode.MARKDOWN
    )
    register_message(user_id, msg.message_id, chat.id, context)

    await log_event(update, "بدأ المستخدم إرسال استفسار أو ملاحظة عبر مركز الدعم الفني")

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
        await query.answer("❌ لا يوجد سجل لهذا الاستفسار.", show_alert=True)
        return

    if record.get("replied_by") and record.get("caption"):
        await query.answer(
            f"🟥 تم الرد على هذا الاستفسار مسبقًا من قبل: {record['replied_by']}",
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
        await query.answer("⚠️ لا توجد جلسة دعم نشطة.", show_alert=True)
        return

    record = suggestion_records.get(user_id, {}).get(suggestion_id)
    if not record:
        await query.answer("⚠️ لا يوجد استفسار أو ملاحظة محفوظ.", show_alert=True)
        return

    text = record.get("text", "")
    media = record.get("media")

    if not text and not media:
        await query.answer("⚠️ لا يمكن إرسال الاستفسار فارغ.", show_alert=True)
        return

    # تنظيف بيانات الرد السابقة
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
        [InlineKeyboardButton("📝 الرد على الاستفسار الوارد", callback_data=f"reply_{user_id}_{suggestion_id}")]
    ])

    record["admin_messages"] = {}

    # إرسال الاستفسار إلى كل مشرف
    for admin_id in AUTHORIZED_USERS:
        try:
            sent = None
            full_caption = header

            if media:
                mtype = media["type"]
                fid = media["file_id"]
                if text:
                    full_caption += f"\n\n📝 <b>الاستفسار الوارد :</b>\n{text}"

                if mtype == "photo":
                    sent = await context.bot.send_photo(
                        admin_id, fid,
                        caption=full_caption,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
                elif mtype == "video":
                    sent = await context.bot.send_video(
                        admin_id, fid,
                        caption=full_caption,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
                elif mtype == "document":
                    sent = await context.bot.send_document(
                        admin_id, fid,
                        caption=full_caption,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
                elif mtype == "voice":
                    sent = await context.bot.send_voice(
                        admin_id, fid,
                        caption=full_caption,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
            else:
                suggestion_block = f"\n\n📝 <b>الاستفسار الوارد:</b>\n<code>{text}</code>" if text else ""
                full_caption += suggestion_block
                sent = await context.bot.send_message(
                    admin_id,
                    text=full_caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard
                )

            if sent:
                record["admin_messages"][admin_id] = sent.message_id

        except Exception as e:
            logging.error(f"[استفسار] فشل في إرسال الاستفسار للمشرف {admin_id}: {e}")

    record["submitted"] = True
    record["timestamp"] = datetime.now()

    # حذف رسالة المعاينة إن أمكن
    try:
        await query.message.delete()
    except:
        pass

    # ✅ رسالة شكر للمستخدم + زر رجوع
    thank_you_message = (
        f"`🧑‍💼 استفسار دعم فني خاص بـ {user_name}`\n\n"
        "🎉 شكرًا لمساهمتك معنا!\n\n"
        "✅ تم إرسال الاستفسار بنجاح إلى فريق الدعم GO.\n"
        "📌 سيتم مراجعة طلبك والرد عليك في هذه المحادثة.\n\n"
        "`يمكنك العودة في أي وقت إلى القائمة الرئيسية من الزر بالأسفل.`"
    )

    back_keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✉️ إرسال استفسار آخر", callback_data=f"suggestion_{user_id}")],
            [InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")],
        ]
    )

    await context.bot.send_message(
        chat_id=user_id,
        text=thank_you_message,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=back_keyboard
    )

    # تفريغ سياق المستخدم بعد الإرسال
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
        await query.answer("❌ لا يوجد سجل لهذا الاستفسار.", show_alert=True)
        return

    existing_admin = record.get("replied_by")
    if existing_admin and existing_admin != admin_name:
        await query.answer(
            f"🟥 تم الرد مسبقًا على هذا الاستفسار من قبل: {existing_admin}",
            show_alert=True
        )
        return

    # 🔁 عدّاد الردود
    reply_count = int(record.get("reply_count", 0) or 0)
    is_additional = reply_count >= 1
    reply_count += 1
    record["reply_count"] = reply_count

    # أول مرة نثبت اسم المشرف المسؤول عن الحالة
    if not existing_admin:
        record["replied_by"] = admin_name

    # 🔁 تصحيح بيانات المجموعة حتى لو كانت الوسائط فقط
    if record.get("group_name") in ["خاص", None] or record.get("group_id") in [None, user_id]:
        record["group_name"] = context.user_data.get(user_id, {}).get("group_title", "غير معروف")
        record["group_id"] = context.user_data.get(user_id, {}).get("group_id", "غير معروف")

    group_name = record.get("group_name", "غير معروف")
    group_id = record.get("group_id", "غير معروف")
    user_name = record.get("user_name", "—")
    original_text = record.get("text") or "❓ لا يوجد استفسار محفوظ."
    reply_text = SUGGESTION_REPLIES.get(reply_key, "📌 تم الرد على استفسارك.")
    has_media = record.get("media")

    # ✅ رسالة المستخدم
    if is_additional:
        user_caption = (
            f"\u200F🔁 *رد إضافي رقم {reply_count} من فريق الدعم GO:*\n\n"
            f"\u200F📝 *استفسارك أو ملاحظتك:*\n"
            f"```{original_text.strip()}```\n\n"
            f"\u200F💬 *رد المشرف:*\n"
            f"```{reply_text.strip()}```\n\n"
            f"\u200F🤖 *شكرًا لمتابعتك معنا.*"
        )
    else:
        user_caption = (
            f"\u200F📣 *رد من قبل فريق الدعم GO:*\n\n"
            f"\u200F📝 *استفسارك أو ملاحظتك:*\n"
            f"```{original_text.strip()}```\n\n"
            f"\u200F💬 *رد المشرف:*\n"
            f"```{reply_text.strip()}```\n\n"
            f"\u200F🤖 *شكرًا لمساهمتك معنا.*"
        )

    # ✅ رسالة المشرفين (إشعار)
    if is_additional:
        admin_caption = (
            f"\u200F🔁 *رد إضافي رقم {reply_count} من فريق الدعم GO:*\n\n"
            f"\u200F👤 `{user_name}`\n"
            f"\u200F🆔 {user_id}\n"
            f"\u200F🏘️ \u202B{group_name}\u202C\n"
            f"\u200F🔢 `{group_id}`\n"
            + (f"\u200F📎 يحتوي على وسائط\n" if has_media else "") + "\n"
            f"\u200F📝 *المداخلة:*\n```{original_text.strip()}```\n\n"
            f"\u200F💬 *رد المشرف:*\n```{reply_text.strip()}```\n\n"
            f"\u200F✅ تم الرد من قبل: `{admin_name}`"
        )
    else:
        admin_caption = (
            f"\u200F📣 *رد من قبل فريق الدعم GO:*\n\n"
            f"\u200F👤 `{user_name}`\n"
            f"\u200F🆔 {user_id}\n"
            f"\u200F🏘️ \u202B{group_name}\u202C\n"
            f"\u200F🔢 `{group_id}`\n"
            + (f"\u200F📎 يحتوي على وسائط\n" if has_media else "") + "\n"
            f"\u200F📝 *المداخلة:*\n```{original_text.strip()}```\n\n"
            f"\u200F💬 *رد المشرف:*\n```{reply_text.strip()}```\n\n"
            f"\u200F✅ تم الرد من قبل: `{admin_name}`"
        )

    try:
        media = record.get("media")

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
            # 🔁 صورة افتراضية مع معالجة في حال عدم توفر الملف
            try:
                with open("GO-CHERY.PNG", "rb") as image:
                    await context.bot.send_photo(user_id, image, caption=user_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception:
                await context.bot.send_message(user_id, text=user_caption, parse_mode=ParseMode.MARKDOWN)

        record["caption"] = user_caption

        # حذف قائمة خيارات الرد من الخاص
        try:
            await query.message.delete()
        except:
            pass

        # ✅ إرسال نسخة للمشرفين مع أزرار النقاش
        for aid in AUTHORIZED_USERS:
            try:
                # أزرار النقاش الداخلي
                buttons = [
                    [InlineKeyboardButton("🟦 دعوة فريق GO للنقاش", callback_data=f"team_main_{aid}")],
                    [InlineKeyboardButton("🗣️ دعوة إبداء رأي", callback_data=f"team_opinion_{user_id}_{suggestion_id}")],
                ]

                if aid == admin_id:
                    # زر إرسال رد آخر لنفس المشرف فقط
                    buttons.insert(
                        0,
                        [InlineKeyboardButton("✉️ إرسال رد آخر", callback_data=f"customreply_{user_id}_{suggestion_id}")]
                    )

                reply_markup = InlineKeyboardMarkup(buttons)

                if media:
                    mtype = media["type"]
                    fid = media["file_id"]
                    if mtype == "photo":
                        await context.bot.send_photo(aid, fid, caption=admin_caption, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
                    elif mtype == "video":
                        await context.bot.send_video(aid, fid, caption=admin_caption, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
                    elif mtype == "document":
                        await context.bot.send_document(aid, fid, caption=admin_caption, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
                    elif mtype == "voice":
                        await context.bot.send_voice(aid, fid, caption=admin_caption, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
                else:
                    # 🔁 نفس منطق الصورة الافتراضية للمشرفين
                    try:
                        with open("GO-CHERY.PNG", "rb") as image:
                            await context.bot.send_photo(aid, image, caption=admin_caption, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
                    except Exception:
                        await context.bot.send_message(aid, text=admin_caption, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)

            except Exception as e:
                logging.warning(f"[HANDLE_SEND_REPLY][admin_notify {aid}] فشل إرسال الإشعار: {e}")

    except Exception as e:
        logging.error(f"[HANDLE_SEND_REPLY] فشل في إرسال الرد للمستخدم {user_id}: {e}")

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
        await query.answer("🚫 فشل في استخراج بيانات الاستفسار.", show_alert=True)
        return

    record = suggestion_records.get(user_id, {}).get(suggestion_id)
    if not record:
        await query.answer("❌ لا يوجد سجل لهذه الاستفسار.", show_alert=True)
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
        await query.answer("❌ لا يوجد سجل لهذه الاستفسار.", show_alert=True)
        return

    admin_name = update.effective_user.full_name
    existing_admin = record.get("replied_by")
    if existing_admin and existing_admin != admin_name:
        await query.answer(
            f"🟥 تم الرد مسبقًا على هذا الاستفسار من قبل: {existing_admin}",
            show_alert=True
        )
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
    original_text = record.get("text", "❓ لا يوجد استفسار محفوظ.")
    has_media = bool(media)

    # ⛑️ تصحيح بيانات المجموعة
    if record.get("group_name") in ["خاص", None] or record.get("group_id") == user_id:
        record["group_name"] = context.user_data.get(user_id, {}).get("group_title", "غير معروف")
        record["group_id"] = context.user_data.get(user_id, {}).get("group_id", "غير معروف")

    group_name = record.get("group_name", "غير معروف")
    group_id = record.get("group_id", "غير معروف")

    user_caption = (
        f"\u200F📣 *رد من قبل فريق الدعم GO:*\n\n"
        f"\u200F📝 *استفسارك أو ملاحظتك:*\n```{original_text.strip()}```\n\n"
        f"\u200F💬 *رد المشرف:*\n```{text.strip()}```\n\n"
        f"\u200F🤖 *شكرًا لمساهمتك معنا.*"
    )

    admin_caption = (
        f"\u200F📣 *رد من قبل فريق الدعم GO:*\n\n"
        f"\u200F👤 `{user_name}`\n"
        f"\u200F🆔 {user_id}\n"
        f"\u200F🏘️ \u202B{group_name}\u202C\n"
        f"\u200F🔢 `{group_id}`\n"
        + (f"\u200F📎 يحتوي على وسائط\n" if has_media else "") + "\n"
        f"\u200F📝 *الاستفسار:*\n```{original_text.strip()}```\n\n"
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
            # 🔁 صورة افتراضية مع معالجة لو الملف غير موجود
            try:
                with open("GO-CHERY.PNG", "rb") as image:
                    await context.bot.send_photo(user_id, image, caption=user_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception:
                await context.bot.send_message(user_id, text=user_caption, parse_mode=ParseMode.MARKDOWN)

        record["replied_by"] = admin_name
        record["caption"] = user_caption

        try:
            await query.message.delete()
        except:
            pass

        # حذف أي منيو قديم للرد من ملفات السجل
        if "reply_menu_chat" in record and "reply_menu_id" in record:
            for aid in AUTHORIZED_USERS:
                try:
                    await context.bot.delete_message(record["reply_menu_chat"], record["reply_menu_id"])
                except:
                    pass
            record.pop("reply_menu_chat", None)
            record.pop("reply_menu_id", None)

        # إشعار جميع المشرفين بالرد
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
                    # 🔁 fallback: صورة افتراضية أو نص فقط
                    try:
                        with open("GO-CHERY.PNG", "rb") as image:
                            await context.bot.send_photo(aid, image, caption=admin_caption, parse_mode=ParseMode.MARKDOWN)
                    except Exception:
                        await context.bot.send_message(aid, text=admin_caption, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logging.warning(f"[رد مخصص - إشعار مشرف {aid}] فشل: {e}")

        # تنظيف حالة المشرف بعد الإرسال
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
        [InlineKeyboardButton("🧹 تنظيف الجلسات", callback_data="clear_sessions")],
        [InlineKeyboardButton("♻️ إعادة تحميل الإعدادات", callback_data="reload_settings")],
        [InlineKeyboardButton("🚧 تفعيل وضع الصيانة", callback_data="ctrl_maintenance_on")],
        [InlineKeyboardButton("✅ إنهاء وضع الصيانة", callback_data="ctrl_maintenance_off")],
        [InlineKeyboardButton("🧨 تدمير البيانات", callback_data="self_destruct")],
        [InlineKeyboardButton("🔁 إعادة تشغيل الجلسة", callback_data="restart_session")],
        [InlineKeyboardButton("💾 النسخ الاحتياطي الآن", callback_data="ctrl_backup")],
        [InlineKeyboardButton("🚪 خروج", callback_data="exit_control")],
    ]

    await update.message.reply_text(
        "🎛️ *لوحة التحكم الخاصة بالمشرفين*\n\nيرجى اختيار الإجراء المطلوب:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

# ✅ معالجة الضغط على أزرار الصيانة
async def handle_control_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    action = query.data
    user_id = query.from_user.id
    image_path = "GO-NOW.PNG"

    if user_id not in AUTHORIZED_USERS:
        await query.answer("🚫 لا تملك صلاحية الوصول.", show_alert=True)
        return

    # ✅ تفعيل وضع الصيانة
    if action == "ctrl_maintenance_on":
        context.bot_data["maintenance_mode"] = True
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ تم تفعيل وضع الصيانة.\nلن يستطيع المستخدمون استخدام الخدمات مؤقتًا.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ عودة", callback_data="control_back")]])
        )
        return

    # ✅ إنهاء وضع الصيانة
    if action == "ctrl_maintenance_off":
        context.bot_data["maintenance_mode"] = False
        await context.bot.send_message(
            chat_id=user_id,
            text="✅ تم إنهاء وضع الصيانة.\nيمكن للمستخدمين استخدام الخدمات الآن.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ عودة", callback_data="control_back")]]
            )
        )
        return

    # ✅ نسخ احتياطي يدوي من لوحة التحكم
    if action == "ctrl_backup":
        await query.answer("⏳ يتم الآن إنشاء نسخة احتياطية للبيانات...", show_alert=True)
        await create_excel_backup(reason="manual", context=context, notify_chat_id=user_id)
        return

    # باقي الإجراءات كما هي
    if action == "control_back":
        await query.message.edit_text(
            "🛠️ *لوحة التحكم:*",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👤 المشرفون", callback_data="admins_menu")],
                [InlineKeyboardButton("🧹 تنظيف الجلسات", callback_data="clear_sessions")],
                [InlineKeyboardButton("♻️ إعادة تحميل الإعدادات", callback_data="reload_settings")],
                [InlineKeyboardButton("🚧 تفعيل وضع الصيانة", callback_data="ctrl_maintenance_on")],
                [InlineKeyboardButton("✅ إنهاء وضع الصيانة", callback_data="ctrl_maintenance_off")],
                [InlineKeyboardButton("🧨 تدمير البيانات", callback_data="self_destruct")],
                [InlineKeyboardButton("🔁 إعادة تشغيل الجلسة", callback_data="restart_session")],
                [InlineKeyboardButton("💾 النسخ الاحتياطي الآن", callback_data="ctrl_backup")],
                [InlineKeyboardButton("🚪 خروج", callback_data="exit_control")]
            ]),
            parse_mode=constants.ParseMode.MARKDOWN
        )
        return

    if query.data == "exit_control":
        await query.message.delete()
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

async def handle_rating(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    if query.data != f"rate_{user_id}":
        await query.answer("⚠️ حدث خطأ في البيانات.", show_alert=True)
        return

    context.user_data.setdefault(user_id, {})["rating_mode"] = True

    await query.answer()

    # فقط باراميترين
    await show_statistics(update, context)

async def save_rating(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data or ""
    parts = data.split("_")

    # شكل الكول باك: ratingval_رقم_رقم
    if len(parts) != 3:
        await query.answer("⚠️ تنسيق غير صالح.", show_alert=True)
        return

    try:
        rating_value = int(parts[1])
        user_id = int(parts[2])
    except ValueError:
        await query.answer("⚠️ بيانات تقييم غير صالحة.", show_alert=True)
        return

    # منع أي أحد غير صاحب الجلسة من التقييم
    if query.from_user.id != user_id:
        requester = await context.bot.get_chat(user_id)
        await query.answer(
            f"❌ هذا التقييم خاص بـ {requester.first_name} {requester.last_name} - استخدم الأمر /go",
            show_alert=True,
        )
        return

    now = datetime.now(timezone.utc) + timedelta(hours=3)
    user_name = query.from_user.full_name

    # محاولة جلب اسم ورقم المجموعة
    group_name = context.user_data.get(user_id, {}).get("group_title", "غير معروف")
    group_id = context.user_data.get(user_id, {}).get("group_id", "غير معروف")

    if group_name == "غير معروف" and user_id in context.bot_data:
        group_name = context.bot_data[user_id].get("group_title", "غير معروف")
        group_id = context.bot_data[user_id].get("group_id", "غير معروف")

    rating_entry = {
        "user_id": user_id,
        "name": user_name,
        "rating": rating_value,
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "group_name": group_name,
        "group_id": group_id,
    }

    try:
        ratings_file = "bot_data.xlsx"

        # قراءة شيت ratings إن وجد
        try:
            df_ratings = pd.read_excel(ratings_file, sheet_name="ratings")
        except Exception:
            df_ratings = pd.DataFrame(
                columns=["user_id", "name", "rating", "timestamp", "group_name", "group_id"]
            )

        # ✅ هل هذا المستخدم قيّم من قبل؟
        already_rated = False
        if not df_ratings.empty and "user_id" in df_ratings.columns:
            try:
                already_rated = int(user_id) in df_ratings["user_id"].astype(int).tolist()
            except Exception:
                already_rated = False

        if already_rated:
            # 🔕 إزالة أزرار التقييم من الرسالة الأصلية (إن أمكن)
            try:
                if query.message:
                    await context.bot.edit_message_reply_markup(
                        chat_id=query.message.chat_id,
                        message_id=query.message.message_id,
                        reply_markup=None,
                    )
            except Exception:
                pass

            # تنظيف مود التقييم
            user_dict = context.user_data.get(user_id)
            if isinstance(user_dict, dict):
                user_dict.pop("rating_mode", None)

            # رسالة شكر خاصة + تنبيه
            await query.answer("✅ تقييمك مسجّل لدينا مسبقًا، شكرًا لدعمك.", show_alert=True)

            thank_again = (
                "🌟 شكرًا لك من جديد على ثقتك ودعمك لنظام GO.\n\n"
                f"`{user_name}`\n"
                "تم تسجيل تقييمك في وقت سابق، ووجودك معنا هو أهم تقييم ❤️"
            )

            back_keyboard = InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]]
            )

            msg = await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=thank_again,
                parse_mode=constants.ParseMode.MARKDOWN,
                reply_markup=back_keyboard,
            )
            register_message(user_id, msg.message_id, query.message.chat_id, context)
            return

        # ✅ مستخدم جديد في التقييم → نضيفه إلى الإكسل
        df_ratings = pd.concat([df_ratings, pd.DataFrame([rating_entry])], ignore_index=True)

        # نستخدم قفل الكتابة على الإكسل حتى لا يتعارض مع عمليات أخرى
        async with EXCEL_LOCK:
            with pd.ExcelWriter(
                ratings_file,
                engine="openpyxl",
                mode="a",
                if_sheet_exists="replace",
            ) as writer:
                df_ratings.to_excel(writer, sheet_name="ratings", index=False)

        # محاولة حذف رسالة أزرار التقييم القديمة (لو ما زالت موجودة)
        try:
            if query.message:
                await context.bot.delete_message(
                    chat_id=query.message.chat_id,
                    message_id=query.message.message_id,
                )
        except Exception:
            pass

        # ✅ تنظيف مود التقييم من user_data بدون أخطاء
        user_dict = context.user_data.get(user_id)
        if isinstance(user_dict, dict):
            user_dict.pop("rating_mode", None)

        # قاموس الايموجيات
        rating_emojis = {
            1: "😞 غير راضٍ",
            2: "😐 مقبول",
            3: "😊 جيد",
            4: "😍 ممتاز",
        }

        thank_you_message = (
            f"🟦 شكراً لتقييمك،\n"
            f"`{user_name}`\n\n"
            f"`تقييمك: {rating_emojis.get(rating_value, '⭐')}`\n\n"
            "🎉 رأيك يهمنا ويساعدنا في تحسين البرنامج!"
        )

        back_keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ رجوع للقائمة الرئيسية", callback_data=f"back_main_{user_id}")]]
        )

        msg = await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=thank_you_message,
            parse_mode=constants.ParseMode.MARKDOWN,
            reply_markup=back_keyboard,
        )
        register_message(user_id, msg.message_id, query.message.chat_id, context)

        # إشعار المشرفين
        for admin_id in AUTHORIZED_USERS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=(
                        "🌟 *تقييم جديد من مستخدم*\n\n"
                        f"👤 الاسم:\n`{user_name}`\n\n"
                        f"👥 المجموعة:\n`{group_name}`\n\n"
                        f"🆔 رقم المجموعة:\n`{group_id}`\n\n"
                        f"📝 التقييم:\n`{rating_emojis.get(rating_value, '⭐')}`\n\n"
                        f"🕓 الوقت:\n`{rating_entry['timestamp']}`"
                    ),
                    parse_mode=constants.ParseMode.MARKDOWN,
                )
            except Exception as e:
                logging.warning(f"❌ فشل إرسال إشعار التقييم للمشرف {admin_id}: {e}")

    except Exception as e:
        logging.error(f"[RATING] ❌ فشل في حفظ التقييم: {e}", exc_info=True)
        await query.answer("⚠️ حدث خطأ أثناء حفظ التقييم، حاول لاحقًا.", show_alert=True)

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
        # قفل الكتابة على ملف الإكسل قبل تعديل شيت managers
        async with EXCEL_LOCK:
            with pd.ExcelWriter("bot_data.xlsx", engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                df_admins.to_excel(writer, sheet_name="managers", index=False)

        await message.reply_text(f"✅ تم إضافة المشرف بنجاح: `{new_admin_id}`", parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        await message.reply_text(f"❌ حدث خطأ أثناء حفظ التغييرات:\n{e}")

    # 🧼 مسح الحالة
    context.user_data[user_id]["action"] = None

application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("go", start))
application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"(?i)^go$"), handle_go_text))
application.add_handler(CommandHandler("go25s", handle_control_panel))

# ✅ أوامر لوحة التحكم العامة + إشعار التحديث + وضع الصيانة
application.add_handler(CallbackQueryHandler(
    handle_control_buttons,
    pattern="^(ctrl_maintenance_on|ctrl_maintenance_off|reload_settings|add_admin|list_admins|clear_sessions|show_stats|self_destruct|exit_control|control_back|admins_menu|restart_session|delete_admin|broadcast_update)$"
))

# ✅ استقبال رسائل المستخدمين والمشرفين (اقتراحات وردود مخصصة)
application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))

# ✅ نظام الاقتراحات (إرسال + ردود سريعة + رد مخصص)
application.add_handler(CallbackQueryHandler(send_suggestion, pattern=r"^send_suggestion$"))
# ✅ نقاشات فريق GO الداخلية
application.add_handler(CallbackQueryHandler(start_team_general_thread, pattern=r"^team_main_\d+$"))
# ✅ إرسال توصية فنية عامة للمجموعات
application.add_handler(CallbackQueryHandler(start_recommendation, pattern=r"^send_reco$"))
application.add_handler(CallbackQueryHandler(broadcast_recommendation, pattern=r"^reco_broadcast$"))
application.add_handler(CallbackQueryHandler(cancel_recommendation, pattern=r"^reco_cancel$"))

application.add_handler(CallbackQueryHandler(start_team_opinion_thread, pattern=r"^team_opinion_\d+_.+$"))
application.add_handler(CallbackQueryHandler(team_reply_existing_thread, pattern=r"^team_reply_\d+$"))

application.add_handler(CallbackQueryHandler(handle_suggestion_reply, pattern=r"^reply_\d+_.+$"))
application.add_handler(CallbackQueryHandler(handle_send_reply, pattern=r"^sendreply_[a-zA-Z0-9]+_\d+_.+$"))
application.add_handler(CallbackQueryHandler(handle_custom_reply, pattern=r"^customreply_\d+_.+$"))
application.add_handler(CallbackQueryHandler(submit_admin_reply, pattern=r"^submit_admin_reply$"))

# ✅ التقييم
application.add_handler(CallbackQueryHandler(show_statistics, pattern=r"^rate_\d+$"))
application.add_handler(CallbackQueryHandler(save_rating, pattern=r"^ratingval_\d+_\d+$"))

# ✅ الصيانة وقطع الغيار
application.add_handler(CallbackQueryHandler(car_choice, pattern=r"^car_.*_\d+$"))
application.add_handler(CallbackQueryHandler(maintenance_brand_choice, pattern=r"^mbrand_.*_\d+$"))
application.add_handler(CallbackQueryHandler(parts_brand_choice, pattern=r"^pbrand_.*_\d+$"))
application.add_handler(CallbackQueryHandler(km_choice, pattern=r"^km_.*_\d+$"))
application.add_handler(CallbackQueryHandler(send_cost, pattern=r"^cost_\d+_\d+$"))
application.add_handler(CallbackQueryHandler(send_part_image, pattern=r"^part_image_\d+_\d+$"))

# ✅ أزرار القوائم الخاصة بالصيانة وقطع الغيار والاقتراحات والأعطال + الرجوع
application.add_handler(CallbackQueryHandler(button, pattern=r"^catpart_.*_\d+$"))
application.add_handler(CallbackQueryHandler(button, pattern=r"^showparts_.*_\d+$"))
application.add_handler(CallbackQueryHandler(button, pattern=r"^(parts|maintenance|consumable|external|suggestion)_\d+$"))
# الأعطال الشائعة من القائمة الرئيسية
application.add_handler(CallbackQueryHandler(button, pattern=r"^faults_\d+$"))
# ✅ تصنيفات الأعطال الفرعية
application.add_handler(CallbackQueryHandler(button, pattern=r"^faultcat_\d+_\d+$"))
# أزرار الرجوع القديمة من نوع back_main_USERID
application.add_handler(CallbackQueryHandler(button, pattern=r"^back_main_\d+$"))
# أزرار الرجوع الموحدة من نوع back:target:user_id
application.add_handler(CallbackQueryHandler(button, pattern=r"^back:"))

application.add_handler(CallbackQueryHandler(select_car_for_parts, pattern=r"^carpart_"))
application.add_handler(CallbackQueryHandler(send_brochure, pattern=r"^brochure_\d+_\d+$"))

# ✅ دليل المالك
application.add_handler(CallbackQueryHandler(show_manual_car_list, pattern=r"^manual_"))
application.add_handler(CallbackQueryHandler(manual_brand_choice, pattern=r"^mnlbrand_.*_\d+$"))
application.add_handler(CallbackQueryHandler(handle_manualcar, pattern=r"^manualcar_.*_\d+$"))
application.add_handler(CallbackQueryHandler(handle_manualdfcar, pattern=r"^openpdf_"))

# ✅ المراكز والمتاجر
application.add_handler(CallbackQueryHandler(handle_service_centers, pattern=r"^service_\d+$"))
application.add_handler(CallbackQueryHandler(handle_branch_list, pattern=r"^branches_\d+$"))
application.add_handler(CallbackQueryHandler(handle_independent, pattern=r"^independent_\d+$"))
application.add_handler(CallbackQueryHandler(show_center_list, pattern=r"^show_centers_\d+$"))
application.add_handler(CallbackQueryHandler(show_store_list, pattern=r"^show_stores_\d+$"))
application.add_handler(CallbackQueryHandler(set_city, pattern=r"^setcity_.*_\d+$"))

# ✅ زر الإلغاء
application.add_handler(CallbackQueryHandler(handle_cancel, pattern=r"^cancel_"))

# ✅ زر غير نشط
application.add_handler(CallbackQueryHandler(
    lambda u, c: asyncio.create_task(u.callback_query.answer("🚫 هذا الزر غير نشط حالياً.")),
    pattern=r"^disabled$"
))

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"message": "Bot is alive"}

@app.post("/webhook")
async def webhook_handler(request: Request):
    json_data = await request.json()

    # 🔎 لوق بسيط كل ما تيجي أبديت من تيليجرام
    logging.info(f"[WEBHOOK] وصل تحديث جديد من تيليجرام: keys={list(json_data.keys())}")

    update = Update.de_json(json_data, application.bot)
    await application.update_queue.put(update)
    return {"ok": True}

@app.on_event("startup")
async def on_startup():
    import requests

    # 🔗 نبني رابط الـ Webhook بشكل مضمون
    base_url = os.getenv("RENDER_EXTERNAL_URL") or "https://chery-go-8a2z.onrender.com"

    # لو حطيت الدومين بدون بروتوكول نضيف https
    if not base_url.startswith("http"):
        base_url = "https://" + base_url.lstrip("/")

    # لو أحد كتبها أصلاً مع /webhook ما نكررها
    if base_url.endswith("/webhook"):
        webhook_url = base_url
    else:
        webhook_url = base_url.rstrip("/") + "/webhook"

    try:
        response = requests.get(
            f"https://api.telegram.org/bot{API_TOKEN}/setWebhook",
            params={"url": webhook_url},
            timeout=10,
        )
        logging.info(f"🔗 Webhook set to {webhook_url} status={response.status_code} body={response.text}")
    except Exception as e:
        logging.error(f"❌ Failed to set webhook: {e}")

    await application.initialize()
    await application.start()

        # ✅ تفعيل JobQueue (تنظيف الجلسات + health + النسخ الاحتياطي اليومي + keepalive)
    if application.job_queue:
        application.job_queue.run_repeating(
            cleanup_old_sessions,
            interval=60 * 60,  # كل ساعة
            first=60           # أول تشغيل بعد 60 ثانية من الإقلاع
        )

        # نبضات صحية دورية داخل الذاكرة فقط
        application.job_queue.run_repeating(
            health_log_job,
            interval=60 * 10,  # كل 10 دقائق
            first=120
        )

        # 🔁 KEEPALIVE: طلب داخلي للخدمة كل 5 دقائق لإبقائها مستيقظة
        try:
            application.job_queue.run_repeating(
                keepalive_ping,
                interval=60 * 5,   # كل 5 دقائق
                first=180,         # أول تشغيل بعد 3 دقائق من الإقلاع
                name="render_keepalive",
            )
        except Exception as e:
            logging.error(f"[KEEPALIVE] ❌ فشل جدولة keepalive: {e}")

        # نسخ احتياطي يومي للبيانات الساعة 4 فجراً بتوقيت السعودية
        try:
            saudi_tz = timezone(timedelta(hours=3))
            application.job_queue.run_daily(
                daily_backup_job,
                time=time(hour=4, minute=0, tzinfo=saudi_tz),
                name="daily_excel_backup",
            )
        except Exception as e:
            logging.error(f"[BACKUP] ❌ فشل جدولة النسخ الاحتياطي اليومي: {e}")

        print("✅ JobQueue تم تشغيلها")
    else:
        print("⚠️ job_queue غير مفعلة أو غير جاهزة")

# =============================
# Broadcast utilities (image+text to all groups/channels)
# =============================

def collect_target_chat_ids(context: ContextTypes.DEFAULT_TYPE) -> set[int]:
    """يجمع chat_id من:
       1) user_sessions (جلسات التفاعل)
       2) شيت group_logs (لتغطية كل المجموعات/القنوات حتى بدون تفاعل حديث)
    """
    targets = set()
    # 1) من الجلسات
    try:
        for sessions in user_sessions.values():
            for s in sessions:
                cid = s.get("chat_id")
                if isinstance(cid, int) and cid < 0:
                    targets.add(cid)
    except Exception:
        pass
    # 2) من شيت group_logs
    try:
        if not df_group_logs.empty and 'chat_id' in df_group_logs.columns:
            for x in df_group_logs['chat_id'].dropna().tolist():
                try:
                    x = int(x)
                    if x < 0:
                        targets.add(x)
                except Exception:
                    continue
    except Exception:
        pass
    return targets

def get_update_image_path() -> Path:
    # يعثر على GO-now.PNG بجانب main.py مهما كان الـ CWD
    return Path(__file__).with_name("GO-NOW.PNG")

async def cmd_broadcast_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # أمر إداري بديل للإرسال (في حال عدم وجود زر باللوحة)
    user_id = update.effective_user.id
    try:
        AUTHORIZED = set(AUTHORIZED_USERS)
    except Exception:
        AUTHORIZED = set()
    if user_id not in AUTHORIZED:
        await update.message.reply_text("🚫 الأمر للمشرفين فقط.")
        return
    await do_broadcast_update(update, context, notify_user_id=user_id)


# ✅ تسجيل أمر /broadcast_update بعد تعريف الدوال
try:
    application.add_handler(CommandHandler("broadcast_update", cmd_broadcast_update))
except Exception as _e:
    logging.warning(f"[init] تعذر تسجيل broadcast_update: {_e}")