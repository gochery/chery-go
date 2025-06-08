import logging
import os
from telegram import Update
from telegram.ext import Application, MessageHandler, ContextTypes, filters

# قراءة التوكن من متغير البيئة
TOKEN = os.environ["TELEGRAM_TOKEN"]

# تفعيل تسجيل المعلومات
logging.basicConfig(level=logging.INFO)

# دالة لمعالجة ملفات PDF المستلمة
async def handle_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document
    if document and document.mime_type == "application/pdf":
        await update.message.reply_text(
            f"📄 file_id:\n`{document.file_id}`",
            parse_mode="Markdown"
        )

# إنشاء التطبيق وتسجيل المعالج
app = Application.builder().token(TOKEN).build()
app.add_handler(MessageHandler(filters.Document.PDF, handle_pdf))

# إعداد Webhook لـ Render
PORT = int(os.environ.get("PORT", 8443))
RENDER_HOSTNAME = os.environ.get("RENDER_EXTERNAL_HOSTNAME")
WEBHOOK_URL = f"https://{RENDER_HOSTNAME}/"  # بدون /webhook لأنك لم تضف webhook_path

if __name__ == "__main__":
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        webhook_url=WEBHOOK_URL
    )
