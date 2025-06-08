import logging
import os
from telegram import Update
from telegram.ext import Application, MessageHandler, ContextTypes, filters

TOKEN = os.environ["BOT_TOKEN"]

logging.basicConfig(level=logging.INFO)

async def handle_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document
    if document and document.mime_type == "application/pdf":
        await update.message.reply_text(
            f"📄 file_id:\n`{document.file_id}`",
            parse_mode="Markdown"
        )

app = Application.builder().token(TOKEN).build()
app.add_handler(MessageHandler(filters.Document.PDF, handle_pdf))

# إعداد Webhook لـ Render فقط
PORT = int(os.environ.get("PORT", 8443))
WEBHOOK_URL = f"https://{os.environ['RENDER_EXTERNAL_HOSTNAME']}/webhook"

if __name__ == "__main__":
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        webhook_url=WEBHOOK_URL
    )
