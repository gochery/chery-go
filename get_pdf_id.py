import logging
import os
from telegram import Update
from telegram.ext import Application, MessageHandler, ContextTypes, filters
from telegram.helpers import escape_markdown

TOKEN = os.environ["TELEGRAM_TOKEN"]

logging.basicConfig(level=logging.INFO)

async def handle_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document
    if document and document.mime_type == "application/pdf":
        file_id_escaped = escape_markdown(document.file_id, version=2)
        await update.message.reply_text(
            f"📄 file_id:\n`{file_id_escaped}`",
            parse_mode="MarkdownV2"
        )

app = Application.builder().token(TOKEN).build()
app.add_handler(MessageHandler(filters.Document.PDF, handle_pdf))

PORT = int(os.environ.get("PORT", 8443))
WEBHOOK_URL = f"https://{os.environ['RENDER_EXTERNAL_HOSTNAME']}/webhook"

if __name__ == "__main__":
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        webhook_url=WEBHOOK_URL
    )
