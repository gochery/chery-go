import logging
from telegram import Update
from telegram.ext import Application, MessageHandler, ContextTypes, filters

TOKEN = "7560777141:AAGTOemLV2nO5U7wt9bqhnfDdj43NHdzV4c"

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

if __name__ == "__main__":
    app.run_polling()
