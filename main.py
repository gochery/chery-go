import os
from telegram import Update
from telegram.ext import Application, MessageHandler, ContextTypes, filters

API_TOKEN = "توكن البوت الخاص بك"

async def handle_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document
    if document and document.mime_type == "application/pdf":
        file_id = document.file_id
        await update.message.reply_text(f"file_id: `{file_id}`", parse_mode="Markdown")

app = Application.builder().token(API_TOKEN).build()
app.add_handler(MessageHandler(filters.Document.PDF, handle_pdf))

if __name__ == "__main__":
    app.run_polling()
