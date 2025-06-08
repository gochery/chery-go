from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.helpers import escape_markdown

TOKEN = "7560777141:AAGTOemLV2nO5U7wt9bqhnfDdj43NHdzV4c"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("مرحباً! أرسل لي ملف PDF لأستخرج منه الـ ID.")

async def handle_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.document and update.message.document.mime_type == "application/pdf":
        pdf_file = update.message.document
        pdf_name = pdf_file.file_name

        # هنا ضع كود استخراج id من pdf حسب حاجتك
        extracted_id = "123456"  # مثال مؤقت، استبدله بالكود الحقيقي

        # هروب النص لتجنب مشكلة التنسيق
        safe_text = escape_markdown(
            f"تم استلام ملف PDF: {pdf_name}\nالـ ID المستخرج هو: {extracted_id}", version=2
        )
        await update.message.reply_text(safe_text, parse_mode="MarkdownV2")
    else:
        await update.message.reply_text("يرجى إرسال ملف PDF فقط.")

if __name__ == "__main__":
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_pdf))

    print("البوت يعمل...")
    app.run_polling()
