from telegram import Bot

TOKEN = "7560777141:AAGTOemLV2nO5U7wt9bqhnfDdj43NHdzV4c"
bot = Bot(token=TOKEN)

bot.delete_webhook()
print("Webhook deleted successfully!")
