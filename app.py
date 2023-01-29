from telegram import Update
from telegram.ext import Updater, Application, CommandHandler, ContextTypes
from config import print_n_log, parse_markdown_v2, send_notification, send_error_message
from datetime import datetime
from dotenv import load_dotenv
from db import *
from trade import calc_price, calc_principal

import ccxt
import os

load_dotenv()

async def start_dual_trading(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        id = len(get_all_trading_strats()) + 1
        coin = context.args[0]
        price = float(context.args[1])
        amount = float(context.args[2])
        temp = datetime.strptime(context.args[3], "%Y/%m/%d")
        date = datetime(temp.year, temp.month, temp.day, 17, 0, 0)
        exchange = context.args[4]
        if exchange not in ccxt.exchanges:
            await update.message.reply_text("Enter a valid exchange name")
        else:
            insert_trading_strat((id, coin, price, amount, date, exchange, None, None, False, False, False))
            assets_in_management = sum(calc_price(strat[1], "bids") * float(strat[3]) for strat in (strats for strats in get_not_settled_strats()))
            assets_in_management_f = "{:,.2f}".format(assets_in_management)
            margin_balance = "{:,.2f}".format(calc_principal())
            recommended_assets = "{:,.2f}".format(assets_in_management / 2)

            # 3x leverage -> recommended margin balance / 2
            msg = "__*📢Inserted a new {} dual\-trading strats as id {}📢*__\n*Current assets under trading:* ${}\n*Current margin balance:* ${}\n*Recommended margin balance:* ${}".format(parse_markdown_v2(context.args[0]), id, parse_markdown_v2(assets_in_management_f), parse_markdown_v2(margin_balance), parse_markdown_v2(recommended_assets))
            await update.message.reply_text(msg, parse_mode='markdownv2')
    except Exception as e:
        await update.message.reply_text(parse_markdown_v2(str(e)), parse_mode='markdownv2')
async def view_dual_trading(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if context.args[0] == "all":
            res = get_all_trading_strats()
            msg = ""
            for strats in res:
                for strat in strats:
                    msg = msg + str(strat) + " "
                msg = msg + "\n"
            await update.message.reply_text(msg)
        elif not(context.args[0]):
            await update.message.reply_text("Enter a valid id to view current trading starts")
        else:
            res = get_trading_strat(int(context.args[0]))
            if res[8]:
                settled = "True"
            else:
                settled = "False"
            if res[9]:
                margin_active = "True"
            else:
                margin_active = "False"
            if res[10]:
                sold = "True"
            else:
                sold = "False"
            msg = "__*ⓘInformation of dual\-trading strat {}ⓘ*__\n*Coin Name* : {}\n*Settlement Price* : ${}\n*Settlement Amount* : {}\n*Settlement Date* : {}\n*Exchange*: {}\n*Last Order Number* : {}\n*Final Price* : {}\n*Settled* : {}\n*Margin Active* : {}\n*Sold* : {}".format(res[0], res[1], "{:,.4f}".format(float(res[2])).replace(".", "\."), parse_markdown_v2(res[3]), parse_markdown_v2(res[4]), res[5], res[6], res[7], settled, margin_active, sold)
            await update.message.reply_text(msg, parse_mode='markdownv2')
    except Exception as e:
        await update.message.reply_text(parse_markdown_v2(str(e)), parse_mode='markdownv2')
async def edit_dual_trading(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # (coin, price, amount, date, exchange) out of 9 cols except for id
    try:
        splitted = context.args[1].split(",")
        res = len(get_trading_strat(int(context.args[0])))
        if not res:
            await update.message.reply_text("Can't modify an empty table. Insert records first.")
        # Excluding id and settled column
        elif len(splitted) < res - 6:
            await update.message.reply_text("Not all arguments provided.\nRequired: {}\nProvided: {}".format(res - 6, len(splitted)))
        elif len(splitted) > res - 6:
            await update.message.reply_text("Too many arguments provided.\nRequired: {}\nProvided: {}".format(res - 6, len(splitted)))
        else:
            if splitted[-2]: # Date
                temp = datetime.strptime(splitted[-2], "%Y/%m/%d")
                splitted[-2] = datetime(temp.year, temp.month, temp.day, 17, 0, 0)
            update_trading_strat(int(context.args[0]), splitted)
            await update.message.reply_text("Updated\n", parse_mode='markdownv2')
    except Exception as e:
        await update.message.reply_text(parse_markdown_v2(str(e)), parse_mode='markdownv2')
async def reset_dual_trading(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        remove_dual_trading_db()
        create_dual_trading_db()
        await update.message.reply_text("Cleared previous trading strat records", parse_mode='markdownv2')
    except Exception as e:
        await update.message.reply_text(parse_markdown_v2(str(e)), parse_mode='markdownv2')

# Main function
def main():
    os.chdir(os.path.dirname(__file__))
    create_dual_trading_db()

    """Start the bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(os.environ['TELEGRAM_BOT_TOKEN']).build()

    # on different commands - answer in Telegram
    application.add_handler(CommandHandler("launch", start_dual_trading))
    application.add_handler(CommandHandler("view", view_dual_trading))
    application.add_handler(CommandHandler("edit", edit_dual_trading))
    application.add_handler(CommandHandler("reset", reset_dual_trading))        

    # Start the Telegram server
    application.run_polling()

if __name__ == "__main__":
    main()