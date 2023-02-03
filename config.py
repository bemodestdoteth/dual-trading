from datetime import datetime
import logging
import os
import telegram
import time

def print_n_log(msg, is_error = False):
    if not(is_error):
        print("{}  {}".format(datetime.strftime(datetime.now(), format="%Y/%m/%d %H:%M:%S"), msg))
        logging.basicConfig(filename='./dual-trading.log', filemode='w', format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO)
        logging.info(msg)
    else:
        print("{}  Error: {}".format(datetime.strftime(datetime.now(), format="%Y/%m/%d %H:%M:%S"), msg))
        logging.basicConfig(filename='./dual-trading.log', filemode='w', format='%(asctime)s - %(name)s - %(message)s', level=logging.ERROR)
        logging.error(msg)
def stopwatch(func):
    def inner(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print("Elapsed time: {}".format(end - start))
        return result
    return inner
def parse_markdown_v2(msg):
    reserved_words = ('_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!')
    for reserved_word in reserved_words:
        msg = str(msg).replace(reserved_word, "\{}".format(reserved_word))
    return msg
async def send_notification(msg):
    # Telegram bot configuration
    bot = telegram.Bot(token = os.environ['TELEGRAM_BOT_TOKEN'])
    chat_id = os.environ['TELEGRAM_CHAT_ID']

    msg = '__*🔔Message from {}🔔*__\n{}'.format("Dual Trading Bot", parse_markdown_v2(msg))
    await bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='markdownv2')
async def send_message(update_info):
    # Resolve reserved characters
    update_name = parse_markdown_v2(update_info['name'])
    update_title = parse_markdown_v2(update_info['title'])
    update_link = parse_markdown_v2(update_info['link'])

    # Telegram bot configuration
    bot = telegram.Bot(token = os.environ['TELEGRAM_BOT_TOKEN'])
    chat_id = os.environ['TELEGRAM_CHAT_ID']

    msg = '__*🔔{} has a new update\!🔔*__\n{}\n{}\n'.format(update_name, update_title, update_link)
    await bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='markdownv2')
async def send_buy_sell_message(header, id, price, amount, current_price, margin_active):
    # Telegram bot configuration
    bot = telegram.Bot(token = os.environ['TELEGRAM_BOT_TOKEN'])
    chat_id = os.environ['TELEGRAM_CHAT_ID']
    msg = '__*💸{}💸*__\nStrat id: {}\nOrder Price: {}\nAmount: {}\nCurrent Price: {}\nMargin Active: {}\n'.format(parse_markdown_v2(header), id, parse_markdown_v2(price), parse_markdown_v2(amount), parse_markdown_v2(current_price), parse_markdown_v2(margin_active))
    await bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='markdownv2')
async def send_error_message(work, msg):
    # Telegram bot configuration
    bot = telegram.Bot(token = os.environ['TELEGRAM_BOT_TOKEN'])
    chat_id = os.environ['TELEGRAM_CHAT_ID']
    
    msg_2 = "__*🚫An error occurred while working on {}\!\!🚫*__\n\n{}".format(parse_markdown_v2(work), parse_markdown_v2(msg))
    await bot.sendMessage(chat_id=chat_id, text=msg_2, parse_mode='markdownv2')