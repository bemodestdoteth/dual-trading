import ccxt

from fp.fp import FreeProxy
from dotenv import load_dotenv

from config import print_n_log, send_notification, send_buy_sell_message
from db import get_not_settled_strats, update_order_number, settle_trade, toggle_margin, insert_final_price
from bybit_restapi import bybitapi

from datetime import datetime
from statistics import mean
import asyncio
import time
import os

# Load environment variables
load_dotenv()

# Private API
binance = ccxt.binance()
binance.apiKey = os.environ["BINANCE_API_KEY"]
binance.secret = os.environ["BINANCE_SECRET_KEY"]
#binance.proxy = FreeProxy(rand=True).get().replace("http://", "")

bybit = ccxt.bybit()
bybit.options["defaultType"] = "spot"
bybit.apiKey = os.environ["BYBIT_API_KEY"]
bybit.secret = os.environ["BYBIT_SECRET_KEY"]
#bybit.proxy = FreeProxy(rand=True).get().replace("http://", "")

# Function to execute the trading strategy
def get_ma(coin, exchange = bybit, interval = 30):
	if exchange == "bybit":
		ohlcvs = ((datetime.utcfromtimestamp(int(i[0]/1000)).strftime("%Y/%m/%d %H:%M:%S"), i[4]) for i in bybit.fetch_ohlcv("{}/USDT".format(coin), "1m")[-interval:])
	elif exchange == "binance":
		ohlcvs = ((datetime.utcfromtimestamp(int(i[0]/1000)).strftime("%Y/%m/%d %H:%M:%S"), i[4]) for i in binance.fetch_ohlcv("{}/BUSD".format(coin), "1m")[-interval:])
	return(mean(ohlcv[1] for ohlcv in ohlcvs))
def calc_price(coin, side):
	return binance.fetch_order_book(symbol="{}/BUSD".format(coin))[side][0][0]
def calc_principal():
    res = 0
    for coin, amount in binance.fetch_balance(params={"type": "margin", "isIsolated": "FALSE"})["free"].items():
        if "USD" in coin:
            res = res + amount
        elif amount != 0: # bids -> conservative amount
            res = res + calc_price(coin, "bids") * amount
    return res
def convert_to_dict(strat):
	# strat = (id, coin, settlement_price, settlement_amount, settlement_date, exchange, order_number, final_price, is_settled, margin_active)
	strat_dict = {}
	strat_dict['id'] = int(strat[0])
	strat_dict['coin'] = strat[1]
	strat_dict['settlement_price'] = float(strat[2])
	strat_dict['settlement_amount'] = float(strat[3])
	strat_dict['settlement_date'] = datetime.strptime(strat[4], "%Y-%m-%d %H:%M:%S")
	strat_dict['exchange'] = strat[5]
	strat_dict['order_number'] = strat[6]
	strat_dict['final_price'] = strat[7]
	strat_dict['is_settled'] = bool(strat[8])
	strat_dict['margin_active'] = bool(strat[9])
	return strat_dict
def rebalance(id, coin, settlement_price, settlement_amount, order_number, margin_active):
	if margin_active:
	    # buy side -> asks for maker order
		price = calc_price(coin, "asks")
		factor = 0.995 # 0.5%

		# Safe
		if price < settlement_price * factor:
			# Sell back any fulfilled order if any
			if order_number is None:
				# Create a new limit buy order if there's no order number right now
				order = binance.create_order(symbol="{}/BUSD".format(coin), type='limit', side='buy', amount=settlement_amount, price=settlement_price)
				update_order_number(id, order['id'])

				msg = "Limit buy order number {} created for {}".format(order['id'], coin)
				print_n_log(msg)
				asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price))
			else:
				fulfilled= binance.fetch_order(order_number, symbol="{}/BUSD".format(coin))['filled']
				if fulfilled > 0:
					binance.create_order(symbol="{}/BUSD".format(coin), type='market', side='sell', amount=fulfilled)

					msg = "Market sold {} with amount {}".format(coin, fulfilled)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, calc_price(coin, "bids")))

					# Refresh order number by re-creating an order
					binance.cancel_order(order_number, symbol="{}/BUSD".format(coin))
					order = binance.create_order(symbol="{}/BUSD".format(coin), type='limit', side='buy', amount=settlement_amount, price=settlement_price)
					update_order_number(id, order['id'])

					msg = "Limit buy order number {} created for {}".format(order['id'], coin)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price))
				else:
					print_n_log("Keep watching trading strat id {}. Margin active: {} Price: {}".format(id, margin_active, price))

		# In 0.5% range between settlement price
		elif (settlement_price * factor <= price) and (price < settlement_price):
			# Create limit buy order if there is no outstanding order
			if order_number is None:
				order = binance.create_order(symbol="{}/BUSD".format(coin), type='limit', side='buy', amount=settlement_amount, price=settlement_price)
				update_order_number(id, order['id'])

				msg = "Limit buy order number {} created for {}".format(order['id'], coin)
				print_n_log(msg)
				asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price))
			else:
				print_n_log("Keep watching trading strat id {}. Margin active: {} Price: {}".format(id, margin_active, price))
		else: # price >= settlement_price
			if order_number is None: # High volatiltiy case: market buy
				binance.create_order(symbol="{}/BUSD".format(coin), type='market', side='buy', amount=settlement_amount)

				msg = "Market bought {} with amount {}".format(coin, settlement_amount)
				print_n_log(msg)
				asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price))
				toggle_margin(id, False)
			else:
				# An outstanding order is fully fulfilled
				if binance.fetch_order(order_number, symbol="{}/BUSD".format(coin))['filled'] == settlement_amount:
					update_order_number(id, None)

					msg = "Limit buy order {} for {} fully fulfilled".format(order_number, coin)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price))
					toggle_margin(id, False)
				else:
					# Should not jump in this line of code
					asyncio.run(send_notification("Anomaly: Order not fully fulfilled even if price goes above the limit buy price"))
	elif not(margin_active):
		# sell side -> bids to perform maker order
		price = calc_price(coin, "bids")
		factor = 1.005 # 0.5%

		# Safe
		if price > settlement_price * factor:
			# Sell back any fulfilled order if any and repay margin
			if order_number is None:
				print_n_log("Keep watching trading strat id {}. Margin active: {} Price: {}".format(id, margin_active, price))
			else:
				fulfilled= binance.fetch_order(order_number, symbol="{}/BUSD".format(coin))['filled']
				if fulfilled > 0:
					binance.create_order(symbol="{}/BUSD".format(coin), type='market', side='buy', amount=fulfilled)

					msg = "Market bought {} with amount {}".format(coin, fulfilled)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, calc_price(coin, "asks")))
					toggle_margin(id, False)

				binance.cancel_order(order_number, symbol="{}/BUSD".format(coin))
				binance.transfer(coin, settlement_amount, "spot", "cross")
				binance.repay_margin(code=coin, amount=settlement_amount, symbol="{}/BUSD".format(coin), params={"type": "margin", "isIsolated": "FALSE"})

				msg = "Margin repaied {}".format(coin)
				print_n_log(msg)
				asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, calc_price(coin, "asks")))
				toggle_margin(id, False)

		# In 0.5% range between settlement price
		elif (price > settlement_amount) and (price <= settlement_price * factor):
			if order_number is None:
				# Borror margin and create limit sell maker order
				binance.borrow_margin(code=coin, amount=settlement_amount, symbol="{}/BUSD".format(coin), params={"type": "margin", "isIsolated": "FALSE"})
				order = binance.create_order(symbol="{}/BUSD".format(coin), type='limit', side='sell', amount=settlement_amount, price=settlement_price)
				update_order_number(id, order['id'])
				
				msg = "Limit sell order number {} created for {}".format(order['id'], coin)
				print_n_log(msg)
				asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price))
			else:
				print_n_log("Keep watching trading strat id {}. Margin active: {} Price: {}".format(id, margin_active, price))

	    # price <= settlementment_price
		else:
			# High volatility case: Borrow and market sell
			if order_number is None:
				binance.borrow_margin(code=coin, amount=settlement_amount, symbol="{}/BUSD".format(coin), params={"type": "margin", "isIsolated": "FALSE"})
				binance.transfer("BUSD", settlement_amount, "cross", "spot")
				binance.create_order(symbol="{}/BUSD".format(coin), type='market', side='sell', amount=settlement_amount)

				msg = "Market sold {} with amount {}".format(coin, fulfilled)
				print_n_log(msg)
				asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price))
			else:
				# An outstanding order is fully fulfilled
				if binance.fetch_order(order_number, symbol="{}/BUSD".format(coin))['filled'] == settlement_amount:
					update_order_number(id, None)

					msg = "Limit sell order {} for {} fully fulfilled".format(order_number, coin)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price))
					toggle_margin(id, False)
				else:
					# Should not jump in this line of code
					asyncio.run(send_notification("Anomaly: Order not fully fulfilled even if price goes below the limit sell price"))
def settle(id, coin, settlement_price, settlement_amount, exchange, final_price, margin_active):
    # Pass settlement time for the first time: determine final settlement status and handle some extreme cases
	if final_price is None:
		insert_final_price(id, get_ma(coin))

		# Extreme case 1: price > settlement but margin active
		if final_price >= settlement_price and margin_active:
				binance.create_order(symbol="{}/BUSD".format(coin), type='market', side='buy', amount=(settlement_amount))
				binance.transfer(coin, settlement_amount, "spot", "cross")
				binance.repay_margin(code=coin, amount=settlement_amount, symbol="{}/BUSD".format(coin), params={"type": "margin", "isIsolated": "FALSE"})
				toggle_margin(id, False)

		# Extreme case 2: price <= settlement but not margin active
		elif final_price < settlement_price and not(margin_active):
				binance.borrow_margin(code=coin, amount=settlement_amount, symbol="{}/BUSD".format(coin), params={"type": "margin", "isIsolated": "FALSE"})
				binance.transfer(coin, settlement_amount, "cross", "spot")
				binance.create_order(symbol="{}/BUSD".format(coin), type='market', side='sell', amount=settlement_amount)
				toggle_margin(id, True)
	# After final price is inserted, wait until balance arrives
	else:
		if margin_active:
			if exchange == "bybit": # Bybit: Sell received coin on earn account
				bb = bybitapi()
				balance = bb.get_balance("investment", coin)["walletBalance"]
				if balance >= settlement_amount:
					print_n_log("Coins arrived")

					bybit.transfer(coin, balance, "investment", "spot")
					bybit.create_order(symbol="{}/USDT".format(coin), type='market', side='sell', amount=balance)
					bybit.transfer("USDT", bb.get_balance("spot", "USDT")["walletBalance"], "spot", "investment")

					msg = "Market sold {} with amount {}".format(coin, balance)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, calc_price(coin, "bids")))
     
					# Buy coins and repay outstanding debt
					binance.create_order(symbol="{}/BUSD".format(coin), type='market', side='buy', amount=settlement_amount)
					binance.transfer(coin, settlement_amount, "spot", "cross")
					binance.repay_margin(code=coin, amount=settlement_amount, symbol="{}/BUSD".format(coin), params={"type": "margin", "isIsolated": "FALSE"})

					settle_trade(id)
				else:
					print_n_log("Coins not arrived yet.")
			elif exchange == "binance": # Binance: Directly repay debt with received coins
				if binance.fetch_balance()[coin]['free'] >= settlement_amount:
					print_n_log("Coins arrived")

					binance.transfer(coin, settlement_amount, "spot", "cross")
					binance.repay_margin(code=coin, amount=settlement_amount, symbol="{}/BUSD".format(coin), params={"type": "margin", "isIsolated": "FALSE"})

					settle_trade(id)
				else:
					print_n_log("Coins not arrived yet.")
			else:
				raise Exception("No valid exchanges")
		elif not(margin_active):
			if exchange == "bybit":
				usdt_balance = bb.get_balance("investment", "USDT")["walletBalance"]
				if usdt_balance >= (calc_price(coin, "bids") * settlement_amount) * 0.99:
					print_n_log("Coins arrived")
					settle_trade(id)
				else:
					print_n_log("Coins not arrived yet.")
			elif exchange == "binance":
				busd_balance = binance.fetch_balance()['BUSD']['free']
				if busd_balance >= (calc_price(coin, "bids") * settlement_amount) * 0.99:
					print_n_log("Coins arrived")
					settle_trade(id)
				else:
					print_n_log("Coins not arrived yet.")
 
# main function
def main():
	while True:
		try:
			#current_datetime = datetime.now()
			#proxy_rotation_interval = 600
			#last_proxy_rotation_time = current_datetime

			current_strats = get_not_settled_strats()
			epoch_interval = 12

			for strat in current_strats:
				# strat = (id, coin, settlement_price, settlement_amount, settlement_date, exchange, order_number, final_price, is_settled, margin_active)
				# type conversion
				strat = convert_to_dict(strat)
				if datetime.now() > strat['settlement_date']:
					# Run until principal is received
					settle(strat['id'], strat['coin'], strat['settlement_price'], strat['settlement_amount'], strat['exchange'], strat['is_settled'])
				else:
					rebalance(strat['id'], strat['coin'], strat['settlement_price'], strat['settlement_amount'], strat['order_number'], strat['margin_active'])

			time.sleep(epoch_interval)
			#if (current_datetime - last_proxy_rotation_time).total_seconds() > proxy_rotation_interval:
			#	print_n_log('Proxy change timer reached. Changing proxy...')
			#	binance.proxy = FreeProxy(rand=True).get().replace("http://", "")
			#	bybit.proxy = FreeProxy(rand=True).get().replace("http://", "")
			#	last_proxy_rotation_time = current_datetime
		except Exception as e:
			print(e)
		#except ccxt.RequestTimeout as e:
		#	binance.proxy = FreeProxy(rand=True).get().replace("http://", "")

if __name__ == "__main__":
	main()