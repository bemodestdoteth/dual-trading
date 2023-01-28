import ccxt

from fp.fp import FreeProxy
from dotenv import load_dotenv

from config import print_n_log, send_notification, send_buy_sell_message, send_error_message
from db import get_not_settled_strats, update_order_number, settle_trade, toggle_margin, insert_final_price
from api_setting import binanceapi, bybitapi

from datetime import datetime
from statistics import mean
import asyncio
import time
import os

# Load environment variables and exchange api data
load_dotenv()
binance = ccxt.binance()
binance.apiKey = os.environ["BINANCE_API_KEY"]
binance.secret = os.environ["BINANCE_SECRET_KEY"]

bybit = ccxt.bybit()
bybit.options["defaultType"] = "spot"
bybit.apiKey = os.environ["BYBIT_API_KEY"]
bybit.secret = os.environ["BYBIT_SECRET_KEY"]

_binance = binanceapi()
_bybit = bybitapi()

# Function to execute the trading strategy
def get_ma(coin, exchange = bybit, interval = 30):
	if exchange == "bybit":
		ohlcvs = ((datetime.utcfromtimestamp(int(i[0]/1000)).strftime("%Y/%m/%d %H:%M:%S"), i[4]) for i in bybit.fetch_ohlcv("{}/USDT".format(coin), "1m")[-interval:])
	elif exchange == "binance":
		ohlcvs = ((datetime.utcfromtimestamp(int(i[0]/1000)).strftime("%Y/%m/%d %H:%M:%S"), i[4]) for i in binance.fetch_ohlcv("{}/BUSD".format(coin), "1m")[-interval:])
	return(mean(ohlcv[1] for ohlcv in ohlcvs))
def calc_price(coin, side):
	res = asyncio.run(_binance.HTTP_public_request("GET", "/api/v3/depth", {
		"symbol": "{}BUSD".format(coin)
	}))
	return float(res[side][0][0])
def calc_principal():
    res = asyncio.run(_binance.HTTP_private_request("GET", "/sapi/v1/margin/account"))['totalAssetOfBtc']
    return (float(res) * calc_price("BTC", "bids"))
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

		# Safe
		if price < settlement_price:
			# Sell back any fulfilled order if any
			if order_number is None:
				# Create a new limit buy order if there's no order number right now
				order = asyncio.run(_binance.HTTP_private_request("POST", "/api/v3/order", {
						"symbol": "{}BUSD".format(coin),
						"side": "BUY",
						"type": "STOP_LOSS_LIMIT",
						"timeInForce": "GTC",
						"quantity": settlement_amount,
						"price": settlement_price,
						"stopPrice": settlement_price
					}))
				update_order_number(id, order['orderId'])

				msg = "Limit buy order number {} created for {}".format(order['orderId'], coin)
				print_n_log(msg)
				asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price, margin_active))
			else:
				fulfilled = float(asyncio.run(_binance.HTTP_private_request("GET", "/api/v3/order", {
					"symbol": "{}BUSD".format(coin),
					"orderId" : order_number
					}))['executedQty'])
				if fulfilled > 0:
					asyncio.run(_binance.HTTP_private_request("POST", "/api/v3/order", {
						"symbol": "{}BUSD".format(coin),
						"side": "SELL",
						"type": "MARKET",
						"quantity": fulfilled
						}))

					msg = "Market sold {} with amount {}".format(coin, fulfilled)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, calc_price(coin, "bids"), margin_active))

					# Refresh order number by re-creating an order
					asyncio.run(_binance.HTTP_private_request("DELETE", "/api/v3/order", {
						"symbol": "{}BUSD".format(coin),
						"orderId": order_number
						}))
					order = asyncio.run(_binance.HTTP_private_request("POST", "/api/v3/order", {
							"symbol": "{}BUSD".format(coin),
							"side": "BUY",
							"type": "STOP_LOSS_LIMIT",
							"timeInForce": "GTC",
							"quantity": settlement_amount,
							"price": settlement_price,
							"stopPrice": settlement_price
						}))
					update_order_number(id, order['orderId'])

					msg = "Limit buy order number {} created for {}".format(order['orderId'], coin)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price, margin_active))
				else:
					print_n_log("Keep watching trading strat id {}. Margin active: {} Price: {}".format(id, margin_active, price, margin_active))
		else: # price >= settlement_price
			if order_number is None: # High volatiltiy case: market buy
				asyncio.run(_binance.HTTP_private_request("POST", "/api/v3/order", {
					"symbol": "{}BUSD".format(coin),
					"side": "BUY",
					"type": "MARKET",
					"quantity": settlement_amount
					}))

				toggle_margin(id, False)
				msg = "Market bought {} with amount {}".format(coin, settlement_amount)
				print_n_log(msg)
				asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price, margin_active))
			else:
				fulfilled = float(asyncio.run(_binance.HTTP_private_request("GET", "/api/v3/order", {
					"symbol": "{}BUSD".format(coin),
					"orderId" : order_number
					}))['executedQty'])
				if fulfilled == settlement_amount:
					update_order_number(id, None)

					toggle_margin(id, False)
					msg = "Limit buy order {} for {} fully fulfilled".format(order_number, coin)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price, margin_active))
				else:
					# Should not reach
					asyncio.run(send_notification("Anomaly: Order not fully fulfilled even if price goes above the limit buy price"))
	elif not(margin_active):
		# sell side -> bids
		price = calc_price(coin, "bids")
		factor = 1.001 # 0.1%
		# Safe
		if price > (settlement_price * factor):
			# Sell back any fulfilled order
			if order_number is None:
				print_n_log("Keep watching trading strat id {}. Margin active: {} Price: {}".format(id, margin_active, price))
			else:
				fulfilled = float(asyncio.run(_binance.HTTP_private_request("GET", "/api/v3/order", {
					"symbol": "{}BUSD".format(coin),
					"orderId" : order_number
					}))['executedQty'])
				if fulfilled > 0:
					asyncio.run(_binance.HTTP_private_request("POST", "/api/v3/order", {
						"symbol": "{}BUSD".format(coin),
						"side": "BUY",
						"type": "MARKET",
						"quantity": fulfilled
						}))

					msg = "Market bought {} with amount {}".format(coin, fulfilled)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, calc_price(coin, "asks"), margin_active))

					# Delete partially created order
					asyncio.run(_binance.HTTP_private_request("DELETE", "/api/v3/order", {
						"symbol": "{}BUSD".format(coin),
						"orderId": order_number
						}))
				# Repay margin
				asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
					"asset": "{}".format(coin),
					"amount": settlement_amount,
					"type": 1 # 1: spot to margin, 2: margin to spot
				}))
				asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/repay", {
					"asset": "{}".format(coin),
					"amount": settlement_amount
				}))

				msg = "{} Margin repaied".format(coin)
				print_n_log(msg)
				asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, calc_price(coin, "asks"), margin_active))
		# In 0.5% range between settlement price
		elif (price > settlement_price) and (price <= settlement_price * factor):
			if order_number is None:
				# Borror margin and create limit sell maker order
				asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/loan", {
					"asset": "{}".format(coin),
					"amount": settlement_amount
				}))
				asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
					"asset": "{}".format(coin),
					"amount": settlement_amount,
					"type": 2 # 1: spot to margin, 2: margin to spot
				}))
				order = asyncio.run(_binance.HTTP_private_request("POST", "/api/v3/order", {
						"symbol": "{}BUSD".format(coin),
						"side": "SELL",
						"type": "STOP_LOSS_LIMIT",
						"timeInForce": "GTC",
						"quantity": settlement_amount,
						"price": settlement_price,
						"stopPrice": settlement_price
					}))
				update_order_number(id, order['orderId'])
				
				msg = "Limit sell order number {} created for {}".format(order['orderId'], coin)
				print_n_log(msg)
				asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price, margin_active))
			else:
				print_n_log("Keep watching trading strat id {}. Margin active: {} Price: {}".format(id, margin_active, price))
	    # price <= settlementment_price
		else:
			# High volatility case: Borrow and market sell
			if order_number is None:
				asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/loan", {
					"asset": "{}".format(coin),
					"amount": settlement_amount
				}))
				asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
					"asset": "{}".format(coin),
					"amount": settlement_amount,
					"type": 2 # 1: spot to margin, 2: margin to spot
				}))
				asyncio.run(_binance.HTTP_private_request("POST", "/api/v3/order", {
					"symbol": "{}BUSD".format(coin),
					"side": "SELL",
					"type": "MARKET",
					"quantity": settlement_amount
					}))

				toggle_margin(id, True)
				msg = "Market sold {} with amount {}".format(coin, settlement_amount)
				print_n_log(msg)
				asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price, margin_active))
			else:
				# An outstanding order is fully fulfilled
				fulfilled = float(asyncio.run(_binance.HTTP_private_request("GET", "/api/v3/order", {
					"symbol": "{}BUSD".format(coin),
					"orderId" : order_number
					}))['executedQty'])
				if fulfilled == settlement_amount:
					update_order_number(id, None)

					toggle_margin(id, True)
					msg = "Limit sell order {} for {} fully fulfilled".format(order_number, coin)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, price, margin_active))
				else:
					# Should not reach
					asyncio.run(send_notification("Anomaly: Order not fully fulfilled even if price goes below the limit sell price"))
def settle(id, coin, settlement_price, settlement_amount, exchange, final_price, margin_active):
    # Pass settlement time for the first time: determine final settlement status and handle some extreme cases
	if final_price is None:
		insert_final_price(id, get_ma(coin))

		# Extreme case 1: price > settlement but margin active
		if final_price >= settlement_price and margin_active:
				asyncio.run(_binance.HTTP_private_request("POST", "/api/v3/order", {
					"symbol": "{}BUSD".format(coin),
					"side": "BUY",
					"type": "MARKET",
					"quantity": settlement_amount
				}))
				asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
					"asset": "{}".format(coin),
					"amount": settlement_amount,
					"type": 1 # 1: spot to margin, 2: margin to spot
				}))
				asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/repay", {
					"asset": "{}".format(coin),
					"amount": settlement_amount
				}))
				toggle_margin(id, False)
		# Extreme case 2: price <= settlement but not margin active
		elif final_price < settlement_price and not(margin_active):
				asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/loan", {
					"asset": "{}".format(coin),
					"amount": settlement_amount
				}))
				asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
					"asset": "{}".format(coin),
					"amount": settlement_amount,
					"type": 2 # 1: spot to margin, 2: margin to spot
				}))
				asyncio.run(_binance.HTTP_private_request("POST", "/api/v3/order", {
					"symbol": "{}BUSD".format(coin),
					"side": "SELL",
					"type": "MARKET",
					"quantity": settlement_amount
				}))
				toggle_margin(id, True)
	# After final price is inserted, wait until balance arrives
	else:
		if margin_active:
			if exchange == "bybit": # Bybit: Sell received coin on earn account
				balance = asyncio.run(_bybit.HTTP_private_request("GET", "/asset/v3/private/transfer/account-coin/balance/query", {
					"accountType": "INVESTMENT",
					"coin": "USDT"
				}))['result']['balance']['walletBalance']
				if balance >= settlement_amount:
					print_n_log("Coins arrived")

					bybit.transfer(coin, balance, "investment", "spot")
					bybit.create_order(symbol="{}/USDT".format(coin), type='market', side='sell', amount=balance)
					balance_usd = asyncio.run(_bybit.HTTP_private_request("GET", "/asset/v3/private/transfer/account-coin/balance/query", {
						"accountType": "SPOT",
						"coin": "USDT"
					}))['result']['balance']['walletBalance']

					bybit.transfer("USDT", balance_usd, "spot", "investment")

					msg = "Market sold {} with amount {}".format(coin, balance)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, calc_price(coin, "bids"), margin_active))

					# Buy coins and repay outstanding debt
					asyncio.run(_binance.HTTP_private_request("POST", "/api/v3/order", {
						"symbol": "{}BUSD".format(coin),
						"side": "BUY",
						"type": "MARKET",
						"quantity": settlement_amount
					}))
					asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
						"asset": "{}".format(coin),
						"amount": settlement_amount,
						"type": 1 # 1: spot to margin, 2: margin to spot
					}))
					asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/repay", {
						"asset": "{}".format(coin),
						"amount": settlement_amount
					}))

					msg = "Market bought {} with amount {}".format(coin, balance)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, settlement_price, settlement_price, calc_price(coin, "asks"), margin_active))

					settle_trade(id)
				else:
					print_n_log("Coins not arrived yet.")
			elif exchange == "binance": # Binance: Directly repay debt with received coins
				balance = asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v3/asset/getUserAsset", {"asset": "{}".format(coin)}))[0]['free']

				if balance >= settlement_amount:
					print_n_log("Coins arrived")

					asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
						"asset": "{}".format(coin),
						"amount": settlement_amount,
						"type": 1 # 1: spot to margin, 2: margin to spot
					}))
					asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v1/margin/repay", {
						"asset": "{}".format(coin),
						"amount": settlement_amount
					}))

					settle_trade(id)
				else:
					print_n_log("Coins not arrived yet.")
			else:
				raise Exception("No valid exchanges")
		elif not(margin_active):
			if exchange == "bybit":
				balance_usd = asyncio.run(_bybit.HTTP_private_request("GET", "/asset/v3/private/transfer/account-coin/balance/query", {
					"accountType": "INVESTMENT",
					"coin": "USDT"
				}))['result']['balance']['walletBalance']

				if balance_usd >= (calc_price(coin, "bids") * settlement_amount) * 0.99:
					print_n_log("USD arrived")
					settle_trade(id)
				else:
					print_n_log("USD not arrived yet.")
			elif exchange == "binance":
				balance_usd = asyncio.run(_binance.HTTP_private_request("POST", "/sapi/v3/asset/getUserAsset", {
					"asset": "{}".format("BUSD")
				}))[0]['free']

				if balance_usd >= (calc_price(coin, "bids") * settlement_amount) * 0.99:
					print_n_log("USD arrived")
					settle_trade(id)
				else:
					print_n_log("USD not arrived yet.")
 
# main function
def main():
	while True:
		#current_datetime = datetime.now()
		#proxy_rotation_interval = 600
		#last_proxy_rotation_time = current_datetime

		current_strats = get_not_settled_strats()
		epoch_interval = 5

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
	#except ccxt.RequestTimeout as e:
	#	binance.proxy = FreeProxy(rand=True).get().replace("http://", "")

if __name__ == "__main__":
    main()
	#try:
    #    main()
    #except Exception as e:
    #    asyncio.run(send_error_message("Dual Trading Trade Part", e))