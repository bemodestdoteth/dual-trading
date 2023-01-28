import ccxt

from fp.fp import FreeProxy
from dotenv import load_dotenv

from config import print_n_log, send_notification, send_buy_sell_message, send_error_message
from db import get_not_settled_strats, update_order_number, settle_trade, toggle_margin, insert_final_price
from api_setting import binanceapi, bybitapi

from datetime import datetime
from statistics import mean
import asyncio
import websockets
import json
import time
import os

# Load environment variables and exchange api data
load_dotenv()
bybit = ccxt.bybit()
bybit.options["defaultType"] = "spot"
bybit.apiKey = os.environ["BYBIT_API_KEY"]
bybit.secret = os.environ["BYBIT_SECRET_KEY"]

binance = binanceapi()
_bybit = bybitapi()

# Function to execute the trading strategy
def get_ma(coin, exchange = bybit, interval = 30):
	if exchange == "bybit":
		ohlcvs = ((datetime.utcfromtimestamp(int(i[0]/1000)).strftime("%Y/%m/%d %H:%M:%S"), i[4]) for i in bybit.fetch_ohlcv("{}/USDT".format(coin), "1m")[-interval:])
	elif exchange == "binance":
		ohlcvs = ((datetime.utcfromtimestamp(int(i[0]/1000)).strftime("%Y/%m/%d %H:%M:%S"), i[4]) for i in binance.fetch_ohlcv("{}/BUSD".format(coin), "1m")[-interval:])
	return(mean(ohlcv[1] for ohlcv in ohlcvs))
def calc_price(coin, side):
	res = asyncio.run(binance.HTTP_public_request("GET", "/api/v3/depth", {
		"symbol": "{}BUSD".format(coin)
	}))
	return float(res[side][0][0])
def calc_principal():
    res = asyncio.run(binance.HTTP_private_request("GET", "/sapi/v1/margin/account"))['totalAssetOfBtc']
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
async def main(coin, settlement_price, settlement_amount):
	sold = False
	borrowed = False
	band = 0.005 # 0.5%
	delay = 0.5
	uri = "wss://stream.binance.com/ws/{}busd".format(coin.lower())

	async with websockets.connect(uri) as websocket:
		await websocket.send(json.dumps({
			"method": "SUBSCRIBE",
			"params": ["{}busd@depth@1000ms".format(coin.lower())],
			"id": 1
		}))
		while True:
			try:
				res = json.loads(await websocket.recv())
				if 'result' in res:
					print("Not Fetched")
				else:
					try:
						bid_price = float(tuple(filter(lambda b: float(b[1]) > 0, res['b']))[0][0])
					except:
						bid_price = 0
					try:
						ask_price = float(tuple(filter(lambda a: float(a[1]) > 0, res['a']))[0][0])
					except:
						ask_price = 0

					# Less invertal if price is outside the band
					if not sold:
						if bid_price > settlement_price * (1 + band):
							# Repay Margin
							await binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
								"asset": "{}".format(coin),
								"amount": settlement_amount,
								"type": 1 # 1: spot to margin, 2: margin to spot
							})
							await binance.HTTP_private_request("POST", "/sapi/v1/margin/repay", {
								"asset": "{}".format(coin),
								"amount": settlement_amount
							})
							print_n_log("Repay Complete")
							borrowed = False
							delay = 30
						elif bid_price > settlement_price and bid_price <= settlement_price * (1 + band):
							if not borrowed:
								# Borror Margin
								await binance.HTTP_private_request("POST", "/sapi/v1/margin/loan", {
									"asset": "{}".format(coin),
									"amount": settlement_amount
								})
								await binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
									"asset": "{}".format(coin),
									"amount": settlement_amount,
									"type": 2 # 1: spot to margin, 2: margin to spot
								})
								print_n_log("Borrow Complete")
								borrowed = True
							delay = 0.5
						else:
							# High volatility case: borrow margin first
							if not borrowed:
								await binance.HTTP_private_request("POST", "/sapi/v1/margin/loan", {
									"asset": "{}".format(coin),
									"amount": settlement_amount
								})
								await binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
									"asset": "{}".format(coin),
									"amount": settlement_amount,
									"type": 2 # 1: spot to margin, 2: margin to spot
								})
								borrowed = True
							# Market sell
							await binance.HTTP_private_request("POST", "/api/v3/order", {
								"symbol": "{}BUSD".format(coin),
								"side": "SELL",
								"type": "MARKET",
								"quantity": settlement_amount
							})
							print_n_log("Sell Complete")
							sold = True
							delay = 0.5
					else: # Sold, and borrowed at the first place
						if ask_price < settlement_price * (1 - band):
							delay = 30
						elif bid_price >= settlement_price * (1 - band) and bid_price < settlement_price:
							delay = 0.5
						else:
							# Market buy
							await binance.HTTP_private_request("POST", "/api/v3/order", {
								"symbol": "{}BUSD".format(coin),
								"side": "BUY",
								"type": "MARKET",
								"quantity": settlement_amount
							})
							print_n_log("Sell Complete")
							sold = False
							delay = 0.5
					print_n_log(bid_price)
					print_n_log(ask_price)
					print_n_log(res['E'] / 1000)
					print_n_log(time.time())
				print_n_log("-"*20)
				time.sleep(delay)
			except Exception as e:
				print_n_log(e)
				await send_error_message("Dual Trading Trading Part", e)
				raise Exception(e)

if __name__ == "__main__":
    asyncio.run(main("BTC", 22950, 0.0005))