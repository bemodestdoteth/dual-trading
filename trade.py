import ccxt

from dotenv import load_dotenv

from config import print_n_log, send_notification, send_buy_sell_message, send_error_message
from db import get_not_settled_strats, settle_trade, toggle_margin, insert_final_price, toggle_sold
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
class Strat:
	def __init__(self, strat) -> None:
		self.id = int(strat[0])
		self.coin = strat[1]
		self.settlement_price = float(strat[2])
		self.settlement_amount = float(strat[3])
		self.settlement_date = datetime.strptime(strat[4], "%Y-%m-%d %H:%M:%S")
		self.exchange = strat[5]
		self.order_number = strat[6]
		self.final_price = strat[7]
		self.is_settled = bool(strat[8])
		self.margin_active = bool(strat[9])
		self.sold = bool(strat[10])

	def __str__(self) -> str:
		return f'({self.id},{self.coin},{self.settlement_price},{self.settlement_amount},{self.settlement_date},{self.exchange},{self.order_number},{self.final_price},{self.is_settled},{self.margin_active},{self.sold})'

	def insert_final_price(self, final_price):
		self.final_price = final_price
		insert_final_price(self.id, self.final_price)

	def set_margin_active(self, is_margin_active):
		self.margin_active = is_margin_active
		toggle_margin(self.id, self.margin_active)

	def set_sold(self, is_sold):
		self.sold = is_sold
		toggle_sold(self.id, self.sold)

	def settle(self):
		self.is_settled = True
		settle_trade(self.id)
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
def refresh_strats():
	strats = []
	for strat in get_not_settled_strats():
		strats.append(Strat(strat))
	return strats
# Todo
def settle(strat):
    # Pass settlement time for the first time: determine final settlement status and handle some extreme cases
	if strat.final_price is None:
		insert_final_price(id, get_ma(strat.coin))

		# Extreme case 1: price > settlement but margin active
		if strat.final_price >= strat.settlement_price and strat.sold:
				asyncio.run(binance.HTTP_private_request("POST", "/api/v3/order", {
					"symbol": "{}BUSD".format(strat.coin),
					"side": "BUY",
					"type": "MARKET",
					"quantity": strat.settlement_amount
				}))
				asyncio.run(binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
					"asset": "{}".format(strat.coin),
					"amount": strat.settlement_amount,
					"type": 1 # 1: spot to margin, 2: margin to spot
				}))
				asyncio.run(binance.HTTP_private_request("POST", "/sapi/v1/margin/repay", {
					"asset": "{}".format(strat.coin),
					"amount": strat.settlement_amount
				}))
				strat.set_margin_active("False")
		# Extreme case 2: price <= settlement but not margin active
		elif strat.final_price < strat.settlement_price and not(strat.margin_active):
				asyncio.run(binance.HTTP_private_request("POST", "/sapi/v1/margin/loan", {
					"asset": "{}".format(strat.coin),
					"amount": strat.settlement_amount
				}))
				asyncio.run(binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
					"asset": "{}".format(strat.coin),
					"amount": strat.settlement_amount,
					"type": 2 # 1: spot to margin, 2: margin to spot
				}))
				asyncio.run(binance.HTTP_private_request("POST", "/api/v3/order", {
					"symbol": "{}BUSD".format(strat.coin),
					"side": "SELL",
					"type": "MARKET",
					"quantity": strat.settlement_amount
				}))
				strat.set_margin_active("True")
	# After final price is inserted, wait until balance arrives
	else:
		if strat.margin_active:
			if strat.exchange == "bybit": # Bybit: Sell received coin on earn account
				balance = asyncio.run(_bybit.HTTP_private_request("GET", "/asset/v3/private/transfer/account-coin/balance/query", {
					"accountType": "INVESTMENT",
					"coin": "USDT"
				}))['result']['balance']['walletBalance']
				if balance >= strat.settlement_amount:
					print_n_log("Coins arrived")

					bybit.transfer(strat.coin, balance, "investment", "spot")
					bybit.create_order(symbol="{}/USDT".format(strat.coin), type='market', side='sell', amount=balance)
					balance_usd = asyncio.run(_bybit.HTTP_private_request("GET", "/asset/v3/private/transfer/account-coin/balance/query", {
						"accountType": "SPOT",
						"coin": "USDT"
					}))['result']['balance']['walletBalance']

					bybit.transfer("USDT", balance_usd, "spot", "investment")

					msg = "Market sold {} with amount {}".format(strat.coin, balance)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, strat.settlement_price, strat.settlement_price, calc_price(strat.coin, "bids"), strat.margin_active))

					# Buy coins and repay outstanding debt
					asyncio.run(binance.HTTP_private_request("POST", "/api/v3/order", {
						"symbol": "{}BUSD".format(strat.coin),
						"side": "BUY",
						"type": "MARKET",
						"quantity": strat.settlement_amount
					}))
					asyncio.run(binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
						"asset": "{}".format(strat.coin),
						"amount": strat.settlement_amount,
						"type": 1 # 1: spot to margin, 2: margin to spot
					}))
					asyncio.run(binance.HTTP_private_request("POST", "/sapi/v1/margin/repay", {
						"asset": "{}".format(strat.coin),
						"amount": strat.settlement_amount
					}))

					msg = "Market bought {} with amount {}".format(strat.coin, balance)
					print_n_log(msg)
					asyncio.run(send_buy_sell_message(msg, id, strat.settlement_price, strat.settlement_price, calc_price(strat.coin, "asks"), strat.margin_active))

					settle_trade(id)
				else:
					print_n_log("Coins not arrived yet.")
			elif strat.exchange == "binance": # Binance: Directly repay debt with received coins
				balance = asyncio.run(binance.HTTP_private_request("POST", "/sapi/v3/asset/getUserAsset", {"asset": "{}".format(strat.coin)}))[0]['free']

				if balance >= strat.settlement_amount:
					print_n_log("Coins arrived")

					asyncio.run(binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
						"asset": "{}".format(strat.coin),
						"amount": strat.settlement_amount,
						"type": 1 # 1: spot to margin, 2: margin to spot
					}))
					asyncio.run(binance.HTTP_private_request("POST", "/sapi/v1/margin/repay", {
						"asset": "{}".format(strat.coin),
						"amount": strat.settlement_amount
					}))

					settle_trade(id)
				else:
					print_n_log("Coins not arrived yet.")
			else:
				raise Exception("No valid exchanges")
		elif not(strat.margin_active):
			if strat.exchange == "bybit":
				balance_usd = asyncio.run(_bybit.HTTP_private_request("GET", "/asset/v3/private/transfer/account-coin/balance/query", {
					"accountType": "INVESTMENT",
					"coin": "USDT"
				}))['result']['balance']['walletBalance']

				if balance_usd >= (calc_price(strat.coin, "bids") * strat.settlement_amount) * 0.99:
					print_n_log("USD arrived")
					settle_trade(id)
				else:
					print_n_log("USD not arrived yet.")
			elif strat.exchange == "binance":
				balance_usd = asyncio.run(binance.HTTP_private_request("POST", "/sapi/v3/asset/getUserAsset", {
					"asset": "{}".format("BUSD")
				}))[0]['free']

				if balance_usd >= (calc_price(strat.coin, "bids") * strat.settlement_amount) * 0.99:
					print_n_log("USD arrived")
					settle_trade(id)
				else:
					print_n_log("USD not arrived yet.")

# main function
async def main():
	os.chdir(os.path.dirname(__file__))
	#await send_notification("Initializing Trading Part...")

	band = 0.005 # 0.5%
	delay = 0.5
	counter = 0
	strats = refresh_strats()
	uri = "wss://stream.binance.com/ws/{}busd".format(strat.coin.lower())

	async with websockets.connect(uri) as websocket:
		await websocket.send(json.dumps({
			"method": "SUBSCRIBE",
			"params": ["{}busd@depth@1000ms".format(strat.coin.lower())],
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

					for strat in strats:
						# Less invertal if price is outside the band
						if not strat.sold:
							if bid_price > strat.settlement_price * (1 + band):
								# Repay Margin
								await binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
									"asset": "{}".format(strat.coin),
									"amount": strat.settlement_amount,
									"type": 1 # 1: spot to margin, 2: margin to spot
								})
								await binance.HTTP_private_request("POST", "/sapi/v1/margin/repay", {
									"asset": "{}".format(strat.coin),
									"amount": strat.settlement_amount
								})
								print_n_log("Repay Complete")
								strat.set_margin_active(False)
								delay = 30
							elif bid_price > strat.settlement_price and bid_price <= strat.settlement_price * (1 + band):
								if not strat.margin_active:
									# Borror Margin
									await binance.HTTP_private_request("POST", "/sapi/v1/margin/loan", {
										"asset": "{}".format(strat.coin),
										"amount": strat.settlement_amount
									})
									await binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
										"asset": "{}".format(strat.coin),
										"amount": strat.settlement_amount,
										"type": 2 # 1: spot to margin, 2: margin to spot
									})
									print_n_log("Borrow Complete")
									strat.set_margin_active(True)
								delay = 0.5
							else:
								# High volatility case: borrow margin first
								if not strat.margin_active:
									await binance.HTTP_private_request("POST", "/sapi/v1/margin/loan", {
										"asset": "{}".format(strat.coin),
										"amount": strat.settlement_amount
									})
									await binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
										"asset": "{}".format(strat.coin),
										"amount": strat.settlement_amount,
										"type": 2 # 1: spot to margin, 2: margin to spot
									})
									strat.set_margin_active(True)
								# Market sell
								await binance.HTTP_private_request("POST", "/api/v3/order", {
									"symbol": "{}BUSD".format(strat.coin),
									"side": "SELL",
									"type": "MARKET",
									"quantity": strat.settlement_amount
								})
								print_n_log("Sell Complete")
								strat.set_sold(True)
								delay = 0.5
						else: # Sold, and borrowed at the first place
							if ask_price < strat.settlement_price * (1 - band):
								delay = 30
							elif bid_price >= strat.settlement_price * (1 - band) and bid_price < strat.settlement_price:
								delay = 0.5
							else:
								# Market buy
								await binance.HTTP_private_request("POST", "/api/v3/order", {
									"symbol": "{}BUSD".format(strat.coin),
									"side": "BUY",
									"type": "MARKET",
									"quantity": strat.settlement_amount
								})
								print_n_log("Buy Complete")
								strat.set_sold(False)
								delay = 0.5
					print_n_log(bid_price)
					print_n_log(ask_price)
					print_n_log(res['E'] / 1000)
					print_n_log(time.time())
				print_n_log("-"*20)
				counter += delay
				if counter >= 60: # Refresh strategy DB every 60 seconds
					strats = refresh_strats()
					print_n_log("Database Refreshed")
					counter = 0
				time.sleep(delay)
			except Exception as e:
				print_n_log(e)
				await send_error_message("Dual Trading Trading Part", e)
				raise Exception(e)

if __name__ == "__main__":
    asyncio.run(main())