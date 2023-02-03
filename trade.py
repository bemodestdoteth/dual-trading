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
		settle(self)
		self.is_settled = True
		settle_trade(self.id)
def calc_price(coin, side):
	res = asyncio.run(binance.HTTP_public_request("GET", "/api/v3/depth", {
		"symbol": "{}BUSD".format(coin)
	}))
	return float(res[side][0][0])
def refresh_strats():
	strats = []
	for strat in get_not_settled_strats():
		strats.append(Strat(strat))
	return strats

# main function
async def settle(strat):
    # Pass settlement time for the first time: determine final settlement status and handle some extreme cases
	if strat.final_price is None:
		ma_30_candles = await binance.HTTP_public_request("GET", "/api/v3/klines", {
			"symbol": "{}BUSD".format("BTC"),
			"interval": "1m"
			})
		ma_30 = mean(tuple(float(ma_30_ohlcv[4]) for ma_30_ohlcv in ma_30_candles)[0:30])

		insert_final_price(strat.id, ma_30)

		# Extreme case 1: price > settlement but margin active
		if ma_30 > strat.settlement_price and strat.sold:
				await binance.HTTP_private_request("POST", "/api/v3/order", {
					"symbol": "{}BUSD".format(strat.coin),
					"side": "BUY",
					"type": "MARKET",
					"quantity": strat.settlement_amount
				})
				await binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
					"asset": "{}".format(strat.coin),
					"amount": strat.settlement_amount,
					"type": 1 # 1: spot to margin, 2: margin to spot
				})
				await binance.HTTP_private_request("POST", "/sapi/v1/margin/repay", {
					"asset": "{}".format(strat.coin),
					"amount": strat.settlement_amount
				})
				print_n_log("Buy Complete")
				print_n_log("Repay Complete")
				strat.set_margin_active(False)
				strat.set_sold(False)
		# Extreme case 2: price <= settlement but not margin active
		elif ma_30 <= strat.settlement_price and not strat.sold:
				await binance.HTTP_private_request("POST", "/sapi/v1/margin/loan", {
					"asset": "{}".format(strat.coin),
					"amount": strat.settlement_amount
				})
				await binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
					"asset": "{}".format(strat.coin),
					"amount": strat.settlement_amount,
					"type": 2 # 1: spot to margin, 2: margin to spot
				})
				await binance.HTTP_private_request("POST", "/api/v3/order", {
					"symbol": "{}BUSD".format(strat.coin),
					"side": "SELL",
					"type": "MARKET",
					"quantity": strat.settlement_amount
				})
				print_n_log("Borrow Complete")
				print_n_log("Sell Complete")
				strat.set_margin_active(True)
				strat.set_sold(True)
	# After final price is inserted, wait until balance arrives
	else:
		if strat.sold:
			if strat.exchange == "bybit": # Bybit: Sell received coin on earn account
				balance = await _bybit.HTTP_private_request("GET", "/asset/v3/private/transfer/account-coin/balance/query", {
					"accountType": "INVESTMENT",
					"coin": "USDT"
				})['result']['balance']['walletBalance']
				if balance >= strat.settlement_amount:
					print_n_log("Coins arrived")

					bybit.transfer(strat.coin, balance, "investment", "spot")
					bybit.create_order(symbol="{}/USDT".format(strat.coin), type='market', side='sell', amount=balance)
					balance_usd = await _bybit.HTTP_private_request("GET", "/asset/v3/private/transfer/account-coin/balance/query", {
						"accountType": "SPOT",
						"coin": "USDT"
					})['result']['balance']['walletBalance']

					bybit.transfer("USDT", balance_usd, "spot", "investment")

					# Buy coins and repay outstanding debt
					await binance.HTTP_private_request("POST", "/api/v3/order", {
						"symbol": "{}BUSD".format(strat.coin),
						"side": "BUY",
						"type": "MARKET",
						"quantity": strat.settlement_amount
					})
					await binance.HTTP_private_request("POST", "/sapi/v1/margin/transfer", {
						"asset": "{}".format(strat.coin),
						"amount": strat.settlement_amount,
						"type": 1 # 1: spot to margin, 2: margin to spot
					})
					await binance.HTTP_private_request("POST", "/sapi/v1/margin/repay", {
						"asset": "{}".format(strat.coin),
						"amount": strat.settlement_amount
					})

					settle_trade(strat.id)
					await send_notification("Trade Strategy ID {} settled.".format(strat.id))
				else:
					print_n_log("Coins not arrived yet.")
			elif strat.exchange == "binance":
				# Binance: Repay debt with received coins
				balance = await binance.HTTP_private_request("POST", "/sapi/v3/asset/getUserAsset", {"asset": "{}".format(strat.coin)})
				if balance != [] and float(balance[0]['free']) >= strat.settlement_amount:
					print_n_log("Coins arrived")

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
					settle_trade(strat.id)
					await send_notification("Trade Strategy ID {} settled. Repayed debt with received coins".format(strat.id))
				else:
					print_n_log("Coins not arrived yet.")
			else:
				raise Exception("No valid exchanges")
		elif not(strat.sold):
			if strat.exchange == "bybit":
				balance_usd = await _bybit.HTTP_private_request("GET", "/asset/v3/private/transfer/account-coin/balance/query", {
					"accountType": "INVESTMENT",
					"coin": "USDT"
				})['result']['balance']['walletBalance']

				if balance_usd >= (calc_price(strat.coin, "bids") * strat.settlement_amount) * 0.99:
					print_n_log("USD arrived")
					settle_trade(strat.id)
					await send_notification("Trade Strategy ID {} settled.".format(strat.id))
				else:
					print_n_log("USD not arrived yet.")
			elif strat.exchange == "binance":
					# Repay margin if margin active
					if strat.margin_active:
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
						
					# No further action required
					settle_trade(strat.id)
					await send_notification("Trade Strategy ID {} settled through receiving stablecoin. No further action required..".format(strat.id))
			else:
				raise Exception("No valid exchanges")
async def main():
	# Check valid strategy input before starting
	strats = refresh_strats()
	if len(strats) == 0:
		print_n_log("No strategy input. Waiting until input...")
		time.sleep(10)
		return
	print_n_log("Database Refreshed")
	counter = 0

	os.chdir(os.path.dirname(__file__))
	await send_notification("Initializing...")

	band_upper = 0.1 # 0.1%
	band_lower = 0.00005 # 0.005%

	# Todo: accomodate two or more websocket streams
	strat = strats[0]
	uri = "wss://stream.binance.com/ws/{}busd".format(strat.coin.lower())
	start_time = int(time.time())

	async with websockets.connect(uri) as websocket:
		await websocket.send(json.dumps({
			"method": "SUBSCRIBE",
			"params": ["{}busd@depth@1000ms".format(strat.coin.lower())],
			"id": 1
		}))

		while True:
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

				price = (bid_price + ask_price) / 2

				refresh_after_end = False
				for strat in strats:
					# Settle trade that is after the settlement date
					if int(time.time()) >= time.mktime(strat.settlement_date.timetuple()):
					#if counter >= 10:
						await settle(strat)
						refresh_after_end = True
					#else:
					elif not strat.is_settled:
						if not strat.sold:
							if price > strat.settlement_price * (1 + band_upper):
								if strat.margin_active:
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
									refresh_after_end = True
							elif price > strat.settlement_price * (1 + band_lower) and price <= strat.settlement_price * (1 + band_upper):
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
									refresh_after_end = True
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
									print_n_log("Borrow Complete")
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
								refresh_after_end = True
						elif strat.sold: # Sold, and borrowed at the first place
							if price >= strat.settlement_price * (1 - band_lower):
								# Market buy
								await binance.HTTP_private_request("POST", "/api/v3/order", {
									"symbol": "{}BUSD".format(strat.coin),
									"side": "BUY",
									"type": "MARKET",
									"quantity": strat.settlement_amount
								})
								print_n_log("Buy Complete")
								strat.set_sold(False)
								refresh_after_end = True
				#print_n_log(bid_price)
				print_n_log(price)
				print_n_log(res['E'] / 1000)
				print_n_log(time.time())
				# Refresh database if any of refreshing event occurs
				if refresh_after_end:
					strats = refresh_strats()
					if len(strats) == 0:
						print_n_log("No strategy input. Waiting until input...")
						time.sleep(10)
						return
					print_n_log("Database Refreshed")
					counter = 0

			# Regularly refresh strategy DB every 60 seconds
			counter += 1
			if counter >= 60: 
				strats = refresh_strats()
				if len(strats) == 0:
					print_n_log("No strategy input. Waiting until input...")
					time.sleep(10)
					return
				print_n_log("Database Refreshed")
				counter = 0
			# Refresh connection every 12 hours
			if int(time.time()) - start_time >= 12 * 60 * 60:
				print_n_log("Refreshing Connection")
				break

			print_n_log("-"*20)
			time.sleep(0.5)

if __name__ == "__main__":
	while True:
		if os.path.isfile("strats.db"):
			try:
				asyncio.run(main())
			except Exception as e:
				print_n_log(e)
				asyncio.run(send_error_message("Dual Trading Trade Part", e))
		else:
			print_n_log("no database yet")
			time.sleep(10)