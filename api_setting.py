import time
import hmac
import hashlib
import asyncio
import aiohttp
import os
import random
import uuid

def parse_payload(payload):
    payload_str = ""
    for (key, value) in payload.items():
        payload_str += "&{}={}".format(key, value)
    return payload_str[1:] # Skip first &
class binanceapi:
    def __init__(self):
        self.apikey = os.environ["BINANCE_API_KEY"]
        self.secret = os.environ["BINANCE_SECRET_KEY"]
        self.binance_endpoint_url = "https://api.binance.com"
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except AttributeError:
            pass
        return

    async def HTTP_public_request(self, method, endpoint, payload = {}):
        httpClient=aiohttp.ClientSession()
        headers = {
            'X-MBX-APIKEY': self.apikey,
            'Content-Type': 'application/json'
        }
        async with httpClient as client:
            try:
                response = await client.request(method, self.binance_endpoint_url + endpoint + "?" + parse_payload(payload=payload), headers=headers)
                assert response.status == 200, f'status code error {response.status}'
                response = await response.json()
                return response
            except AssertionError as e:
                print(e)
                response = await response.json()
                print(response)


    async def HTTP_private_request(self, method, endpoint, payload = {}):
        httpClient=aiohttp.ClientSession()
        time_stamp=str(int(time.time() * 10 ** 3))
        payload = parse_payload(payload=payload) + "&timestamp={}".format(time_stamp)
        signature=self.genSignature(payload)
        payload = payload + "&signature={}".format(signature)
        headers = {
            'X-MBX-APIKEY': self.apikey,
            'Content-Type': 'application/json'
        }
        async with httpClient as client:
            try:
                if(method=="POST"):
                    response = await client.request(method, self.binance_endpoint_url + endpoint + "?" + payload, headers=headers)
                else:
                    response = await client.request(method, self.binance_endpoint_url + endpoint + "?" + payload, headers=headers)

                assert response.status == 200, f'status code error {response.status}'
                response = await response.json()
                return response
            except AssertionError as e:
                print(e)
                response = await response.json()
                print(response)

    def genSignature(self, payload):
        return hmac.new(self.secret.encode("utf-8"), payload.encode("utf-8"),hashlib.sha256).hexdigest()
class bybitapi:
    def __init__(self):
        self.apikey = os.environ["BYBIT_API_KEY"]
        self.secret = os.environ["BYBIT_SECRET_KEY"]
        self.bybit_endpoint_url = "https://api.bybit.com"
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except AttributeError:
            pass
        return

    def uuid(self, a = None):
        if a:
            return format((a ^ random.random() * 16 >> a / 4), 'x')
        else:
            return str(uuid.uuid4())

    async def HTTP_private_request(self, method, endpoint, payload = {}):
        httpClient=aiohttp.ClientSession()
        recv_window=str(5000)
        time_stamp=str(int(time.time() * 10 ** 3))
        payload = parse_payload(payload=payload)
        signature=self.genSignature(payload, time_stamp, recv_window)
        headers = {
            'X-BAPI-API-KEY': self.apikey,
            'X-BAPI-SIGN': signature,
            'X-BAPI-SIGN-TYPE': '2',
            'X-BAPI-TIMESTAMP': time_stamp,
            'X-BAPI-RECV-WINDOW': recv_window,
            'Content-Type': 'application/json'
        }
        async with httpClient as client:
            try:
                if(method=="POST"):
                    response = await client.request(method, self.bybit_endpoint_url + endpoint + "?" + payload, headers=headers)
                else:
                    response = await client.request(method, self.bybit_endpoint_url + endpoint + "?" + payload, headers=headers)

                assert response.status == 200, f'status code error {response.status}'
                response = await response.json()
                return response
            except AssertionError as e:
                print(e)
                response = await response.json()
                print(response)

    def _genSignature(self, payload):
        return hmac.new(self.secret.encode("utf-8"), payload.encode("utf-8"),hashlib.sha256).hexdigest()

    def genSignature(self, payload, time_stamp, recv_window):
        param_str= str(time_stamp) + self.apikey + recv_window + payload
        return hmac.new(bytes(self.secret, "utf-8"), param_str.encode("utf-8"), hashlib.sha256).hexdigest()