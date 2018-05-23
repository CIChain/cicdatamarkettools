import okex_web_api
import sys
import os
path = os.getcwd()
sys.path.append(path)
import common_fun
import _thread

symbols = []
base_url = 'https://www.okex.com/api/v1/'
headers = {
        "Content-type" : "application/x-www-form-urlencoded",
        }

request_url = base_url + 'tickers.do'
res_json = common_fun.get_url_json(request_url, headers)
for ticker in res_json['tickers']:
    if ticker['symbol'] not in symbols:
        symbols.append(ticker['symbol'])


def run_client(*args):
    web_client = okex_web_api.WebClient()
    web_client.make_events(symbols, args[0])
    web_client.run()

_thread.start_new_thread(run_client, ('_deals',))
_thread.start_new_thread(run_client, ('_ticker',))
_thread.start_new_thread(run_client, ('_depth',))
_thread.start_new_thread(run_client, ('_kline_1min',))