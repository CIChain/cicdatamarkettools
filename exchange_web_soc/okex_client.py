import okex_web_api
import sys
import os
path = os.getcwd()
sys.path.append(path)
import common_fun

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

web_client = okex_web_api.WebClient()
web_client.get_all_symbols()
web_client.make_events(symbols, '_deals')
web_client.run()