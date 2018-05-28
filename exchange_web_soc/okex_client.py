import okex_web_api
import sys
import os
path = os.getcwd()
sys.path.append(path)
import common_fun


base_url = 'https://www.okex.com/api/v1/'
headers = {
        "Content-type" : "application/x-www-form-urlencoded",
        }

web_client = okex_web_api.WebClient()
request_url = base_url + 'tickers.do'
res_json = common_fun.get_url_json(request_url, headers)
for ticker in res_json['tickers']:
    if ticker['symbol'] not in web_client.symbols:
        web_client.symbols.append(ticker['symbol'])

web_client.make_ticker_events()
web_client.run()
