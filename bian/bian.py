import requests
from pymongo import MongoClient
import time
    
class Bian(object):
    def __init__(self): 
        self.base_url = 'https://api.binance.com'
        client = MongoClient("mongodb://user:pw@ip:port")
        self.db_mongo = client.Simba
        self.bian_collection = self.db_mongo.bian
        self.symbols = []
        self.quot_asset = ['BTC', 'ETH', 'USDT', 'BNB']
        
        self.headers = {
                "Content-type": "application/x-www-form-urlencoded",
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36'
                }
        
    def get_url_json(self, url):
        res = False
        while res == False:
            time.sleep(0.3)
            try:
                response  = requests.get(url, headers=self.headers)
                res = True
            except:
                res = False
        return response.json()
        
    def kline_data(self,symbol, date_type):
        request_url = self.base_url + '/api/v1/klines?symbol=' + symbol['symbol'] + '&interval=' + date_type
        print(request_url)
        res_json = self.get_url_json(request_url)
        tx_name = symbol['baseAsset'] + '_' + symbol['quoteAsset']
        kline_data = {'tx_name':tx_name, 'kline' : res_json}

        self.bian_collection.insert_one(kline_data)
        
    def all_kline_datas(self, date_type):
        print(len(self.symbols))
        for symbol in self.symbols:
            if symbol['quoteAsset'] not in self.quot_asset:
                print(symbol)
                continue
            
            self.kline_data(symbol, date_type)
        
    def symbol_data(self):
        request_url = self.base_url + '/api/v1/exchangeInfo'
        res_json = self.get_url_json(request_url)
        for symbol in res_json['symbols']:
            one_symbol = {}
            one_symbol['symbol'] = symbol['symbol']
            one_symbol['baseAsset'] = symbol['baseAsset']
            one_symbol['quoteAsset'] = symbol['quoteAsset']
            self.symbols.append(one_symbol)

        
if __name__ == "__main__":
    bi_an = Bian()
    bi_an.symbol_data()
    bi_an.all_kline_datas('1d')
    #bi_an.kline_data('LTCBTC', '1d')