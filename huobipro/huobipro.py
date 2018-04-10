import requests
from pymongo import MongoClient
import time
    
class Huobi(object):
    def __init__(self): 
        self.base_url = 'https://api.huobipro.com'
        client = MongoClient("mongodb://user:pw@ip:port")
        self.db_mongo = client.Simba
        self.huobi_collection = self.db_mongo.huobi
        self.headers = {
            "Content-type": "application/x-www-form-urlencoded",
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36'
        }
        self.symbols = []
        
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
    
    def kline_data(self, symbol, date_type):
        tx_name = symbol['base-currency'] + '_' + symbol['quote-currency']
        request_url = self.base_url + '/market/history/kline?period=' + date_type +'&size=200&symbol='+ symbol['base-currency'] + symbol['quote-currency']
        print(request_url)
        res_json = self.get_url_json(request_url)
        
        res_json['tx_name'] = tx_name

        self.huobi_collection.insert_one(res_json)
    
    def all_kline_datas(self, date_type):
        print(len(self.symbols))
        for symbol in self.symbols:
            self.kline_data(symbol, date_type)
        
    def symbol_data(self):
        request_url = self.base_url + '/v1/common/symbols'
        res_json = self.get_url_json(request_url)
        
        for symbol in res_json['data']:
            one_symbol = {}
            one_symbol['base-currency'] = symbol['base-currency']
            one_symbol['quote-currency'] = symbol['quote-currency']
            self.symbols.append(one_symbol)
        
if __name__ == "__main__":
    huobi = Huobi()
    huobi.symbol_data()
    huobi.all_kline_datas('1day')
    #huobi.kline_data()