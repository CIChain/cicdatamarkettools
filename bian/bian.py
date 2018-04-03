import requests
from pymongo import MongoClient
import time
    
class Bian(object):
    def __init__(self): 
        self.base_url = 'https://api.binance.com/api/v1/klines?symbol='
        client = MongoClient("mongodb://lionking:Tv6pAzDp@60.205.187.223:27017/Simba?authMechanism=SCRAM-SHA-1")
        self.db_mongo = client.Simba
        self.bian_collection = self.db_mongo.bian
        
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
                print('error====================')
                res = False
        return response.json()
        
    def kline_data(self,tx_name, date_type):
        request_url = self.base_url + tx_name + '&interval=' + date_type
        res_json = self.using_requests(request_url)
        print(request_url)
        print(res_json)
        kline_data = {'kline' : res_json}

        self.bian_collection.insert_one(kline_data)
        
        
if __name__ == "__main__":
    bi_an = Bian()
    bi_an.kline_data('LTCBTC', '1d')