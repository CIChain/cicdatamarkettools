import requests
from pymongo import MongoClient
from lxml import etree
import time
import config
 
class Okex(object):
    def __init__(self): 
        self.base_url = 'https://www.okex.com/api/v1/'    
        client = MongoClient(config.MongodbStrTest)
        self.db_mongo = client.Simba
        self.okex_collection = self.db_mongo.okex
        self.symbols = []
        self.headers = {
                "Content-type" : "application/x-www-form-urlencoded",
                }
        
    def ticker_do(self,tx_name, api_type):
        request_url = self.base_url + 'ticker.do?symbol=' + tx_name
        print(request_url)
        res_json = self.get_url_json(request_url)
        print(res_json)

        self.okex_collection.insert_one(res_json)
        
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
        
    def kline_do(self,tx_name, date_type):
        request_url = self.base_url + 'kline.do?symbol=' + tx_name + '&type=' + date_type
        print(request_url)
        res_json = self.get_url_json(request_url)
        
        
        kline_data = {'tx_name': tx_name ,'kline' : res_json}

        self.okex_collection.insert_one(kline_data)
    
    def get_all_data(self, time_type):
        for symbol in self.symbols:
            if self.okex_collection.find_one({'tx_name': symbol}) == None:
                #print('==========', symbol)
                self.kline_do(symbol, time_type)
    
    def get_url_html(self, url):
        time.sleep(0.3)
        response  = requests.get(url, headers=self.headers)
        html = etree.HTML(response .text)
        return html
    
    def get_symbol_byfxh(self):
        url = config.OkexDataUrl
        html = self.get_url_html(url)
        for xpath_str in html.xpath('//*[@class="table noBg"]/tbody/tr'):
            cur_name_text = xpath_str.xpath('td[3]/a/text()')
            if len(cur_name_text) == 0:
                cur_name_text = xpath_str.xpath('td[3]/text()')
            
            symbol = cur_name_text[0].replace('/', '_').lower()
            self.symbols.append(symbol)
        print(self.symbols)
        
    def get_symbol_by_configs(self):
        with open('symbols.config', encoding = 'utf8') as file_object:
            for line in file_object:
                if line.find('币对') > -1:
                    continue
                if line.startswith('|:---'):
                    continue
                
                self.symbols.append(line.split('|')[2])
            #contents = file_object.readlines
        
if __name__ == "__main__":
    okex = Okex()
    #okex.ticker_do('cic_usdt')
    
    okex.get_symbol_by_configs()
    okex.get_all_data('1day')