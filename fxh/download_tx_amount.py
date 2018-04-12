import requests
from lxml import etree
from pymongo import MongoClient
import time
import config

dic_url = config.DicUrl

client = MongoClient(config.MongodbStrTest)
db_mongo = client.Simba
ex_amount_collection = db_mongo.fxh_ex_amount

source_data = {}
source_data['time_data'] = int(time.time())

recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print('start_time = ', recordDate)
for key in dic_url.keys():
    source_data[key] = []
    response  = requests.get(dic_url[key])
    
    html = etree.HTML(response .text)
    for xpath_str in html.xpath('//*[@class="table noBg"]/tbody/tr'): 
        one_data = {}
        tx_from_to = xpath_str.xpath('td[3]/text()')
        if tx_from_to == []:
            tx_from_to = xpath_str.xpath('td[3]/a/text()')
        
        token_name = tx_from_to[0].strip().split('/')[0]
        one_data['token_name'] = token_name
        
        exchange_amount_24h = xpath_str.xpath('td[6]/@data-cny')[0]
        one_data['exchange_amount_24h'] = exchange_amount_24h
                
        exchange_price = xpath_str.xpath('td[4]/@data-cny')[0]
        one_data['exchange_price'] = exchange_price
        
        source_data[key].append(one_data)

recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
ex_amount_collection.insert_one(source_data)
print('end_time = ',recordDate)                