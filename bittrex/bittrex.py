import requests
from Crypto.Cipher import AES
import getpass
import json
from pymongo import MongoClient
import time

def encrypt(api_key, api_secret, export=True, export_fn='secrets.json'):
    cipher = AES.new(getpass.getpass(
        'Input encryption password (string will not show)'))
    api_key_n = cipher.encrypt(api_key)
    api_secret_n = cipher.encrypt(api_secret)
    api = {'key': str(api_key_n), 'secret': str(api_secret_n)}
    if export:
        with open(export_fn, 'w') as outfile:
            json.dump(api, outfile)
    return api

def using_requests(request_url):
    return requests.get(
        request_url,
        timeout=10
    ).json()
    
    
class Bittrex(object):
    def __init__(self):
        self.base_url = 'https://bittrex.com/api/v1.1/public/'
        client = MongoClient("mongodb://lionking:Tv6pAzDp@60.205.187.223:27017/Simba?authMechanism=SCRAM-SHA-1")
        self.db_mongo = client.Simba
        self.bitt_collection = self.db_mongo.bittrex
        
    def getmarketsummaries(self):
        request_url = self.base_url + 'getmarketsummaries'
        res_json = using_requests(request_url)
        res_json['updata_time'] = int(time.time())

        self.bitt_collection.insert_one(res_json)
        
        
if __name__ == "__main__":
    bitt = Bittrex()
    bitt.getmarketsummaries()