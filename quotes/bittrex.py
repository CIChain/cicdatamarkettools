import time
import common_fun
import config
from mysqldb import Mysqldb
    
class Bittrex(object):
    def __init__(self):
        self.base_url = 'https://bittrex.com/api/v1.1/public/'
        self.db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)
        
    def getmarketsummaries(self):
        request_url = self.base_url + 'getmarketsummaries'
        res_json = common_fun.get_url_json(request_url)
        res_json['updata_time'] = int(time.time())
        insert_list = []
        data_time = int(time.time())
        
        for ticker in res_json['result']:
            market_name = ticker['MarketName'].split('-')
            token_name = market_name[1] + '_' + market_name[0]
            insert_str = "INSERT INTO bittrex_tickers (date_time, currency_pair, high, low, last, sell, buy, vol)"
            insert_str += "VALUES (" + str(data_time) + ",'" + token_name + "'," + str(
                    ticker['High']) +  "," + str(ticker['Low'])  + "," + str(ticker['Last']) + "," + str(
                    ticker['Ask']) + "," + str(ticker['Bid']) + "," + str(ticker['Volume']) + ");"
            insert_list.append(insert_str)
        
        try:
            self.db.execute_list(insert_list)
        except:
            print(insert_str)
            print('insert_list tickers err  data_time = ', data_time)
        
        
if __name__ == "__main__":
    bitt = Bittrex()
    bitt.getmarketsummaries()