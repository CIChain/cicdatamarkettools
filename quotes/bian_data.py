import config
from mysqldb import Mysqldb
import common_fun
import time
 
class Bian_data(object):
    def __init__(self): 
        self.db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)
        self.base_url = 'https://api.binance.com'
        self.quot_asset = ['BTC', 'ETH', 'USDT', 'BNB']
        self.headers = {
                "Content-type": "application/x-www-form-urlencoded",
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36'
                }
        
    def tickers(self):
        request_url = self.base_url + '/api/v1/ticker/24hr'
        res_json = common_fun.get_url_json(request_url, self.headers)
        
        insert_list = []
        data_time = int(time.time())
        for currencys in res_json:
            currency_pair = ''
            for quot in self.quot_asset:
                if currencys['symbol'].endswith(quot):
                    currency_pair = currencys['symbol'].replace(quot, '_' + quot)
                    
            high = currencys['highPrice']
            low = currencys['lowPrice']
            last = currencys['lastPrice']
            sell = currencys['askPrice']
            buy = currencys['bidPrice']
            vol = currencys['volume']
            
            insert_str = "INSERT INTO bian_tickers (date_time, currency_pair, high, low, last, sell, buy, vol)"
            insert_str += "VALUES (" + str(data_time) + ",'" + currency_pair + "'," + str(high) +  "," + str(
                    low)  + "," + str(last) + "," + str(sell) + "," + str(buy) + "," + str(vol) + ");"
            insert_list.append(insert_str)
            
        
        try:
            self.db.execute_list(insert_list)
        except:
            print(insert_str)
            print('insert_list tickers err  data_time = ', data_time)
            
        
if __name__ == "__main__":
    bian = Bian_data()
    bian.tickers()
    