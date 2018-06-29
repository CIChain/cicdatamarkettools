import config
from mysqldb import Mysqldb
import common_fun
 
class Okex_data(object):
    def __init__(self): 
        self.db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)
        self.base_url = 'https://www.okex.com/api/v1/'  
        self.headers = {
                "Content-type" : "application/x-www-form-urlencoded",
                }
        
    def tickers(self):
        request_url = self.base_url + 'tickers.do'
        res_json = common_fun.get_url_json(request_url, self.headers)
        
        insert_list = []
        data_time = res_json['date']
        for ticker in res_json['tickers']:
            insert_str = "INSERT INTO okex_tickers (date_time, currency_pair, high, low, last, sell, buy, vol)"
            insert_str += "VALUES (" + str(data_time) + ",'" + str(ticker['symbol']) + "'," + str(
                    ticker['high']) +  "," + str(ticker['low'])  + "," + str(ticker['last']) + "," + str(
                    ticker['sell']) + "," + str(ticker['buy']) + "," + str(ticker['vol']) + ");"
            insert_list.append(insert_str)
        
        try:
            self.db.execute_list(insert_list)
        except:
            print(insert_str)
            print('insert_list tickers err  data_time = ', data_time)
        
if __name__ == "__main__":
    okex = Okex_data()
    okex.tickers()
    
        