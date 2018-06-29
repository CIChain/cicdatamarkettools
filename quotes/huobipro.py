import common_fun
import config
import time
from mysqldb import Mysqldb
    
class Huobi(object):
    def __init__(self): 
        self.db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)
        self.base_url = 'https://api.huobipro.com'
        self.headers = {
            "Content-type": "application/x-www-form-urlencoded",
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36'
        }
        self.symbols = []
    
    def kline_data(self, symbol, date_type):
        tx_name = symbol['base-currency'] + '_' + symbol['quote-currency']
        request_url = self.base_url + '/market/history/kline?period=' + date_type +'&size=200&symbol='+ symbol['base-currency'] + symbol['quote-currency']
        print(request_url)
        res_json = common_fun.get_url_json(request_url)
        
        res_json['tx_name'] = tx_name
    
    def all_kline_datas(self, date_type):
        print(len(self.symbols))
        for symbol in self.symbols:
            self.kline_data(symbol, date_type)

    def ticker(self, symbol):
        tx_name = symbol['base-currency'] + '_' + symbol['quote-currency']
        request_url = self.base_url + '/market/detail/merged?symbol=' + symbol['base-currency'] + symbol['quote-currency']
        res_json = common_fun.get_url_json(request_url)
        ticker = res_json['tick']
        
        insert_str = "INSERT INTO huobi_tickers_copy (date_time, currency_pair, high, low, last, sell, buy, vol)"
        insert_str += "VALUES (" + str(res_json['ts']/1000) + ",'" + str(tx_name) + "'," + str(
            ticker['high']) +  "," + str(ticker['low'])  + "," + '-1' + "," + str(
            ticker['ask'][0]) + "," + str(ticker['bid'][0]) + "," + str(ticker['amount']) + ");"
        
        try:
            self.db.insert(insert_str)
        except:
            print(insert_str)
            print('insert_list tickers err  tx_name = ', tx_name)
        
    def tickers(self):
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('start', recordDate)
        for symbol in self.symbols:
            print(symbol)
            self.ticker(symbol)
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('end', recordDate)
        
    def symbol_data(self):
        request_url = self.base_url + '/v1/common/symbols'
        res_json = common_fun.get_url_json(request_url)
        
        for symbol in res_json['data']:
            one_symbol = {}
            one_symbol['base-currency'] = symbol['base-currency']
            one_symbol['quote-currency'] = symbol['quote-currency']
            self.symbols.append(one_symbol)

        
if __name__ == "__main__":
    huobi = Huobi()
    huobi.symbol_data()
    #symbol = {'base-currency': 'btc', 'quote-currency': 'usdt'}
    huobi.tickers()
    #huobi.all_kline_datas('1day')
    #huobi.kline_data()