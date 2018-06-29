import common_fun
import csv
import time

class KlineData():
    def __init__(self):
        self.head_line = ['open_time', 'open_price', 'high', 'low', 'close_price', 'vol']
        return
    
    def write_csv_file(self, file_name, write_data, head_line = None):
        csvFile = open(file_name, 'w', newline='', encoding = 'utf8')
        writer = csv.writer(csvFile)
        if head_line != None:
            writer.writerow(head_line)
        for line in write_data:
            writer.writerow(line)
            
        csvFile.close()
        
    def zb_Kline_data(self, symbol, date_type):
        request_url = 'http://api.zb.com/data/v1/kline?market=' + symbol + '&type=' + date_type
        res_json = common_fun.get_url_json(request_url)
        file_name = 'zb_' + symbol + '.csv'
        
        write_data = []
        for data in res_json['data']:
            line = []
            recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(data[0]/1000))
            line.append(recordDate)
            line.append(data[1])
            line.append(data[2])
            line.append(data[3])
            line.append(data[4])
            line.append(data[5])
            
            write_data.append(line)
            
        self.write_csv_file(file_name, write_data, self.head_line)
        return
    
    def huobi_data(self, symbol, date_type):
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36'
        }
        request_url = 'https://api.huobipro.com/market/history/kline?period=' + date_type +'&size=200&symbol='+ symbol
        res_json = common_fun.get_url_json(request_url, headers)
        file_name = 'huobi_' + symbol + '.csv'
        
        write_data = []
        for data in res_json['data']:
            line = []
            recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(data['id']))
            line.append(recordDate)
            line.append(data['open'])
            line.append(data['high'])
            line.append(data['low'])
            line.append(data['close'])
            line.append(data['amount'])
            
            write_data.append(line)
            
        self.write_csv_file(file_name, write_data, self.head_line)
        return
    
    def bian_data(self, symbol, date_type):
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36'
        }
        
        request_url = 'https://api.binance.com/api/v1/klines?symbol=' + symbol + '&interval=' + date_type
        res_json = common_fun.get_url_json(request_url, headers)
        file_name = 'bian_' + symbol + '.csv'

        write_data = []
        for data in res_json:
            line = []
            recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(data[0]/1000))
            line.append(recordDate)
            line.append(data[1])
            line.append(data[2])
            line.append(data[3])
            line.append(data[4])
            line.append(data[5])
            
            write_data.append(line)
            
        self.write_csv_file(file_name, write_data, self.head_line)
        return
    
    def kraken_data(self, symbol, date_type):
        request_url = 'https://api.kraken.com/0/public/OHLC?pair=' + symbol + '&interval=' + date_type
        res_json = common_fun.get_url_json(request_url)
        file_name = 'kraken_' + symbol + '.csv'
        
        write_data = []

        for data in res_json['result'][symbol]:
            line = []
            recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(data[0]))
            line.append(recordDate)
            line.append(data[1])
            line.append(data[2])
            line.append(data[3])
            line.append(data[4])
            line.append(data[6])
            
            write_data.append(line)
            
        self.write_csv_file(file_name, write_data, self.head_line)
        return
    
    def okex_data(self, symbol, date_type):
        request_url = 'https://www.okex.com/api/v1/kline.do?symbol=' + symbol + '&type=' + date_type
        res_json = common_fun.get_url_json(request_url)
        file_name = 'okex_' + symbol + '_' + date_type + '.csv'
        
        write_data = []

        for data in res_json:
            line = []
            #recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(data[0]/1000))
            line.append(data[0])
            line.append(data[1])
            line.append(data[2])
            line.append(data[3])
            line.append(data[4])
            line.append(data[5])
            
            write_data.append(line)
            
        self.write_csv_file(file_name, write_data, self.head_line)
        return
    
if __name__ == "__main__":
    kline_data = KlineData()
    kline_data.okex_data('eth_usdt', '1day')
    #kline_data.bian_data('EOSUSDT', '1d')
    
    #kline_data.zb_Kline_data('eos_usdt', '1day')
    #kline_data.zb_Kline_data('zb_qc', '1day')
    #kline_data.zb_Kline_data('tv_qc', '1day')
    #kline_data.zb_Kline_data('ae_qc', '1day')
    
    #kline_data.huobi_data('eosusdt','1day')
    #kline_data.huobi_data('tnbeth','1day')
    #kline_data.huobi_data('socusdt','1day')
    
    #kline_data.kraken_data('EOSUSD', '1440')