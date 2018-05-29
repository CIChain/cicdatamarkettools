import websocket
import time
import _thread
import json
import os
import sys
path = os.getcwd()
sys.path.append(path)
from mysqldb import Mysqldb
import config

events = []
symbols = []
db = Mysqldb(config.MySqlHostTicker, config.MySqlUserTicker, config.MySqlPasswdTicker, config.MySqlDbTicker,
             config.MySqlPortTicker)

current_ticker = {}
ticker_sql_list = []
updata_time = int(time.time())

def on_open(self):
    def ping(*args):
        while True:
            recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print('ping time', recordDate)
            self.send("{'event':'ping'}")
            time.sleep(15)
    _thread.start_new_thread(ping, ())
    
    def run(n_start, n_end, sleep_time):
        time.sleep(sleep_time)
        self.send(str(events[n_start:n_end]))
    for index in range(int(len(events)/100) + 1):
        if (index + 1) * 100 < len(events):
            _thread.start_new_thread(run, (index * 100, (index + 1) * 100, index))
        else:
            _thread.start_new_thread(run, (index * 100, len(events), index))
            
def insert_sql_data(insert_list):
    try:
        db.insert_list(insert_list)
    except:
        print(insert_list)

def on_message(self,evt):
    res_data = json.loads(evt)
    try:
        for one_res in res_data:
            channel_list = one_res['channel'].split('_')
            symbol = channel_list[3] + '_' + channel_list[4]
            if one_res['channel'].endswith('_ticker'):
                if symbol in symbols:
                    data = one_res['data']
                    insert_str = "INSERT INTO okex_ticker (currency, vol_24h, high_24h, low_24h, last, change_price, timestamp)";
                    insert_str += "VALUES ('" + symbol + "','" + str(data['vol']) + "','" + str(data['high']) + "','" + str(
                            data['low']) + "','" + str(data['last']) + "','" + str(data['change']) + "','" + str(data['timestamp']) + "')"
                    
                    ticker_sql_list.append(insert_str)
        
        curr_time = int(time.time())
        if curr_time - updata_time > 60:
            insert_list = ticker_sql_list[:]
            ticker_sql_list.clear()
            insert_sql_data(insert_list)
    except:
        pass
    
def on_error(self,evt):
    print (evt)

def on_close(self):
    print ('DISCONNECT')
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(self.url, on_message = on_message, on_error = on_error, on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
    
class WebClient():
    def __init__(self):
        self.url = "wss://real.okex.com:10440/websocket/okexapi"

    def make_events(self, event_type):
        #event_type: _ticker, _depth, _kline_1min, _deals
        for symbol in symbols:
            web_event = {'event':'addChannel','channel':'ok_sub_spot_' + symbol + event_type}
            events.append(web_event)
                         
    def run(self):
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(self.url,
                                    on_message = on_message,
                                    on_error = on_error,
                                    on_close = on_close)
        
        self.ws.on_open = on_open
        self.ws.run_forever()

if __name__ == "__main__":
    web_client = WebClient()
    web_client.get_all_symbols()
    web_client.make_events('_deals')
    web_client.run()
    


