import websocket
import time
import _thread
import sys
import os
path = os.getcwd()
sys.path.append(path)
import common_fun

class WebClient():
    def __init__(self, data_type):
        self.url = "wss://real.okex.com:10440/websocket/okexapi"
        self.base_url = 'https://www.okex.com/api/v1/'  
        self.headers = {
                "Content-type" : "application/x-www-form-urlencoded",
                }
        self.symbols = []
        self.events = []
        self.data_type = data_type
        
    def on_open(self):
        def run(n_start, n_end):
            self.ws.send(str(self.events[n_start:n_end]))
            
        for index in range(len(self.events)/100):
            if (index + 1) * 100 < len(self.events):
                _thread.start_new_thread(run, (index * 100, (index + 1) * 100))
            else:
                _thread.start_new_thread(run, (index * 100, len(self.events)))
        
        def ping(*args):
            while True:
                time.sleep(20)
                recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                print('pint time', recordDate)
                self.send("{'event':'ping'}")
        _thread.start_new_thread(run, ())
    
    def on_message(self,evt):
        print(evt)
        
    def on_error(self,evt):
        print (evt)
    
    def on_close(self,evt):
        print ('DISCONNECT')
    
    def get_all_symbols(self):
        request_url = self.base_url + 'tickers.do'
        res_json = common_fun.get_url_json(request_url, self.headers)
        for ticker in res_json['tickers']:
            if ticker['symbol'] not in self.symbols:
                self.symbols.append[ticker['symbol']]
    
    def creat_deals_event(self):    
        for symbol in self.symbols:
            web_event = {'event':'addChannel','channel':'ok_sub_spot_' + symbol + self.data_type}
            #web_event = {'event':'addChannel','channel':'ok_sub_spot_' + ticker['symbol'] +'_ticker'}
            #web_event = {'event':'addChannel','channel':'ok_sub_spot_' + ticker['symbol'] +'_depth'}
            #web_event = {'event':'addChannel','channel':'ok_sub_spot_' + ticker['symbol'] +'_kline_1min'}

            self.events.append(web_event)
                         
    def run(self):
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(self.url,
                                    on_message = self.on_message,
                                    on_error = self.on_error,
                                    on_close = self.on_close)
        
        self.ws.on_open = self.on_open
        self.ws.run_forever()

if __name__ == "__main__":
    web_client = WebClient('_deals')
    web_client.make_events_by_symbols()
    web_client.run()
