import websocket
import time
import _thread
import sys
import os
path = os.getcwd()
sys.path.append(path)
import common_fun

web_events = []

    
class WebClient(websocket):
    def __init__(self):
        self.url = "wss://real.okex.com:10440/websocket/okexapi"
        self.base_url = 'https://www.okex.com/api/v1/'  
        self.headers = {
                "Content-type" : "application/x-www-form-urlencoded",
                }
    def on_open(self):
        '''
        def run(n_start, n_end):
            self.send(str(web_events[n_start:n_end]))
            
        for index in range(len(web_events)/100):
            if (index + 1) * 100 < len(web_events):
                _thread.start_new_thread(run, (index * 100, (index + 1) * 100))
            else:
                _thread.start_new_thread(run, (index * 100, len(web_events)))
        '''
        
        def ping(*args):
            while True:
                time.sleep(5)
                recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                print('pint time', recordDate)
                self.send("{'event':'ping'}")
        _thread.start_new_thread(ping, ())
    
    def on_message(self,evt):
        print(evt)
        
    def on_error(self,evt):
        print (evt)
    
    def on_close(self,evt):
        print ('DISCONNECT')
        
    def on_ping(self):
        self.send("{'event':'ping'}")
    
    def make_events_by_symbols(self):
        request_url = self.base_url + 'tickers.do'
        res_json = common_fun.get_url_json(request_url, self.headers)
        for ticker in res_json['tickers']:
            web_event = {'event':'addChannel','channel':'ok_sub_spot_' + ticker['symbol'] +'_deals'}
            #web_event = {'event':'addChannel','channel':'ok_sub_spot_' + ticker['symbol'] +'_ticker'}
            #web_event = {'event':'addChannel','channel':'ok_sub_spot_' + ticker['symbol'] +'_depth'}
            #web_event = {'event':'addChannel','channel':'ok_sub_spot_' + ticker['symbol'] +'_kline_1min'}

            web_events.append(web_event)
                         
    def run(self):
        websocket.enableTrace(False)
        self.ws = self.WebSocketApp(self.url,
                                    on_message = self.on_message,
                                    on_error = self.on_error,
                                    on_close = self.on_close)
        
        self.ws.on_open = self.on_open
        self.ws.run_forever()

if __name__ == "__main__":
    web_client = WebClient()
    web_client.make_events_by_symbols()
    
    web_client.enableTrace(False)
    web_client.WebSocketApp(web_client.url,
                            on_message = web_client.on_message,
                            on_error = web_client.on_error,
                            on_close = web_client.on_close)
    web_client.on_open = web_client.on_open
    web_client.run_forever()

