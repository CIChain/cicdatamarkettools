import websocket
import time
import _thread
import sys
import os
path = os.getcwd()
sys.path.append(path)
import common_fun

web_events = []
def on_open(self):
    self.send(str(web_events[0:100]))
    
    def run(*args):
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
    
def on_ping(self):
    self.send("{'event':'ping'}")
    
class WebClient():
    def __init__(self):
        self.url = "wss://real.okex.com:10440/websocket/okexapi"
        self.base_url = 'https://www.okex.com/api/v1/'  
        self.headers = {
                "Content-type" : "application/x-www-form-urlencoded",
                }
    
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
        self.ws = websocket.WebSocketApp(self.url,
                                    on_message = on_message,
                                    on_error = on_error,
                                    on_close = on_close)
        
        self.ws.on_open = on_open
        self.ws.run_forever()

if __name__ == "__main__":
    web_client = WebClient()
    web_client.make_events_by_symbols()
    web_client.run()
