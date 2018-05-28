import websocket
import time
import _thread
import json

events = []
symbols = []

def on_open(self):
    def ping(*args):
        while True:
            recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print('ping time', recordDate)
            self.send("{'event':'ping'}")
            time.sleep(5)
    _thread.start_new_thread(ping, ())
    
    def run(n_start, n_end, sleep_time):
        time.sleep(sleep_time)
        self.send(str(events[n_start:n_end]))
    for index in range(int(len(events)/100)):
        if (index + 1) * 100 < len(events):
            _thread.start_new_thread(run, (index * 100, (index + 1) * 100, index))
        else:
            _thread.start_new_thread(run, (index * 100, len(events), index))

def on_message(self,evt):
    print(evt)
    res_data = json.loads(evt)
    try:
        for one_res in res_data:
            channel_list = one_res['channel'].split('_')
            symbol = channel_list[3] + '_' + channel_list[4]
            if one_res['channel'].endswith('_ticker'):
                print(symbol)
                if symbol in symbols:
                    print(one_res)
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
        self.symbols = []

    def make_ticker_events(self):
        for symbol in symbols:
            web_event = {'event':'addChannel','channel':'ok_sub_spot_' + symbol + '_ticker'}
            events.append(web_event)
            
    def make_depth_events(self):
        for symbol in symbols:
            web_event = {'event':'addChannel','channel':'ok_sub_spot_' + symbol + '_depth'}
            events.append(web_event)
    
    def make_kline_events(self):
        for symbol in symbols:
            web_event = {'event':'addChannel','channel':'ok_sub_spot_' + symbol + '_kline_1min'}
            events.append(web_event)
            
    def make_deals_events(self):
        for symbol in symbols:
            web_event = {'event':'addChannel','channel':'ok_sub_spot_' + symbol + '_deals'}
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
    


