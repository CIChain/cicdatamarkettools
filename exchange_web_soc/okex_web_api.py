import websocket
import time
import _thread

events = []

def on_open(self):
    def ping(*args):
        while True:
            time.sleep(5)
            recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print('pint time', recordDate)
            self.send("{'event':'ping'}")
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
    print(evt[0]['channel'])
    
def on_error(self,evt):
    print (evt)

def on_close(self,evt):
    print ('DISCONNECT')
    
class WebClient():
    def __init__(self):
        self.url = "wss://real.okex.com:10440/websocket/okexapi"
        self.symbols = []

    def make_events(self, symbols):
        for symbol in symbols:
            web_event = {'event':'addChannel','channel':'ok_sub_spot_' + symbol + '_deals'}
            events.append(web_event)
            web_event = {'event':'addChannel','channel':'ok_sub_spot_' + symbol + '_ticker'}
            events.append(web_event)
            web_event = {'event':'addChannel','channel':'ok_sub_spot_' + symbol + '_depth'}
            events.append(web_event)
            web_event = {'event':'addChannel','channel':'ok_sub_spot_' + symbol + '_kline_1min'}
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
    


