import websocket
import time
import _thread

events = []

def on_open_deals(self):
    def run(n_start, n_end):
        self.send(str(events[n_start:n_end]))
        
    for index in range(len(events)/100):
        if (index + 1) * 100 < len(events):
            _thread.start_new_thread(run, (index * 100, (index + 1) * 100))
        else:
            _thread.start_new_thread(run, (index * 100, len(events)))
    
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
    
class WebClient():
    def __init__(self):
        self.url = "wss://real.okex.com:10440/websocket/okexapi"
        self.symbols = []

    def make_events(self, symbols, event_type):
        for symbol in symbols:
            web_event = {'event':'addChannel','channel':'ok_sub_spot_' + symbol + event_type}
            events.append(web_event)

                         
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
    web_client.get_all_symbols()
    web_client.make_events('_deals')
    web_client.run()
    


