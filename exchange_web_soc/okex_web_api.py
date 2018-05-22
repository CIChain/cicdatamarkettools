import websocket
import time
import thread

def on_open(self):
    self.send("{'event':'addChannel','channel':'ok_sub_spot_eth_btc_deals'}")
    def run(*args):
        while True:
            time.sleep(20)
            self.send("{'event':'ping'}")
    thread.start_new_thread(run, ())

def on_message(self,evt):
    print(evt)
    
def on_error(self,evt):
    print('==================')
    print (evt)

def on_close(self,evt):
    print ('DISCONNECT')
    
def on_ping(self):
    self.send("{'event':'ping'}")

if __name__ == "__main__":
    url = "wss://real.okex.com:10440/websocket/okexapi"      #if okcoin.cn  change url wss://real.okcoin.cn:10440/websocket/okcoinapi
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(url,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
