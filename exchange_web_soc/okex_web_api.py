import websocket
import sys
import zlib


def on_open(self):
    self.send("{'event':'addChannel','channel':'ok_sub_futureusd_btc_ticker_this_week'}")

def on_message(self,evt):
    print('------------------')
    print(evt)
    data = inflate(evt) #data decompress
    print (data)
    
def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated

def on_error(self,evt):
    print('==================')
    print (evt)

def on_close(self,evt):
    print ('DISCONNECT')

if __name__ == "__main__":
    url = "wss://real.okex.com:10440/websocket/okexapi"      #if okcoin.cn  change url wss://real.okcoin.cn:10440/websocket/okcoinapi

    websocket.enableTrace(False)
    if len(sys.argv) < 2:
        host = url
    else:
        host = sys.argv[1]
    ws = websocket.WebSocketApp(host,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
