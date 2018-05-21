from binance.client import Client
from binance.websockets import BinanceSocketManager

#api_key = 'wEfVeIfGmq5YwKCrI8LCFycWp4M4jTMHG1HVprtfZP7jfyxeZybVuVibO5GyJ82j'
#api_secret = 'wQlqm6SFJtWphpl0ChCelgmeZLphvM3ynQqCeTOam6jYuQJFhcLfE1DEavWfnkTO'


client = Client('', '')

def process_message(msg):
    print("message type: {}".format(msg['e']))
    print(msg)
    # do something
    

def process_depth_message(msg):
    print('----depth-----')
    print(msg)

bm = BinanceSocketManager(client)
bm.start_aggtrade_socket(symbol='ETHBTC', callback = process_message)
bm.start_depth_socket(symbol='ETHBTC', callback = process_depth_message)
bm.start()


def process_m_message(msg):
    print('---------start_multiplex_socket msg----')
    print("message type: {}".format(msg['e']))
    print(msg)
conn_key = bm.start_multiplex_socket(['bnbbtc@aggTrade', 'neobtc@ticker'], process_m_message)

'''
def process_message(msg):
    print("message type: {}".format(msg['e']))
    print(msg)
    # do something
    
bm = BinanceSocketManager(client)
#conn_key = bm.start_trade_socket('ethbtc', process_message)
#conn_key1 = bm.start_symbol_ticker_socket('BNBBTC', process_message)

def process_m_message(msg):
    print(msg)
    #print("stream: {} data: {}".format(msg['stream'], msg['data']))

# pass a list of stream names
conn_key = bm.start_multiplex_socket(['bnbbtc@aggTrade', 'neobtc@ticker'], process_m_message)
bm.start()
'''
while True:
    pass

