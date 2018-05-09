# !/usr/bin/python
# -*- coding:utf-8 -*-

from erc20_token.erc20_data import Erc20Data
from erc20_token.get_tx_data import TxDataPro

if __name__ == "__main__":
    erc20_pro = Erc20Data()
    tx_data = TxDataPro()
    try:
        erc20_pro.erc20_data_key()
        erc20_pro.get_erc20_data()
        erc20_pro.get_top100_hold()
    except:
        print('erc20_pro err')
        
    try:
        tx_data.get_tx_data()
    except:
        print('tx_data err')
