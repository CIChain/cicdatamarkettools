# !/usr/bin/python
# -*- coding:utf-8 -*-

from erc20_token.erc20_data import Erc20Data
from erc20_token.get_tx_data import TxDataPro

# 日志
# access_log 用于记录每一个访问请求
# app_log 用于记录程序运行过程中，所有未被处理的异常
# gen_log 用于记录tornado自己运行过程中报的错误和警告

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
