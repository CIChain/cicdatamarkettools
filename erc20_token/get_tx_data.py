# !usr/bin/python
# -*- coding:utf-8 -*-
from mysqldb import Mysqldb
import time
import common_fun
import config

class TxDataPro():
    def __init__(self):
        self.db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)
        
    def get_tx_data(self):
        sel_str = "SELECT id, fxh_address from token_base WHERE fxh_address <> ''"
        db_res = self.db.select(sel_str)
        timestamps = int(time.time())
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('get_tx_data', recordDate)
        for token_bace in db_res:
            id_timestamps = str(token_bace[0]) + '_' + str(timestamps)
            res_html = common_fun.get_url_html(token_bace[1])
            price_list = res_html.xpath('//*[@id="baseInfo"]//*[@class="coinprice"]//text()')
            
            price_cny = price_list[0].split('￥')[1].replace(',', '').replace('?','0')
            gains_24h = price_list[1].replace('%', '')
            
            price_usdt = res_html.xpath('//*[@id="baseInfo"]//*[@class="sub"]//text()')[0].replace('≈', '')
            price_btc = res_html.xpath('//*[@id="baseInfo"]//*[@class="sub"]//text()')[2].replace('≈', '')
            price_usdt = price_usdt.split('$')[1].replace(',', '')
            price_btc = price_btc.split('BTC')[0].replace(',', '')
            
            flow_num = res_html.xpath('//*[@id="baseInfo"]/div[1]/div[3]/div[2]/text()')[0].split(' ')[0].replace(',', '')
            tx_amount_24h = res_html.xpath('//*[@id="baseInfo"]/div[1]/div[4]/div[2]/text()')[0].replace(',', '')
            if tx_amount_24h == '?' or tx_amount_24h == '？':
                pass
            else:
                tx_amount_24h = tx_amount_24h.split('¥')[1].replace(',', '')
            issued_num = res_html.xpath('//*[@id="baseInfo"]/div[1]/div[3]/div[4]/text()')[0].split(' ')[0].replace(',', '')
            change_rate_24h = res_html.xpath('//*[@id="baseInfo"]/div[1]/div[4]/div[5]/div/span/text()')[0].replace('%', '')
            
            insert_str = "INSERT INTO erc20_tx_data (id_timestamps, issued_num, flow_num, tx_amount_24h, change_rate_24h, gains_24h, price_cny, price_usdt, price_btc, created_at)"
            insert_str += "VALUES ('" + str(id_timestamps) + "','" + issued_num + "','" + str(flow_num) + "','"  + str(
                    tx_amount_24h) + "','"  + str(change_rate_24h) + "','"  + str(gains_24h) + "','"  + str(
                    price_cny) + "','"  + str(price_usdt) + "','"  + str(price_btc) + "','" + str(recordDate) + "')"
                
            try:
                self.db.insert(insert_str)
            except Exception as e:
                print(insert_str)
                print('insert err, token = ', token_bace[0])
                
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('get_tx_data', recordDate)

if __name__ == "__main__":
    tx_data = TxDataPro()
    recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print('start', recordDate)
    
    tx_data.get_tx_data()
    
    recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print('end', recordDate)