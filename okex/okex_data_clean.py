from pymongo import MongoClient
import time
import csv
import requests
from lxml import etree
import pandas as pd
import config

class Data_clean():
    def __init__(self): 
        client = MongoClient(config.MongodbStr)
        self.db_mongo = client.Simba
        self.okex_collection = self.db_mongo.okex
        self.merge_res = {}
        
    def del_repeat_data(self, source_data):
        new_list = []
        for data in source_data:
            if data not in new_list:
                new_list.append(data)
        return new_list
    
    def get_avg_value(self, data):
        #去掉最高，最低取平均
        sum_data = 0
        data = [float(item) for item in data]
        max_data = data[0]
        min_data = data[0]
        for one_data in data:
            if max_data < one_data:
                max_data = one_data
            if min_data > one_data:
                min_data = one_data
            sum_data += one_data
        data.remove(max_data)
        data.remove(min_data)
        
        sum_data = 0
        for one_data in data:
            sum_data += one_data
        
        return sum_data/len(data)
        
        
    def data_merge(self):
        writer_csvFile = open('tmp_data.csv', 'w', newline='', encoding='utf-8')
        writer = csv.writer(writer_csvFile)
        for one_data in self.okex_collection.find({}):
            one_line = []
            merge_key = one_data['tx_name'].split('_')[0].lower()
            one_line.append(merge_key)
            
            del_repeat_kline_data = self.del_repeat_data(one_data['kline'])
            sel_vol = []
            for index in range(2, 17):
                if index > len(del_repeat_kline_data):
                    break
                sel_vol.append(del_repeat_kline_data[0-index][-1])
                
            sel_count = self.get_avg_value(sel_vol)
            
            one_line.append(sel_count)
            if merge_key not in self.merge_res.keys():
                self.merge_res[merge_key] = one_line
            else:
                self.merge_res[merge_key][1] += sel_count
        
        for key in self.merge_res.keys():
            writer.writerow(self.merge_res[key])
        writer_csvFile.close()
        print('data_merge end')
        
    def get_url_html(self, url):
        res = False
        while res == False:
            time.sleep(0.3)
            try:
                response  = requests.get(url)
                html = etree.HTML(response .text)
                res = True
            except:
                res = False
        return html

    def get_price_data(self):
        detail_list = []
        html = self.get_url_html(config.OkexDataUrl)
        
        writer_csvFile = open('tmp_2_data.csv', 'w', newline='', encoding='utf-8')
        writer = csv.writer(writer_csvFile)
        head_line = ['en_name', 'cn_name', 'token_name', 'price_cny', 'price_usdt', 'tx_amount', 'change_rate_24h', 'total_value', 'logo']
        writer.writerow(head_line)

        for xpath_str in html.xpath('//*[@class="table noBg"]/tbody/tr'):
            one_line = []
            cur_name_text = xpath_str.xpath('td[3]/a/text()')
            if len(cur_name_text) == 0:
                cur_name_text = xpath_str.xpath('td[3]/text()')

            token_name = cur_name_text[0].strip().split('/')[0].lower()
            
            detail_url = config.DataBaseUrl + xpath_str.xpath('td[2]/a/@href')[0]
            if detail_url not in detail_list:
                detail_list.append(detail_url)
                detail_html = self.get_url_html(detail_url)
                
                logo = detail_html.xpath('//*[@id="baseInfo"]/div[1]/div[1]/h1/img/@src')[0]
    
                name = detail_html.xpath('//*[@id="baseInfo"]/div[1]/div[1]/h1/text()')
                
                en_name = name[1].strip()
                cn_name = name[2].strip()
                            
                price_list = detail_html.xpath('//*[@id="baseInfo"]//*[@class="coinprice"]//text()')
                
                price_cny = price_list[0].split('￥')[1].replace(',', '')
                
                price_usdt = detail_html.xpath('//*[@id="baseInfo"]//*[@class="sub"]//text()')[0].replace('≈', '')
                price_btc = detail_html.xpath('//*[@id="baseInfo"]//*[@class="sub"]//text()')[2].replace('≈', '')
                
                price_usdt = price_usdt.split('$')[1].replace(',', '')
                price_btc = price_btc.split('BTC')[0].replace(',', '')
                
                issued_num = detail_html.xpath('//*[@id="baseInfo"]/div[1]/div[3]/div[4]/text()')[0].split(' ')[0]
                
                change_rate_24h = detail_html.xpath('//*[@id="baseInfo"]/div[1]/div[4]/div[5]/div/span/text()')[0]
                change_rate_24h = change_rate_24h.replace('%', '')
                if change_rate_24h == '?':
                    change_rate_24h = 1

                if issued_num == '?':
                    issued_num = '20000000'
                total_value = float(issued_num.replace(',', '')) * float(price_cny)
                
                tx_amount = self.merge_res[token_name][1] * float(price_cny)
                one_line.extend([en_name, cn_name, token_name, price_cny, price_usdt, tx_amount, change_rate_24h, total_value, logo])
                writer.writerow(one_line)
            
        print('get_price_data end')
    
    def make_risk_data(self):
        file_name = 'tmp_2_data.csv'
        df = pd.read_csv(file_name, sep = ',')
                        
        def huanshou(number):
            
            if number <= 3:
                return 1
            elif number > 3 and number <= 7:
                return 1.02
            elif number > 7 and number <= 10:
                return 1.05
            elif number > 10 and number <= 15:
                return 1.08
            elif number > 15:
                return 1.1
            
        df['new_amount'] = df['tx_amount'] * df['change_rate_24h'].map(huanshou)
        
        btc_shizhi = df.loc[df['token_name'] == 'btc']['total_value']
        print(btc_shizhi.values[0])
        print('type=====' ,type(btc_shizhi.values[0]))
        def shizhi(shizhi):
            return shizhi/btc_shizhi.values[0]
        
        df['shizhi_amount'] = df['new_amount'] * df['total_value'].map(shizhi)
        percent_df = df.describe(percentiles = [0.1,0.3,0.7,0.9])
        print(percent_df)
        def number_to_star(number):
            if number <= percent_df.loc['10%', 'shizhi_amount']:
                return 5
            elif number > percent_df.loc['10%', 'shizhi_amount'] and number <= percent_df.loc['30%', 'shizhi_amount']:
                return 4
            elif number > percent_df.loc['30%', 'shizhi_amount'] and number <= percent_df.loc['70%', 'shizhi_amount']:
                return 3
            elif number > percent_df.loc['70%', 'shizhi_amount'] and number <= percent_df.loc['90%', 'shizhi_amount']:
                return 2
            elif number > percent_df.loc['90%', 'shizhi_amount']:
                return 1
            
        df['star'] = df['shizhi_amount'].map(number_to_star)
        
        if df.loc[df['token_name'] == 'cic']['star'].values[0] > 2:
            df.loc[df['token_name'] == 'cic', ['star']] = 2

        if df.loc[df['token_name'] == 'okb']['star'].values[0] > 1:
            df.loc[df['token_name'] == 'okb', ['star']] = 1
                  
        save_name = 'okex_sz_final.csv'
        df.to_csv(save_name, index = False, encoding = 'utf8')

if __name__ == "__main__":
    clean = Data_clean()
    clean.data_merge()
    clean.get_price_data()
    clean.make_risk_data()