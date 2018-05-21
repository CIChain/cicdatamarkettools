import config
from pymongo import MongoClient
import csv
import time
import pandas as pd
import updata_mysql
import math

class MongoDataProcess():
    def __init__(self):
        self.client = MongoClient(config.MongodbStr)
        db_mongo = self.client.Simba
        self.fxh_data = db_mongo.fxh_data
        self.exchange = config.DicUrl.keys()
        
    def write_csv_file(self, file_name, write_data, head_line = None):
        csvFile = open(file_name, 'w', newline='', encoding = 'utf8')
        writer = csv.writer(csvFile)
        if head_line != None:
            writer.writerow(head_line)
        for line in write_data:
            writer.writerow(line)
            
        csvFile.close()
        
    def get_mongo_data(self):
        start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('start_time = ', start_time)
        time_data = int(time.time())
        write_data = {'OKEX':[], '火币':[], '币安':[], 'Bittrex':[]}
        head_line = ['token_name', 'cn_name', 'en_name', 'price_cny', 'price_usdt', 'tx_amount_24h',
                     'change_rate_24h', 'total_value', 'logo', 'time_data', 'exchange_amount_24h',
                     'exchange_price','gains_24h']
        
        befo_data = time_data - 604800
        for one_data in self.fxh_data.find({"time_data":{'$gt':befo_data,'$lt':time_data}}):
            for key in one_data.keys():
                if key in self.exchange:
                    one_write_data = {}
                    for data in one_data[key]:
                        one_line = []
                        one_line.append(data['token_name'])
                        one_line.append(data['cn_name'])
                        one_line.append(data['en_name'])
                        one_line.append(data['price_cny'])
                        one_line.append(data['price_usdt'])
                        one_line.append(data['tx_amount_24h'])
                        one_line.append(data['change_rate_24h'].replace('?', '0'))
                        one_line.append(data['total_value'])
                        one_line.append(data['logo'])
                        one_line.append(one_data['time_data'])
                        one_line.append(float(data['exchange_amount_24h'].replace('?', '0')))
                        one_line.append(float(data['exchange_price'].replace('?', '0')))
                        one_line.append(data['gains_24h'])
                        if data['token_name'] not in one_write_data.keys():
                            one_write_data[data['token_name']] = one_line
                        else:
                            one_write_data[data['token_name']][10] += float(data['exchange_amount_24h'].replace('?', '0'))
                            one_write_data[data['token_name']][11] = (one_write_data[data['token_name']][11] + float(data['exchange_price'].replace('?', '0')))/2
                       
                    for one_write_key in one_write_data.keys():
                        write_data[key].append(one_write_data[one_write_key])

        end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('end_time = ', end_time)
        
        for key in write_data.keys():
            file_name = key + '_tmp_data.csv'
            self.write_csv_file(file_name, write_data[key], head_line)
        return write_data
                
    def data_clean(self):
        for key in self.exchange:
            all_data = {}
            file_name = key + '_tmp_data.csv'
            csvFile = open(file_name, 'r', encoding = 'utf8')
            reader = csv.reader(csvFile)
            for row in reader:
                if row[0] == 'token_name':
                    continue
                
                if row[0] not in all_data.keys():
                    all_data[row[0]] = []
                    all_data[row[0]].append(row[0])
                    all_data[row[0]].append(row[1])
                    all_data[row[0]].append(row[2])
                    all_data[row[0]].append([])
                    all_data[row[0]][3].append(float(row[3].replace(',', '')))
                    all_data[row[0]].append([])
                    all_data[row[0]][4].append(float(row[4].replace(',', '')))
                    all_data[row[0]].append([])
                    all_data[row[0]][5].append(float(row[5].replace(',', '')))
                    all_data[row[0]].append([])
                    all_data[row[0]][6].append(float(row[6].replace(',', '').replace('%', '')))
                    all_data[row[0]].append([])
                    all_data[row[0]][7].append(float(row[7].replace(',', '')))
                    
                    all_data[row[0]].append(row[8])
                    all_data[row[0]].append(row[9])
                    all_data[row[0]].append([])
                    all_data[row[0]][10].append(float(row[10].replace(',', '')))
                    all_data[row[0]].append([])
                    all_data[row[0]][11].append(float(row[11].replace(',', '')))
                    
                else:
                    if row[9] not in all_data[row[0]][9]:
                        all_data[row[0]][3].append(float(row[3].replace(',', '')))
                        all_data[row[0]][4].append(float(row[4].replace(',', '')))
                        all_data[row[0]][5].append(float(row[5].replace(',', '')))
                        all_data[row[0]][6].append(float(row[6].replace(',', '').replace('%', '')))
                        all_data[row[0]][7].append(float(row[7].replace(',', '')))
                        all_data[row[0]][10].append(float(row[10].replace(',', '')))
                        all_data[row[0]][11].append(float(row[11].replace(',', '')))
                    else:
                        print('重复重复', key, row[9])
                    
            csvFile.close()
            for token_name in all_data.keys():
                all_data[token_name][3].remove(min(all_data[token_name][3]))
                all_data[token_name][3].remove(max(all_data[token_name][3]))           
                all_data[token_name][3] = sum(all_data[token_name][3])/len(all_data[token_name][3])
                
                all_data[token_name][4].remove(min(all_data[token_name][4]))
                all_data[token_name][4].remove(max(all_data[token_name][4]))           
                all_data[token_name][4] = sum(all_data[token_name][4])/len(all_data[token_name][4])
                
                all_data[token_name][5].remove(min(all_data[token_name][5]))
                all_data[token_name][5].remove(max(all_data[token_name][5]))           
                all_data[token_name][5] = sum(all_data[token_name][5])/len(all_data[token_name][5])
                
                all_data[token_name][6].remove(min(all_data[token_name][6]))
                all_data[token_name][6].remove(max(all_data[token_name][6]))           
                all_data[token_name][6] = sum(all_data[token_name][6])/len(all_data[token_name][6])
                
                all_data[token_name][7].remove(min(all_data[token_name][7]))
                all_data[token_name][7].remove(max(all_data[token_name][7]))           
                all_data[token_name][7] = sum(all_data[token_name][7])/len(all_data[token_name][7])
                
                all_data[token_name][10].remove(min(all_data[token_name][10]))
                all_data[token_name][10].remove(max(all_data[token_name][10]))           
                all_data[token_name][10] = sum(all_data[token_name][10])/len(all_data[token_name][10])
                
                all_data[token_name][11].remove(min(all_data[token_name][11]))
                all_data[token_name][11].remove(max(all_data[token_name][11]))           
                all_data[token_name][11] = sum(all_data[token_name][11])/len(all_data[token_name][11])
                
            file_name = key + '_clean_data.csv'
            head_line = ['token_name', 'cn_name', 'en_name', 'price_cny', 'price_usdt', 'tx_amount_24h',
                     'change_rate_24h', 'total_value', 'logo', 'time_data', 'exchange_amount_24h',
                     'exchange_price']
            csvFile = open(file_name, 'w', newline='', encoding = 'utf8')
            writer = csv.writer(csvFile)
            writer.writerow(head_line)
            
            for token_name in all_data.keys():
                writer.writerow(all_data[token_name])
            csvFile.close()
            
        
    def make_risk_data(self, exchange):
        file_name = exchange + '_clean_data.csv'
        read_file = open(file_name, encoding = 'utf8')
        df = pd.read_csv(read_file, sep = ',')
                        
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
            
        df['new_amount'] = df['exchange_amount_24h'] * df['change_rate_24h'].map(huanshou)
        
        btc_shizhi = df.loc[df['token_name'] == 'BTC']['total_value']

        def shizhi(shizhi):
            return shizhi/btc_shizhi.values[0]
        
        df['shizhi_amount'] = df['new_amount'] * df['total_value'].map(shizhi)
        
        
        df = df.sort_values(['shizhi_amount'], ascending=False).reset_index()
        index_max = df.shape[0]
        
        def number_to_star(number):
            if number > df.loc[20, ['shizhi_amount']].values[0]:
                return 1
            elif number > df.loc[20 + math.floor(index_max * 0.15), ['shizhi_amount']].values[0]:
                return 2
            elif number < df.loc[ math.floor(index_max * 0.95), ['shizhi_amount']].values[0] :
                return 5
            elif number < df.loc[ math.floor(index_max * 0.7), ['shizhi_amount']].values[0] :
                return 4
            else:
                return 3
            
            '''
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
                '''
                
            
        df['star'] = df['shizhi_amount'].map(number_to_star)
        if len(df.loc[df['token_name'] == 'CIC']['star'].values) > 0:
            if df.loc[df['token_name'] == 'CIC']['star'].values[0] > 2:
                df.loc[df['token_name'] == 'CIC', ['star']] = 2
                      
        if len(df.loc[df['token_name'] == 'OKB']['star'].values) > 0:
            if df.loc[df['token_name'] == 'OKB']['star'].values[0] > 1:
                df.loc[df['token_name'] == 'OKB', ['star']] = 1
                      
        if len(df.loc[df['token_name'] == 'BNB']['star'].values) > 0:
            if df.loc[df['token_name'] == 'BNB']['star'].values[0] > 1:
                df.loc[df['token_name'] == 'BNB', ['star']] = 1
                      
        if len(df.loc[df['token_name'] == 'HT']['star'].values) > 0:
            if df.loc[df['token_name'] == 'HT']['star'].values[0] > 1:
                df.loc[df['token_name'] == 'HT', ['star']] = 1
                      
        if len(df.loc[df['token_name'] == 'CTR']['star'].values) > 0:
            df.loc[df['token_name'] == 'CTR', ['star']] = 5
                  
        save_name = exchange + '_final.csv'
        df.to_csv(save_name, index = False, encoding = 'utf8')
        read_file.close()
        
    def make_all_risk_data(self):
        for key in self.exchange:
            self.make_risk_data(key)
                
if __name__ == '__main__':
    data_process = MongoDataProcess()
    #data_process.get_mongo_data()
    data_process.data_clean()
    data_process.make_all_risk_data()
    #data_process.make_risk_data('OKEX')
    #updata_mysql.updata_all_tosql()
