# !usr/bin/python
# -*- coding:utf-8 -*-
import config
import csv
from mysqldb import Mysqldb
import time
import pandas as pd

class RiskDataPro():
    def __init__(self):
        self.db = Mysqldb()        
        
    def clean_data(self):
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(recordDate)
        new_data = []
        en_name_list = []
        
        for key in config.DicExchangeName.keys():
            print(key)
            
            
            file_name = 'source_data\\' + key + '_clean_data.csv'
            with open(file_name, 'r', encoding = 'utf8') as csvfile:
                reader = csv.reader(csvfile)
                for line in reader:
                    if line[0] == 'token_name':
                        continue
                    
                    new_line = []
                    new_line.append(line[0])
                    new_line.append(line[2])
                    new_line.append(line[5])
                    new_line.append(line[6])
                    new_line.append(line[7])
                    
                    if line[2] not in en_name_list:
                        en_name_list.append(line[2])
                        new_data.append(new_line)
        print(len(new_data))
            
        head_line = ['token_name', 'en_name', 'tx_amount_24h', 'change_rate_24h', 'total_value']
        print(head_line)
        file_name = 'risk_clean.csv'
        csvFile = open(file_name, 'w', newline='', encoding = 'utf8')
        writer = csv.writer(csvFile)
        if head_line != None:
            writer.writerow(head_line)
        for line in new_data:
            writer.writerow(line)
            
        csvFile.close()
        
    def make_risk_data(self):
        file_name = 'risk_clean.csv'
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
            
        df['new_amount'] = df['tx_amount_24h'] * df['change_rate_24h'].map(huanshou)
        
        btc_shizhi = df.loc[df['token_name'] == 'BTC']['total_value']

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
        print(len(df.loc[df['token_name'] == 'CIC']['star'].values))
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
                  
        save_name = 'risk_final.csv'
        df.to_csv(save_name, index = False, encoding = 'utf8')
        
    def risk_data_tosql(self):
        file_name = 'risk_final.csv'
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        with open(file_name, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'token_name':
                    continue
                
                select_str = "SELECT id FROM token_base WHERE en_name = '" + line[1] +"'"
                token_id = self.db.select(select_str)[0][0]
                
                select_str = "SELECT * FROM risk_data WHERE token_id = " + str(token_id)
                sel_res = self.db.select(select_str)
                
                if sel_res:
                    updata_str = "UPDATE risk_data SET drop_off_risk_data=" + line[7] + ",tx_amount=" + str(
                            line[2]) + ",change_rate=" + str(line[3]) + ",total_value='" + str(line[4])
                    updata_str += " where token_id = " + str(token_id)                                           
                    try:
                        self.db.update(updata_str)
                    except Exception as e:
                        print(updata_str)
                        print('update err, token = ', line[0])
                else:
                    insert_str = "INSERT INTO risk_data (token_id, drop_off_risk_data, tx_amount, change_rate, total_value, created_at)"
                    
                    insert_str += "VALUES (" + str(token_id) + ","+ str(line[7]) + "," + str(line[2]) + "," + str(
                            line[3]) + "," + str(line[4]) + ",'" + str(recordDate) + "')"
                    
                    try:
                        self.db.update(insert_str)
                    except Exception as e:
                        print(insert_str)
                        print('insert err, token = ', line[0])
        try:            
            self.db.update("UPDATE risk_data a INNER JOIN (SELECT a.total_value ,COUNT(*) AS pm FROM risk_data a LEFT JOIN  risk_data b ON a.total_value <= b.total_value GROUP BY a.total_value) c ON a.total_value=c.total_value SET a.total_value_rank =c.pm")
            self.db.update("UPDATE risk_data a INNER JOIN (SELECT a.tx_amount ,COUNT(*) AS pm FROM risk_data a LEFT JOIN  risk_data b ON a.tx_amount <= b.tx_amount GROUP BY a.tx_amount) c ON a.tx_amount=c.tx_amount SET a.tx_amount_rank =c.pm")
        except Exception as e:
            print('update err rank')
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        
        
if __name__ == "__main__":
    risk_data = RiskDataPro()
    #risk_data.clean_data()
    #risk_data.make_risk_data()
    risk_data.risk_data_tosql()