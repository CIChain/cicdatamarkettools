# !usr/bin/python
# -*- coding:utf-8 -*-
import config
import csv
from mysqldb import Mysqldb
import time

class TokenDataPro():
    def __init__(self):
        self.db = Mysqldb()
        
    def token_data_tosql(self):
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(recordDate)
        for key in config.DicExchangeName.keys():
            print(key)
            file_name = 'source_data\\' + key + '_clean_data.csv'
            with open(file_name, 'r', encoding = 'utf8') as csvfile:
                reader = csv.reader(csvfile)
                for line in reader:
                    if line[0] == 'token_name':
                        continue
                    
                    sel_str = "SELECT exchange FROM token_base WHERE en_name = '"+ line[2] +"'"
        
                    dbres = self.db.select(sel_str)
                    if len(dbres) > 1:
                        print(len(dbres), key, line[2], line[0])
                    if dbres:
                        exchange_str = dbres[0][0]
                        if dbres[0][0].find(config.DicExchangeName[key]) == -1: 
                            exchange_str = dbres[0][0] + ',' + config.DicExchangeName[key]
                        else:
                            continue
                        updata_str = "UPDATE token_base SET exchange='" + exchange_str + "',updata_at='" + recordDate + "'"
                        updata_str += " where en_name = '" + line[2] + "'"                                           
                        try:
                            self.db.update(updata_str)
                        except Exception as e:
                            print(updata_str)
                            print('update err, token = ', line[0])
                    else:
                        insert_str = "INSERT INTO token_base (token_name, en_name, cn_name, exchange, created_at, updata_at )"
                        if line[2] == 'Bitcoin Cash(BCC)':
                            insert_str += "VALUES ('BCH','"+ line[2] + "','" + line[1] + "','" + config.DicExchangeName[key] + "','" + str(
                                    recordDate) + "','" + str(recordDate) + "')"
                        else:
                            insert_str += "VALUES ('" + line[0] + "','"+ line[2] + "','" + line[1] + "','" + config.DicExchangeName[key] + "','" + str(
                                    recordDate) + "','" + str(recordDate) + "')"
                        
                        try:
                            self.db.update(insert_str)
                        except Exception as e:
                            print(insert_str)
                            print('insert err, token = ', line[0], key)
                    
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(recordDate)
        return
    
    
if __name__ == "__main__":
    token_data = TokenDataPro()
    token_data.token_data_tosql()