from weather_data_clean import WeatherDataClean
import pandas as pd
import csv
import config
from mysqldb import Mysqldb
import common_fun

class ForeignData(WeatherDataClean):
    def clean_data(self):
        df =pd.read_csv('weather_data_other.csv', sep = ',')
        
        def get_year(data_str):
            return int(data_str.split('/')[0].strip())
        
        df['date_year'] = df['date'].map(get_year)
        df.rename(columns={'city_name_en':'city_name'}, inplace = True)
        
        def temperature_int(tem_str):
            temp = tem_str.replace('℃', '')
            try:
                res = int(temp)
                return res
            except:
                return 0
            
        df['temperature_int'] = df['max_temperature'].map(temperature_int)
        
        df = df[(df['temperature_int'] >= 30) & (df['date_year'] < 2018)]
        del df['id'], df['max_temperature'], df['date'], df['weather'], df['wind_direction'], df['wind_speed'], df['min_temperature']
        del df['state'], df['cn_county'], df['city_name_cn']

        def group_top_data(group_data, n=25, column='temperature_int'):
            return group_data.sort_index(by=column)[-n:]
            
        group_data = df.groupby(['city_name', 'date_year']).apply(group_top_data)
        group_data.to_csv('foreign_top_data.csv', index = False, encoding = 'utf-8')
        
        group_data_tem_len = df.groupby(['city_name', 'date_year', 'temperature_int']).size()
        group_data_tem_len.to_csv('foreign_top_tem_len.csv', encoding = 'utf-8')
        
    def add_cn_county(self):
        city_data = {}
        with open('weather_data_other.csv', 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'id':
                    continue
                
                if line[3] not in city_data.keys():
                    city_data[line[3]] = line[2]
                
        write_data = []
        with open('foreign_city_index.csv', 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                line.append(city_data[line[0]])
                write_data.append(line)
                
            self.write_csv_file('foreign_county_data.csv', write_data)    
        
                
        
    def foreign_city_name(self, index_file):
        city_name_extra = ['-oh', '-ca', '-mi', '-ga', '-tx', '-ca', '-wa', '-ma', '-new-town', '-estate', '-fl',
                           '-ia', '-az', '-wv', '-nc', '-va', '-il', '-tn', '-co', '-or', '-in', '-hi', '-ny',
                           '-ut', '-ct', '-dc', '-ok', '-sd', '-la', '-pa', '-ne', '-nv',]
        write_data = []
        db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)
        i = 0
        with open(index_file, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                for del_str in city_name_extra:
                    if line[0].endswith(del_str):
                        end_index = -len(del_str)
                        line[0] = line[0][:end_index]
                        line[0] = line[0].replace('-', ' ')
                        
                        sel_str = "SELECT * from city_location_copy_bk WHERE city like '" + line[0] + "%'"
                        
                        res_db = db.select(sel_str)
                        if len(res_db) == 1:
                            line.append(res_db[0][3])
                            line.append(res_db[0][4])
                            i += 1
                        elif len(res_db) > 1:
                            print(line[0])
                            
                write_data.append(line)
                
            print(i)
            
            self.write_csv_file('foreign_loc_index.csv', write_data)
            
    def foreign_data_to_sql(self):
        db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)
        county_list = ['united-states', 'japan', 'south-korea', 'singapore']
        with open('foreign_loc_index_shougong.csv', 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == '城市名':
                    continue
                if line[3] == '' or line[4] == '':
                    continue
                else:
                    url = 'http://api.map.baidu.com/geocoder/v2/?location=' + str(line[3]) + ',' + str(
                            line[4]) + '&output=json&pois=0&ak=lS5SlxcGqXfkuj3pcwRGBv90'
                    res_data = common_fun.get_url_json(url)
                    city_code = res_data['result']['cityCode']
                    country = ''
                    for county_str in county_list:
                        if county_str in line[2]:
                            country = county_str
                    
                    insert_str = "INSERT INTO city_foreign (country, city, latitude, longitude, altitude, is_use, city_code, claim_temperature)"
                    insert_str += "VALUES ('"+ country +"', '" + line[0] + "'," + str(line[3]) + "," + str(line[4]) + ", 0, 0," + str(
                            city_code) + "," + str(line[1]) +")"
                    db.insert(insert_str)
                    
        
if __name__ == "__main__":
    data_pro = ForeignData()
    #data_pro.clean_data()
    #data_pro.grop_top_data('foreign_top_data.csv', 'foreign_top_25_tem.csv')
    #data_pro.top_25_tem('foreign_top_25_tem.csv', 'foreign_city_index.csv')
    #data_pro.del_invalid_data('foreign_city_index.csv', 'foreign_top_tem_len.csv', 'foreign_tem_days.csv')
    #data_pro.pro_top_data('foreign_city_index.csv', 'foreign_tem_days.csv', 'foreign_index_days.csv')
    #data_pro.days_percent('foreign_index_days.csv', 'foreign_tem_days.csv', 'foreign_days_percent.csv')
    #data_pro.city_total_payout('foreign_city_index.csv', 'foreign_tem_days.csv',
    #                           'foreign_days_amount.csv', 'foreign_total_amount.csv')
    #data_pro.city_avg_amount('foreign_total_amount.csv', 'foreign_avg_amount.csv')
    #data_pro.foreign_city_name('foreign_county_data.csv')
    #data_pro.add_cn_county()
    data_pro.foreign_data_to_sql()