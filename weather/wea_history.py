# !usr/bin/python
# -*- coding:utf-8 -*-

import config
import common_fun
from mysqldb import Mysqldb
import re
import time

class HistorWeather():
    def __init__(self):
        self.county_index = {}
        self.db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)
        self.insert_list = []
    
    def city_index(self):
        url = 'http://tianqi.2345.com/js/citySelectData.js'
        res_str = common_fun.get_url_text(url, 'error.log')
        new_str = res_str.split('var provqx=new Array();')[1]
        for i in range(10, 44):
            new_str = new_str.replace('provqx[' + str(i) +']=', '')
        
        new_str = new_str.replace('\n', '').replace('[\'', '').replace('\']', '')

        str_list = new_str.split('\r')
        for province in str_list:
            province_list = province.split(',')
            for citys in province_list:
                citys_list = citys.split('|')
                for county in citys_list:
                    county_str_list = re.split('[- ]', county)
                    try:
                        if county_str_list[0].replace('\'', '') == county_str_list[3].replace('\'', ''):
                            self.county_index[county_str_list[2]] = county_str_list[0].replace('\'', '')
                    except:
                        print(county_str_list)

        with open('citys.txt', 'w') as f:
            f.write(str(self.county_index.keys()))
            f.write(str(len(self.county_index.keys())))
        #json_city = json.loads(new_str)
        #print(json_city)
        
    def make_weather_url(self, key, year, month):
        weather_date = ''
        if year == 2016:
            if month < 3:
                weather_date = str(year) + str(month)
                url = config.weather_js_base + self.county_index[key] + '_' + weather_date + '.js'
            elif month < 10:
                weather_date = str(year) + '0' + str(month)
                url = config.weather_js_base + weather_date + '/' + self.county_index[key] + '_' + weather_date + '.js'
            else:
                weather_date = str(year) + str(month)
                url = config.weather_js_base + weather_date + '/' + self.county_index[key] + '_' + weather_date + '.js'
        elif year < 2016:
                weather_date = str(year) + str(month)
                url = config.weather_js_base + self.county_index[key] + '_' + weather_date + '.js'
        elif year > 2016:
            if year == 2018 and month < 7:
                if month < 10:
                    weather_date = str(year) + '0' + str(month)
                else:
                    weather_date = str(year) + str(month)
            else:
                if month < 10:
                    weather_date = str(year) + '0' + str(month)
                else:
                    weather_date = str(year) + str(month)
            url = config.weather_js_base + weather_date + '/' + self.county_index[key] + '_' + weather_date + '.js'
        return url

    def analyze_data(self, res_text):
        try:
            res_text = res_text.replace('var weather_str=', '').replace(',{}', '')
            res_text = res_text[0 : -1]
            res_dic = re.sub(r'(?!={|, )(\w*):', r'"\1":', res_text)
            res_dic = eval(res_dic)
        except:
            print(res_text)
        try:
            for tianqi in res_dic['tqInfo']:
                insert_str = "INSERT INTO weather_data (city_name, max_temperature, min_temperature, weather, wind_direction, wind_speed, date)"
                insert_str += "VALUES ('" + res_dic['city'] + "','" + str(tianqi['bWendu']) +"','" + str(
                    tianqi['yWendu']) + "', '" + tianqi['tianqi'] + "', '" + tianqi['fengxiang'] + "','" + tianqi['fengli'] + "','"+ tianqi['ymd'] +"')"
                self.insert_list.append(insert_str)
        except:
            print('dic err', res_text)
            
    def updata_to_mysql(self):
        if len(self.insert_list ) > 0:
            try:
                self.db.execute_list(self.insert_list)
                self.insert_list.clear()
            except:
                print('insert err, len(insert_list) = ', len(self.insert_list))
                with open('insert.txt', 'a+') as f:
                    for line in self.insert_list:
                        f.write(line)
                        f.write('\n')
                            
    def get_weather_data(self):
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('start_time', recordDate)
        retry_url = []
        for key in self.county_index.keys():
            for year in range(2011, 2019):
                time.sleep(1)
                for month in range(1, 13):                    
                    url = self.make_weather_url(key, year, month)
                    if url == '':
                        continue
                    res_text = common_fun.get_url_text(url, 'error.log')
                    if res_text == '':
                        continue
                    elif res_text == 503:
                        retry_data = {'key':key, 'url': url}
                        retry_url.append(retry_data)
                        continue
                    self.analyze_data(res_text)
                self.updata_to_mysql()
                    
        for retry_data in retry_url:
            print(retry_data)
            res_text = common_fun.get_url_text(retry_data['url'], 'error.log')
            if res_text == '':
                continue
            self.analyze_data(res_text)
            self.updata_to_mysql()
                    
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('end_time', recordDate)
        
            
if __name__ == "__main__":
    his_wea = HistorWeather()
    his_wea.city_index()
    #his_wea.get_weather_data()
    #his_wea.get_prov_index()
