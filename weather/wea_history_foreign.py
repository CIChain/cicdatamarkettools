# !usr/bin/python
# -*- coding:utf-8 -*-

import config
import common_fun
from wea_history import HistorWeather
import time
import re

class ForeignHistorWeather(HistorWeather):
    county_list = ['united-states', 'japan', 'south-korea', 'singapore']
    def foreign_city_index(self):
        self.foreign_citys = {}
        url = 'http://tianqi.2345.com/js/interCitySelectData.js'
        res_str = common_fun.get_url_text(url, 'foreign_err.log')
        new_str = res_str.split('var city = [];')[1]
        
        state_datas = new_str.split(';')
        for state_data in state_datas:
            if len(state_data) < 10:
                continue
            
            state_data = state_data.replace('city[\'', '')
            state = state_data.split('\']=')[0].strip()
            self.foreign_citys[state] = []
            state_data = state_data.split('\']=')[1].strip()
            state_data = state_data.replace('\"', '').replace('[', '').replace(']', '')
            city_list_en = []
            
            county_datas = state_data.split(',')
            for county_data in county_datas:
                city_datas = county_data.split('|')
                for city_data in city_datas:
                    if len(city_data) < 4:
                        continue
                    city_split = city_data.split(' ')
                    city_name_en = city_split[0][:-2]
                    city_name_cn = city_split[1]
                    city_name_en = city_name_en.replace('\'', '\\\'')
                    
                    if city_name_en not in city_list_en:
                        city_list_en.append(city_name_en)
                        city_info = {'city_name': city_name_en, 'cn_county': city_name_cn}
                        if city_info not in self.foreign_citys[state]:
                            self.foreign_citys[state].append(city_info)
                        
    def make_foreign_url(self, state, city_name, year, month):
        url = ''
        weather_date = ''
        
        if year == 2018 and month < 7:
            weather_date = str(year) + '0' + str(month)
        else:
            if month < 10:
                weather_date = str(year) + '0' + str(month)
            else:
                weather_date = str(year) + str(month)
                
        url = config.weather_inter_base + state + '/' + weather_date + '/' + city_name + '_' + weather_date + '.js'
        return url
        
    def analyze_data(self, res_text, key, city_data):
        try:
            res_text = res_text.replace('var weather_str=', '').replace(',{}', '')
            res_text = res_text[0 : -1]
            res_dic = re.sub(r'(?!={|, )(\w*):', r'"\1":', res_text)
            res_dic = eval(res_dic)
        except:
            print(res_text)
        try:
            for tianqi in res_dic['tqInfo']:
                insert_str = 'INSERT INTO weather_data_other (state, cn_county, city_name_en, city_name_cn, max_temperature, min_temperature, weather, wind_direction, wind_speed, date)'
                insert_str += 'VALUES ("' + key + '","' + city_data['cn_county'] + '","' + city_data['city_name'] + '","' + str(
                        res_dic['city']) + '","' + tianqi['bWendu'] + '","' + tianqi['yWendu'] + '","' + str(
                                tianqi['tianqi']) + '","' + tianqi['fengxiang'] + '","' + tianqi['fengli'] + '","' + tianqi['ymd'] +'")'
                self.insert_list.append(insert_str)
        except:
            print('dic err', res_text)
                
    def get_foreign_weather(self):  #获取所有得国家的数据
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('start_time', recordDate)
        retry_url = []
        for key in self.foreign_citys.keys():
            for city_data in self.foreign_citys[key]:
                select_str = 'SELECT * FROM weather_data_other WHERE city_name_en = "' + city_data['city_name'] + '"'
                res_sel = self.db.select(select_str)
                if len(res_sel) > 0:
                    continue
                
                for year in range(2011, 2019):
                    for month in range(1, 13):
                        url = self.make_foreign_url(key, city_data['city_name'], year, month)
                        res_text = common_fun.get_url_text(url, 'foreign_err.log')
                        if res_text == '':
                            continue
                        elif res_text == 503:
                            retry_data = {'key':key, 'url': url, 'city_data': city_data}
                            retry_url.append(retry_data)
                            continue
                        self.analyze_data(res_text, key, city_data)
                        
                    self.updata_to_mysql()
                    
        for retry_data in retry_url:
            print(retry_data)
            res_text = common_fun.get_url_text(retry_data['url'], 'foreign_err.log')
            if res_text == '':
                continue
            self.analyze_data(res_text, retry_data['key'], retry_data['city_data'])
            self.updata_to_mysql()
        return
    
    def get_list_county_data(self):
        retry_url = []
        for key in self.foreign_citys.keys():
            for city_data in self.foreign_citys[key]:
                for one_county in self.county_list:
                    if one_county in city_data['cn_county']:
                        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        print(recordDate, one_county)
                        select_str = 'SELECT * FROM weather_data_other WHERE city_name_en = "' + city_data['city_name'] + '"'
                        res_sel = self.db.select(select_str)
                        if len(res_sel) > 0:
                            continue
                        
                        for year in range(2011, 2019):
                            for month in range(1, 13):
                                url = self.make_foreign_url(key, city_data['city_name'], year, month)
                                res_text = common_fun.get_url_text(url, 'foreign_err.log')
                                if res_text == '':
                                    continue
                                elif res_text == 503:
                                    retry_data = {'key':key, 'url': url, 'city_data': city_data}
                                    retry_url.append(retry_data)
                                    continue
                                self.analyze_data(res_text, key, city_data)
                                
                            self.updata_to_mysql()
        
        for retry_data in retry_url:
            res_text = common_fun.get_url_text(retry_data['url'], 'foreign_err.log')
            if res_text == '':
                continue
            self.analyze_data(res_text, retry_data['key'], retry_data['city_data'])
            self.updata_to_mysql()
        return
    
if __name__ == "__main__":
    foreign_his_wea = ForeignHistorWeather()
    foreign_his_wea.foreign_city_index()
    #foreign_his_wea.get_foreign_weather()
    foreign_his_wea.get_list_county_data()
    #foreign_his_wea()