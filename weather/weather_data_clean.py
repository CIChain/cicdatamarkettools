import pandas as pd
import csv
import config
from mysqldb import Mysqldb
import common_fun

class WeatherDataClean():
    def __init__(self):
        return
    
    def write_csv_file(self, file_name, write_data, head_line = None):
        csvFile = open(file_name, 'w', newline='', encoding = 'utf8')
        writer = csv.writer(csvFile)
        if head_line != None:
            writer.writerow(head_line)
        for line in write_data:
            writer.writerow(line)
            
        csvFile.close()
    
    def clean_data(self):
        df =pd.read_csv('weather_data.csv', sep = ',')
        
        def get_year(data_str):
            return int(data_str.split('/')[0].strip())
        
        df['date_year'] = df['date'].map(get_year)
        
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

        def group_top_data(group_data, n=25, column='temperature_int'):
            return group_data.sort_index(by=column)[-n:]
            
        group_data = df.groupby(['city_name', 'date_year']).apply(group_top_data)
        group_data.to_csv('group_top_data.csv', index = False, encoding = 'utf-8')
        
        group_data_tem_len = df.groupby(['city_name', 'date_year', 'temperature_int']).size()
        group_data_tem_len.to_csv('group_top_tem_len.csv', encoding = 'utf-8')
    
    def grop_top_data(self, top_file_name, out_file_name):
        dict_data = {}
        with open(top_file_name, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    continue
                
                if line[0] not in dict_data.keys():
                    dict_data[line[0]] = {}
                    
                if line[1] not in dict_data[line[0]].keys():
                    dict_data[line[0]][line[1]] = {}
                    dict_data[line[0]][line[1]]['tem'] = int(line[2])
                    dict_data[line[0]][line[1]]['days'] = 1
                else:
                    dict_data[line[0]][line[1]]['days'] += 1
                    if dict_data[line[0]][line[1]]['tem'] > int(line[2]):
                        dict_data[line[0]][line[1]]['tem'] = int(line[2])
            
            write_data = []
            for key in dict_data.keys():
                for year in dict_data[key].keys():
                    write_data.append([key, year, dict_data[key][year]['tem'], dict_data[key][year]['days']])
            self.write_csv_file(out_file_name, write_data)
            
    def top_25_tem(self, top_25_file_name, out_file):
        dict_data = {}
        with open(top_25_file_name, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] not in dict_data.keys():
                    dict_data[line[0]] = {}
                    dict_data[line[0]]['tem'] = int(line[3])
                    dict_data[line[0]]['tatol_tem'] = int(line[3]) * int(line[2])
                else:
                    dict_data[line[0]]['tem'] += int(line[3])
                    dict_data[line[0]]['tatol_tem'] += int(line[3]) * int(line[2])
                    
            write_data = []
            for key in dict_data.keys():
                import math
                write_data.append([key, math.ceil(dict_data[key]['tatol_tem']/dict_data[key]['tem'])])
            self.write_csv_file(out_file, write_data)

        
    def pro_top_data(self, city_index_file, tem_len_file, out_file):
        dict_data = {}
        max_tem = {}
        head_line = []
        
        with open(city_index_file, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    continue
                
                if line[0] not in max_tem.keys():
                    max_tem[line[0]] = line[1]        
        
        with open(tem_len_file, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    head_line = line
                    continue
                if line[0] not in dict_data.keys():
                    dict_data[line[0]] = {}
                    dict_data[line[0]][line[1]] = 0
                
                if int(line[2]) >= (int(max_tem[line[0]])):
                    if line[1] not in dict_data[line[0]].keys():
                        dict_data[line[0]][line[1]] = int(line[3])
                    else:
                        dict_data[line[0]][line[1]] += int(line[3])
                        
            write_data = []
            for key in dict_data.keys():
                for year in dict_data[key].keys():
                    write_data.append([key, year, int(max_tem[key]), dict_data[key][year]])
            self.write_csv_file(out_file, write_data, head_line)
        
    def del_invalid_data(self, city_index_file, tem_len_file, out_file):
        max_tem = {}
        with open(city_index_file, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    continue
                if line[0] not in max_tem.keys():
                    max_tem[line[0]] = int(line[1])
           
        write_data = []
        head_line = []
        with open(tem_len_file, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    head_line = line
                    continue
                
                if int(line[2]) >= max_tem[line[0]]:
                    write_data.append(line)
                
            self.write_csv_file(out_file, write_data, head_line)
            
    def days_percent(self, index_days_file, tem_days_file, out_file):
        city_year_days = {}
        with open(index_days_file, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    continue
                
                key = line[0] + line[1]
                if key not in city_year_days.keys():
                    city_year_days[key] = int(line[3])
        
        write_data = []
        head_line = []
        with open(tem_days_file, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    line.append('percent')
                    head_line = line
                    continue
                
                key = line[0] + line[1]
                
                line.append(int(line[3]) / city_year_days[key])
                write_data.append(line)
                
            self.write_csv_file(out_file, write_data, head_line)
            
    def city_days_percent(self):
        city_year_days = {}
        with open('city_index_days.csv', 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    continue
                
                key = line[0] + line[1]
                if key not in city_year_days.keys():
                    city_year_days[key] = {}
                    city_year_days[key]['tem'] = int(line[2])
                    city_year_days[key]['percent'] = 0
                    
        percent = []
        with open('city_days_percent.csv', 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    continue
                
                key = line[0] + line[1]
                if int(line[2]) == city_year_days[key]['tem'] + 6:
                    city_year_days[key]['percent'] = float(line[4])
                    percent.append(float(line[4]))
                    
        data = pd.DataFrame(city_year_days)
        data = data.T
        print(data.describe([0.1, 0.3, 0.5, 0.7, 0.9]))
        
    def city_total_payout(self, index_file, tem_days_file, out_file, total_out):
        #pay_amount = [10, 12, 14, 20, 30, 40, 60, 100]
        #pay_amount = [10, 15, 20, 30]
        pay_amount = [12, 18, 30]
        #pay_amount_eth = [0.000375, 0.00056, 0.00075, 0.001, 0.0015, 0.002, 0.003, 0.005]
        min_tem = {}
        with open(index_file, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    continue
                if line[0] not in min_tem.keys():
                    min_tem[line[0]] = int(line[1])
        
        write_data = []
        head_line = []         
        with open(tem_days_file, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    line.append('amount')
                    head_line = line
                    continue
                
                des_tem = int(line[2]) - min_tem[line[0]]
                index = int(des_tem / 2)
                if index >= len(pay_amount):
                    index = len(pay_amount) - 1
                                 

                line.append(pay_amount[index] * int(line[3]))
                write_data.append(line)
                
            self.write_csv_file(out_file, write_data, head_line)
        
        total_amount = {}
        for one_data in write_data:
            key = one_data[0] + one_data[1]
            if key not in total_amount.keys():
                total_amount[key] = one_data
            else:
                total_amount[key][4] += one_data[4]
        
        self.write_csv_file(total_out, total_amount.values(), head_line)
        
    def city_avg_amount(self, amount_file, out_file):
        amount_data = {}
        with open(amount_file, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    line.append('amount')
                    continue
                
                if line[0] not in amount_data.keys():
                    amount_data[line[0]] = {}
                    amount_data[line[0]]['year_len'] = 1
                    amount_data[line[0]]['total_amount'] = float(line[4])
                else:
                    amount_data[line[0]]['year_len'] += 1
                    amount_data[line[0]]['total_amount'] += float(line[4])
        write_data = []
        for key in amount_data.keys():
            avg = float(amount_data[key]['total_amount'] / amount_data[key]['year_len'])
            write_data.append([key, avg])
            
        self.write_csv_file(out_file, write_data)
        
    def city_name(self):
        #db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)
        name_code = {}
        with open('BaiduMap_cityCode_1102.csv', 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'area_id':
                    continue
                
                name_code[line[1]] = line[0]
        
        write_data = []        
        with open('city_index.csv', 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    continue
                i = 0
                for name_str in name_code.keys():
                    if name_str.startswith(line[0]):
                        line.append(name_code[name_str])
                        i += 1
                
                if i > 1 :
                    print(line)
                write_data.append(line)
                
            self.write_csv_file('city_index_code.csv', write_data)
            
    def city_id_loc(self):
        city_index = {}
        with open('city_index.csv', 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    continue
                if line[0] not in city_index.keys():
                    city_index[line[0]] = int(line[1])
        
        db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)

        with open('cityidloc.csv', 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[1] == line[2]:
                    if city_index.get(line[1], '') != '':
                        
                        url = 'http://api.map.baidu.com/geocoder/v2/?location=' + str(line[4]) + ',' + str(
                                line[5]) + '&output=json&pois=0&ak=lS5SlxcGqXfkuj3pcwRGBv90'
                        res_data = common_fun.get_url_json(url)
                        city_code = res_data['result']['cityCode']

                        insert_str = "INSERT INTO city_location (country, city, latitude, longitude, altitude, is_use, city_code, claim_temperature)"
                        insert_str += "VALUES ('China', '" + line[1] + "'," + str(line[4]) + "," + str(line[5]) + ", 0, 0," + str(
                                city_code) + "," + str(city_index[line[1]]) +")"
                        #write_data.append([line[1], line[4], line[5], city_index[line[1]]])
                        db.insert(insert_str)
                        
    def city_index_change(self, source_file, new_file, add_tem):
        df = pd.read_csv(source_file)
        print(df.head(10))
        
        df['tem'] = df['tem'] + add_tem
        print(df.head(10))

        df.to_csv(new_file, index = False, encoding = 'utf-8')
        
    def city_index_days(self):
        df = pd.read_csv('city_index_days.csv')
        print(df.describe([0.1,0.3,0.5,0.7,0.9]))
        
        df = df[df['count'] >= 1]
        df = df[df['count'] <= 10]
        
        print(df.describe([0.1,0.3,0.5,0.7,0.9]))
        
    def jion_3_days(self):
        source_data = []
        head_line = []
        city_index = {}
        key_days = {}
        count = 0
        with open('city_index_new.csv', 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'city_name':
                    continue
                if line[0] not in city_index.keys():
                    city_index[line[0]] = int(line[1])
                    
        with open('weather_data.csv', 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            tmp_data = []
            for line in reader:
                if line[0] == 'id':
                    head_line = line
                    continue
                
                tem = 0
                if line[1] in city_index:
                    tem_str = line[2].replace('℃', '')
                    if tem_str != '':
                        tem = int(tem_str)
                    
                    if tem >= city_index[line[1]]:
                        tmp_data.append(line)
                    else:
                        tmp_data.clear()
                        
                key = line[1] + line[7].split('/')[0].strip()
                
                if len(tmp_data) >= 3:
                    source_data.extend(tmp_data)
                    tmp_data.clear()
                    count += 1
                    if key_days.get(key, '') == '':    
                        key_days[key] = 1
                    else:
                        key_days[key] += 1
                    
            self.write_csv_file('jion_3_day.csv', source_data, head_line)
            write_list = []
            for key_w in key_days.keys():
                write_list.append([key_w, key_days[key_w]])
            self.write_csv_file('jion_3_day_count.csv', write_list, ['key', 'count'])
        print(count)
                        
if __name__ == "__main__":
    data_pro = WeatherDataClean()
    #data_pro.clean_data()
    #data_pro.grop_top_data('group_top_data.csv', 'top_25_tems.csv')
    #data_pro.top_25_tem('top_25_tems.csv', 'city_index.csv')
    #data_pro.pro_top_data('city_index_new.csv', 'group_top_tem_len.csv', 'city_index_days.csv')
    #data_pro.city_index_days()
    #data_pro.del_invalid_data('city_index_new.csv', 'group_top_tem_len.csv', 'city_tem_days.csv')
    #data_pro.days_percent('city_index_days.csv', 'city_tem_days.csv', 'city_days_percent.csv')
    #data_pro.city_total_payout('city_index_new.csv', 'city_tem_days.csv', 'city_days_amount_new.csv', 'city_total_amount_new.csv')
    #data_pro.city_total_payout('city_index.csv', 'city_tem_days.csv', 'city_days_amount_eth.csv', 'city_total_amount_eth.csv')
    #data_pro.city_avg_amount('city_total_amount_new.csv', 'city_avg_amount_new.csv')
    #data_pro.city_avg_amount('city_total_amount_eth.csv', 'city_avg_amount_eth.csv')
    #data_pro.city_name()
    #data_pro.city_days_percent()
    #data_pro.city_id_loc()
    #data_pro.city_index_change('city_index.csv', 'city_index_new.csv', 2)
    data_pro.jion_3_days()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    