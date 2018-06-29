import csv
import common_fun
import pandas as pd

class explosion_data():
    def change_data(self, source_file, days, new_file_name):
        head_line = []
        write_data = []
        with open(source_file, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'open_time':
                    head_line = line
                    continue
                write_data.append(line)
            
        for index in range(len(write_data)):
            if index + days < len(write_data):
                gains = (float(write_data[index + days][1]) - float(write_data[index][1]))/ float(write_data[index][1]) * 100
                write_data[index].append(gains)
        
        head_line.append('gains')
        common_fun.write_csv_file(new_file_name, write_data, head_line)
        
    def until_day(self, source_file, start_index, until_index, new_file):
        head_line = []
        write_data = []
        key_data = {}
        with open(source_file, 'r', encoding = 'utf8') as csvfile:
            reader = csv.reader(csvfile)
            for line in reader:
                if line[0] == 'open_time':
                    head_line = line
                    continue
                key_data[int(line[0]) / 1000] = line[4]
                write_data.append(line)
                
        for index in range(len(write_data)):
            high_week = float(write_data[index][2])
            low_week = float(write_data[index][3])

            in_index = index
            while int(write_data[in_index][0]) / 1000 <= start_index:
                if high_week < float(write_data[in_index][2]):
                    high_week = float(write_data[in_index][2])
                if low_week > float(write_data[in_index][3]):
                    low_week = float(write_data[in_index][3])
                    
                in_index += 1
                if in_index >= len(write_data):
                    break
            
            write_data[index].append(high_week)
            write_data[index].append(low_week)
            write_data[index].append(start_index)
            
            #gains = (float(key_data[start_index]) - float(write_data[index][1]))/ float(write_data[index][1]) * 100
            #write_data[index].append(gains)
                
            if int(write_data[index][0]) / 1000 == start_index:
                start_index += until_index
                
                if key_data.get(start_index, '') == '':
                    start_index = int(write_data[len(write_data) - 1][0]) /1000
                print(start_index, key_data[start_index])
                
        head_line.append('high_week')
        head_line.append('low_week')
        head_line.append('week_index')
        #head_line.append('gains')
        common_fun.write_csv_file(new_file, write_data, head_line)
        
    def get_count_data_until_friday(self):
        df = pd.read_csv('until_friday_change.csv')
        
        df['open_high_gains'] = df.apply(lambda x: (x['high_week'] - x['open_price']) / x['open_price'] * 100, axis=1)
        df['open_low_gains'] = df.apply(lambda x: (x['low_week'] - x['open_price']) / x['open_price'] * 100, axis=1)
        df['close_high_gains'] = df.apply(lambda x: (x['high_week'] - x['close_price']) / x['close_price'] * 100, axis=1)
        df['close_low_gains'] = df.apply(lambda x: (x['low_week'] - x['close_price']) / x['close_price'] * 100, axis=1)
        
        def is_baocang(high, low):
            if 10 > high > -10 and 10 > low > -10:
                return 0
            return 1
            
        df['is_baocang_open'] = df.apply(lambda row:is_baocang(row['open_high_gains'], row['open_low_gains']), axis=1)
        df['is_baocang_close'] = df.apply(lambda row:is_baocang(row['close_high_gains'], row['close_low_gains']), axis=1)
         
        df.to_csv('30_min_gains.csv', index = False, encoding = 'utf8')
        
        
    def get_count_data(self):
        df = pd.read_csv('days_rise_change.csv')
        print(df.shape[0])
        rise_data = df[df['gains'] > 10]
        print(rise_data.shape[0])
        fall_data = df[df['gains'] < -10]
        print(fall_data.shape[0])
        
        df_week = pd.read_csv('week_rise_change.csv')
        print(df_week.shape[0])
        rise_data = df_week[df_week['gains'] > 10]
        print(rise_data.shape[0])
        fall_data = df_week[df_week['gains'] < -10]
        print(fall_data.shape[0])
        
        df_month = pd.read_csv('month_rise_change.csv')
        print(df_month.shape[0])
        rise_data = df_month[df_month['gains'] > 10]
        print(rise_data.shape[0])
        fall_data = df_month[df_month['gains'] < -10]
        print(fall_data.shape[0])
        
        df_until_friday = pd.read_csv('until_friday_change.csv')
        print(df_until_friday.shape[0])
        rise_data = df_until_friday[df_until_friday['gains'] > 10]
        print(rise_data.shape[0])
        fall_data = df_until_friday[df_until_friday['gains'] < -10]
        print(fall_data.shape[0])
        
                        
if __name__ == "__main__":
    data_pro = explosion_data()
    #data_pro.change_data('okex_eth_usdt.csv', 1, 'days_rise_change.csv')
    #data_pro.change_data('okex_eth_usdt.csv', 7, 'week_rise_change.csv')
    #data_pro.change_data('okex_eth_usdt.csv', 30, 'month_rise_change.csv')
    #data_pro.get_count_data()
    #data_pro.until_day('okex_eth_usdt_30min.csv', 1526630400, 604800, 'until_friday_change.csv')
    data_pro.get_count_data_until_friday()