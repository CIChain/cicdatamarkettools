# !usr/bin/python
# -*- coding:utf-8 -*-

import requests
import time
from lxml import etree
import csv

def get_url_json(url, headers = None):
    res = False
    while res == False:
        time.sleep(0.3)
        try:
            response  = requests.get(url, headers=headers)
            if response.status_code == 500 or response.status_code == 404:
                return response.status_code
            res = True
            return response.json()
        except:
            print('get_url_json err,err url =', url)
            res = False
    
def get_url_html(url):
    res = False
    retry_time = 10
    while res == False:
        time.sleep(0.3)
        try:
            response  = requests.get(url)
            if response.status_code == 500 or response.status_code == 404:
                return response.status_code
            html = etree.HTML(response.text)
            res = True
            return html
        except:
            print('get_url_html err,err url =', url)
            res = False
            retry_time = retry_time - 1
            if retry_time <= 0:
                return ''
                
def get_url_text(url, err_file):
    res = False
    retry_time = 5
    while res == False:
        time.sleep(0.3)
        try:
            response  = requests.get(url)
            if response.status_code == 404:
                with open(err_file, 'a+') as f:
                    recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    err_srt = recordDate + ':' + 'get err, url = ' + url + ', err code = ' + str(response.status_code) + '\n'
                    f.write(err_srt)
                return ''
            if response.status_code != 200:
                retry_time = retry_time - 1
                if retry_time <= 0:
                    with open(err_file, 'a+') as f:
                        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        err_srt = recordDate + ':' + 'get err, url = ' + url + ', err code = ' + str(response.status_code) + '\n'
                        f.write(err_srt)
                    return response.status_code
                continue
            return response.text
        except:
            print('get_url_html err,err url =', url)
            res = False
            retry_time = retry_time - 1
            if retry_time <= 0:
                return ''
            
def write_csv_file(file_name, write_data, head_line = None):
    csvFile = open(file_name, 'w', newline='', encoding = 'utf8')
    writer = csv.writer(csvFile)
    if head_line != None:
        writer.writerow(head_line)
    for line in write_data:
        writer.writerow(line)
        
    csvFile.close()

if __name__ == "__main__":
    res_html = get_url_html('https://t.me/joinchat/GtLjl1JMYiTHzhaQA5CGPw')
    print(res_html)