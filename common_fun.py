# !usr/bin/python
# -*- coding:utf-8 -*-

import requests
import time
from lxml import etree

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
            

if __name__ == "__main__":
    res_html = get_url_html('https://t.me/joinchat/GtLjl1JMYiTHzhaQA5CGPw')
    print(res_html)