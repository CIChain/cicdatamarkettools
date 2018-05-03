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
            res = True
            return response.json()
        except:
            print('get_url_json err,err url =', url)
            res = False
    

def get_url_html(url):
    res = False
    while res == False:
        time.sleep(0.3)
        try:
            response  = requests.get(url)
            if response.status_code == 500:
                return ''
            html = etree.HTML(response.text)
            res = True
            return html
        except:
            print('get_url_html err,err url =', url)
            res = False