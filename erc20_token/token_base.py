# !usr/bin/python
# -*- coding:utf-8 -*-
from mysqldb import Mysqldb
import time
import common_fun
import config
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

class TokenBacePro():
    def __init__(self):
        self.db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)
        #self.driver = webdriver.Chrome(executable_path = config.chromedriver)
        self.driver = webdriver.Firefox()

    def get_token_address(self):
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(recordDate)
        self.driver.get(config.eth_tokens_url)
        
        xpath_str = '//*[@id="ContentPlaceHolder1_divpagingpanel"]/div[2]/p/span/b[1]'
        element_present = EC.text_to_be_present_in_element((By.XPATH, xpath_str),'1')
        WebDriverWait(self.driver, 30, 1).until(element_present)
        page_max = self.driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_divpagingpanel"]/div[2]/p/span/b[2]').text
        print(page_max)
        for page_index in range(1, int(page_max) + 1):
            page_url = config.eth_tokens_url + '?p=' + str(page_index)
            res_html = common_fun.get_url_html(page_url)
        
            for xpath_str in res_html.xpath('//*[@id="ContentPlaceHolder1_divresult"]/table/tbody/tr'):
                href_str = xpath_str.xpath('./td[3]/h5/a/@href')[0]
                token_str = xpath_str.xpath('./td[3]/h5/a/text()')[0]
                
                address = href_str.split('/')[-1]
                en_name = token_str.split('(')[0].strip()
                token_name = token_str.split('(')[1].split(')')[0]
                
                sel_str = "select * from token_base where token_name ='" + token_name + "'and en_name='" + en_name + "'"
                sel_res = self.db.select(sel_str)
                if len(sel_res) == 1:
                    updata_str = "UPDATE token_base SET erc20_contract='" + address
                    updata_str += "' where token_name = '" + token_name + "' and en_name = '" + en_name + "'"
                    try:
                        self.db.update(updata_str)
                    except Exception as e:
                        print(updata_str)
                        print('update err, token = ', token_name)
                elif len(sel_str) == 2:
                    print('token_name repeat: token_name = ', token_name, ',address = ', address)
                else:
                    insert_str = "INSERT INTO token_base (token_name, en_name, erc20_contract, created_at, updata_at )"
                    insert_str += "VALUES ('" + token_name + "','"+ en_name + "','" + address + "','" + str(
                                    recordDate) + "','" + str(recordDate) + "')"
                    
                    try:
                        self.db.update(insert_str)
                    except Exception as e:
                        print(insert_str)
                        print('insert err, token = ', token_name)

    def get_token_data(self):
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(recordDate)
        sel_str = "SELECT id, erc20_contract, github_address, twitter_address, facebook_address, telegraph_address, whitepaper_address from token_base WHERE erc20_contract <> ''"
        db_res = self.db.select(sel_str)
        
        for token in db_res:
            url = config.eth_token_url + token[1]
            self.driver.get(url)
                        
            git_address = token[2]
            twitter_address = token[3]
            facebook_address = token[4]
            telegram_address = token[5]
            whitepaper_address = token[6]
            
            total_issued = self.driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_divSummary"]/div[1]/table/tbody/tr[1]/td[2]').text
            total_issued = total_issued.split('(')[0]
            
            for element in self.driver.find_elements_by_xpath('//*[@id="ContentPlaceHolder1_tr_officialsite_2"]/td[2]/ul/li'):
                original_str = element.find_element_by_xpath('./a').get_attribute('data-original-title')
                if original_str.startswith('Github'):
                    if git_address == '':
                        git_address = element.find_element_by_xpath('./a').get_attribute('href')
                elif original_str.startswith('Telegram'):
                    if telegram_address == '':
                        telegram_address = element.find_element_by_xpath('./a').get_attribute('href')
                elif original_str.startswith('Facebook'):
                    if facebook_address == '':
                        facebook_address = element.find_element_by_xpath('./a').get_attribute('href')
                elif original_str.startswith('Twitter'):
                    if twitter_address == '':
                        twitter_address = element.find_element_by_xpath('./a').get_attribute('href')
                elif original_str.startswith('Whitepaper'):
                    if whitepaper_address == '':
                        whitepaper_address = element.find_element_by_xpath('./a').get_attribute('href')
                    
            updata_str = "UPDATE token_base SET github_address='" + git_address + "', twitter_address='" + str(
                    twitter_address) + "', facebook_address='" + facebook_address + "', telegraph_address='" + str(
                    telegram_address) + "', whitepaper_address='" + whitepaper_address + "', total_issued='" + total_issued + "',created_at='" + recordDate + "'"
            updata_str += " where id =" + str(token[0])
            
            try:
                self.db.update(updata_str)
            except Exception as e:
                print(updata_str)
                print('update err internet_data, token_id = ', token[0])
       
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(recordDate)
        
    def get_fxh_address(self):
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(recordDate)
        
        sel_str = "SELECT en_name, erc20_contract, fxh_address from token_base WHERE erc20_contract <> ''"
        db_res = self.db.select(sel_str)
        print(len(db_res))
        
        for token in db_res:
            fxh_address = token[2]
            if fxh_address == '':
                fxh_address = config.fxh_base_url + token[0].replace('.', '')
            res_html = common_fun.get_url_html(fxh_address)
            if len(res_html):
                cn_name = ''
                name = res_html.xpath('//*[@id="baseInfo"]/div[1]/div[1]/h1/text()')
                if len(name) > 1:
                    cn_name = name[1].strip()
                
                updata_str = "UPDATE token_base SET fxh_address='" + fxh_address + "', cn_name='" + cn_name + "'"
                updata_str += " where en_name ='" + str(token[0]) + "'"
                
                try:
                    self.db.update(updata_str)
                except Exception as e:
                    print(updata_str)
                    print('update err internet_data, token_id = ', token[0])
            else:
                print(token[0], token[1])
                print(fxh_address)
                
        
    def driver_close(self):
        self.driver.close()
                        
if __name__ == "__main__":
    token_bace = TokenBacePro()
    token_bace.get_token_address()
    token_bace.get_token_data()
    token_bace.get_fxh_address()
    token_bace.driver_close()