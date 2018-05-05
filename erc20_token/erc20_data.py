# !usr/bin/python
# -*- coding:utf-8 -*-
from mysqldb import Mysqldb
import time
import config
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

class Erc20Data():
    def __init__(self):
        self.db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)
        #self.driver = webdriver.Chrome(executable_path = config.chromedriver)
    
    #由于网络原因，数据获取经常性的中断，所以需要分成多步来获取数据，原则上数据每天更新一次，每天0点启动脚本
    #首先通过时间戳和token id生成erc20_data的唯一标识，并且给每条数据标记未获取
    #通过数据库筛选出来所有未获取的数据得唯一标识和erc20_contract，并更新数据
    def erc20_data_key(self):
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('erc20_data_key', recordDate)
        timestamps = int(time.time())
        sel_str = "SELECT id, erc20_contract from token_base WHERE erc20_contract <> ''"
        
        res_sel = self.db.select(sel_str)
        for token_bace in res_sel:
            insert_str = "INSERT INTO erc20_data (token_id, get_data_time, top100_detail)"
            insert_str += "VALUES (" + str(token_bace[0]) + "," + str(timestamps) + "," + '0' +")"
            
            try:
                self.db.insert(insert_str)
            except Exception as e:
                print(insert_str)
                print('INSERT err internet_data, token_id = ', token_bace[0])
                continue
        
    def get_erc20_data(self):
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('get_erc20_data', recordDate)
        sel_str = "SELECT a.token_id, a.get_data_time, b.erc20_contract from erc20_data as a, token_base as b WHERE (a.token_hold_num = -1 or a.token_tx_num = -1)AND a.token_id = b.id"
        while True:
            res_sel = self.db.select(sel_str)
            print(len(res_sel))
            if len(res_sel) <= 0:
                break
            driver = webdriver.Chrome(executable_path = config.chromedriver)
            for token_bace in res_sel:
                url = config.eth_token_url + token_bace[2]
                driver.get(url)
                try:
                    token_holders = driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_divSummary"]/div[1]/table/tbody/tr[3]/td[2]').text
                    token_holders = token_holders.strip().split(' ')[0]
                    tx_num = driver.find_element_by_xpath('//*[@id="totaltxns"]').text
        
                    updata_str = "UPDATE erc20_data SET token_hold_num=" + token_holders + ", token_tx_num=" + str(tx_num)
                    updata_str += " where token_id =" + str(token_bace[0]) + " and get_data_time=" + str(token_bace[1])

                    try:
                        self.db.update(updata_str)
                    except Exception as e:
                        print(updata_str)
                        print('UPDATE err internet_data, token_id = ', token_bace[0])
                        continue
                except:
                    print('xpath err')
                
            driver.close()
            driver.service.stop()

        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('get_erc20_data', recordDate)
        
        
    def get_top100_hold(self):
        sel_str = "SELECT a.token_id, a.get_data_time, b.erc20_contract, a.token_hold_num from erc20_data as a, token_base as b WHERE top100_detail = 0 AND a.token_id = b.id LIMIT 30"
        
        while True:
            res_sel = self.db.select(sel_str)
            print(len(res_sel))
            if len(res_sel) <= 0:
                break
            driver = webdriver.Chrome(executable_path = config.chromedriver)
            driver.maximize_window()
            for token_bace in res_sel:
                url = config.eth_token_url + token_bace[2]
                driver.get(url)
                try:
                    driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_li_balances"]/a').click()
                except:
                    print('click err', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                    time.sleep(5)
                    continue
                
                try:
                    driver.switch_to_alert().accept()
                except:
                    pass
                
                driver.switch_to.frame('tokeholdersiframe')
                xpath_str = '//*[@id="maintable"]/table/tbody/tr[2]/td[1]'
                try:
                    element_present = EC.text_to_be_present_in_element((By.XPATH, xpath_str),'1')
                    WebDriverWait(driver, 20, 1).until(element_present)
                    
                    recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    if len(driver.find_elements_by_xpath('//*[@id="maintable"]/table/tbody/tr')) < 2:
                        print('rank data len less')
                        continue
                    insert_list = []
                    for element in driver.find_elements_by_xpath('//*[@id="maintable"]/table/tbody/tr'):
                        try:
                            rank = element.find_element_by_xpath('./td[1]').text.replace(',', '')
                            address = element.find_element_by_xpath('./td/span').text.replace(',', '')
                            quantity = element.find_element_by_xpath('./td[3]').text.replace(',', '')
                            percentage = element.find_element_by_xpath('./td[4]').text.replace(',', '').replace('%', '')
                            insert_str = "INSERT INTO hold_top_100 (token_id, get_data_time, rank, address, quantity, percentage, creat_at)"
                            insert_str += "VALUES (" + str(token_bace[0]) + "," + str(token_bace[1]) + "," + str(
                                rank) +  ",'" + str(address)  + "'," + str(quantity) + "," + str(percentage) + ",'" + recordDate + "');"
                            insert_list.append(insert_str)
                        except:
                            continue
                        
                    if token_bace[3] > 50:
                        driver.find_element_by_xpath('//*[@id="PagingPanel"]/a[3]').click()
                        element_present = EC.text_to_be_present_in_element((By.XPATH, xpath_str),'51')
                        WebDriverWait(driver, 20, 1).until(element_present)
                        for element in driver.find_elements_by_xpath('//*[@id="maintable"]/table/tbody/tr'):
                            try:
                                rank = element.find_element_by_xpath('./td[1]').text.replace(',', '')
                                address = element.find_element_by_xpath('./td/span').text.replace(',', '')
                                quantity = element.find_element_by_xpath('./td[3]').text.replace(',', '')
                                percentage = element.find_element_by_xpath('./td[4]').text.replace(',', '').replace('%', '')
                                insert_str = "INSERT INTO hold_top_100 (token_id, get_data_time, rank, address, quantity, percentage, creat_at)"
                                insert_str += "VALUES (" + str(token_bace[0]) + "," + str(token_bace[1]) + "," + str(
                                    rank) +  ",'" + str(address)  + "'," + str(quantity) + "," + str(percentage) + ",'" + recordDate + "');"
                                
                                insert_list.append(insert_str)
                            except Exception as e:
                                continue
                            
                    if len(insert_list) == 100 or len(insert_list) == token_bace[3]:
                        try:
                            self.db.insert_list(insert_list)
                            
                            updata_str = "UPDATE erc20_data SET top100_detail=" + '1'
                            updata_str += " where token_id =" + str(token_bace[0]) + " and get_data_time=" + str(token_bace[1])
                            try:
                                self.db.update(updata_str)
                            except Exception as e:
                                print(updata_str)
                                print('UPDATE err updata_str, token_id = ', token_bace[0])
                        except Exception as e:
                            print('INSERT err internet_data, >50 token_id = ', token_bace[0])
                except:
                    print('Timed out waiting for page to load. token_id:', token_bace[0])
            
            driver.close()
            driver.service.stop()
            time.sleep(5)
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('get_top100_hold', recordDate)
        
    
if __name__ == "__main__":
    erc20_pro = Erc20Data()
    #erc20_pro.erc20_data_key()
    erc20_pro.get_erc20_data()
    erc20_pro.get_top100_hold()
    