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

class Erc20Data():
    def __init__(self):
        self.db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)
        chromedriver = 'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe'
        self.driver = webdriver.Chrome(executable_path = chromedriver)
          
    def get_erc20_data(self):
        sel_str = "SELECT id, erc20_contract from token_base WHERE erc20_contract <> ''"
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(recordDate)
        timestamps = int(time.time())
        
        res_sel = self.db.select(sel_str)
        for token_bace in res_sel:
            time.sleep(1)
            id_timestamps = str(token_bace[0]) + '_' + str(timestamps)
            url = config.eth_token_url + token_bace[1]
            self.driver.get(url)
            
            token_holders = self.driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_divSummary"]/div[1]/table/tbody/tr[3]/td[2]').text
            token_holders = token_holders.strip().split(' ')[0]
            tx_num = self.driver.find_element_by_xpath('//*[@id="totaltxns"]').text
            insert_str = "INSERT INTO erc20_data (id_timestamps, token_id, token_hold_num, token_tx_num)"
            insert_str += "VALUES ('" + id_timestamps + "'," + str(token_bace[0])  + "," + str(token_holders) + "," + str(tx_num) +")"
            
            try:
                self.db.insert(insert_str)
            except Exception as e:
                print(insert_str)
                print('INSERT err internet_data, token_id = ', token_bace[0])
                continue
                
            self.driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_li_balances"]/a').click()
            self.get_top100_hold(id_timestamps)
        
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(recordDate)
            
    def get_top100_hold(self, id_timestamps):
        self.driver.switch_to.frame('tokeholdersiframe')
        xpath_str = '//*[@id="maintable"]/table/tbody/tr[2]/td[1]'
        
        try:
            element_present = EC.text_to_be_present_in_element((By.XPATH, xpath_str),'1')
            WebDriverWait(self.driver, 20, 1).until(element_present)
            
            recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            for element in self.driver.find_elements_by_xpath('//*[@id="maintable"]/table/tbody/tr'):
                try:
                    rank = element.find_element_by_xpath('./td[1]').text
                    address = element.find_element_by_xpath('./td/span').text
                    quantity = element.find_element_by_xpath('./td[3]').text
                    percentage = element.find_element_by_xpath('./td[4]').text
                    percentage = percentage.replace('%', '')
                                                              
                    insert_str = "INSERT INTO hold_top_100 (id_timestamps, rank, address, quantity, percentage, creat_at)"
                    insert_str += "VALUES ('" + id_timestamps + "'," + str(rank) +  ",'" + str(address)  + "'," + str(quantity) + "," + str(percentage) + ",'" + recordDate + "')"
                    
                    try:
                        self.db.insert(insert_str)
                    except Exception as e:
                        print(insert_str)
                        print('INSERT err internet_data, id_timestamps = ', id_timestamps)
                except:
                    continue
                
            self.driver.find_element_by_xpath('//*[@id="PagingPanel"]/a[3]').click()
            element_present = EC.text_to_be_present_in_element((By.XPATH, xpath_str),'51')
            WebDriverWait(self.driver, 20, 1).until(element_present)
            for element in self.driver.find_elements_by_xpath('//*[@id="maintable"]/table/tbody/tr'):
                try:
                    rank = element.find_element_by_xpath('./td[1]').text
                    address = element.find_element_by_xpath('./td/span').text
                    quantity = element.find_element_by_xpath('./td[3]').text
                    percentage = element.find_element_by_xpath('./td[4]').text
                    percentage = percentage.replace('%', '')
                                                              
                    insert_str = "INSERT INTO hold_top_100 (id_timestamps, rank, address, quantity, percentage, creat_at)"
                    insert_str += "VALUES ('" + id_timestamps + "'," + str(rank) +  ",'" + str(address)  + "'," + str(quantity) + "," + str(percentage) + ",'" + recordDate + "')"
                    
                    try:
                        self.db.insert(insert_str)
                    except Exception as e:
                        print(insert_str)
                        print('INSERT err internet_data, id_timestamps = ', id_timestamps)
                except:
                    continue
        except:
            print('Timed out waiting for page to load. id_timestamps:', id_timestamps)
                

        
    def driver_close(self):
        print()
        self.driver.close()
        
        
if __name__ == "__main__":
    erc20_pro = Erc20Data()
    erc20_pro.get_erc20_data()
    erc20_pro.driver_close()