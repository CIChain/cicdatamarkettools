# !usr/bin/python
# -*- coding:utf-8 -*-
from mysqldb import Mysqldb
import time
import common_fun
import config
from selenium import webdriver
from selenium.common.exceptions import TimeoutException

class GithubDataPro():
    def __init__(self):
        self.db = Mysqldb(config.MySqlHost, config.MySqlUser, config.MySqlPasswd, config.MySqlDb, config.MySqlPort)
        self.git_url = 'https://github.com/eosio'
        
    def get_gitaddress(self):
        sel_str = "SELECT project_name FROM github_base"
        project_name_list = self.db.select(sel_str)
        sel_str = "SELECT id, github_address FROM token_base WHERE github_address <>''"
        res_sel = self.db.select(sel_str)
        for token_bace in res_sel:
            res_html = common_fun.get_url_html(token_bace[1])
            if res_html == 404:
                print('404:', token_bace[1])
                continue
            elif res_html == 500:
                print('500', token_bace[1])
                continue
            elif res_html == '':
                print('err:', token_bace[1])
                continue
            #res_html = common_fun.get_url_html(self.git_url)
            
            for pro_data_xpath in res_html.xpath('//*[@id="org-repositories"]/div[1]/div/li'):
                project_name = pro_data_xpath.xpath('div[1]/h3/a/text()')[0].strip()
                project_address = pro_data_xpath.xpath('div[1]/h3/a/@href')[0].strip()
                if (project_name,) not in project_name_list:
                    project_address = 'https://github.com' + project_address
                    
                    insert_str = "INSERT INTO github_base (token_id, project_name, project_address)"
                    insert_str += "VALUES (" + str(token_bace[0]) + ",'" + project_name + "','" + project_address +"')"
                
                    try:
                        self.db.insert(insert_str)
                    except Exception as e:
                        print(insert_str)
                        print('INSERT err internet_data, token_id = ', token_bace[0])
                        continue
                else:
                    pass
                
    def open_driver(self):
        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        driver = webdriver.Firefox(options=options)
        driver.set_page_load_timeout(20)
        driver.maximize_window()
        return driver
    
    def init_git_detail_data(self):
        sel_str = "SELECT id FROM github_base WHERE project_address <> ''"
        res_sel = self.db.select(sel_str)
        timestamps = int(time.time())
        
        for git_base in res_sel:
            insert_str = "INSERT INTO github_detail_data (project_id, get_data_time)"
            insert_str += "VALUES (" + str(git_base[0]) + "," + str(timestamps) +")"
            
            try:
                self.db.insert(insert_str)
            except Exception as e:
                print(insert_str)
                print('INSERT err internet_data, project_id = ', git_base[0])
                continue
    
    def get_detail_data(self):
        sel_str = "SELECT b.project_id, b.get_data_time, a.project_address from github_base as a, github_detail_data as b WHERE (b.star_num = -1 or b.watch_num = -1 or b.fork_num = -1 or b.commits_num = -1 or b.branches_num = -1 or b.releases_num = -1 or b.contributors_num = -1)AND a.id = b.project_id"
        
        retry_times = 5
        while True:
            res_sel = self.db.select(sel_str)
            retry_times = retry_times -1
            if retry_times < 0:
                for git_base in res_sel:
                    updata_str  = "UPDATE github_detail_data SET star_num=" + str(-2) + ", watch_num=" + str(
                            -2) + ", fork_num=" + str(-2) + ", commits_num=" + str(-2) + ",branches_num=" + str(
                            -2) + ", releases_num=" + str(-2) + ", contributors_num=" + str(-2)
                    updata_str += " where project_id =" + str(git_base[0]) + " and get_data_time=" + str(git_base[1])
                    
                    try:
                        self.db.update(updata_str)
                    except Exception as e:
                        print(updata_str)
                        print('UPDATE err retry_times < 0, gitaddress = ', git_base[2])
                        continue
                break
            driver = self.open_driver()
            for git_base in res_sel:
                try:
                    driver.get(git_base[2])
                except TimeoutException:
                    driver.execute_script('window.stop()')
                
                try:
                    watch_num = driver.find_element_by_xpath('//*[@id="js-repo-pjax-container"]/div[1]/div/ul/li[1]/a[2]').text.replace(',','')
                    star_num = driver.find_element_by_xpath('//*[@id="js-repo-pjax-container"]/div[1]/div/ul/li[2]/a[2]').text.replace(',','')
                    fork_num = driver.find_element_by_xpath('//*[@class="pagehead-actions"]/li[3]/a[2]').text.replace(',','')
                    commit_num = driver.find_element_by_xpath('/html/body/div[4]/div/div/div[2]/div[1]/div[3]/div/div/ul/li[1]/a/span').text.replace(',','')
                    branches_num = driver.find_element_by_xpath('/html/body/div[4]/div/div/div[2]/div[1]/div[3]/div/div/ul/li[2]/a/span').text.replace(',','')
                    releases_num = driver.find_element_by_xpath('/html/body/div[4]/div/div/div[2]/div[1]/div[3]/div/div/ul/li[3]/a/span').text.replace(',','')
                    contributors_num = driver.find_element_by_xpath('/html/body/div[4]/div/div/div[2]/div[1]/div[3]/div/div/ul/li[4]/a/span').text.replace(',','')
                
                    updata_str = "UPDATE github_detail_data SET star_num=" + str(star_num) + ", watch_num=" + str(
                            watch_num) + ", fork_num=" + str(fork_num) + ", commits_num=" + str(commit_num) + ",branches_num=" + str(
                            branches_num) + ", releases_num=" + str(releases_num) + ", contributors_num=" + str(contributors_num)
                    
                    updata_str += " where project_id =" + str(git_base[0]) + " and get_data_time=" + str(git_base[1])
                    
                    try:
                        self.db.update(updata_str)
                    except Exception as e:
                        print(updata_str)
                        print('UPDATE err, git_address = ', git_base[2], '-----project_id = ', git_base[0])
                        continue
                except:
                    pass
                    
            driver.close()
            driver.service.stop()

            
if __name__ == "__main__":
    recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print('start', recordDate)
    git_pro = GithubDataPro()
    git_pro.get_gitaddress()
    git_pro.init_git_detail_data()
    git_pro.get_detail_data()
    recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print('end', recordDate)