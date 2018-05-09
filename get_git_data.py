# !/usr/bin/python
# -*- coding:utf-8 -*-

from github_data.github_data import GithubDataPro
import time

if __name__ == "__main__":
    git_pro = GithubDataPro()
    try:
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('start', recordDate)
        git_pro = GithubDataPro()
        git_pro.get_gitaddress()
        git_pro.init_git_detail_data()
        git_pro.get_detail_data()
        recordDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('end', recordDate)
    except:
        print('erc20_pro err')
        
