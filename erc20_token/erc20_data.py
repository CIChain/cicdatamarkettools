# !usr/bin/python
# -*- coding:utf-8 -*-
from mysqldb import Mysqldb
import time
import common_fun
import config
from selenium import webdriver

class TokenBacePro():
    def __init__(self):
        self.db = Mysqldb()