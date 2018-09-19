#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
__author__ = 'Kandy.Ye'
__mtime__ = '2017/4/12'
"""

import logging
import json
import requests
import MySQLdb
from scrapy import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from JDSpider.items import *
from twisted.enterprise import adbapi

key_word = ['book', 'e', 'channel', 'mvd', 'list']
Base_url = 'https://list.jd.com'
price_url = 'https://p.3.cn/prices/mgets?skuIds=J_'
price_prefix = 'https://item.jd.com/'
price_after = '.html'
comment_url = 'https://club.jd.com/comment/productPageComments.action?productId=%s&score=0&sortType=5&page=%s&pageSize=10'
favourable_url = 'https://cd.jd.com/promotion/v2?skuId=%s&area=1_72_2799_0&shopId=%s&venderId=%s&cat=%s'

from selenium import webdriver
import time
from datetime import datetime


class JDSpider(Spider):
    name = "JDSpider"
    allowed_domains = ["jd.com"]
    conn = MySQLdb.connect(host="localhost", user="root", passwd="root", db="test", charset="utf8")
    cursor = conn.cursor()
    # start_urls = [
    #     'https://www.jd.com/allSort.aspx'
    # ]
    sql="SELECT product_id,url from product_item WHERE original_price=0.0000 OR really_price=0.0000 LIMIT 0,100000"
    cursor.execute(sql)
    result = cursor.fetchall()
    conn.close()
    logging.getLogger("requests").setLevel(logging.ERROR)  # 将requests的日志级别设成WARNING


    def start_requests(self):
        for url in self.result:
            target_url=price_url+str(url[0])
            yield Request(url=target_url, callback=self.parse_product_price)

    def parse_product_price(self, response):
        # print(url)
        # try:
        # print("you get response can you save data")
        # self.chrome_opt = webdriver.ChromeOptions()
        # # 设置chromedriver不加载图片
        # self.prefs = {"profile.managed_default_content_settings.images": 2}
        # self.chrome_opt.add_experimental_option("prefs", self.prefs)
        # self.driver = webdriver.Chrome(chrome_options=self.chrome_opt,executable_path='D:\Software\chromedriver_win32/chromedriver.exe')
        # # self.driver = webdriver.Chrome(executable_path='E:/video course/Python/py3.5.2/Lib/site-packages/selenium/webdriver/chromedriver.exe')
        # # self.driver = webdriver.PhantomJS(executable_path='D:\Software\phantomjs\bin\phantomjs.exe')
        # self.driver.maximize_window()
        # self.driver.get(response.)
        #     # time.sleep(0.2)
        #     consult = self.driver.find_element_by_id("jd-price")
        #     price = consult.text.replace(u'￥', u'')
        #
        # except Exception as e:
        #     print(e)
        #     pass
        productsItem = ProductsItem()
        try:
            price_json = eval(response.text.strip()[1:-1])
            productsItem['reallyPrice'] = price_json['p']
            productsItem['_id'] = price_json['id'][2:]
        except Exception as e:
            self.chrome_opt = webdriver.ChromeOptions()
            self.chrome_opt.add_argument("headless")
            # 设置chromedriver不加载图片
            self.prefs = {"profile.managed_default_content_settings.images": 2}
            self.chrome_opt.add_experimental_option("prefs", self.prefs)
            self.driver = webdriver.Chrome(chrome_options=self.chrome_opt,executable_path='D:\Software\chromedriver_win32/chromedriver.exe')
            # self.driver = webdriver.Chrome(executable_path='E:/video course/Python/py3.5.2/Lib/site-packages/selenium/webdriver/chromedriver.exe')
            # self.driver = webdriver.PhantomJS(executable_path='D:\Software\phantomjs\bin\phantomjs.exe')
            # self.driver.maximize_window()
            myurl = price_prefix+response.url[37:]+price_after
            productId = response.url[37:]
            productsItem['_id'] = productId
            self.driver.get(price_prefix+productId+price_after)
                # time.sleep(0.2)
            try:
                consultPrice1 = self.driver.find_element_by_id("jd-price")
                price = consultPrice1.text.replace(u'￥', u'')
                productsItem['reallyPrice'] = price
                print("jd-price")
            except Exception as e :
                consultPrice = self.driver.find_element_by_class_name("p-price")
                priceConsult = consultPrice.text.replace(u'￥', u'')
                productsItem['reallyPrice'] = priceConsult
                print("p-price")
            # price = consult.text.replace(u'￥', u'')
            # productsItem['reallyPrice'] = price

            productsItem['originalPrice'] = 0
            print(productsItem)
            self.driver.quit()
        yield productsItem

