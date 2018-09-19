# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import csv

import pandas as pd
from JDSpider.items import *
from scrapy import log
import MySQLdb.cursors
from twisted.enterprise import adbapi

from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals


class JdspiderPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool
        # Instantiate DB
        # self.dbpool = adbapi.ConnectionPool('MySQLdb',
        #                                     host='locahost',
        #                                     user='root',
        #                                     passwd='root',
        #                                     port='3306',
        #                                     db='test',
        #                                     charset='utf8',
        #                                     use_unicode=True,
        #                                     cursorclass=MySQLdb.cursors.DictCursor
        #                                     )
        # dispatcher.connect(self.close_spider, signals.spider_closed)

        # self.categoryCSV = open('category.csv', 'a', newline='')  # 设置newline，否则两行之间会空一行
        # self.categoryWriter = csv.writer(self.categoryCSV)
        # self.csvProductFile = open('product.csv', 'a', newline='')  # 设置newline，否则两行之间会空一行
        # self.productWriter = csv.writer(self.csvProductFile)
        # self.csvShopFile = open('shop.csv', 'a', newline='')  # 设置newline，否则两行之间会空一行
        # self.shopWriter = csv.writer(self.csvShopFile)
        # self.csvCommentFile = open('CommentItem.csv', 'a', newline='')  # 设置newline，否则两行之间会空一行
        # self.commentWriter = csv.writer(self.csvCommentFile)

        # self.Categories = pd.DataFrame()
        # self.Products = pd.DataFrame()
        # self.Shop = pd.DataFrame()
        # self.Comment = pd.DataFrame()
        # self.CommentImage = pd.DataFrame()
        # self.CommentSummary = pd.DataFrame()
        # self.HotCommentTag = pd.DataFrame()

    @classmethod
    def from_settings(cls, settings):
        dbparms = dict(
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DBNAME"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PASSWORD"],
            charset='utf8',
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool("MySQLdb", **dbparms)

        return cls(dbpool)

    def close_spider(self, spider):
        # self.Categories.to_csv("categories.csv", sep='\t')
        # self.Products.to_csv("products.csv", sep='\t')
        # self.Shop.to_csv("shop.csv", sep='\t')
        # self.Comment.to_csv("comment.csv", sep='\t')
        # self.CommentImage.to_csv("commentImage.csv", sep='\t')
        # self.CommentSummary.to_csv("commentSummary.csv", sep='\t')
        # self.HotCommentTag.to_csv("hotCommentTag.csv", sep='\t')
        # self.categoryCSV.close()
        # self.csvProductFile.close()
        # self.csvShopFile.close()
        # self.csvCommentFile.close()
        print("End of Program")

    def process_item(self, item, spider):
        """ 判断item的类型，并作相应的处理，再入数据库 """

        if isinstance(item, ProductsItem):
            try:
                query = self.dbpool.runInteraction(self._insert_product_record, item)
                query.addErrback(self.handle_error, item, spider)
            except Exception:
                pass

        return item

    def _insert_product_record(self, cursor, item):
        product_id = int(item['_id'])
        really_price = item['reallyPrice']
        # update_sql="""update product_item set really_price='%s' where product_id = '%d'
        # """
        sqlOpt = "update product_item set really_price='%s' where product_id = '%d'" % (
            really_price, product_id)
        cursor.execute(sqlOpt)
        print("_insert_product_record ")


    def handle_error(self, failure, item, spider):
        # 处理异步插入的异常
        print(failure)
