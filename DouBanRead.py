#!/usr/bin/env python
# coding=utf8

# version:2.0
# kali linux python 2.7.13
# author:TClion
# update:2017-10-07
# 豆瓣读书信息抓取，redis去重，数据存入mongodb中

import time
import redis
import gevent
import urllib
import random
import pymongo
import requests

from lxml import etree
from gevent import monkey
from urlparse import urljoin
from gevent.pool import Pool
monkey.patch_all()

class spider():
    def __init__(self):
        self.Url = 'https://book.douban.com/tag/?view=type'
        self.conn = pymongo.MongoClient('localhost', 27017)
        self.R = redis.Redis(host='localhost', port=6379)
        self.db = self.conn['douban']
        self.data_coll = self.db['booktest']
        self.ip_coll = self.conn['ipdb']['ip_good']
        self.redis_key = 'booktest'
        self.ip_lst = self.get_ip_lst_m()
        self.ip = random.choice(self.ip_lst)

    #从ip库中读取ip
    def get_ip_lst_m(self):
        ip_lst = []
        for item in self.ip_coll.find():
            ip_str = item['ip']
            ip = ip_str.split(':')[0]
            port = ip_str.split(':')[1]
            new_ip_str = 'http://' + ip + ':' + port
            ip_dict = {
                'http': new_ip_str,
                'https': new_ip_str,
            }
            ip_lst.append(ip_dict)
        return ip_lst

    #目录链接
    def parse_item_lst(self):
        content = requests.get(self.Url).content
        data = etree.HTML(content)
        item_lst = data.xpath('//table[@class="tagCol"]/tbody/tr/td/a/text()')
        self.item_lst = ['https://book.douban.com/tag/' + urllib.quote(u.encode('utf-8')) for u in item_lst]

    #使用代理ip打开网页,返回content
    def open_url(self, url):
        while True:
            try:
                page = requests.get(url, proxies=self.ip, timeout=5)
                break
            except:
                try:
                    self.ip_lst.remove(self.ip)
                    self.ip = random.choice(self.ip_lst)
                except:
                    print 'ip list empty'
                    self.ip_lst = self.get_ip_lst_m()
        content = page.text
        return content

    #抓取列表页url
    def parse_info_url(self, url):
        content = self.open_url(url)
        data = etree.HTML(content)
        url_lst = data.xpath('//div[@class="info"]/h2/a/@href')
        for i in url_lst:
            self.R.sadd(self.redis_key, i)
        print 'list url %s parse successful' % url
        try:
            next_page = data.xpath('//span[@class="next"]/a/@href')[0]
        except:
            return
        new_url = urljoin(url, next_page)
        self.parse_info_url(new_url)

    #抓取详情页信息
    def parse(self, url):
        if self.data_coll.find_one({'url': url}) != None:
            return
        content = self.open_url(url)
        if u'呃...你想访问的页面不存在' in content:
            return
        data = etree.HTML(content)
        try:
            title = data.xpath('//h1/span[1]/text()')[0]
        except:
            return
        post = {
            'url': url,
            'title': title,
        }
        self.data_coll.insert(post)
        print 'info url %s parse successful' % url


if __name__ == '__main__':
    douban = spider()
    douban.parse_item_lst()
    gp = Pool(100)
    gp.map(douban.parse_info_url, douban.item_lst)

    # gp2 = Pool(200)
    # gp2.map(douban.parse, douban.R.smembers(douban.redis_key))



