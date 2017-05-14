"""豆瓣读书的爬虫，主要抓取豆瓣读书上书籍的信息，包括书名，图片链接，作者，出版社，价钱等内容。
并且存放在数据库中,需要填上自己的数据库信息"""

import requests
from lxml import etree
import pymysql
import urllib.parse
import re
from multiprocessing.dummy import Pool
import time
import multiprocessing



bookheader = {
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding':'gzip, deflate, sdch, br',
    'Accept-Language':'zh-CN,zh;q=0.8',
    'Cache-Control':'max-age=0',
    'Connection':'keep-alive',
    'Host':'book.douban.com',
    'Upgrade-Insecure-Requests':'1',
    'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
}

class DouBanBook():
    def __init__(self):
        self.Url = "https://book.douban.com/tag/" #豆瓣读书分类
        self.S = requests.Session()
        self.N = 0          #计数器
        self.L = []         #去重列表
        self.db = pymysql.connect('localhost','root','password','doubanbook',charset='utf8') #连接数据库
        self.cursor = self.db.cursor()
        self.db.autocommit(True)
        manager = multiprocessing.Manager() #创建队列
        self.Queue = manager.Queue()



    #取得读书所有分类的标题和url
    def GetList(self):
        bookhtml = self.S.get(self.Url,headers=bookheader).content
        Item = etree.HTML(bookhtml)
        List = Item.xpath('//tbody/tr/td/a/text()')
        self.classlist = [self.Url+urllib.parse.quote(x) for x in List]
        self.ClassList = list(zip(List,self.classlist))
        print(self.ClassList)



    #抓取每个书籍的url
    def GetBook(self,key,url):
        for k in range(0,20,20):       #分页url
            Url =  url +'?start='+str(k)+'&type=T'
            bookhtml = requests.get(Url,headers=bookheader).text
            booklist = etree.HTML(bookhtml)
            Bookurl = booklist.xpath('//div[@class="info"]/h2/a/@href')
            for u in Bookurl:
                if u not in self.L:     #去重
                    self.L.append(u)
                    self.Queue.put(u)
        if len(self.L)>5000:
            self.L = []
        self.Queue.put('stop')          #代表一个分类抓取完毕
        time.sleep(10)


    #从url抓取信息
    def GetInfo(self):
        while True:
            if self.N == 145:       #豆瓣读书一共145个分类
                break
            if not self.Queue.empty():
                u = self.Queue.get()
                if u == 'stop':
                    print(self.ClassList[self.N][0]+'抓取完毕')
                    self.N+=1
                    continue
                BookHtml = requests.get(u,headers=bookheader).text
                BookInfo = etree.HTML(BookHtml)
                try:
                    bookname = BookInfo.xpath('//a[@class="nbg"]/@title')[0]
                    imgurl   = BookInfo.xpath('//a[@class="nbg"]/@href')[0]
                    author   = BookInfo.xpath('//span/a[@class=""]/text()')[0]
                    publish  = re.compile(r'出版社:</span>(.*)<br/>')
                    Publish = publish.findall(BookHtml)[0]
                    bornyear = re.compile(r'出版年:</span>(.*)<br/>')
                    Bornyear = bornyear.findall(BookHtml)[0]
                    price    = re.compile(r'定价:</span>(.*)<br/>')
                    Price = price.findall(BookHtml)[0]
                    ISBN     = re.compile(r'ISBN:</span>(.*)<br/>')
                    isbn = ISBN.findall(BookHtml)[0]
                    score    = BookInfo.xpath('//strong[@class="ll rating_num "]/text()')[0]
                    number   = BookInfo.xpath('//a[@class="rating_people"]/span/text()')[0]
                    #evaluate = ' | '.join(BookInfo.xpath('//div[@class="intro"]/p/text()'))
                    #review   = ' | '.join(BookInfo.xpath('//p[@class="comment-content"]/text()'))
                    self.List = [key,bookname,imgurl,author,Publish,Bornyear,Price,isbn,score,number]
                    self.SaveData(self.List)
                    time.sleep(2)
                except Exception as e:
                    print(e)
                    self.db.rollback()

    #创建储存书籍信息的表
    def CreateTable(self):
        try:
            sql = """create table book(
                      classify char(100) not null,
                      bookname char(200) not null,
                      url char(200) not null,
                      author char(150) not null,
                      publish char(200) not null,
                      bornyear char(100) not null,
                      price char(100) not null,
                      isbn text not null,
                      score char(100) not null,
                      pnumber char(100) not null
)"""
            self.cursor.execute(sql)
        except Exception as e:
            print(e)
            self.db.rollback()

    #存库
    def SaveData(self,List):
        try:
            sql2 = """insert into book values"""
            sql2+="""(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            self.cursor.execute(sql2,List)
        except Exception as e:
            print(e)
            self.db.rollback()


if __name__ == '__main__':
    Book = DouBanBook()
    Book.CreateTable()
    Book.GetList()
    pool = Pool(processes=1)
    for key,url in Book.ClassList:
        pool.apply_async(Book.GetBook,(key,url,))
    Book.GetInfo()
    pool.close()
    pool.join()
