"""豆瓣电影爬虫，抓取豆瓣电影上的电影信息，需要填写自己的数据库信息"""
import requests
from lxml import  etree
import urllib.parse
import pymysql
from multiprocessing.dummy import Pool
import time
import multiprocessing


moviehead = {
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding':'gzip, deflate, sdch, br',
    'Accept-Language':'zh-CN,zh;q=0.8',
    'Cache-Control':'max-age=0',
    'Connection':'keep-alive',
    'Host':'movie.douban.com',
    'Upgrade-Insecure-Requests':'1',
    'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
}



class DouBan():
    def __init__(self):
        self.Url = "https://movie.douban.com/tag/"      #豆瓣电影分类
        self.L = []                 #去重列表
        self.N = 0                  #计数器
        self.db = pymysql.connect('localhost','root','topcoder','doubanmovie',charset='utf8')  #连接数据库
        self.cursor = self.db.cursor()
        self.db.autocommit(True)
        manager = multiprocessing.Manager()     #创建队列
        self.Queue = manager.Queue()



    #获取电影所有分类
    def GetList(self):
        bookhtml = requests.get(self.Url,headers=moviehead).content
        Item = etree.HTML(bookhtml)
        List = Item.xpath('//tbody/tr/td/a/text()')
        self.classlist = [self.Url+urllib.parse.quote(x) for x in List]
        self.ClassList = list(zip(List,self.classlist))
        print(self.ClassList)


    #抓取电影信息url
    def GetMovie(self,key,url):
        for i in range(0,40,20):       #制作分页url
            Url =url + '?start='+str(i)+'&type=T'
            Moviehtml = requests.get(Url,headers=moviehead).text
            movielist = etree.HTML(Moviehtml)
            Movieurl = movielist.xpath('//div[@class="pl2"]/a/@href')
            for movieurl in Movieurl:
                if movieurl not in self.L:  #去重
                    self.L.append(movieurl)
                    self.Queue.put(movieurl)
        if len(self.L)>5000:
            self.L = []
        self.Queue.put('stop')  #代表一个分类抓取完毕
        time.sleep(10)


    def GetInfo(self):
        while True:
            if self.N == 136:   #豆瓣电影一共136个分类
                break
            if not self.Queue.empty():
                movieurl = self.Queue.get()
                if movieurl == 'stop':
                    print(self.ClassList[self.N][0]+'抓取完毕')
                    self.N+=1
                    continue
                MovieHtml = requests.get(movieurl,headers=moviehead).text
                InfoPage = etree.HTML(MovieHtml)
                try:
                    moviename = InfoPage.xpath('//a[@data-mode="plain"]/@data-name')[0]
                    basicinfo = InfoPage.xpath('//a[@data-mode="plain"]/@data-desc')[0]
                    evaluate  = ' '.join(InfoPage.xpath('//div[@class="indent"]/span/text()'))
                    hotreview = ' '.join(InfoPage.xpath('//div[@class="comment"]/p/text()'))
                    Moviename = moviename.replace('\u200e ','')
                    eva = evaluate.replace(' ','').replace('\n','').replace('\u3000\u3000','').strip()
                    result = [key,Moviename,basicinfo,eva]
                    time.sleep(2)       #设置间隔时间
                    self.SaveMovie(result)
                except Exception as e:
                    print(e)
                    self.db.rollback()

    #创建储存信息的表
    def CreateTable1(self):
        try:
            sql1 = """create table movie(
                      classify char(200) not null,
                      moviename char(200) not null,
                      info text not null,
                      eva text not null
)"""
            self.cursor.execute(sql1)
        except Exception as e:
            print(e)
            self.db.rollback()

    #存放入库
    def SaveMovie(self,List):
        try:
            sqli = """insert into movie values (%s,%s,%s,%s)"""
            self.cursor.execute(sqli,List)
        except Exception as e:
            print(e)
            self.db.rollback()


if __name__ == '__main__':
    movie = DouBan()
    movie.CreateTable1()
    movie.GetList()
    pool = Pool(processes=1)
    for key,url in movie.ClassList:
        pool.apply_async(movie.GetMovie,(key,url,))
    movie.GetInfo()
    pool.close()
    pool.join()