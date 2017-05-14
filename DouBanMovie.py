"""豆瓣电影爬虫，抓取豆瓣电影上的电影信息，放入Mongodb中"""
import requests
from lxml import  etree
import urllib.parse
import time
import pymongo


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
<<<<<<< HEAD
        conn = pymongo.MongoClient('localhost',27017)     #连接mongodb
        self.db = conn.douban
        self.movie = self.db.movie
=======
        self.L = []                 #去重列表
        self.N = 0                  #计数器
        self.db = pymysql.connect('localhost','root','password','doubanmovie',charset='utf8')  #连接数据库
        self.cursor = self.db.cursor()
        self.db.autocommit(True)
        manager = multiprocessing.Manager()     #创建队列
        self.Queue = manager.Queue()
>>>>>>> origin/master



    #获取电影所有分类
    def GetList(self):
        bookhtml = requests.get(self.Url,headers=moviehead).text
        Item = etree.HTML(bookhtml)
        List = Item.xpath('//table[@class="tagCol"]/tbody/tr/td/a/text()')
        self.classlist = [self.Url+urllib.parse.quote(x) for x in List]
        self.ClassList = list(zip(List,self.classlist))
        return self.ClassList


    #抓取电影信息url
    def GetMovie(self,key,url):
<<<<<<< HEAD
        for i in range(0,200,20):       #制作分页url
=======
        for i in range(0,20,20):       #制作分页url
>>>>>>> origin/master
            Url =url + '?start='+str(i)+'&type=T'
            Moviehtml = requests.get(Url,headers=moviehead).text
            movielist = etree.HTML(Moviehtml)
            Movieurl = movielist.xpath('//div[@class="pl2"]/a/@href')
            for u in Movieurl:
                if self.movie.find_one({'url':u})==None:    #如果数据库中没有影片对应的url就抓取
                    self.GetInfo(key,u)
            time.sleep(2)
        print('%s抓取完毕' % key)


    def GetInfo(self,key,movieurl):
        MovieHtml = requests.get(movieurl,headers=moviehead).text
        InfoPage = etree.HTML(MovieHtml)
        try:
            moviename = InfoPage.xpath('//a[@data-mode="plain"]/@data-name')[0]
            basicinfo = InfoPage.xpath('//a[@data-mode="plain"]/@data-desc')[0]
            evaluate  = ' '.join(InfoPage.xpath('//div[@class="indent"]/span/text()'))
            #hotreview = ' '.join(InfoPage.xpath('//div[@class="comment"]/p/text()'))
            Moviename = moviename.replace('\u200e ','')
            #eva = evaluate.replace(' ','').replace('\n','').replace('\u3000\u3000','').strip()
            result = [key,Moviename,movieurl,basicinfo]
            time.sleep(2)       #设置间隔时间
            self.SaveMovie(result)
        except Exception as e:
            print(e)

    #向mongodb中插入数据
    def SaveMovie(self,List):
        D = {}
        D['key'] = List[0]
        D['moviename'] = List[1]
        D['url'] = List[2]
        D['info'] = List[3]
        self.movie.insert(D)
        print(D)


if __name__ == '__main__':
    movie = DouBan()
<<<<<<< HEAD
    movielist = movie.GetList()
    for key,url in movielist:
        movie.GetMovie(key,url)
=======
    movie.CreateTable1()
    movie.GetList()
    pool = Pool(processes=1)
    for key,url in movie.ClassList:
        pool.apply_async(movie.GetMovie,(key,url,))
    movie.GetInfo()
    pool.close()
    pool.join()
>>>>>>> origin/master
