from scrapy.spider import Spider
from scrapy.contrib.linkextractors import LinkExtractor
from spiderApp.utils import db
from urlparse import urlparse
from utils import log

import time
import redis


########################################################################
class listPageSpider(Spider):

    name = 'listPageSpider'

    def __init__(self, taskId, *a, **kw):
        """Constructor"""
        self.name +='_'+str(taskId)
        super(listPageSpider, self).__init__(*a, **kw)
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        self.redis = redis.Redis(connection_pool=pool)
        self.dbUtils = db.dbUtils()
        self.taskId = int(taskId)
        self.domain = None
        self.project = None
        self.hasCrawlSet = set()
        self.hasInsertSet = set()
        self.isExit = 0
        project = self.dbUtils.queryRow('SELECT * FROM project_setting WHERE iStatus=1 AND iPid=%d' % self.taskId)
        if project :
            self.project = project
            self.domain = ".".join(urlparse(project['szDomain']).hostname.split(".")[-2:])
#             self.start_urls = ['http://www.ty2016.com/cn/2.html', 'http://www.ty2016.com/cn/3.html', 'http://www.ty2016.com/cn/4.html']


    def stopSpider(self):
        self.isExit = 1

    def getStartUrl(self):
        url = self.redis.rpop('scrapy:startPageSpider:listQuque')
        if not url:
            self.getStartUrl()
        return url


    def start_requests(self):
#         url = self.getStartUrl()
#         print '=====================>',url
#         yield self.make_requests_from_url(url)
        while True :
            #if self._crawler.engine is not None:
                #if self._crawler.engine.paused: break
                #if not self._crawler.engine.running: break
            url = self.redis.rpop('scrapy:startPageSpider:listQuque:%s' % self.taskId)
            #print 'listPageSpider==========================>',url
            if url:
                #self.redis.sadd('scrapy:startPageSpider:startPage:1', url)
                yield self.make_requests_from_url(url)
            #else:
                #self._crawler.signals.send_catch_log('emptyListQuque')
                #print 'listPageSpider---------send_catch_log->emptyListQuque'


    def parse(self, response):
        #self.redis.sadd('scrapy:startPageSpider:startPage:3', response.url)
        if response.url not in self.hasCrawlSet:
            #self.redis.sadd('scrapy:startPageSpider:startPage:4', response.url)
            self.hasCrawlSet.add(response.url)
            _allow = ( _allow for _allow in self.project['szUrlReg'].split('~'))
            self.linkExtractor = LinkExtractor(allow_domains=self.domain, allow=_allow)
            links = [ link for link in self.linkExtractor.extract_links(response) if link.url not in self.hasInsertSet ]
            #self.redis.hset('scrapy:startPageSpider:listPage:count', response.url, len(links))
            for link in links:
                if link.url in self.hasInsertSet : continue
                insertSql = 'INSERT INTO project_list_page(iPid, szUrl, szTitle, szSourceUrl,dtLastScrapyTime) VALUES(%d, "%s", "%s", "%s", "%s")' % (self.taskId,link.url, link.text, response.url, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
                self.dbUtils.insert(insertSql)
                self.hasInsertSet.add(link.url)
                log.msg(format='spider=listPageSpider iPid=%(i)s, title=%(t)s url=%(u)s', i = self.taskId, t=link.text, u=link.url)

