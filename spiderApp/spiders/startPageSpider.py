from scrapy.spider import Spider
from scrapy.contrib.linkextractors import LinkExtractor
from spiderApp.utils import db
from urlparse import urlparse
from utils import log

import time
import re
import redis
import hashlib

time.asctime()




#######################################################################
class startPageSpider(Spider):

    name = 'startPageSpider'

    def __init__(self, taskId, *a, **kw):
        """Constructor"""
        self.name +='_'+str(taskId)
        super(startPageSpider, self).__init__(*a, **kw)
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        self.redis = redis.Redis(connection_pool=pool)
        self.dbUtils = db.dbUtils()
        self.taskId = int(taskId)

        self.project = None
        self.domain = None
        self.hasCrawlSet = set()
        self.hasInsertSet = set()
        

        project = self.dbUtils.queryRow('SELECT * FROM project_setting WHERE iStatus=1 AND iPid=%d' % self.taskId)
        if  project :
            self.project = project
            self.start_urls = str(project['szStartUrl']).split('~')
            self.domain = ".".join(urlparse(project['szDomain']).hostname.split(".")[-2:])

    def parse(self, response):
        print 'startPageSpider==========================>',response.url
#         log.msg(format='%(iPid)s, %(url)s, %(project)s ', iPid = self.taskId, url = response.url, project=self.project)
        listQuqueCount = self.redis.llen('scrapy:startPageSpider:listQuque:%s' % self.taskId)
        if listQuqueCount == 1:
            self._crawler.signals.send_catch_log('writeListQuque')
        elif listQuqueCount == 0:
            self._crawler.signals.send_catch_log('emptyListQuque')
            print 'startPageSpider---------send_catch_log->emptyListQuque'
        if response.url not in self.hasCrawlSet:
            pattern = re.compile(r'%s' % self.project['szStartUrlReg'])
            self.hasCrawlSet.add(response.url)
            if pattern.match(response.url) and response.url not in self.hasInsertSet:
                title = "|".join(response.xpath('/html/head/title/text()').extract())
                insertSql = 'INSERT INTO project_start_page(iPid, szUrl, szTitle,dtLastScrapyTime) VALUES(%d, "%s", "%s", "%s")' % (self.taskId, response.url,  title,  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
                self.dbUtils.insert(insertSql)
                self.hasInsertSet.add(response.url)
                self.redis.lpush('scrapy:startPageSpider:listQuque:%s' % self.taskId, response.url)
                #self.redis.sadd('scrapy:startPageSpider:startPage:2', response.url)
                log.msg(format='spider=startPageSpider iPid=%(i)s, title=%(t)s url=%(u)s', i = self.taskId, t=title, u=response.url)

            _allow = ( _allow for _allow in self.project['szStartUrlReg'].split('~'))
            self.linkExtractor = LinkExtractor(allow_domains=self.domain, allow=_allow)
            links = [ link for link in self.linkExtractor.extract_links(response) if link.url not in self.hasCrawlSet ]
            for link in links:
                yield self.make_requests_from_url(link.url)
