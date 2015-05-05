'''
Created on 2015-4-11

@author: SmokeAngle0421
'''
from scrapy.crawler import Crawler
from twisted.internet import defer

import redis

class appCrawler(Crawler):
    def __init__(self, settings):
        Crawler.__init__(self, settings)
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        self.redis = redis.Redis(connection_pool=pool)

    @defer.inlineCallbacks
    def start(self):
        yield defer.maybeDeferred(self.configure)
        if self._spider and self.redis.scard('scrapy:startPageSpider:listQuque') > 0:
            yield self.engine.open_spider(self._spider, self._start_requests())
        yield defer.maybeDeferred(self.engine.start)