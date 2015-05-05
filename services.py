'''
Created on 2015-4-9

@author: SmokeAngle0421
'''
from twisted.application import internet,service
from spiderApp.spiders.startPageSpider import startPageSpider
from spiderApp.spiders.listPageSpider import listPageSpider
from scrapy.utils.project import get_project_settings
from scrapy.crawler import Crawler
from scrapy import signals
from utils import log

from appCrawler import appCrawler

settings = get_project_settings()
class spiderService(service.MultiService):

    def __init__(self, taskId, taskService):
        service.MultiService.__init__(self)
        self.taskId = taskId
        self.taskService = taskService

        _startPageSpiderService = startPageSpiderService(self)
        _startPageSpiderService.setName('startPageSpiderService_%s' % taskId)
        _listPageSpiderService = listPageSpiderService(self)
        _listPageSpiderService.setName('listPageSpiderService_%s' % taskId)
        self.addService(_startPageSpiderService)
        self.addService(_listPageSpiderService)
        self._startPageSpiderService = _startPageSpiderService
        self._listPageSpiderService = _listPageSpiderService

    def startService(self):
        service.MultiService.startService(self)
        log.msg(format='start spiderService serviceName=(%(serviceName)s)',serviceName=self.name)
        self._startPageSpiderService._crawler.signals.connect(self._listPageSpiderService.startCrawl, 'writeListQuque')
        self._startPageSpiderService._crawler.signals.connect(self._listPageSpiderService.pausedCrawl, 'emptyListQuque')
        self._listPageSpiderService._crawler.signals.connect(self._listPageSpiderService.pausedCrawl, 'emptyListQuque')


    def removeSpiderService(self):
        _running = 0
        for serviceName in self.namedServices:
            if self.namedServices[serviceName].running == 1:
                _running = 1
                break

        if _running == 0 :
            log.msg(format='spiderService->removeSpiderService stop spiderService serviceName=(%(serviceName)s)',serviceName=self.name)
            if self.name in self.taskService.namedServices:
                self.taskService.removeService(self)


    def stopService(self):
        log.msg(format='spiderService->stopService stop spiderService serviceName=(%(serviceName)s)',serviceName=self.name)
        service.MultiService.stopService(self)
        self.removeSpiderService()



class startPageSpiderService(service.Service):

    def __init__(self, parent):
        self.spiderService = parent
        self._crawler = Crawler(settings)
        self._crawler.configure()
        self._spider = startPageSpider(taskId=self.spiderService.taskId)

    def getStats(self):
        return self._crawler.stats.get_stats()

    def startService(self):
        service.Service.startService(self)
        #dispatcher.connect(self.stopService, signals.spider_closed)
        self._crawler.signals.connect(self.stopService, signals.spider_closed)
#         self._crawler.signals.connect(self.test2, 'writeListQuque')
        #_startPageSpider = startPageSpider(taskId=self.spiderService.taskId)
        self._crawler.crawl(self._spider)
        #self._crawler.start()
        self.startCrawl()
        
    def startCrawl(self):
        if not self._crawler.engine.running:
            self._crawler.start()
#     def test2(self):
#         print '================>111111111111111111111111<=========================='
    def stopService(self):
        log.msg(format='startPageSpiderService->stopService stop startPageSpiderService serviceName=(%(serviceName)s)',serviceName=self.name)
        service.Service.stopService(self)
        self.spiderService.removeSpiderService()
        self._crawler.stop()
        if self.name in self.spiderService.namedServices:
            self.spiderService.removeService(self)

class listPageSpiderService(service.Service):
    def __init__(self, parent):
        self.spiderService = parent
        self._crawler = Crawler(settings)
        self._crawler.configure()
        self._spider = listPageSpider(taskId=self.spiderService.taskId)
        
    def getStats(self):
        return self._crawler.stats.get_stats()
    def startService(self):
        service.Service.startService(self)
        self._crawler.signals.connect(self.stopService, signals.spider_closed)
        #_listPageSpider = listPageSpider(taskId=self.spiderService.taskId)
#         self._crawler.start()

    def startCrawl(self):
        print '------------->listPageSpiderService->startCrawl'
        if self._crawler._spider is None:
            self._crawler.crawl(self._spider)
        else:
            print '>>>>>>>>>>>>>>>>>>>>>',self._crawler._spider
        if not self._crawler.engine.running:
            print '>>>>>>>>>>>>>>>>>>>>> _crawler.engine.running'
            self._crawler.start()
        else:
            if self._crawler.engine.paused :
                print '>>>>>>>>>>>>>>>>>>>>> _crawler.engine.unpause'
                if self._crawler._spider is not None:
                    print '>>>>>>>>>>>>>>>>>>>>> _crawler._spider.start_requests()'
                    self._crawler._spider.start_requests()
                    
                self._crawler.engine.unpause()
        

    def pausedCrawl(self):
        print 'listPageSpiderService->pausedCrawl'
        if self._crawler._spider is not None:
            if not self.spiderService._startPageSpiderService._crawler.engine.running:
                print '------------------->_crawler.stop()'
                self._crawler.stop()
            else:
                if not self._crawler.engine.paused :
                    self._crawler.engine.pause()
        #if self._crawler.engine.running :
            #if not self._crawler.engine.paused :
                #print '?????????????????????????', 'pausedCrawl'
                #self._crawler.engine.pause()
            

    def stopService(self):
        log.msg(format='listPageSpiderService->stopService stop listPageSpiderService serviceName=(%(serviceName)s)',serviceName=self.name)
        service.Service.stopService(self)
        self.spiderService.removeSpiderService()
        self._crawler._spider.stopSpider()
        self._crawler.stop()
        if self.name in self.spiderService.namedServices:
            self.spiderService.removeService(self)


class TaskService(service.MultiService):
    def __init__(self):
        service.MultiService.__init__(self)

    def startTask(self, taskId):
        serviceName = 'spiderService_%s' % taskId
        if serviceName not in self.namedServices:
            _spiderService = spiderService(taskId=taskId, taskService=self)
            _spiderService.setName('spiderService_%s' % taskId)
            self.addService(_spiderService)
            log.msg(format='TaskService->startTask(%(taskId)s)',taskId=taskId)
            return True
        else:
            log.msg(format='TaskService->startTask(%(taskId)s) serviceName=%(serviceName)s Exists',taskId=taskId, serviceName=serviceName)
            return False
    def stopTask(self,taskId):
        serviceName = 'spiderService_%s' % taskId
        log.msg(format='TaskService->stopTask(%(taskId)s) serviceName=%(serviceName)s ',taskId=taskId, serviceName=serviceName)
        if serviceName in self.namedServices:
            self.namedServices[serviceName].stopService()
        return True

    def stateTask(self, taskId):
        serviceName = 'spiderService_%s' % taskId
        result = {}
        if serviceName in self.namedServices:
            for _spiderService in self.namedServices[serviceName].namedServices:
                result[_spiderService] = self.namedServices[serviceName].namedServices[_spiderService].getStats()
        return result

    def startService(self):
        service.Service.startService(self)
        log.msg('TaskService->startService')
