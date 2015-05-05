# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class Page(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    size = scrapy.Field()
    referer = scrapy.Field()
    newcookies = scrapy.Field()
    body = scrapy.Field()

class ScrapyItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
