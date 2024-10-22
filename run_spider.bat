@echo off

REM Run the spider for each site
scrapy crawl categories_spider -a site_name=news247
scrapy crawl categories_spider -a site_name=news