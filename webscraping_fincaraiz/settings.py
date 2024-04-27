# Scrapy settings for webscraping_fincaraiz project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "webscraping_fincaraiz"

SPIDER_MODULES = ["webscraping_fincaraiz.spiders"]
NEWSPIDER_MODULE = "webscraping_fincaraiz.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "webscraping_fincaraiz (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "webscraping_fincaraiz.middlewares.WebscrapingFincaraizSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    "webscraping_fincaraiz.middlewares.WebscrapingFincaraizDownloaderMiddleware": 543,
#    "webscraping_fincaraiz.middlewares.ScrapeOpsFakeUserAgentMiddleware": 400
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    "webscraping_fincaraiz.pipelines.NoDuplicates": 200,
    "webscraping_fincaraiz.pipelines.WebscrapingFincaraizPipeline": 100,
    "webscraping_fincaraiz.pipelines.SaveToPostgreSQLPipeline":400,
#    "webscraping_fincaraiz.pipelines.SaveToSQLitePipelineUpdated":400,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

import os
from dotenv import load_dotenv

load_dotenv()

SCRAPEOPS_API_KEY = os.getenv('SCRAPEOPS_API_KEY')
SCRAPEOPS_FAKE_USER_AGENT_ENDPOINT = "https://headers.scrapeops.io/v1/user-agents"
SCRAPEOPS_FAKE_BROWSER_HEADER_ENDPOINT = "https://headers.scrapeops.io/v1/browser-headers"
SCRAPEOPS_FAKE_USER_AGENT_ENABLED = True
SCRAPEOPS_FAKE_BROWSER_HEADER_ENABLED = True
SCRAPEOPS_NUM_RESULTS = 50

# FEEDS = {
#     'data_final.csv' : {'format':'csv', 'overwrite': True}
# }

# settings.py

# # Splash Server Endpoint
# SPLASH_URL = 'http://localhost:8050'


# # Enable Splash downloader middleware and change HttpCompressionMiddleware priority
# DOWNLOADER_MIDDLEWARES = {
#     'scrapy_splash.SplashCookiesMiddleware': 723,
#     'scrapy_splash.SplashMiddleware': 725,
#     'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
# }

# # Enable Splash Deduplicate Args Filter
# SPIDER_MIDDLEWARES = {
#     'scrapy_splash.SplashDeduplicateArgsMiddleware': 100,
# }

# # Define the Splash DupeFilter
# DUPEFILTER_CLASS = 'scrapy_splash.SplashAwareDupeFilter'
