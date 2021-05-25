##### Crawler Settings #####
# Crawler name: This is name of crawler will connect database and check categories to crawl
crawler_name = "tor_5"

# Number of crawler run in background
# This is apply for crawl projects creator
no_crawler = 25

# Type crawler will run crawl data: both | http_request | selenium
# http_request: Only send HTTP request and crawl API
# selenium: Use browser to crawl data
crawl_type = "http_request"

# Choose random by second: Action and request
# Use this prevent the kickstater block crawler
action_delay = (1,3)
request_delay = (5,10)

# When choose crawl type both or selenium. The crawler will run browser in background
# True: in background
# False: not in background
headless_browser = True

# Use proxies to crawl data: Recommended
proxy_address = '127.0.0.1'
proxy_port = '8118'
proxy_username = None
proxy_password = None

# Use Tor proxy to crawl data
# Read guide to deploy a tor proxy: https://gist.github.com/luantechrius/5ac0c484ecf9dbb212a9fbe72bbef5cc
use_tor = True
tor_password = '18091987'
tor_port=9051


##### Database Settings #####
db_host = '207.148.66.216'
db_user = 'tor_5'
db_password = 'Zipphong18091987@@'
db_name = 'kickstarter_com'