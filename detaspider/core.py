from random import paretovariate
import shutil
import logging
import re
from threading import local
import time
import pandas as pd
import sys
import os
import pickle
import requests
import json

from .mysql_piplines import create_database, create_table, insert_data, update_crawl_status, check_project_id
from .browser import SeleniumHander
from .utils import random_sleep, load_cookie, random_user_agent, save_cookie, check_and_create_file, check_and_create_folder, save_excel, load_json_file, update_json_file, convert_html_to_text, convert_html_to_json
from logging.handlers import RotatingFileHandler
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
from datetime import datetime
from requests.auth import HTTPProxyAuth
from toripchanger import TorIpChanger

class TSpider:

    def __init__(
            self,
            spider_name: str = None,
            default_url: str= None,
            action_delay: tuple = (1,3),
            request_delay: tuple = (5,10),
            show_logs: bool = True,
            user_agent: str = None,
            proxy_address: str = None,
            proxy_port: str = None,
            proxy_username: str = None,
            proxy_password: str = None,
            headless_browser: bool = True,
            disable_image_load: bool = True,
            json_file: str= None,
            limit_scropes: list = None,
            exclude_hosts: list= None,
            crawl_type=None,
            use_tor: bool = False,
            tor_password=None,
            tor_port=None
        ):
        self.default_url = default_url
        self.action_delay = action_delay
        self.request_delay = request_delay
        self.show_logs = show_logs
        self.json_file = json_file
        self.proxy_address = proxy_address
        self.proxy_port = proxy_port
        self.proxy_username = proxy_username
        self.proxy_password = proxy_password
        self.crawl_type = crawl_type
        self.use_tor = use_tor,
        self.tor_password = tor_password
        self.tor_port = tor_port

        if spider_name is None:
            raise Exception('Please add the spider name!')
        else:
            self.spider_name = spider_name

        
        if user_agent is None:
            self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
        else:
            self.user_agent = user_agent

        ############ Browser Settings ############

        # Add chromedriver path
        if sys.platform == 'win32':
            chromedriver_path = shutil.which('assets/windows/chromedriver.exe')
        elif sys.platform == 'darwin':
            chromedriver_path = shutil.which('assets/macos/chromedriver')
        else:
            chromedriver_path = '/usr/local/bin/chromedriver'

        # Default header
        self.headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            # 'accept-encoding': 'gzip, deflate, br',
            # 'accept-language': 'en,vi;q=0.9,en-US;q=0.8',
        }
        # logs path
        self.logfolder = 'logs/'
    
        # Add logger
        self.logger = self.get_logger(self.show_logs)

        # #Path to your chrome profile
        # user_data_dir = f'assets/profiles/{spider_name}/'

        # set limit catpture
        if exclude_hosts is None:
            exclude_hosts = [
                    'accounts.google.com',
                    'clients2.google.com',
                    'clientservices.googleapis.com',
                    'www.googleapis.com',
                    'chrome.google.com',
                    'clients2.googleusercontent.com',
                    'a.kickstarter.com',
                    'www.facebook.com',
                    'ksr-static.imgix.net'
                    
                ]
        if limit_scropes is None:
            limit_scropes = []

        if crawl_type == 'http_request':
            self.browser = None
        else:
            self.selenium = SeleniumHander(
                chrome_driver_path = chromedriver_path,
                headless_browser = headless_browser,
                disable_image_load = disable_image_load,
                page_delay = 30,
                logfolder = self.logfolder,
                # user_agent = self.user_agent,
                proxy_address = proxy_address,
                proxy_port = proxy_port,
                proxy_username = proxy_username,
                proxy_password = proxy_password,
            )
            self.browser = self.selenium.driver
        
        self.logger.info('Checking the connect to MYSQL server....')
        create_database(self.logger)
        create_table(self.logger)
        self.logger.info('Complete to connect to MYSQL server....')
        

    def block_fix(self, url_block, try_fix=5):
        count_fix = 1
        time_fix = 1
        status_fix = False
        while True:
            status_code = self.selenium.http_status_code(url_block)
            if count_fix > try_fix:
                break

            if status_code == 403:
                time_fix *= 2
                time.sleep(time_fix)
                self.browser.refresh()
            elif status_code == 429:
                time_fix *= 5
                time.sleep(time_fix)
                self.browser.refresh()
            else:
                status_fix = True
                break

            count_fix += 1
            
        return status_fix

    def try_request(self, session, method, url, payloads=None, type_name='default',headers=None):
        random_sleep(self.request_delay, self.logger)
        rstatus = False
        while True:
            if method == 'get':
                r = session.get(url, params=payloads)
            else:
                r = session.post(url, data=payloads)
            
            if r.status_code == 200:
                self.logger.info(f'Type: {type_name} | HTTP Request Successful: {r.status_code} | Method: {method} | URL: {url}')
                rstatus = True
                break
            if r.status_code == 400:
                self.logger.warning(f'Type: {type_name} | HTTP Request Successful: {r.status_code} | Method: {method} | URL: {url}')
                rstatus = True
                break
            if r.status_code == 403:
                self.logger.info(f'Type: {type_name} | HTTP Request Error: {r.status_code} | Method: {method} | URL: {url}')
                self.logger.info(f'Type: {type_name} | HTTP Request again | Method: {method} | URL: {url}')
                headers = session.headers
                session.cookies.clear_session_cookies()
                session = self.api_session()
                session.headers = headers
                random_sleep((20,40))
            if r.status_code == 429:
                self.logger.info(f'Type: {type_name} | HTTP Request Error: {r.status_code} | Method: {method} | URL: {url}')
                self.logger.info(f'Type: {type_name} | HTTP Request again | Method: {method} | URL: {url}')
                if self.use_tor:
                    tor_ip_changer = TorIpChanger(tor_password=self.tor_password, tor_port=self.tor_port, local_http_proxy=f'{self.proxy_address}:{self.proxy_port}')
                    old_ip = tor_ip_changer.get_current_ip()
                    new_ip = None
                    while True:
                        random_sleep((5,30), self.logger)
                        tor_ip_changer.get_new_ip()
                        new_ip = tor_ip_changer.get_current_ip()
                        if new_ip != old_ip:
                            break
                    location = session.get('https://api.myip.com/')
                    loc = location.json()
                    loc_ip = loc['ip']
                    loc_country = loc['country']
                    self.logger.info(f'Type: {type_name} | Change New IP Tor: {loc_ip} | Country: {loc_country} | URL: {url}')

                    headers = session.headers
                    session.cookies.clear_session_cookies()
                    session = self.api_session()
                    session.headers = headers
                else:
                    random_sleep((20,40))
            else:
                self.logger.error(f'Type: {type_name} | HTTP Request Error: {r.status_code} | Method: {method} | URL: {url}')
                break
         
        return r, rstatus

    def api_session(self):
        # cookie_file_path = f'assets/cookies/{self.spider_name}.pkl'
        # if os.path.isfile(cookie_file_path):
        #     cookies = pickle.load(open(cookie_file_path, "rb"))
        #     jar = requests.cookies.RequestsCookieJar()
        #     for cookie in cookies:
        #         if isinstance(cookie.get('expiry'), float): #Checks if the instance expiry a float 
        #             cookie['expiry'] = int(cookie['expiry']) # it converts expiry cookie to a int 

        #         jar.set(
        #                 cookie.get('name'),
        #                 cookie.get('value'),
        #                 domain=cookie.get('domain'),
        #                 path=cookie.get('path'),
        #                 secure=cookie.get('secure'),
        #                 rest={'HttpOnly': cookie.get('httpOnly')},
        #                 expires=cookie.get('expiry'),
        #             )
        #     self.logger.info('Requests package | API Session load cookies')
        #     session = requests.Session()
        #     if self.proxy_username and self.proxy_password and self.proxy_address and self.proxy_port:
        #         proxies = {
        #             'http': f'http://{self.proxy_username}:{self.proxy_password}@{self.proxy_address}:{self.proxy_port}'
        #         }
        #         session.proxies.update(proxies)
        #     # session.cookies = jar
        #     session.headers.update(self.headers)
        #     data = session.get('http://extreme-ip-lookup.com/json/')
        #     self.logger.info(f'IP:{data.json()["query"]} | Country:{data.json()["country"]} | City:{data.json()["city"]} | State: {data.json()["continent"]}| ipType: {data.json()["ipType"]}')
        #     return session
        # else:
        #     return False
        session = requests.Session()
        session.headers = self.headers
        if self.proxy_address and self.proxy_port:
            session.proxies = {
                'http': f'{self.proxy_address}:{self.proxy_port}',
                'https': f'{self.proxy_address}:{self.proxy_port}'
            }
            if self.proxy_username and self.proxy_password:
                session.auth = HTTPProxyAuth(self.proxy_username, self.proxy_password)

            if self.use_tor:
                self.logger.info(f'You use Tor proxy: {self.proxy_address}:{self.proxy_port}')
            else:
                self.logger.info(f'You use proxy: {self.proxy_address}:{self.proxy_port}')
        else:
            self.logger.warning('You don\'t use proxies at this time....')

        return session

    def session_load(self):
        self.logger.info('### SESSION LOAD ###')
        # cookie_status = load_cookie(self.browser, self.default_url, self.spider_name, self.logger)
        # if cookie_status:
        #     self.logger.info('Cookies loaded Successful...')
        # else:
        #     save_cookie(self.browser, self.spider_name, self.logger)

    def session_quit(self):
        # save_cookie(self.browser, self.spider_name, self.logger)
        self.logger.info('### SESSION QUIT ###')
        if self.crawl_type != 'http_request':
            self.browser.quit()
        
    def get_logger(self, show_logs: bool, log_handler=None):
        """
        Handles the creation and retrieval of loggers to avoid
        re-instantiation.
        """
        # initialize and setup logging system for the InstaPy object
        logger = logging.getLogger(self.spider_name)
        if (logger.hasHandlers()):
            logger.handlers.clear()

        logger.setLevel(logging.DEBUG)
        # log name and format
        general_log = f"{self.logfolder}{self.spider_name}.log"
        check_and_create_file(general_log)

        file_handler = logging.FileHandler(general_log)
        # log rotation, 5 logs with 10MB size each one
        file_handler = RotatingFileHandler(
            general_log, maxBytes=10 * 1024 * 1024, backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        extra = {"website": self.spider_name}
        logger_formatter = logging.Formatter(
            "%(levelname)s [%(asctime)s] [%(website)s]  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(logger_formatter)
        logger.addHandler(file_handler)
        # otherwise root logger prints things again
        logger.propagate = False
        # add custom user handler if given
        if log_handler:
            logger.addHandler(log_handler)

        if show_logs is True:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(logger_formatter)
            logger.addHandler(console_handler)

        logger = logging.LoggerAdapter(logger, extra)
        return logger
    
    def get_session_http_requests(self, url):
        type_crawl = 'Get HTTP Session'
        api_session = self.api_session()
        # Send the request for getting CSRF token
        r_html = self.try_request(session=api_session, method='get', url=url, type_name='Getting CSRF token')
        if r_html[1]:
            csrf_token_tag = SoupStrainer('meta', attrs={'name':'csrf-token'})
            soup = BeautifulSoup(r_html[0].text, 'lxml', parse_only=csrf_token_tag)
            csrf_token = soup.meta.get('content')

            # Send post to get data from API
            api_session.headers.update({
                'content-type': 'application/json',
                'x-csrf-token': csrf_token
            })
            self.logger.info(f'Type: {type_crawl} | Status: Get Session and csrf-token | URL: {url}')
            return api_session
        else:
            self.logger.error(f'Type: {type_crawl} | Status: Can\'t get session | URL: {url}')
            return False

    def crawl_pages(self, category_id, current_page, current_project_id, state, crawl_id):
        crawl_type = self.crawl_type
        project_id = None
        item_count = 0
        reach_limit_page = False
        while True:
            type_crawl = 'List Projects Category'
            has_more = None
            #### Scrape categories ######
            if state is None:
                s_state = ''
            else:
                s_state = f'&state={state}'
            url_page = f'https://www.kickstarter.com/projects/search.json?search={s_state}&category_id={category_id}&page={current_page}'
            
            if crawl_type == 'selenium':
                self.browser.get(url_page)
                if self.block_fix(url_page):
                    status_code = self.selenium.http_status_code(url_page)
                    json_data = convert_html_to_json(self.browser.page_source, self.browser)
                else:
                    status_code = self.selenium.http_status_code(url_page)
            elif crawl_type == 'http_request':
                api_session = self.api_session()
                request_http = self.try_request(session=api_session, method='get', url=url_page, type_name=type_crawl)
                status_code = request_http[0].status_code
                if request_http[1]:
                    json_data = request_http[0].json()
                    self.logger.info(f'Type: {type_crawl} | Status: Crawled {type_crawl} (HTTP requests) |  URL: {url_page}')
            else:
                api_session = self.api_session()
                request_http = self.try_request(session=api_session, method='get', url=url_page, type_name=type_crawl)
                if request_http[1]:
                    status_code = request_http[0].status_code
                    json_data = request_http[0].json()
                    self.logger.info(f'Type: {type_crawl} | Status: Crawled {type_crawl} (HTTP requests) |  URL: {url_page}')
                else:
                    self.browser.get(url_page)
                    if self.block_fix(url_page):
                        status_code = self.selenium.http_status_code(url_page)
                        json_data = convert_html_to_json(self.browser.page_source, self.browser)
                    else:
                        status_code = self.selenium.http_status_code(url_page)
            
            
            # Check status code and crawl data
            if status_code == 200:
                # Load json data

                """++++++++++++ Explore Json DATA ++++++++++++"""
                has_more = json_data.get('has_more')
                projects = json_data.get('projects')

                if projects:
                    # Get project ID for crawling continue
                    project_count = 0
                    if current_project_id:
                        for project in projects:
                            project_id = project.get('profile').get('project_id')
                            if project_id == current_project_id:
                                project_id = current_project_id
                                break
                            else:
                                project_count += 1
                    if project_count  == len(projects):
                        project_count = 0

                    for project in projects[project_count:]:
                        project_id = self.crawl_project(project, crawl_type)
                        update_crawl_status((current_page, project_id, reach_limit_page, crawl_id), self.logger)
                        item_count += 1
                        self.logger.info(f'Website crawl total: {item_count} | Current pages: {current_page}')
                else:
                    self.logger.warning(f'Type: Crawl Projects of Category | Status: Not found the projects | URL: {url_page}')
                    reach_limit_page = True
                    break
                if has_more:
                    current_page += 1
                else:
                    self.logger.warning(f'Type: Crawl Projects of Category | Status: Not Has More page| URL: {url_page}')
                    reach_limit_page = True
                    break
            elif status_code == 404:
                self.logger.warning(f'Type: Crawl Projects of Category | Status: Reached to limit | URL: {url_page}')
                reach_limit_page = True
                break
            else:
                self.logger.error(f'Type: Crawl Projects of Category | Status: HTTP request error {status_code} code |  URL: {url_page}')
        
        update_crawl_status((current_page, project_id, reach_limit_page, crawl_id), self.logger)
        return current_page, reach_limit_page, project_id

    def crawl_projects_creator(self, creator_id):
        crawl_type = self.crawl_type
        type_crawl = 'Crawl Creator Projects'
        crawl_status = False
        active_projects = True
        list_project = []
        api_session = self.api_session()

        backed_projects = self.crawl_backed_creator(creator_id, api_session)
        if backed_projects:
            list_project = backed_projects + list_project
        
        created_projects = self.crawl_created_creator(creator_id, api_session)
        if created_projects:
            list_project = created_projects + list_project

        if list_project:
            self.logger.info(f'Type: {type_crawl} | Status: Found {len(list_project)} projects | Creator ID: {creator_id}')
            for project in list_project:
                project_id = self.crawl_project(project, crawl_type, creator_id)
                if project_id:
                    crawl_status = True
                    self.logger.info(f'Type: {type_crawl} | Status: Success to crawled project ID ({project_id}) | Creator ID: {creator_id}')
                else:
                    self.logger.warning(f'Type: {type_crawl} | Status: Fails to crawled project ID ({project_id}) | Creator ID: {creator_id}')
        else:
            self.logger.warning(f'Type: {type_crawl} | Status: Not found the projects | Creator ID: {creator_id}')
            active_projects = False
        
        return crawl_status, active_projects

    def crawl_created_creator(self, creator_id, api_session):
        type_crawl = 'Crawl Created Creator'
        profile_url = f'https://www.kickstarter.com/profile/{creator_id}/created'
        request_http = self.try_request(session=api_session, method='get', url=profile_url, type_name=type_crawl)
        if request_http[1]:
            soup = BeautifulSoup(request_http[0].text, 'lxml')
            projects = soup.find('div', id='react-profile-created')
            if projects:
                self.logger.info(f'Type: {type_crawl} | Status: Crawled {type_crawl} data (HTTP request)|  URL: {profile_url}')
                return json.loads(projects.get('data-projects'))
            else:
                self.logger.warning(f'Type: {type_crawl} | Status: Not found {type_crawl} data (HTTP request)|  URL: {profile_url}')
                return None
        return None

    def crawl_backed_creator(self, creator_id, api_session):
        type_crawl = 'Crawl Backed Creator'
        list_project = []
        next_page = None
        crawl_status = True
        while True:
            if not next_page:
                profile_url = f'https://www.kickstarter.com/profile/{creator_id}'
            else:
                profile_url = next_page

            request_http = self.try_request(session=api_session, method='get', url=profile_url, type_name=type_crawl)
            if request_http[1]:
                soup = BeautifulSoup(request_http[0].text, 'lxml')
                projects = soup.select('#profile_projects_list ul li .react-user-prof-card')
                if projects:
                    for project in projects:
                        list_project.append(json.loads(project['data-project']))

                    next_page_element = soup.find('a', class_='next_page')
                    if next_page_element and next_page_element.get('href'):
                        next_page = 'https://www.kickstarter.com/' + next_page_element.get('href')
                    else:
                        crawl_status = False
                else:
                    crawl_status = False
            else:
                crawl_status = False

            if crawl_status == False:
                break
        
        if list_project:
            self.logger.info(f'Type: {type_crawl} | Status: Crawled {type_crawl} data (HTTP request)|  URL: {profile_url}')
        else:
            self.logger.warning(f'Type: {type_crawl} | Status: Not found {type_crawl} data (HTTP request)|  URL: {profile_url}')
        
        return list_project

    def crawl_project(self, project, crawl_type, creator_id=None):
        project_id = project.get('profile').get('project_id')
        project_value = check_project_id(project_id, self.logfolder)
        if project_value is None:
            url_api = 'https://www.kickstarter.com/graph'
            # slug url & url
            url = project.get('urls').get('web').get('project')
            url_reward = project.get('urls').get('web').get('rewards')
            url_api_user = project.get('creator').get('urls').get('api').get('user')
            creator_slug = project.get('creator').get('slug')
            if creator_slug is None and creator_id:
                creator_slug = creator_id
            project_slug = project.get('slug')
            full_project_slug = f'{creator_slug}/{project_slug}'

            # Format time
            format_time = '%Y-%m-%d'
            now_utc = datetime.utcnow()
            deadline = datetime.utcfromtimestamp(project.get('deadline'))
            launched = datetime.utcfromtimestamp(project.get('launched_at'))
            days_to_go_cal = deadline.date() - now_utc.date()
            
            # Cal day to go
            if days_to_go_cal.days < 0:
                days_to_go = 0
            else:
                days_to_go = days_to_go_cal.days

            # Crawl Details:
            if crawl_type == 'selenium':
                story = self.crawl_story(url, url_api)
                creator = self.crawl_creator(url, url_api_user)
                updates = self.crawl_updates(url, url_api, project_id)
                comments = self.crawl_comments(url, url_api, project_id)
            elif crawl_type== 'http_request':
                api_session = self.get_session_http_requests(url)
                if api_session:
                    http_crawl_story = self.http_crawl_story(url,url_api, api_session, full_project_slug)
                    if http_crawl_story[1] is False:
                        story = None
                    else:
                        story = http_crawl_story[0]

                    http_crawl_creator = self.http_crawl_creator(url, url_api, url_api_user, api_session, full_project_slug)
                    if http_crawl_creator[1] is False:
                        creator = None
                    else:
                        creator = http_crawl_creator[0]

                    http_crawl_updates =  self.http_crawl_updates(api_session, url, url_api, project_slug, project_id)
                    if http_crawl_updates[1] is False:
                        updates = None
                    else:
                        updates = http_crawl_updates[0]
                
                    http_crawl_comments =  self.http_crawl_comments(api_session, url, url_api, project_id)
                    if http_crawl_comments[1] is False:
                        comments = None
                    else:
                        comments = http_crawl_comments[0]
            else:
                api_session = self.get_session_http_requests(url)
                if api_session:
                    http_crawl_story = self.http_crawl_story(url,url_api, api_session, full_project_slug)
                    if http_crawl_story[1] is False:
                        story = self.crawl_story(url, url_api)
                    else:
                        story = http_crawl_story[0]

                    http_crawl_creator = self.http_crawl_creator(url, url_api, url_api_user, api_session, full_project_slug)
                    if http_crawl_creator[1] is False:
                        creator = self.crawl_creator(url, url_api_user)
                    else:
                        creator = http_crawl_creator[0]

                    http_crawl_updates =  self.http_crawl_updates(api_session, url, url_api, project_slug, project_id)
                    if http_crawl_updates[1] is False:
                        updates = self.crawl_updates(url, url_api, project_id)
                    else:
                        updates = http_crawl_updates[0]
                
                    http_crawl_comments =  self.http_crawl_comments(api_session, url, url_api, project_id)
                    if http_crawl_comments[1] is False:
                        comments = self.crawl_comments(url, url_api, project_id)
                    else:
                        comments = http_crawl_comments[0]
                else:
                    story = self.crawl_story(url, url_api)
                    creator = self.crawl_creator(url, url_api_user)
                    updates = self.crawl_updates(url, url_api, project_id)
                    comments = self.crawl_comments(url, url_api, project_id)
                

            rewards = self.crawl_rewards(url_reward, project_id, crawl_type)

            if project.get('goal') and project.get('usd_exchange_rate'):
                goal = project.get('goal') * project.get('usd_exchange_rate')
            else:
                goal = None

            if project.get('location'):
                location = project.get('location').get('displayable_name')
            else:
                location = None

            results = {
                    'project_id': project_id,
                    'title': project.get('name').strip(),
                    'blurb': project.get('blurb').strip(),
                    'feature_image': project.get('profile').get('feature_image_attributes').get('image_urls').get('default'),
                    'category': project.get('category').get('name'),
                    'category_id': project.get('category').get('id'),
                    'parent_category': project.get('category').get('parent_name'),
                    'parent_category_id': project.get('category').get('parent_id'),
                    'currency': project.get('current_currency'),
                    'pledged': project.get('converted_pledged_amount'),
                    'goal': goal, 
                    'backers': project.get('backers_count'),
                    'day_to_go': days_to_go,
                    'launched': launched.strftime(format_time),
                    'deadline': deadline.strftime(format_time),
                    'location': location,
                    'creator_id': project.get('creator').get('id'),
                    'url': url,
                    'story': story,
            }
            insert_data(creator, results, updates, comments, rewards, self.logger)
        else:
            self.logger.info(f'The project ID {project_id} is exist.....')
        return project_id

    def http_crawl_story(self, url, url_api, session, full_slug):
        """ ++++++++++++ Crawling story in project campaign ++++++++++++"""
        type_crawl = 'Story'
        crawl_status = True
        story = None
        payloads = [
            {
                'operationName': 'Campaign',
                'query': 'query Campaign($slug: String!) {\n  project(slug: $slug) {\n    id\n    isSharingProjectBudget\n    risks\n    story(assetWidth: 680)\n    currency\n    spreadsheet {\n      displayMode\n      public\n      url\n      data {\n        name\n        value\n        phase\n        rowNum\n        __typename\n      }\n      dataLastUpdatedAt\n      __typename\n    }\n    environmentalCommitments {\n      id\n      commitmentCategory\n      description\n      __typename\n    }\n    __typename\n  }\n}\n',
                'variables': {
                    'slug': full_slug
                }
            },
        ]
        request = self.try_request(session=session, method='post', url=url_api, payloads=json.dumps(payloads), type_name='Crawl Story')
        self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} (HTTP request) | URL: {url}')
        if request[1]:
            json_data = request[0].json()
            self.logger.info(f'Crawling story from API URL: {url}')
            story = self.crawl_story_json(json_data)
        else:
            crawl_status = False

        if story:
            self.logger.info(f'Type: {type_crawl} | Status: Crawled {type_crawl} data (HTTP request)|  URL: {url}')
        else:
            self.logger.warning(f'Type: {type_crawl} | Status: Not found {type_crawl} data (HTTP request) |  URL: {url}')
            
        return story, crawl_status

    def crawl_story(self, url, url_api):
        url = re.sub(r'\?.+', '', url)
        url = url + '/description'
        story = None
        """ ++++++++++++ Crawling story in project campaign ++++++++++++"""
        type_crawl = 'Story'
        self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} | URL: {url}')
        self.browser.get(url)
        # random_sleep(self.request_delay, self.logger)
        self.block_fix(url)
        status_code = self.selenium.http_status_code(url)
        
        if status_code == 200:
            # random_sleep(self.request_delay, self.logger)
            body_list = []
            for request in self.browser.requests:
                if request.response:
                    if request.url == url_api and request.response.status_code == 200 :
                        body_list.append(request.response.body) 
            
            
            if body_list:
                for body in body_list:
                    json_data = json.loads(body)
                    story = self.crawl_story_json(json_data)
                    if story:
                        break
  
            self.logger.info(f'Type: {type_crawl} | Status: Crawled {type_crawl} data |  URL: {url}')
        else:
            self.logger.warning(f'Type: {type_crawl} | Status: HTTP request error {status_code} code |  URL: {url}')
        return story

    def crawl_story_json(self, json_data):
        story = None
        for data in json_data:
            try:
                # Check nested json data
                story = data.get('data').get('project').get('story')
                if story:
                    return story
            except:
                pass
        return story

    def http_crawl_creator(self, url, url_api, url_api_user, session, full_slug):
        crawl_status = False
        creator = None
        verified_name = None
        """ ++++++++++++ Crawling Verified Identity in project campaign ++++++++++++"""
        payloads = [
            {
                'query': 'query CreatorSection($slug: String!) {\n  me {\n    id\n    name\n  }\n  project(slug: $slug) {\n    id\n    verifiedIdentity\n    creator {\n      id\n      name\n      imageUrl(width: 100)\n      url\n      lastLogin\n      biography\n      isFacebookConnected\n      allowsFollows\n      backingsCount\n      location {\n        displayableName\n      }\n      launchedProjects {\n        totalCount\n      }\n      websites {\n        url\n        domain\n      }\n    }\n    collaborators {\n      edges {\n        node {\n          name\n          imageUrl(width: 200)\n          url\n        }\n        title\n      }\n    }\n  }\n}\n',
                'variables': {
                    'slug': full_slug
                }
            },
        ]
        request = self.try_request(session=session, method='post', url=url_api, payloads=json.dumps(payloads), type_name='Crawl Creator')
        type_crawl = 'Verified Identity Name'
        if request[1]:
            data = request[0].json()
            self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} (HTTP request)| URL: {url}')
            verified_identity = data[0].get('data').get('project').get('verifiedIdentity')
            if verified_identity:
                self.logger.info(f'Type: {type_crawl} | Status: Crawled {type_crawl} (HTTP request)|  URL: {url}')
                verified_name = verified_identity
            else:
                self.logger.warning(f'Type: {type_crawl} | Status: Not found {type_crawl} data (HTTP request)|  URL: {url}')

        """ ++++++++++++ Crawling Creators in project campaign ++++++++++++"""
        type_crawl = 'Creators'
        session.headers.update(self.headers)
        request_api_user = self.try_request(session=session, method='get', url=url_api_user, type_name='Crawl API User')
        if request_api_user[1]:
            data = request_api_user[0].json()
            self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} (HTTP request)| URL: {url_api_user}')
            if data.get('location'):
                location = data.get('location').get('country')
            else:
                location = None
            format_time = '%Y-%m-%d'
            join_date = datetime.utcfromtimestamp(data.get('created_at')).strftime(format_time)
            creator = {
                    'creator_id': data.get('id'),
                    'name': data.get('name'),
                    'verified_name': verified_name,
                    'slug': data.get('slug'),
                    'location': location,
                    'project': data.get('created_projects_count'),
                    'backed_project': data.get('backed_projects'),
                    'join_date': join_date,
                    'biography': data.get('biography'),
            }
            crawl_status = True
        if creator:
            self.logger.info(f'Type: {type_crawl} | Status: Crawled {type_crawl} (HTTP request)|  URL: {url_api_user}')
        else:
            self.logger.warning(f'Type: {type_crawl} | Status: Not found {type_crawl} data |  URL: {url_api_user}')
    
        return creator, crawl_status

    def crawl_creator(self, url, url_api_user):
        url = re.sub(r'\?.+', '', url)
        url = url + '/description'
        creator = None
        verified_identity = None
        self.browser.get(url)
        # random_sleep(self.request_delay, self.logger)
        self.block_fix(url)
        status_code = self.selenium.http_status_code(url)
        type_crawl = 'Verified Identity Name'
        if status_code == 200:
            """ ++++++++++++ Crawling Verified Identity in project campaign ++++++++++++"""
            self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} | URL: {url}')
            try:
                bio_element = self.browser.find_element_by_xpath('(//div[@class="creator-name"]//a)[1]')
                if bio_element:
                    bio_element.click()
                    random_sleep(self.action_delay, self.logger)
            except:
                pass

            verified_identity = self.crawl_verified_identity(self.browser.page_source)
            self.logger.info(f'Type: {type_crawl} | Status: Crawled {type_crawl} |  URL: {url}')
           
        else:
            self.logger.error(f'Type: {type_crawl} | Status: HTTP request error {status_code} code |  URL: {url}')
            
        """ ++++++++++++ Crawling Creators in project campaign ++++++++++++"""
        type_crawl = 'Creators'
        self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} | URL: {url_api_user}')
        self.browser.get(url_api_user)
        self.block_fix(url_api_user)
        # random_sleep(self.request_delay, self.logger)
        status_code = self.selenium.http_status_code(url_api_user)

        if status_code == 200:
            json_data = convert_html_to_json(self.browser.page_source, self.browser)
            if json_data.get('location'):
                location = json_data.get('location').get('country')
            else:
                location = None
            format_time = '%Y-%m-%d'
            join_date = datetime.utcfromtimestamp(json_data.get('created_at')).strftime(format_time)
            creator = {
                    'creator_id': json_data.get('id'),
                    'name': json_data.get('name'),
                    'verified_name': verified_identity,
                    'slug': json_data.get('slug'),
                    'location': location,
                    'project': json_data.get('created_projects_count'),
                    'backed_project': json_data.get('backed_projects'),
                    'join_date': join_date,
                    'biography': json_data.get('biography'),
            }
            self.logger.info(f'Type: {type_crawl} | Status: Crawled {type_crawl} |  URL: {url_api_user}')
        else:
            self.logger.error(f'Type: {type_crawl} | Status: HTTP request error {status_code} code |  URL: {url_api_user}')
        
        return creator

    def crawl_verified_identity(self, source):
        soup = BeautifulSoup(source, 'lxml')
        if soup.find('span', class_='identity_name'):
            return soup.find('span', class_='identity_name').text.strip()
        elif soup.find('div', id='react-project-header'):
            data = soup.find('div', id='react-project-header')['data-initial']
            json_data = json.loads(data)
            return json_data['project']['verifiedIdentity']
        else:
            return None

    def crawl_updates(self, url, url_api, project_id):
        url = re.sub(r'\?.+', '', url)
        url = url + '/posts'
        update_list = None
        """ ++++++++++++ Crawling updates in project campaign ++++++++++++"""
        type_crawl = 'Updates'
        self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} | URL: {url}')
        self.browser.get(url)
        self.block_fix(url)
        # random_sleep(self.request_delay, self.logger)

        status_code = self.selenium.http_status_code(url)
        if status_code == 200:
            # Click load more until load more is not show
            self.selenium.load_more_click(3, '//span[contains(text(),"Load more")]')
            random_sleep(self.action_delay, self.logger)
            body_list = []
            for request in self.browser.requests:
                if request.response:
                    if request.url == url_api and request.response.status_code == 200 :
                        body_list.append(request.response.body) 
            
            
            if body_list:
                for body in body_list:
                    json_data = json.loads(body)
                    data = self.crawl_update_json(json_data, project_id)
                    if data:
                        if update_list:
                            update_list = update_list + data
                        else:
                            update_list = data
           
            self.logger.info(f'Type: {type_crawl} | Status: Crawled {len(update_list)} {type_crawl} data |  URL: {url}')

        else:
            self.logger.error(f'Type: {type_crawl} | Status: HTTP request error {status_code} code |  URL: {url}')

    def http_crawl_updates(self, api_session, url, url_api, slug, project_id):
        type_crawl = 'Updates'
        request_success = True
        cursor = None
        updates_data = []
        page_count = 1
        while True:
            payloads = [
                {
                    'operationName': 'PostsFeed',
                    'query': 'query PostsFeed($projectSlug: String!, $cursor: String) {\n  me {\n    id\n    isKsrAdmin\n    __typename\n  }\n  project(slug: $projectSlug) {\n    id\n    slug\n    state\n    canUserRequestUpdate\n    timeline(first: 10, after: $cursor) {\n      totalCount\n      pageInfo {\n        hasNextPage\n        endCursor\n        __typename\n      }\n      edges {\n        node {\n          type\n          timestamp\n          data {\n            ... on Project {\n              goal {\n                currency\n                amount\n                __typename\n              }\n              pledged {\n                currency\n                amount\n                __typename\n              }\n              backersCount\n              __typename\n            }\n            ... on Postable {\n              id\n              type\n              title\n              publishedAt\n              pinnedAt\n              number\n              actions {\n                read\n                pin\n                __typename\n              }\n              author {\n                name\n                imageUrl(width: 120)\n                __typename\n              }\n              authorRole\n              isPublic\n              likesCount\n              ... on CreatorInterview {\n                commentsCount(withReplies: true)\n                answers {\n                  nodes {\n                    id\n                    body\n                    question {\n                      id\n                      body\n                      __typename\n                    }\n                    __typename\n                  }\n                  __typename\n                }\n                __typename\n              }\n              ... on FreeformPost {\n                commentsCount(withReplies: true)\n                body\n                nativeImages {\n                  id\n                  url\n                  __typename\n                }\n                __typename\n              }\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n',
                    'variables': {
                        'cursor': cursor,
                        'projectSlug': slug
                    }
                },
            ]
            self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} (HTTP request)| URL: {url}')
            request = self.try_request(session=api_session, method='post', url=url_api, payloads=json.dumps(payloads), type_name='Crawl Updates')
            if request[1]:
                data_json = request[0].json()
                has_next_page = data_json[0].get('data').get('project').get('timeline').get('pageInfo').get('hasNextPage')
                result = self.crawl_update_json(data_json, project_id)
                updates_data = updates_data + result

                self.logger.info(f'Type: {type_crawl} | Status: Crawling Next Page (HTTP request)| Current Page: {page_count} | URL: {url}')
                if has_next_page:
                    cursor = data_json[0].get('data').get('project').get('timeline').get('pageInfo').get('endCursor')
                else:
                    break
            else:
                self.logger.warning(f'Type: {type_crawl} | Status: Not Found API data {type_crawl} (HTTP request)| Current Page: {page_count} URL: {url}')
                request_success = False
                break
            page_count += 1
        
        if updates_data:
            self.logger.info(f'Type: {type_crawl} | Status: Completed to crawl {len(updates_data)} {type_crawl} (HTTP request) | Current Page: {page_count} URL: {url}')
        return updates_data, request_success

    def crawl_update_json(self, json_data, project_id):
        updates_data = []
        for data in json_data:
            try:
                # Check nested json data
                edges = data.get('data').get('project').get('timeline').get('edges')
    
                if edges:
                    for edge in edges:
                        edge_data = edge.get('node').get('data')
                        title = edge_data.get('title')
                        if title:
                            if edge_data.get('body'):
                                body = convert_html_to_text(edge_data.get('body'))
                            else:
                                body = None
                            timestamp = edge.get('node').get('timestamp')
                            format_time = '%Y-%m-%d'
                            date = datetime.utcfromtimestamp(timestamp).strftime(format_time)

                            if edge_data.get('author'):
                                author = edge_data.get('author').get('name').strip()
                                author_role = edge_data.get('authorRole').strip()
                            else:
                                author = None
                                author_role = None
                            
                            if edge_data.get('commentsCount'):
                                comment_count = edge_data.get('commentsCount')
                            else:
                                comment_count = None

                            if edge_data.get('likesCount'):
                                like_count = edge_data.get('likesCount')
                            else:
                                like_count = None

                            updates_data.append({
                                'project_id': project_id,
                                'title': title.strip(),
                                'body': body,
                                'comment_count': comment_count,
                                'like_count': like_count,
                                'date' : date,
                                'author': author,
                                'author_role': author_role
                            })
            except:
                pass
        return updates_data

    def crawl_comments(self, url, url_api, project_id):
        url = re.sub(r'\?.+', '', url)
        url = f'{url}/comments'
        comment_list = None
        """ ++++++++++++ Crawling comments in project campaign ++++++++++++"""
        type_crawl = 'Comments'
        self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} | URL: {url}')
        self.browser.get(url)
        self.block_fix(url)
        # random_sleep(self.request_delay, self.logger)

        status_code = self.selenium.http_status_code(url)
        if status_code == 200:
        # Click load more until load more is not show
            self.selenium.load_more_click(3, '//span[contains(text(),"Load more")]')
            random_sleep(self.action_delay, self.logger)
            body_list = []
            for request in self.browser.requests:
                if request.response:
                    if request.url == url_api and request.response.status_code == 200:
                        body_list.append(request.response.body) 
            
            
            if body_list:
                for body in body_list:
                    json_data = json.loads(body)
                    data = self.crawl_comments_json(json_data, project_id)
                    if data:
                        if comment_list:
                            comment_list = comment_list + data
                        else:
                            comment_list = data
    
            self.logger.info(f'Type: {type_crawl} | Status: Crawled {len(comment_list)} {type_crawl} data|  URL: {url}')

        else:
            self.logger.error(f'Type: {type_crawl} | Status: HTTP request error {status_code} code |  URL: {url}')
    
    def http_crawl_comments(self, api_session, url, url_api, project_id):
        type_crawl = 'Comments'
        request_success = True
        url = re.sub(r'\?.+', '', url)
        url = f'{url}/comments'
        comments_data = []
        # Find commend ID
        api_session.headers = self.headers
        r_html = self.try_request(session=api_session, method='get', url=url, type_name='Find Comment ID')

        commend_id = None
        if r_html[1]:
            soup = BeautifulSoup(r_html[0].text, 'lxml')
            commend_id = soup.find('div', id='react-project-comments')['data-commentable_id']
        else:
            request_success = False
        
        if commend_id:
            self.logger.info(f'Type: {type_crawl} | Status: Found Comment ID | URL: {url}')
            # Send a request API
            api_session.headers.update({
                    'content-type': 'application/json',
                })

            next_cursor = None
            page_count = 1
            while True:
                payloads = [
                        {
                            'operationName': None,
                            'query': 'query ($commentableId: ID!, $nextCursor: String, $previousCursor: String, $replyCursor: String, $first: Int, $last: Int) {\n  commentable: node(id: $commentableId) {\n    id\n    ... on Project {\n      url\n      __typename\n    }\n    ... on Commentable {\n      canComment\n      commentsCount\n      projectRelayId\n      canUserRequestUpdate\n      comments(first: $first, last: $last, after: $nextCursor, before: $previousCursor) {\n        edges {\n          node {\n            ...CommentInfo\n            ...CommentReplies\n            __typename\n          }\n          __typename\n        }\n        pageInfo {\n          startCursor\n          hasNextPage\n          hasPreviousPage\n          endCursor\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  me {\n    id\n    name\n    imageUrl(width: 200)\n    isKsrAdmin\n    url\n    __typename\n  }\n}\n\nfragment CommentInfo on Comment {\n  id\n  body\n  createdAt\n  parentId\n  author {\n    id\n    imageUrl(width: 200)\n    name\n    url\n    __typename\n  }\n  authorBadges\n  canReport\n  canDelete\n  hasFlaggings\n  deletedAuthor\n  deleted\n  authorCanceledPledge\n  __typename\n}\n\nfragment CommentReplies on Comment {\n  replies(last: 3, before: $replyCursor) {\n    totalCount\n    nodes {\n      ...CommentInfo\n      __typename\n    }\n    pageInfo {\n      startCursor\n      hasPreviousPage\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n',
                            'variables': {
                                'commentableId': commend_id,
                                'first': 25,
                                'last': None,
                                'nextCursor': next_cursor,
                                'previousCursor': None,
                                'replyCursor': None
                            }
                        },
                    ]
                self.logger.info(f'Type: {type_crawl} | Status: Begin to crawl {type_crawl} data (HTTP request)| URL: {url}')
                request = self.try_request(session=api_session, method='post', url=url_api, payloads=json.dumps(payloads), type_name='Crawl Comments')
                if request[1]:
                    data = request[0].json()
                    comments = data[0].get('data').get('commentable').get('comments')
                    page_info = comments.get('pageInfo')
                    has_next_page = page_info.get('hasNextPage')
                    end_cursor = page_info.get('endCursor')
                    result = self.crawl_comments_json(data, project_id)
                    comments_data = comments_data + result

                    self.logger.info(f'Type: {type_crawl} | Status: Crawling Next Page (HTTP request)| Current Page: {page_count} | URL: {url}')
                    if has_next_page:
                        next_cursor = end_cursor
                    else:
                        break
                else:
                    self.logger.warning(f'Type: {type_crawl} | Status: Not Found API data {type_crawl} (HTTP request)| Current Page: {page_count} URL: {url}')
                    request_success = False
                    break
                page_count += 1
            if comments_data:
                self.logger.info(f'Type: {type_crawl} | Status: Completed to crawl {len(comments_data)} {type_crawl} (HTTP request)| Current Page: {page_count} URL: {url}')
            return comments_data, request_success

    def crawl_comments_json(self, json_data, project_id):
        comments_data = []
        for data in json_data:
            try:
                # Check nested json data
                edges = data.get('data').get('commentable').get('comments').get('edges')
    
                if edges:
                    for edge in edges:
                        node = edge.get('node')
                        if node:
                            if node.get('body'):
                                body = convert_html_to_text(node.get('body'))
                            else:
                                body = None

                            if node.get('parentId'):
                                parent_id = node.get('parentId')
                            else:
                                parent_id = None

                            if node.get('id'):
                                commend_id = node.get('id')
                            else:
                                commend_id = None

                            if node.get('author').get('name'):
                                author = node.get('author').get('name')
                            else:
                                author = None
                            comments_data.append({
                                'project_id': project_id,
                                'author': author,
                                'body': body,
                                'id': commend_id,
                                'parent_id': parent_id,
                                'comment_type': 'Comment'
                            })
                            replies_count = node.get('replies').get('totalCount')
                            if replies_count > 0:
                                node_replys = node.get('replies').get('nodes')
                                if node_replys:
                                    for node_reply in node_replys:
                                        if node_reply.get('parentId'):
                                            reply_parent_id = node_reply.get('parentId')
                                        else:
                                            reply_parent_id = None

                                        if node_reply.get('body'):
                                            body_reply = convert_html_to_text(node_reply.get('body'))
                                        else:
                                            body_reply = None

                                        if node_reply.get('id'):
                                            reply_id = node_reply.get('id')
                                        else:
                                            reply_id = None
                                        
                                        if node_reply.get('author'):
                                            reply_author = node_reply.get('author').get('name')
                                        else:
                                            reply_author = None

                                        comments_data.append({
                                        'project_id': project_id,
                                        'author': reply_author,
                                        'body': body_reply,
                                        'id': reply_id,
                                        'parent_id': reply_parent_id,
                                        'comment_type': 'Reply Comment'
                                    })
            except:
                pass
        return comments_data

    def crawl_rewards(self, url_reward, project_id, crawl_type):
        rewards = []
        """ ++++++++++++ Crawling rewards in project campaign ++++++++++++"""
        type_crawl = 'Rewards'
        if crawl_type == 'selenium':
            self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} | URL: {url_reward}')
            self.browser.get(url_reward)
            self.block_fix(url_reward)
            text_html = self.browser.page_source
            status_code = self.selenium.http_status_code(url_reward)
        elif crawl_type == 'http_request':
            api_session = self.api_session()
            request_http = self.try_request(session=api_session, method='get', url=url_reward, type_name='Crawl Rewards')
            self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} (HTTP request) | URL: {url_reward}')
            text_html = request_http[0].text
            status_code = request_http[0].status_code
        else:
            api_session = self.api_session()
            request_http = self.try_request(session=api_session, method='get', url=url_reward, type_name='Crawl Rewards')
            if request_http[1]:
                self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} (HTTP request) | URL: {url_reward}')
                text_html = request_http[0].text
                status_code = request_http[0].status_code
            else:
                self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} | URL: {url_reward}')
                self.browser.get(url_reward)
                self.block_fix(url_reward)
                text_html = self.browser.page_source
                status_code = self.selenium.http_status_code(url_reward)
        # random_sleep(self.request_delay, self.logger)

        if status_code == 200:
            reward_elements = SoupStrainer('div', class_='NS_projects__rewards_list js-project-rewards')
            soup = BeautifulSoup(text_html.encode('ascii', errors='ignore').decode("utf-8"), 'lxml', parse_only=reward_elements)
            li_elements = soup.find_all('li', class_="hover-group js-reward-available pledge--available pledge-selectable-sidebar") or soup.find_all('li', class_="hover-group pledge--inactive pledge-selectable-sidebar")

            if li_elements:
                self.logger.info(f'Crawling Rewawrds from API URL: {url_reward}')
                for li in li_elements[1:]:
                    ship_elements = li.find_all('span', class_='pledge__detail-info')
                    if len(ship_elements) > 1:
                        ship_to = ship_elements[1].text.strip()
                        ship_status = True
                    else:
                        ship_to = None
                        ship_status = False
                    desc_elements = li.find('div', class_='pledge__reward-description pledge__reward-description--expanded')
                    if desc_elements:
                        description = desc_elements.text.strip()
                    else:
                        description = None

                    reward_stats = li.find('div', class_='pledge__backer-stats')
                    if reward_stats:
                        if 'Reward no longer available' in reward_stats.text.strip():
                            reward_status = False
                        else:
                            reward_status = True
                    else:
                        reward_status = False

                    pm_element = li.find('h2', class_='pledge__amount')
                    if pm_element:
                        pledge_minimum = pm_element.find('span', class_='money').text.strip()
                    else:
                        pledge_minimum = None

                    ed_element = li.find('time', class_='js-adjust-time')
                    if ed_element:
                        estimated_delivery = ed_element.get('datetime')
                    else:
                        estimated_delivery = None
                    bc_element = li.find('div', class_='pledge__backer-stats').find('div',class_="mr1 mb1")
                    if bc_element:
                        backers_count = bc_element.text.strip().replace(' backers','')
                    else:
                        backers_count = None
                    
                    if li.find('h3', class_='pledge__title'):
                        title = li.find('h3', class_='pledge__title').text.strip()
                    else:
                        title = None
                    rewards.append({
                        'project_id': project_id,
                        'title': title,
                        'description': description,
                        'reward_status': reward_status,
                        'pledge_minimum': pledge_minimum,
                        'ship_status': ship_status,
                        'ship_to': ship_to,
                        'estimated_delivery': estimated_delivery,
                        'backers_count': backers_count,
                    })
            self.logger.info(f'Type: {type_crawl} | Status: Crawled {len(rewards)} {type_crawl} data|  URL: {url_reward}')
            return rewards
            
        else:
            self.logger.error(f'Type: {type_crawl} | Status: HTTP request error {status_code} code |  URL: {url_reward}')
            return None

    def data_process(self, results=None, read_file=True, file_path=None, df=None, output=False):
        if df is None and read_file:
            if os.path.isfile(file_path):
                df = pd.read_excel(file_path, engine='openpyxl')
            else:
                df = pd.DataFrame()
        elif df is None and read_file is False:
            raise ValueError('Please sure have read_file or df pandas')

        if results is not None:
            rows = []
            for key, value in results.items():
                if df.get(key) is None:
                    df[key] = value
                else:
                    rows.append(value)
            
            if rows:
                df.loc[len(df)] = rows
        if output is True:
            save_excel(df, file_path, self.logger, message=f'Update data in {file_path}')
        else:
            return df
        