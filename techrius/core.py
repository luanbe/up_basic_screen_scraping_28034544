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

from .mysql_piplines import create_database, create_table, insert_data
from .browser import SeleniumHander
from .utils import random_sleep, load_cookie, save_cookie, check_and_create_file, save_excel, load_json_file, update_json_file, convert_html_to_text, convert_html_to_json
from logging.handlers import RotatingFileHandler
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
from datetime import datetime
from requests.sessions import session

class TSpider:

    def __init__(
            self,
            spider_name: str = None,
            default_url: str= None,
            action_delay: tuple = (1,3),
            request_delay: tuple = (2,3),
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
        ):
        self.default_url = default_url
        self.action_delay = action_delay
        self.request_delay = request_delay
        self.show_logs = show_logs
        self.json_file = json_file


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
            chromedriver_path = shutil.which('assets/linux/chromedriver')

        # logs path
        self.logfolder = 'logs/'
    
        # Add logger
        self.logger = self.get_logger(self.show_logs)

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

        self.selenium = SeleniumHander(
            chrome_driver_path = chromedriver_path,
            headless_browser = headless_browser,
            disable_image_load = disable_image_load,
            page_delay = 30,
            logfolder = self.logfolder,
            user_agent = self.user_agent,
            proxy_address = proxy_address,
            proxy_port = proxy_port,
            proxy_username = proxy_username,
            proxy_password = proxy_password
        )
        self.browser = self.selenium.driver
        self.logger.info('Checking the connect to MYSQL server....')
        create_database(self.logger)
        create_table(self.logger)
        self.logger.info('Complete to connect to MYSQL server....')
        

    def check_errors(self):
        errors = ['Backer or bot?', 'You are sending too many requests to Kickstarter at this time.']
        soup = BeautifulSoup(self.browser.page_source, 'lxml')
        text = soup.text.strip()
        error_show = False
        for error in errors:
            if error in text:
                self.logger.info(f'Detected error:{error}')
                error_show = True
                break
        
        if error_show == False:
            self.logger.info(f'Not found errors....')
            return False
        return True

    def block_fix(self, url_block):
        self.browser.get(url_block)
        try_fix = 5
        count_fix = 1
        time_fix = 1
        status_fix = False
        while True:
            if count_fix > 5:
                break

            if self.check_errors():
                time.sleep(time_fix)
                self.browser.refresh()
                count_fix += 1
                time_fix += 3
            else:
                status_fix = True
                break
        save_cookie(self.browser, self.spider_name, self.logger)
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
                self.logger.info(f'Type: {type_name} | HTTP Request Successful: {r.status_code} | Method: {method} | URL: {url}')
                rstatus = True
                break
            if r.status_code == 429:
                self.logger.info(f'Type: {type_name} | HTTP Request Error: {r.status_code} | Method: {method} | URL: {url}')
                self.logger.info(f'Type: {type_name} | HTTP Request again | Method: {method} | URL: {url}')
                session.cookies.clear()
                headers = session.headers
                session = requests.Session()
                session.headers = headers
                random_sleep((60,120))
            else:
                self.logger.info(f'Type: {type_name} | HTTP Request Error: {r.status_code} | Method: {method} | URL: {url}')
                break
        if rstatus:
            return r
        else:
            return rstatus


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
        if self.proxy_username and self.proxy_password and self.proxy_address and self.proxy_port:
            proxies = {
                'http': f'http://{self.proxy_username}:{self.proxy_password}@{self.proxy_address}:{self.proxy_port}'
            }
            session.proxies.update(proxies)
        session.headers = self.headers
        data = session.get('http://extreme-ip-lookup.com/json/')
        self.logger.info(f'IP:{data.json()["query"]} | Country:{data.json()["country"]} | City:{data.json()["city"]} | State: {data.json()["continent"]}| ipType: {data.json()["ipType"]}')
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
        general_log = "{}general.log".format(self.logfolder)
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
    


    def crawl_pages(self, category_id, current_page, current_project_id, json_file=None):
        project_id = None
        item_count = 0
        page_now = None
        reach_limit_page = False
        while True:
            has_more = None
            url_api = 'https://www.kickstarter.com/graph'
            #### Scrape categories ######
            url_page = f'https://www.kickstarter.com/projects/search.json?search=&category_id={category_id}&page={current_page}'
            self.logger.info(f'Type: Crawl Projects of Category | Status: Crawling | URL: {url_page}')
            self.browser.get(url_page)
            # random_sleep(self.request_delay, self.logger)

            # get the status code
            status_code = self.selenium.http_status_code(url_page)
            # Check status code and crawl data
            if status_code == 200:
                # Load json data
                
                json_data = convert_html_to_json(self.browser.page_source, self.browser)

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
                        # slug url & url
                        url = project.get('urls').get('web').get('project')
                        print(url)
                        url_reward = project.get('urls').get('web').get('rewards')
                        url_api_user = project.get('creator').get('urls').get('api').get('user')
                        creator_slug = project.get('creator').get('slug')
                        project_slug = project.get('slug')
                        full_project_slug = f'{creator_slug}/{project_slug}'
                        
                        project_id = project.get('profile').get('project_id')

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
                        crawl_project =  self.crawl_project(url=url, url_api=url_api, url_api_user = url_api_user)
                        story = crawl_project[0]
                        creator = crawl_project[1]
                        updates = self.crawl_updates(url, url_api, project_id)
                        comments = self.crawl_comments(url, url_api, project_id)
                        rewards = self.crawl_rewards(url_reward, project_id)

                        if project.get('goal') and project.get('usd_exchange_rate'):
                            goal = project.get('goal') * project.get('usd_exchange_rate')
                        else:
                            goal = None


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
                                'location': project.get('location').get('displayable_name'),
                                'creator_id': project.get('creator').get('id'),
                                'url': url,
                                'story': story,
                        }
                       
                        
                        insert_data(creator, results, updates, comments, rewards, self.logger)
                        json = load_json_file(f'./assets/jsons/{json_file}')
                        if json:
                            for data in json:
                                if data['id'] == category_id:
                                    data['page'] = current_page
                                    data['project_id'] = project_id
                            update_json_file(f'./assets/jsons/{json_file}', json)
                        
                        page_now = current_page
                            
                        item_count += 1
                        self.logger.info(f'Website crawl total: {item_count} | Current pages: {current_page}')
                else:
                    self.logger.warning(f'Type: Crawl Projects of Category | Status: Not found the project| URL: {url}')
                    break
                if has_more:
                    current_page += 1
                else:
                    self.logger.warning(f'Type: Crawl Projects of Category | Status: Not Has More page| URL: {url}')
                    break
            elif status_code == 404:
                self.logger.warning(f'Type: Crawl Projects of Category | Status: Reached to limit | URL: {url}')
                reach_limit_page = True
                json = load_json_file(f'./assets/jsons/{json_file}')
                if json:
                    for data in json:
                        if data['id'] == category_id:
                            data['page'] = current_page
                            data['project_id'] = project_id
                            data['limit_page'] = reach_limit_page
                    update_json_file(f'./assets/jsons/{json_file}', json)
                break
            else:
                self.logger.warning(f'Type: Crawl Projects of Category | Status: HTTP request error {status_code} code |  URL: {url_page}')

        return page_now, reach_limit_page, project_id

    def crawl_project(self, url, url_api, url_api_user):
        url = re.sub(r'\?.+', '', url)
        url = url + '/description'
        story = None
        verified_identity = None
        creator = None
        """ ++++++++++++ Crawling story in project campaign ++++++++++++"""
        type_crawl = 'Story'
        self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} | URL: {url}')
        self.browser.get(url)
        # random_sleep(self.request_delay, self.logger)

        status_code = self.selenium.http_status_code(url)
        if status_code == 200:
            random_sleep(self.request_delay, self.logger)
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
            if story:
                self.logger.info(f'Type: {type_crawl} | Status: Crawled {type_crawl} data |  URL: {url}')
            else:
                self.logger.warning(f'Type: {type_crawl} | Status: Not found {type_crawl} data |  URL: {url}')

            """ ++++++++++++ Crawling Verified Identity in project campaign ++++++++++++"""

            type_crawl = 'Verified Identity Name'
            self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} | URL: {url}')
            try:
                bio_element = self.browser.find_element_by_xpath('(//div[@class="creator-name"]//a)[1]')
                if bio_element:
                    bio_element.click()
                    random_sleep(self.action_delay)
            except:
                pass

            verified_identity = self.crawl_verified_identity(self.browser.page_source)
            if verified_identity:
                self.logger.info(f'Type: {type_crawl} | Status: Crawled {type_crawl} |  URL: {url}')
            else:
                self.logger.warning(f'Type: {type_crawl} | Status: Not found {type_crawl} data |  URL: {url}')
        else:
            self.logger.warning(f'Type: {type_crawl} | Status: HTTP request error {status_code} code |  URL: {url}')

        """ ++++++++++++ Crawling Creators in project campaign ++++++++++++"""
        type_crawl = 'Creators'
        self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} | URL: {url_api_user}')
        self.browser.get(url_api_user)
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
            self.logger.info(f'Type: {type_crawl} | Status: Crawled {type_crawl} |  URL: {url}')
        else:
            self.logger.warning(f'Type: {type_crawl} | Status: HTTP request error {status_code} code |  URL: {url}')
        
        return story, creator


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
        # random_sleep(self.request_delay, self.logger)

        status_code = self.selenium.http_status_code(url)
        if status_code == 200:
            # Click load more until load more is not show
            self.selenium.load_more_click(3, '//span[contains(text(),"Load more")]')
            random_sleep(self.request_delay, self.logger)
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
            if update_list:
                self.logger.info(f'Type: {type_crawl} | Status: Crawled {len(update_list)} {type_crawl} data |  URL: {url}')
            else:
                self.logger.warning(f'Type: {type_crawl} | Status: Not found {type_crawl} data |  URL: {url}')
        else:
            self.logger.warning(f'Type: {type_crawl} | Status: HTTP request error {status_code} code |  URL: {url}')

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
                            updates_data.append({
                                'project_id': project_id,
                                'title': edge_data.get('title').strip(),
                                'body': body,
                                'comment_count': edge_data.get('commentsCount'),
                                'like_count': edge_data.get('likesCount'),
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
        # random_sleep(self.request_delay, self.logger)

        status_code = self.selenium.http_status_code(url)
        if status_code == 200:
        # Click load more until load more is not show
            self.selenium.load_more_click(3, '//span[contains(text(),"Load more")]')
            random_sleep(self.request_delay, self.logger)
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
            if comment_list:
                self.logger.info(f'Type: {type_crawl} | Status: Crawled {len(comment_list)} {type_crawl} data|  URL: {url}')
            else:
                self.logger.warning(f'Type: {type_crawl} | Status: Not found {type_crawl} data |  URL: {url}')
        else:
            self.logger.warning(f'Type: {type_crawl} | Status: HTTP request error {status_code} code |  URL: {url}')

    def crawl_comments_json(self, json_data, project_id):
        comments_data = []
        for data in json_data:
            try:
                # Check nested json data
                edges = data.get('data').get('commentable').get('comments').get('edges')
    
                if edges:
                    for edge in edges:
                        node = edge.get('node')
                        body = convert_html_to_text(node.get('body'))

                        comments_data.append({
                            'project_id': project_id,
                            'author': node.get('author').get('name'),
                            'body': body,
                            'id': node.get('id'),
                            'parent_id': node.get('parentId'),
                            'comment_type': 'Comment'
                        })
                        replies_count = node.get('replies').get('totalCount')
                        if replies_count > 0:
                            node_replys = node.get('replies').get('nodes')
                            for node_reply in node_replys:
                                body_reply = convert_html_to_text(node_reply.get('body'))
                                comments_data.append({
                                'project_id': project_id,
                                'author': node_reply.get('author').get('name'),
                                'body': body_reply,
                                'id': node_reply.get('id'),
                                'parent_id': node_reply.get('parentId'),
                                'comment_type': 'Reply Comment'
                            })
            except:
                pass
        return comments_data

    def crawl_rewards(self, url_reward, project_id):
        rewards = []
        """ ++++++++++++ Crawling rewards in project campaign ++++++++++++"""
        type_crawl = 'Rewards'
        self.logger.info(f'Type: {type_crawl} | Status: Begin for crawling {type_crawl} | URL: {url_reward}')
        self.browser.get(url_reward)
        # random_sleep(self.request_delay, self.logger)

        status_code = self.selenium.http_status_code(url_reward)

        if status_code == 200:
            reward_elements = SoupStrainer('div', class_='NS_projects__rewards_list js-project-rewards')
            soup = BeautifulSoup(self.browser.page_source, 'lxml', parse_only=reward_elements)
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
                    rewards.append({
                        'project_id': project_id,
                        'title': li.find('h3', class_='pledge__title').text.strip(),
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
            self.logger.warning(f'Type: {type_crawl} | Status: HTTP request error {status_code} code |  URL: {url}')
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
        