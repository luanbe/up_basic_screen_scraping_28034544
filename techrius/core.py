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
from .browser import set_selenium_session
from .utils import random_sleep, load_cookie, save_cookie, check_and_create_file, save_excel, load_json_file, update_json_file
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from logging.handlers import RotatingFileHandler
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
from datetime import datetime
from requests.sessions import session

class TSpider:

    def __init__(
        self,
        website_name: str = None,
        website_default_url: str= None,
        page_delay: int = 25,
        action_delay: tuple = (1,3),
        request_delay: tuple = (1,3),
        batch_analysis_limit: int = 200,
        save_data_limit: int = 100,
        show_logs: bool = True,
        user_agent: str = None,
        proxy_address: str = None,
        proxy_port: str = None,
        proxy_username: str = None,
        proxy_password: str = None,
        headless_browser: bool = True,
        disable_image_load: bool = True,
        geckodriver_log_level: str = "info",  # "info" by default
        folder_data: str = 'data',
        json_file: str= None,
    ):
        self.page_delay = page_delay
        self.website_default_url = website_default_url
        self.action_delay = action_delay
        self.request_delay = request_delay
        self.batch_analysis_limit = batch_analysis_limit
        self.save_data_limit = save_data_limit
        self.proxy_address = proxy_address
        self.proxy_port = proxy_port
        self.proxy_username = proxy_username
        self.proxy_password = proxy_password
        self.headless_browser = headless_browser
        self.disable_image_load = disable_image_load
        self.geckodriver_log_level = geckodriver_log_level
        self.show_logs = show_logs
        self.folder_data = 'data/'
        self.json_file = json_file


        if website_name is None:
            raise Exception('Please add website name!')
        else:
            self.website_name = website_name


        if user_agent is None:
            self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
        else:
            self.user_agent = user_agent

        ############ Browser Settings ############
        # Add profile browser path
        self.browser_profile_path = None

        # Add geckodriver path
        if sys.platform == 'win32':
            self.geckodriver_path = shutil.which('assets/windows/geckodriver.exe')
        elif sys.platform == 'darwin':
            self.geckodriver_path = shutil.which('assets/macos/geckodriver')
        else:
            self.geckodriver_path = shutil.which('assets/linux/geckodriver')

        # Add custom browser version path
        # self.browser_executable_path = 'assets/FirefoxPortable/App/Firefox64/firefox.exe'
        self.browser_executable_path = None

        # logs path
        self.logfolder = 'logs/'
    
        # Add logger
        self.logger = self.get_logger(self.show_logs)

        self.headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en,vi;q=0.9,en-US;q=0.8',
        }
        
        # self.browser = set_selenium_session(
        #                 self.proxy_address,
        #                 self.proxy_port,
        #                 self.proxy_username,
        #                 self.proxy_password,
        #                 self.headless_browser,
        #                 self.browser_profile_path,
        #                 self.disable_image_load,
        #                 self.page_delay,
        #                 self.geckodriver_path,
        #                 self.browser_executable_path,
        #                 self.logfolder,
        #                 self.logger,
        #                 self.geckodriver_log_level,
        #                 self.user_agent
        # )
        create_database(self.logger)
        create_table(self.logger)

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
        save_cookie(self.browser, self.website_name, self.logger)
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
            else:
                self.logger.info(f'Type: {type_name} | HTTP Request Error: {r.status_code} | Method: {method} | URL: {url}')
                break
        if rstatus:
            return r
        else:
            return rstatus


    def api_session(self):
        # cookie_file_path = f'assets/cookies/{self.website_name}.pkl'
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
        self.browser.get(self.website_default_url)
        cookie_status = load_cookie(self.browser, self.website_name, self.logger)
        if cookie_status:
            self.logger.info('Cookies loaded Successful...')
        else:
            save_cookie(self.browser, self.website_name, self.logger)

    def session_quit(self):
        save_cookie(self.browser, self.website_name, self.logger)
        self.logger.info('### SESSION QUIT ###')
        self.browser.quit()
        
    def get_logger(self, show_logs: bool, log_handler=None):
        """
        Handles the creation and retrieval of loggers to avoid
        re-instantiation.
        """
        # initialize and setup logging system for the InstaPy object
        logger = logging.getLogger(self.website_name)
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
        extra = {"website": self.website_name}
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
    


    def crawl_pages(self, category_id, current_page, current_project_id):
        api_session = self.api_session()
        project_id = None
        item_count = 0
        page_now = None
        reach_limit_page = False
        while True:
            has_more = None
            #### Scrape categories ######
            headers={
                'accept': 'application/json, text/javascript, */*; q=0.01',
                'x-requested-with': 'XMLHttpRequest',
            }
            api_session.headers = headers
            
            url = f'https://www.kickstarter.com/discover/advanced?google_chrome_workaround&category_id={category_id}&sort=magic&seed=2701536&page={current_page}'
            
            r = self.try_request(session=api_session, method='get', url=url, type_name='Crawl List Projects')
            if r:
                if r.status_code == 200:
                    json_data = r.json()
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

                        for project in projects[project_count:]:
                            # slug url & url
                            url = project.get('urls').get('web').get('project') 
                            url_reward = project.get('urls').get('web').get('rewards')
                            url_api_user = project.get('creator').get('urls').get('api').get('user')
                            creator_slug = project.get('creator').get('slug')
                            project_slug = project.get('slug')
                            full_project_slug = f'{creator_slug}/{project_slug}'
                            
                            project_id = project_id = project.get('profile').get('project_id')

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

                            # Crawl API Details:
                            crawl_details = self.crawl_details(api_session, url, url_reward, url_api_user, project_slug, full_project_slug, project_id)
                            if crawl_details:
                                if crawl_details[0]:
                                    story = crawl_details[0]
                                else:
                                    story = None

                                if crawl_details[1]:
                                    creators = crawl_details[1]
                                else:
                                    creators = None

                                if crawl_details[2]:
                                    updates = crawl_details[2]
                                else:
                                    updates = None

                                if crawl_details[3]:
                                    comments = crawl_details[3]
                                else:
                                    comments = None
                                
                                if crawl_details[4]:
                                    rewards = crawl_details[4]
                                else:
                                    rewards = None
                                
                                # Check all data is None
                                if project.get('name'):
                                    title = project.get('name').strip()
                                else:
                                    title = None
                                
                                if project.get('blurb'):
                                    blurb = project.get('blurb').strip()
                                else:
                                    blurb = None

                                if project.get('goal') and project.get('usd_exchange_rate'):
                                    goal = project.get('goal') * project.get('usd_exchange_rate')
                                else:
                                    goal = None


                                results = {
                                    'project_id': project_id,
                                    'title': title,
                                    'blurb': blurb,
                                    'feature_image': project.get('profile').get('feature_image_attributes').get('image_urls').get('default'),
                                    'category': project.get('category').get('name'),
                                    'parent_category': project.get('category').get('parent_name'),
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
                                # Process pipline for inserting to database
                                if creators:
                                    creators_list = [creators]
                                else:
                                    creators_list = None
                                
                                if results:
                                    results_list = [results]
                                else:
                                    results_list = None

                                if updates:
                                    updates_list = updates
                                else:
                                    updates_list = None

                                if comments:
                                    comments_list = comments
                                else:
                                    comments_list = None

                                if rewards:
                                    rewards_list = rewards
                                else:
                                    rewards_list = None
                                
                                insert_data(creators_list, results_list, updates_list, comments_list, rewards_list, self.logger)
                                json = load_json_file(f'./assets/jsons/{self.json_file}')
                                if json:
                                    for data in json:
                                        if data['id'] == category_id:
                                            data['page'] = current_page
                                            data['project_id'] = project_id
                                    update_json_file(f'./assets/jsons/{self.json_file}', json)
                               
                                page_now = current_page
                                
                                item_count += 1
                                self.logger.info(f'Website crawl total: {item_count} | Current pages: {current_page}')
                            else:
                                has_more = False
                    if has_more:
                        current_page += 1
                    else:
                        break
                elif r.status_code == 404:
                    self.logger.info('Reached to limit page....')
                    reach_limit_page = True
                    break
            else:
                self.logger.info('Can\'t fix error: HTTP Response Error')
                break
        return page_now, reach_limit_page, project_id


    def crawl_details(self, api_session, url, url_reward, url_api_user, slug, full_slug, project_id):
        url_api = 'https://www.kickstarter.com/graph'

        api_session.headers = self.headers 
        # Send the request for getting CSRF token
        r_html = self.try_request(session=api_session, method='get', url=url, type_name='Getting CSRF token')
        if r_html:
            csrf_token_tag = SoupStrainer('meta', attrs={'name':'csrf-token'})
            soup = BeautifulSoup(r_html.text, 'lxml', parse_only=csrf_token_tag)
            csrf_token = soup.meta.get('content')

            
            # Send post to get data from API
            api_session.headers.update({
                'content-type': 'application/json',
                'x-csrf-token': csrf_token
            })
            crawl_story = self.crawl_story(url, url_api, api_session, full_slug)
            crawl_updates = self.crawl_updates(url, url_api, api_session, slug, project_id)
            crawl_comments = self.crawl_comments(url, url_api, api_session, project_id)
            crawl_creator = self.crawl_creator(url, url_api, url_api_user, api_session, full_slug)
            crawl_rewards = self.crawl_rewards(url_reward, api_session, project_id)
            
            
            return crawl_story, crawl_creator, crawl_updates, crawl_comments, crawl_rewards
        else:
            self.logger.info('HTTP Request Error: Not found HTTP Status Code 200')
 

    def crawl_creator(self, url, url_api, url_api_user, session, full_slug):
        payloads = [
            {
                'query': 'query CreatorSection($slug: String!) {\n  me {\n    id\n    name\n  }\n  project(slug: $slug) {\n    id\n    verifiedIdentity\n    creator {\n      id\n      name\n      imageUrl(width: 100)\n      url\n      lastLogin\n      biography\n      isFacebookConnected\n      allowsFollows\n      backingsCount\n      location {\n        displayableName\n      }\n      launchedProjects {\n        totalCount\n      }\n      websites {\n        url\n        domain\n      }\n    }\n    collaborators {\n      edges {\n        node {\n          name\n          imageUrl(width: 200)\n          url\n        }\n        title\n      }\n    }\n  }\n}\n',
                'variables': {
                    'slug': full_slug
                }
            },
        ]
        request = self.try_request(session=session, method='post', url=url_api, payloads=json.dumps(payloads), type_name='Crawl Creator')
        if request:
            data = request.json()
            self.logger.info(f'Crawling Creator Verify Name from API URL: {url}')
            verified_identity = data[0].get('data').get('project').get('verifiedIdentity')
            if verified_identity:
                verified_name = verified_identity
            else:
                verified_name = None
        else:
            self.logger.info(f'Not Found API data: Creator Verify Name| URL: {url}')
            verified_name = None
        
        session.headers.update(self.headers)
        request_api_user = self.try_request(session=session, method='get', url=url_api_user, type_name='Crawl API User')
        if request_api_user:
            data = request_api_user.json()
            self.logger.info(f'Crawling Createtor API Uuser from API URL: {url_api_user}')
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
        else:
            self.logger.info(f'Not Found API data: Creator API User| URL: {url_api_user}')
            creator = None
        return creator

    def crawl_story(self, url, url_api, session, full_slug):
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
        if request:
            data = request.json()
            self.logger.info(f'Crawling story from API URL: {url}')
            story_data = data[0].get('data').get('project').get('story')
            if story_data:
                soup = BeautifulSoup(story_data, 'lxml')
                story = soup.text.strip()
            else:
                story = None
        else:
            self.logger.info(f'Not Found API data: Story | URL: {url}')
            story = None
        
        return story

    def crawl_updates(self, url, url_api, session, slug, project_id):
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
            request = self.try_request(session=session, method='post', url=url_api, payloads=json.dumps(payloads), type_name='Crawl Updates')
            if request:
                data = request.json()
                edges = data[0].get('data').get('project').get('timeline').get('edges')
                has_next_page = data[0].get('data').get('project').get('timeline').get('pageInfo').get('hasNextPage')
                if edges:
                    for edge in edges:
                        edge_data = edge.get('node').get('data')
                        title = edge_data.get('title')
                        if title:
                            if edge_data.get('body'):
                                soup = BeautifulSoup(edge_data.get('body'), 'lxml')
                                body = soup.text.strip()
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
                else:
                    break

                self.logger.info(f'Crawling Updates from API URL: {url} | Page: {page_count}')
                if has_next_page:
                    cursor = 'MTA='
                else:
                    break
            else:
                self.logger.info(f'Not Found API data: Project Updates | URL: {url} | Page: {page_count}')
                break
            page_count += 1
        return updates_data

    def crawl_comments(self, url, url_api, session, project_id ):
        url = re.sub(r'\?.+', '', url)
        url = f'{url}/comments'
        
        # Find commend ID
        session.headers.update(self.headers)
        r_html = self.try_request(session=session, method='get', url=url, type_name='Find Comment ID')

        commend_id = None
        if r_html:
            soup = BeautifulSoup(r_html.text, 'lxml')
            commend_id = soup.find('div', id='react-project-comments')['data-commentable_id']
        
        if commend_id:
            # Send a request API
            session.headers.update({
                    'content-type': 'application/json',
                })

            next_cursor = None
            comments_data = []
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
                request = self.try_request(session=session, method='post', url=url_api, payloads=json.dumps(payloads), type_name='Crawl Comments')
                if request:
                    data = request.json()
                    comments = data[0].get('data').get('commentable').get('comments')
                    edges = comments.get('edges')
                    page_info = comments.get('pageInfo')
                    has_next_page = page_info.get('hasNextPage')
                    end_cursor = page_info.get('endCursor')

                    if edges:
                        for edge in edges:
                            node = edge.get('node')
                            soup_comment = BeautifulSoup(node.get('body'), 'lxml')

                            comments_data.append({
                                'project_id': project_id,
                                'author': node.get('author').get('name').strip(),
                                'body': soup_comment.text.strip(),
                                'id': node.get('id'),
                                'parent_id': node.get('parentId'),
                                'comment_type': 'Comment'
                            })
                            replies_count = node.get('replies').get('totalCount')
                            if replies_count > 0:
                                node_replys = node.get('replies').get('nodes')
                                for node_reply in node_replys:
                                    soup_reply= BeautifulSoup(node_reply.get('body'), 'lxml')
                                    comments_data.append({
                                    'project_id': project_id,
                                    'author': node_reply.get('author').get('name').strip(),
                                    'body': soup_reply.text.strip(),
                                    'id': node_reply.get('id'),
                                    'parent_id': node_reply.get('parentId'),
                                    'comment_type': 'Reply Comment'
                                })
                    else:
                        break

                    self.logger.info(f'Crawling Updates from API URL: {url} | Page: {page_count}')
                    if has_next_page:
                        next_cursor = end_cursor
                    else:
                        break
                else:
                    self.logger.info(f'Not Found API data: Comment | URL: {url} | Page: {page_count}')
                    break
                page_count += 1
            return comments_data

    def crawl_rewards(self, url_reward, session, project_id):
        session.headers.update(self.headers)
        request = self.try_request(session=session, method='get', url=url_reward, type_name='Crawl Rewards')
        if request:
            reward_elements = SoupStrainer('div', class_='NS_projects__rewards_list js-project-rewards')
            soup = BeautifulSoup(request.text, 'lxml', parse_only=reward_elements)
            li_elements = soup.find_all('li', class_="hover-group js-reward-available pledge--available pledge-selectable-sidebar")
            rewards = []
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

            return rewards
        else:
            self.logger.info(f'Not Found API data: Rewards| URL: {url_reward} | ')
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
        