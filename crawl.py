from os import truncate
import threading

from toripchanger.changer import TOR_PORT
from techrius.utils import load_json_file
from techrius.mysql_piplines import fetch_crawl, fetch_all_crawler
from techrius.utils import smart_run
from techrius.core import TSpider
from time import sleep
import settings

def crawl_data(thread_name, category_id, current_page, project_id, state, crawl_id):
    limit_page = False
    while True:
        
        session = TSpider(
            spider_name=thread_name,
            default_url='https://www.kickstarter.com/',
            headless_browser=settings.headless_browser,
            action_delay=settings.action_delay,
            request_delay=settings.request_delay,
            proxy_username=settings.proxy_username,
            proxy_password=settings.proxy_password,
            proxy_address=settings.proxy_address,
            proxy_port=settings.proxy_port,
            use_tor=settings.use_tor,
            tor_password=settings.tor_password,
            tor_port=settings.tor_port,
            crawl_type=settings.crawl_type,
            # user_agent=random_user_agent(),
        )
        try:
            with smart_run(session):
                crawl = session.crawl_pages(category_id, current_page, project_id, state, crawl_id)

                current_page = crawl[0]
                limit_page = crawl[1]
                project_id = crawl[2]
        except:
            print('=============HAVE SOME ERROR | SLEEP 50\'s================')
            sleep(50)
            crawl = fetch_crawl(crawl_id, logger=None)
            current_page = crawl['page_number']
            if crawl['limit_status'] == 0:
                limit_page = False
            else:
                limit_page = True
            project_id = crawl['project_id']

        # When reach to limit page
        if limit_page:
            break


if __name__ == '__main__':
    crawler = settings.crawler_name
    crawler_data  = fetch_all_crawler(crawler, logger=None)
    threads = list()
    for data in crawler_data:
        if data['limit_status'] == 0:
            limit_status = False
        else:
            limit_status = True
        if not limit_status:
            category_name = data['category_name']
            category_id = data['category_id']
            page_number = data['page_number']
            project_id = data['project_id']
            state = data['state']
            crawl_id = data['crawl_id']

            thread_name = f'{crawler}_{category_name}_{state}'
            print(f"Begin to start and create thread: {thread_name}")
            x = threading.Thread(
                        target=crawl_data,
                        args=(
                                thread_name,
                                category_id,
                                page_number,
                                project_id,
                                state,
                                crawl_id
                            ),
                        daemon=True
                )
            threads.append(x)
            x.start()
            sleep(10)

    for index, thread in enumerate(threads):
        print("Main    : before joining thread %d.", index)
        thread.join()
        print("Main    : thread %d done", index)