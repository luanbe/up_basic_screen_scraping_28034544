import threading
import random
from toripchanger.changer import TOR_PORT
from techrius.mysql_piplines import fetch_creators_not_crawl, update_crawl_creator_status
from techrius.utils import smart_run
from techrius.core import TSpider
from time import sleep
import settings

def crawl_projects_creator(thread_name):
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
    )
    while True:
        try:
            creators  = fetch_creators_not_crawl(logger=None)
            if creators:
                creator = random.choice(creators)
                creator_id = creator['creator_id']
                result = session.crawl_projects_creator(creator_id)
                crawl_status = result[0]
                active_projects = result[1]
                update_crawl_creator_status(creator_id, crawl_status, active_projects, logger=session.logger)
            else:
                print('khong co creators')
                break
        except:
            pass


if __name__ == '__main__':
    no_crawler = settings.no_crawler
    crawl_name = settings.crawler_name
    threads = list()
    for i in range(no_crawler):
            i += 1
            thread_name = f'{crawl_name}_{i}'
            print(f"Begin to start and create thread: {thread_name}")
            x = threading.Thread(
                        target=crawl_projects_creator,
                        args=(
                                thread_name,
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
