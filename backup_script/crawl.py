import threading
from techrius.utils import load_json_file, update_json_file, random_user_agent

from techrius.utils import smart_run
from techrius.core import TSpider
from time import sleep
import random


def crawl_data(category_id, category_name, current_page, project_id, json_file):
    
    while True:
        try:
            session = TSpider(
                website_name=f'kickstarter_{category_name}',
                website_default_url='https://www.kickstarter.com/',
                headless_browser=True,
                proxy_username='keipavqn',
                proxy_password='AlK0chgA9pwjUYu7',
                proxy_address='3.224.74.126',
                proxy_port='31112',
                # user_agent=random_user_agent(),
                page_delay=50,
                json_file=json_file
            )
            
            crawl = session.crawl_pages(category_id, current_page, project_id)

            current_page = crawl[0]
            limit_page = crawl[1]
            project_id = crawl[2]
            json = load_json_file(f'./assets/jsons/{json_file}')
            if json:
                for data in json:
                    if data['id'] == category_id:
                        data['page'] = current_page
                        data['limit_page'] = limit_page
                        data['project_id'] = project_id
                update_json_file(f'./assets/jsons/{json_file}', json)

            # When reach to limit page
            if limit_page:
                break
        except:
            print('=============HAVE SOME ERROR | SLEEP 50\'s================')
            sleep(50)
 

if __name__ == '__main__':
    json_file = 'support_4.json'
    json = load_json_file(f'./assets/jsons/{json_file}')
    threads = list()
    for data in json:
        if data['limit_page'] is False:
            thread_name = data['category_name']
            print(f"Begin to start and create thread: {thread_name}")
            x = threading.Thread(target=crawl_data, args=(data['id'], data['category_name'], data['page'], data['project_id'], json_file), daemon=True)
            threads.append(x)
            x.start()

    for index, thread in enumerate(threads):
        print("Main    : before joining thread %d.", index)
        thread.join()
        print("Main    : thread %d done", index)