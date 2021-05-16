from seleniumwire.undetected_chromedriver.v2 import Chrome, ChromeOptions
from .utils import check_and_create_file
from time import sleep


from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

class SeleniumHander:
    def __init__(self,
            chrome_driver_path = None,
            headless_browser = False,
            disable_image_load = False,
            page_delay = 10,
            logfolder = 'logs/',
            user_agent = None,
            proxy_address = None,
            proxy_port = None,
            proxy_username = None,
            proxy_password = None,
            limit_scropes = None,
            exclude_hosts = None,
        ):
        """Starts local session for a selenium server. Default case scenario."""

        # Chrome driver log in specific user logfolder
        chromedriver_log = f"{logfolder}chromedriver.log"
        check_and_create_file(chromedriver_log)

        # load self for using global
        self.disable_image_load = disable_image_load

        # Create Chrome options
        chrome_options = ChromeOptions()
        seleniumwire_options = {}
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--disable-notifications')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--mute-audio')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')

        # Disable Image loading, this will replace with request_interceptor
        # prefs = {"profile.managed_default_content_settings.images": 2}
        # chrome_options.add_experimental_option("prefs", prefs)

        # The list exclude hosts for capturing
        if exclude_hosts:
            seleniumwire_options['exclude_hosts'] = exclude_hosts

        # Add proxy with username and password authentication
        if proxy_address and proxy_port:
            if proxy_username and proxy_password:
                seleniumwire_options['proxy'] = {
                    'http': f'http://{proxy_username}:{proxy_password}@{proxy_address}:{proxy_port}',
                    'https': f'https://{proxy_username}:{proxy_password}@{proxy_address}:{proxy_port}',
                    'no_proxy': 'localhost,127.0.0.1'
                }
            else:
                seleniumwire_options['proxy'] = {
                    'http': f'http://{proxy_address}:{proxy_port}',
                    'https': f'https://{proxy_address}:{proxy_port}',
                    'no_proxy': 'localhost,127.0.0.1'
                }

        # headless option 
        if headless_browser:
            chrome_options.headless = True
            chrome_options.add_argument('--headless')

        # User-Agent
        chrome_options.add_argument(f'--user-agent={user_agent}')

        self.driver = Chrome(executable_path=chrome_driver_path, options=chrome_options, seleniumwire_options=seleniumwire_options, service_log_path=chromedriver_log)

        # Limit capture urls with regulater expression
        if limit_scropes:
            self.driver.scopes = limit_scropes

        # Intercepting Requests and Responses
        self.driver.request_interceptor = self.interceptor
        # self.driver.response_interceptor = self.interceptor

        # Set implicitly wait
        self.driver.implicitly_wait(page_delay)

        # Set maximum windows
        self.driver.maximize_window()


    def interceptor(self, request):
        """Intercepting Requests and Responses

        Args:
            request: The http request
            response:The http response
        """
        # Disable image loading
        if self.disable_image_load:
            # Block PNG, JPEG and GIF images
            if request.path.endswith(('.png', '.jpg', '.gif')):
                request.abort()

    def http_status_code(self, url):
        for request in self.driver.requests:
                if request.response:
                    if request.url == url:
                        status_code = request.response.status_code
        if status_code:
            return status_code
        else:
            return False

    def scroll_down_to_bottom(self):
        """
        The Script Copy by: https://stackoverflow.com/questions/20986631/how-can-i-scroll-a-web-page-using-selenium-webdriver-in-python
        """
        SCROLL_PAUSE_TIME = 0.5

        # Get scroll height
        last_height = self.driver.execute_script("return document.body.scrollHeight")

        while True:
            # Scroll down to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait to load page
            sleep(SCROLL_PAUSE_TIME)

            # Calculate new scroll height and compare with last scroll height
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def load_more_click(self, time_wait, load_more_xpath):
        while True:
            try:
                wait = WebDriverWait(self.driver, time_wait)
                element = wait.until(EC.element_to_be_clickable((By.XPATH, load_more_xpath)))
                element.click()
                self.scroll_down_to_bottom()
            except:
                break
