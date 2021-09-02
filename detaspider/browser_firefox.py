from seleniumwire import webdriver
from selenium.webdriver.firefox.options import Options as Firefox_Options
from .utils import check_and_create_file
from time import sleep


from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

class SeleniumHander:
    def __init__(self,
            geckodriver_path = None,
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
            browser_profile_path = None,
            browser_executable_path = None,
            geckodriver_log_level = 'info'
        ):
        """Starts local session for a selenium server. Default case scenario."""
        firefox_options = Firefox_Options()
        seleniumwire_options = {}

        if headless_browser:
            firefox_options.add_argument("-headless")

        if browser_profile_path is not None:
            firefox_profile = webdriver.FirefoxProfile(browser_profile_path)
        else:
            firefox_profile = webdriver.FirefoxProfile()

        if browser_executable_path is not None:
            firefox_options.binary = browser_executable_path


        # set "info" by default
        # set "trace" for debubging, Development only
        firefox_options.log.level = geckodriver_log_level

        # set English language
        firefox_profile.set_preference("intl.accept_languages", "en-US")

        # set User-Agent
        if user_agent is not None:
            firefox_profile.set_preference("general.useragent.override", user_agent)

        if disable_image_load:
            # permissions.default.image = 2: Disable images load,
            # this setting can improve pageload & save bandwidth
            firefox_profile.set_preference("permissions.default.image", 2)
            
        # mute audio while watching stories
        firefox_profile.set_preference("media.volume_scale", "0.0")

        # prevent Hide Selenium Extension: error
        firefox_profile.set_preference("dom.webdriver.enabled", False)
        firefox_profile.set_preference("useAutomationExtension", False)
        firefox_profile.set_preference("general.platform.override", "iPhone")
        firefox_profile.update_preferences()

        # geckodriver log in specific user logfolder
        geckodriver_log = "{}geckodriver.log".format(logfolder)
        check_and_create_file(geckodriver_log)


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



        self.driver = webdriver.Firefox(
                firefox_profile=firefox_profile,
                executable_path=geckodriver_path,
                log_path=geckodriver_log,
                options=firefox_options,
                seleniumwire_options=seleniumwire_options
            )
        # Limit capture urls with regulater expression
        if limit_scropes:
            self.driver.scopes = limit_scropes

        # Set implicitly wait
        self.driver.implicitly_wait(page_delay)

        # Set maximum windows
        self.driver.maximize_window()


    def http_status_code(self, url):
        status_code = None
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
