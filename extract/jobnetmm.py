from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException
from datetime import datetime
import time
import pandas as pd

## Set up logging
from utils.logger import get_module_logger
logger = get_module_logger(__name__, group='extract')

## Class for extracting jobs
class JobNetScraper:
    def __init__(self, email:str, password:str, headless:bool=True):
        self.email = email
        self.password = password
        self.headless = headless
        self.driver = None
        self.wait = None
        self.jobs = []

    def start_driver(self):
        options = Options()
        if self.headless:
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.wait = WebDriverWait(self.driver, 18)

    def login(self):
        self.driver.get("https://www.jobnet.com.mm/login")
        self.wait.until(EC.presence_of_element_located((By.ID, "BodyPlaceHolder_txtEmail"))).send_keys(self.email)
        self.driver.find_element(By.ID, "BodyPlaceHolder_txtLoginPassword").send_keys(self.password)
        self.driver.find_element(By.ID, "BodyPlaceHolder_btnSignIn").click()

        try:
            self.wait.until(EC.url_contains("dashboard"))
            logger.info("Login Successful! Dashboard loaded.")
        except TimeoutException:
            logger.error("Login failed! Check credentials/captcha.")
            self.driver.quit()
            raise Exception("Login failed!")
        
    def scrape_jobs(self, job_function:int, location:int=0):
        try:
            self.driver.get(f"https://www.jobnet.com.mm/jobs?keyword=&jobfunction={job_function}&location")
            logger.info("Redirected to jobs page")
        except Exception as e:
            logger.error(f"Error navigating to jobs page: {e}")
            self.driver.quit()
            raise

        page = 1

        try:
            while True:
                self.wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "serp-item")))
                job_cards = self.driver.find_elements(By.CLASS_NAME, "serp-item")

                for job in job_cards:
                    try:
                        headings = job.find_elements(By.CLASS_NAME, "search__job-heading")
                        title = ""
                        if len(headings) >= 1:
                            primary_anchor = headings[0].find_element(By.TAG_NAME, "a")
                            primary_title = primary_anchor.text.strip()
                            title = primary_title if primary_title else logger.warning("Title not found.")

                        if len(headings) >= 2:
                            secondary_anchor = headings[1].find_element(By.TAG_NAME, "a")
                            secondary_title = secondary_anchor.text.strip("()")
                            if secondary_title:
                                title += f" ({secondary_title})"

                        # Try to get the company name
                        if job.find_elements(By.CSS_SELECTOR, "a.ClickTrack-EmpProfile"):
                            company = job.find_element(By.CSS_SELECTOR, "a.ClickTrack-EmpProfile").text.strip()
                        else:
                            logger.warning("Company name not found.")
                            company = None

                        # Try to get the location
                        location_element = job.find_element(By.CSS_SELECTOR, "p.search__job-location span")
                        location = location_element.text.strip() if location_element else None
                        
                        # Try to get salary
                        salary_elements = job.find_elements(By.CSS_SELECTOR, "a.search__job-sign.ClickTrack-JobDetail span")
                        if salary_elements:
                            salary = salary_elements[0].text.strip() if salary_elements else None
                        else:
                            salary = None
                            logger.warning("Salary not found.")

                        # Try to get the date posted
                        date_element = job.find_element(By.CSS_SELECTOR, "p.search__job-posted u")
                        date = date_element.text.strip() if date_element else None

                        # Try to get the job link
                        job_link_element = job.find_element(By.CSS_SELECTOR, "div.c-btn__wrapper a.c-btn")
                        job_link = job_link_element.get_attribute("href") if job_link_element else None

                        # Append job data to the list
                        self.jobs.append({
                            "Title": title,
                            "Company": company,
                            "Location": location,
                            'Salary': salary,
                            "Date_Posted": date,
                            "Job_Link": job_link
                        })

                    except Exception as e:
                        logger.warning(f"Error scraping on page {page}: {e}")
                        continue
                
                logger.info(f"Page {page}: Scraped. {len(self.jobs)} jobs.")

                ## Go to next page
                try:
                    buttons = self.driver.find_elements(By.CSS_SELECTOR, "a.search__action-btn")
                    next_button = None
                    for button in buttons:
                        if button.text.strip() == ">" and not button.get_attribute("disabled"):
                            next_button = button
                            break
                    
                    if not next_button:
                        logger.info("No more pages to scrape.")
                        break

                    page += 1

                    # Use execute cause button is javaScript generated
                    self.driver.execute_script("__doPostBack('ctl00$BodyPlaceHolder$pagerControl','{}')".format(page))
                    time.sleep(2) # Wait for the page to load

                    try:
                        self.wait.until(EC.staleness_of(job_cards[0]))
                    except (TimeoutException, StaleElementReferenceException):
                        pass
                except Exception as e:
                    logger.error(f"Error navigating to next page: {e}")
                    break
        except TimeoutException:
            logger.error("Timeout while waiting for job cards to load.")

    def get_jobs(self, job_function:int):
        self.start_driver()
        try:
            self.login()
            self.scrape_jobs(job_function)
        finally:
            self.driver.quit()
            logger.info("Driver closed.")
        logger.info(f"Total jobs scraped: {len(self.jobs)}")
        return pd.DataFrame(self.jobs)