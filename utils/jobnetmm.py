from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException

import time
import pandas as pd
import logging

## Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('logs/jobnetmm_e_logs.log', mode='a')
fomatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(fomatter)
logger.addHandler(handler)

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
                        # Try to get the job title(handle both regular and top list jobs)
                        try:
                            title = job.find_element(By.CSS_SELECTOR, "a.search__job-title.ClickTrack-JobDetail").text.strip()
                        except NoSuchElementException:
                            title = job.find_element(By.CSS_SELECTOR, "a.search__job-title.ClickTrack-TopList").text.strip()

                        try:
                            company = job.find_element(By.CSS_SELECTOR, "a.ClickTrack-EmpProfile").text.strip()
                        except NoSuchElementException:
                            try:
                                # Alternative way to find company element - sometimes the class is on the element without "a."
                                company = job.find_element(By.XPATH, ".//a[@class='ClickTrack-EmpProfile']").text.strip() 
                            except NoSuchElementException:
                                company = None
                        
                        try:
                            location = job.find_element(By.CLASS_NAME, "p.search__job-location").text.strip()
                        except NoSuchElementException:
                            location = None

                        try:
                            date = job.find_element(By.CLASS_NAME, "p.search__job-posted u").text.strip()
                        except NoSuchElementException:
                            date = None

                        try:
                            job_link = job.find_element(By.CSS_SELECTOR, "div.c-btn__wrapper a.c-btn").get_attribute("href")
                        except NoSuchElementException:
                            job_link = None

                        # Append job data to the list
                        self.jobs.append({
                            "Title": title,
                            "Company": company,
                            "Location": location,
                            "Date_Posted": date,
                            "Job_Link": job_link
                        })

                    except exception as e:
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