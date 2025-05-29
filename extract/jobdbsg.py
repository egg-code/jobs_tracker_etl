from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
import random
from datetime import datetime

# Setup logging
from utils.logger import get_module_logger
logger = get_module_logger(__name__, group='extract')


class JobsDBScraper:
    def __init__(self, max_pages=2, headless=True):
        self.max_pages = max_pages
        self.headless = headless
        self.driver = None
        self.jobs = []

    def start_driver(self):
        print("Starting WebDriver...")
        options = Options()
        if self.headless:
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        # self.driver.set_page_load_timeout(20)

    def extract_jobs(self):
        roles = [
            "Software-Developer",
            # "Web-Developer",            
            # "Data-Scientist",
            # "Data-Analyst",
            # "AI-Engineer",
            # "Machine-Learning-Engineer",
            # "DevOps-Engineer",
            # "Cloud-Engineer",
            # "Cybersecurity"
        ]

        for role in roles:
            print(f"Role {role}")
            for page in range(1, self.max_pages + 1):
                try:
                    url = f"https://sg.jobsdb.com/{role}-jobs?page={page}"
                    print(f"URL {url}")
                    
                    logger.info(f"Scraping role: {role}, page: {page}, URL: {url}")
                    self.driver.get(url)
                    time.sleep(5)

                    job_cards = self.driver.find_elements(By.CSS_SELECTOR, "div.job-card")
                    if not job_cards:
                        logger.info(f"No jobs found for {role} on page {page}")
                        break

                    for card in job_cards:
                        elements = card.find_elements(By.CSS_SELECTOR, "h2.job-title")
                        title = elements[0].text.strip() if elements else ""
                        
                        elements = card.find_elements(By.CSS_SELECTOR, "span.job-company")
                        company = elements[0].text.strip() if elements else ""
                        
                        elements = card.find_elements(By.CSS_SELECTOR, "a.job-location")
                        location = elements[0].text.strip() if elements else ""
                        
                        elements = card.find_elements(By.CSS_SELECTOR, "a.job-link")
                        link = elements[0].get_attribute("href").strip() if elements else ""
                        
                        elements = card.find_elements(By.CSS_SELECTOR, "span.job-listed-date")
                        date_posted = elements[0].text.strip() if elements else ""

                        # Initialize fields
                        salary = ''
                        job_type = ''
                        work_arrangements = []

                        # Extract all badge elements
                        badges = card.find_elements(By.CSS_SELECTOR, 'div.badges div.badge')

                        for badge in badges:
                            content = badge.find_element(By.CSS_SELECTOR, 'div.content').text.strip()
                            class_name = badge.get_attribute('class')

                            if '-default-badge' in class_name:
                                # Heuristics for identifying salary vs job type
                                if any(x in content.lower() for x in ['$', '฿', 'per hour', 'per month', 'per year']):
                                    salary = content
                                else:
                                    job_type = content
                            elif '-work-arrangement-badge' in class_name:
                                work_arrangements.append(content)

                        self.jobs.append({
                            "Role": role.replace("-", " "),
                            "Title": title,
                            "Category": role,
                            "Company": company,
                            "Location": location,
                            "Salary": salary,
                            "Job_Type": job_type,
                            "Work_Arrangement": ', '.join(work_arrangements),
                            "Job_Link": link,
                            "Date_Posted": date_posted
                        })

                except NoSuchElementException as e:
                    logger.warning(f"Missing element in card for {role} on page {page}: {e}")
                    continue

    def run(self):
        self.start_driver()
        try:
            self.extract_jobs()
        finally:
            self.driver.quit()
            logger.info("WebDriver closed.")

        logger.info(f"Scraping completed. Total jobs collected: {len(self.jobs)}")
        return pd.DataFrame(self.jobs)
