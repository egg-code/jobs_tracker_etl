from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
import logging
import random

## Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('logs/jobsdbsg_e_logs.log', mode='a')
fomatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(fomatter)
logger.addHandler(handler)

class JobsDBScraper:
    def __init__(self, max_pages=2, headless=True):
        self.max_pages = max_pages
        self.headless = headless
        self.driver = None
        self.jobs = []

    def start_driver(self):
        options = Options()
        if self.headless:
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)


    def extract_jobs(self):
        roles = [
            "Software-Developer",
            "Web-Developer",
            "Data-Scientist",
            "Data-Analyst",
            "AI-Engineer",
            "Machine-Learning-Engineer",
            "DevOps-Engineer",
            "Cloud-Engineer",
            "Cybersecurity"
        ]
    
        for role in roles:
            for page in range(1, self.max_pages + 1):
                try:
                    url = f"https://sg.jobsdb.com/{role}-jobs?page={page}"
                    logging.info(f"Scraping role: {role}, page {page}: {url}")
                    self.driver.get(url)
                    time.sleep(random.uniform(1, 4))  # Wait for JS to load
                    job_cards = self.driver.find_elements(By.CSS_SELECTOR, "div.job-card")
    
                    if not job_cards:
                        logging.info(f"No jobs found for {role} on page {page}.")
                        break
                    
                    for card in job_cards:
                        try:
                            title = card.find_element(By.CSS_SELECTOR, 'h2.job-title').text.strip()
                            company = card.find_element(By.CSS_SELECTOR, 'span.job-company').text.strip()
                            location = card.find_element(By.CSS_SELECTOR, 'a.job-location').text.strip()
                            link = card.find_element(By.CSS_SELECTOR, 'a.job-link').get_attribute('href').strip()
                            date_posted = card.find_element(By.CSS_SELECTOR, 'span.job-listed-date').text.strip()

                            # For job type and salary
                            badge_elements = card.find_elements(By.CSS_SELECTOR, 'div.badges div.content')
                            badges_text = [badge.text.strip() for badge in badge_elements if badge.text.strip()]
                            salary = ""
                            job_type = ""
                            work_arrangement = ""

                            # Check conditions for salary and job type
                            if len(badges_text) == 1:
                                job_type = badges_text[0]
                            elif len(badges_text) == 2:
                                salary = badges_text[0]
                                job_type = badges_text[1]
                            elif len(badges_text) >= 3:
                                salary = badges_text[0]
                                job_type = badges_text[1]
                                work_arrangement = badges_text[2]
    
                            self.jobs.append({
                                "Role": role.replace("-", " "),
                                "Title": title,
                                "Company": company,
                                "Location": location,
                                "Salary": salary,
                                "Job_Type": job_type,
                                "Work_Arrangement": work_arrangement,
                                "Country_Code": "SG",
                                "Job_Link": link,
                                "Date_Posted": date_posted
                            })
                            print(f"Scraped job for role '{role}' from page {page}: {len(self.jobs)} jobs collected.")

                        except NoSuchElementException as e:
                            logging.warning(f"Missing element in card for {role} on page {page}: {e}")
                            continue
                        
                except Exception as e:
                    logging.error(f"Error processing role {role} on page {page}: {e}")
                    continue

    def run(self):
        self.start_driver()
        try:
            self.extract_jobs()
        finally:
            self.driver.quit()
            logging.info("Driver closed.")

        logging.info(f"Scraping completed. Total jobs: {len(self.jobs)}")
        return  pd.DataFrame(self.jobs)
