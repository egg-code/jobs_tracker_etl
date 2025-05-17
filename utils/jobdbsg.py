from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
import logging
import random

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('logs/jobsdbsg_e_logs.log', mode='a')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


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

                        salary = ""
                        job_type = ""
                        
                        badges = card.find_elements(By.CSS_SELECTOR, "div.badges div.content")
                        
                        if badges:
                            first = badges[0].text.strip().lower()
                            if "$" in first or "per month" in first:
                                salary = badges[0].text.strip()
                                if len(badges) > 1:
                                    job_type = badges[1].text.strip()
                            else:
                                job_type = badges[0].text.strip()
                                if len(badges) > 1:
                                    salary = badges[1].text.strip()
                        else:
                            logger.info("No badges found for this job card.")


                        self.jobs.append({
                            "Role": role.replace("-", " "),
                            "Title": title,
                            "Company": company,
                            "Location": location,
                            "Job_Type": job_type,
                            "Salary": salary,
                            "Job_Link": link,
                            "Date_Posted": date_posted
                        })

                except Exception as e:
                    logger.error(f"Error processing role {role} on page {page}: {e}")
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
