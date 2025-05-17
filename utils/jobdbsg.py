from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
import logging

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('logs/jobsdbsg_e_logs.log', mode='a')
fomatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(fomatter)
logger.addHandler(handler)

class JobsDBScraper:
    def __init__(self, max_pages=5, headless=True):
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
        base_url = "https://sg.jobsdb.com/Software-Developer-jobs?page="

        for page in range(1, self.max_pages + 1):
            try:
                url = base_url + str(page)
                logger.info(f"Scraping page {page}: {url}")
                self.driver.get(url)
                time.sleep(5)  # Wait for JS to load
                job_cards = self.driver.find_elements(By.CSS_SELECTOR, "div.job-card")

                if not job_cards:
                    logger.info(f"No jobs found on page {page}. Ending scrape.")
                    break

                for card in job_cards:
                    try:
                        title = card.find_element(By.CSS_SELECTOR, 'h2.job-title').text.strip()
                        company = card.find_element(By.CSS_SELECTOR, 'span.job-company').text.strip()
                        location = card.find_element(By.CSS_SELECTOR, 'a.job-location').text.strip()
                        job_type = card.find_element(By.CSS_SELECTOR, 'div.badges div.content').text.strip()
                        link = card.find_element(By.CSS_SELECTOR, 'a.job-link').get_attribute('href').strip()
                        date_posted = card.find_element(By.CSS_SELECTOR, 'span.job-listed-date').text.strip()

                        self.jobs.append({
                            "Title": title,
                            "Company": company,
                            "Location": location,
                            "Job_Type": job_type,
                            "Job_Link": link,
                            "Date_Posted": date_posted
                        })

                    except NoSuchElementException as e:
                        logger.warning(f"Missing element in card on page {page}: {e}")
                        continue

            except Exception as e:
                logger.error(f"Error processing page {page}: {e}")
                continue

    def run(self):
        self.start_driver()
        try:
            self.extract_jobs()
        finally:
            self.driver.quit()
            logger.info("Driver closed.")

        # df = pd.DataFrame(self.jobs)
        # df.to_csv("jobsdb_software_developer_jobs.csv", index=False)
        logger.info(f"Scraping completed. Total jobs: {len(self.jobs)}")
        return  pd.DataFrame(self.jobs)

if __name__ == "__main__":
    scraper = JobsDBScraper(max_pages=10, headless=True)
    df = scraper.run()
    df.to_csv("jobsdb_software_developer_jobs.csv", index=False)
    print("✅ Scraping done. Check 'jobsdb_software_developer_jobs.csv' and logs for details.")
