import requests
import pandas as pd
import time
import logging
import random
from datetime import datetime

# Ensure the logs directory exists

# Setup logging
logfile_name = datetime.now().strftime('jobstreetmalaysia_e_%Y%m%d_%H%M%S.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logfile_name)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class JobStreetMalaysia:
    def __init__(self):
        self.base_url = "https://my.jobstreet.com/api/jobsearch/v5/search"
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://my.jobstreet.com/",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://my.jobstreet.com"
        }

    def fetch_jobs(self, classification_id: str, page_size: int = 100):
        all_jobs = []
        page = 1

        while True:
            params = {
                'siteKey': 'MY-Main',
                'page': page,
                'classification': classification_id,
                'pageSize': page_size,
                'locale': 'en-MY'
            }

            try:
                response = requests.get(self.base_url, headers=self.headers, params=params)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                break

            try:
                data = response.json()
            except ValueError as e:
                logger.error(f"Failed to parse JSON on page {page}: {e}")
                break

            jobs = data.get("data", [])

            if not jobs:
                logger.info("No more jobs found.")
                break

            all_jobs.extend(jobs)
            logger.info(f"Fetched page {page} with {len(jobs)} jobs.")
            page += 1
            time.sleep(random.uniform(1, 3))

        def extract_work_arrangement(job):
            try:
                return job.get('workArrangements', {}).get('data', [{}])[0].get('label', {}).get('text', '')
            except Exception:
                return ''


        df = pd.DataFrame([
            {
                    'job_title': job.get('title'),
                    'company': job.get('companyName', ''),
                    'location': job.get('locations', [{}])[0].get('label', ''),
                    'country_code': job.get('locations', [{}])[0].get('countryCode', ''),
                    'salary': job.get('salaryLabel', ''),
                    'job_type': ', '.join(map(str, job.get('workTypes', []))),
                    'work_arrangement': extract_work_arrangement(job),
                    'date_posted': job.get('listingDate'),
                    'job_link': f"https://my.jobstreet.com/job/{job.get('id')}"
                }
            for job in all_jobs
        ])
        logger.info(f"Scraping completed. Total jobs scraped: {len(all_jobs)}")

        return df
