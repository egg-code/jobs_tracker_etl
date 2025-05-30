import requests
import pandas as pd
import time
import random
from datetime import datetime

# Ensure the logs directory exists

# Setup logging
from utils.logger import get_module_logger
logger = get_module_logger(__name__, group='extract')

class JobStreetMalaysia:
    def __init__(self, classification_id: str, base_params, page_size: int = 100):
        self.base_url = "https://my.jobstreet.com/api/jobsearch/v5/search"
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://my.jobstreet.com/",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://my.jobstreet.com"
        }
        self.classification_id = '6281'  # IT Jobs
        self.page_size = page_size
        self.base_params = base_params
        self.base_params['classification'] = self.classification_id
        self.base_params['pageSize'] = self.page_size

    def fetch_jobs(self):
        all_jobs = []
        page = 1

        while True:
            params = self.base_params.copy()
            params['page'] = page
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
