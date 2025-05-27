import requests
import pandas as pd
import time
import random
from datetime import datetime

import logging

## Set up logging
from utils.logger import get_module_logger
logger = get_module_logger(__name__, group='extract')

class JobsDBThScraper:
    def __init__(self, classification_id, page_size=100):
        self.url = "https://th.jobsdb.com/api/jobsearch/v5/search"
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://th.jobsdb.com/th",
            "Accept-Language": "en-US,en;q=0.9"
        }
        self.classification_id = classification_id
        self.page_size = page_size


    def scrape_jobs(self):
        all_jobs = []
        page = 1

        while True:
            params = {
                'siteKey': 'TH-Main',
                'page': page,
                'classification': self.classification_id,
                'pageSize': self.page_size,
                'locale': 'en-TH'
            }
            try:
                response = requests.get(self.url, headers=self.headers, params=params)
                data = response.json()
                jobs = data.get('data', [])
                total_jobs = data.get('totalCount', 0)
                logger.info(f"Total jobs found: {total_jobs}")
                
                if not jobs:
                    logger.info(f"No more jobs found on page {page}. Ending scrape.")
                    break

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed on page {page}: {e}")
                break

            for job in jobs:
                data_list = job.get('workArrangements', {}).get('data', [])
                if data_list and isinstance(data_list, list) and len(data_list) > 0:
                    work_arrangement = data_list[0].get('label', {}).get('text', '')
                else:
                    work_arrangement = ''
                job_data = {
                    'job_title': job.get('title'),
                    'company': job.get('companyName', ''),
                    'location': job.get('locations', [{}])[0].get('label', ''),
                    'country_code': job.get('locations', [{}])[0].get('countryCode', ''),
                    'salary': job.get('salaryLabel', ''),
                    'job_type': ', '.join(map(str, job.get('workTypes', []))),
                    'work_arrangement': work_arrangement,
                    'date_posted': job.get('listingDate'),
                    'job_link': f"https://th.jobsdb.com/job/{job.get('id')}"
                }
                all_jobs.append(job_data)
            logger.info(f"Scraped job from page {page}: {len(all_jobs)} jobs collected.")
            page += 1
            time.sleep(random.uniform(1, 4)) # Randon sleep time between requests

        logger.info(f"Scraping completed. Total jobs scraped: {len(all_jobs)}")
        return pd.DataFrame(all_jobs)