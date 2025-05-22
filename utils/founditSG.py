import requests
import datetime
import os
from urllib.parse import urlencode #To safely encode query parameters in the URL.
import json
import time  
import logging
import pandas as pd


#  Ensure logs folder exists
os.makedirs("logs", exist_ok=True)

#  Configure logging (custom filename: founditsg_YYYYMMDD_HHMMSS.log)
log_filename = datetime.datetime.now().strftime("logs/founditsg_%Y%m%d_%H%M%S.log")
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

#  Also print logs to console
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

#1. Class Initialization
class FounditScraper:
    def __init__(self, headless=True):
        self.headless = headless #headless: Reserved for future browser-based automation. Not used here, but shows potential for using Selenium or Puppeteer later.
        
        # to seem more like a real user.
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://www.foundit.sg/",
        }

        #The main API endpoint    
        self.base_endpoint = "https://www.foundit.sg/middleware/jobsearch"
        self.query_params = {
            "sort": 1,
            "limit": 15,
            "query": '""',
            "quickApplyJobs": "true",
            "industries": [
                "software",
                "information technology",
                "software engineering",
                "it management",
                "it infrastructure",
                "cyber security",
                "cloud computing",
                "enterprise software",
                "data center",
                "cloud data services"                

            ],

      }


      

#2. Building the API URL
    def build_url(self, start):
        params = self.query_params.copy()
        params["start"] = start

        industries = params.pop("industries", [])
        encoded = urlencode(params)
        industry_params = "&".join([f"industries={industry.replace(' ', '%20')}" for industry in industries])

        return f"{self.base_endpoint}?{encoded}&{industry_params}"

#3. Main Scraper Logic – run()
    def extract_jobs(self):
        start = 0
        all_jobs = []
        seen_job_ids = set() # to avoid duplicates
        max_pages_without_new_jobs = 3  # tolerate 3 consecutive pages without new jobs to  prevent infinite loops
        pages_without_new_jobs = 0

        
        desired_fields = [

            "jobId", "title", "locations", "exp", "updatedAt", "postedBy",

            "industries", "roles", "jobTypes", "qualifications",

            "companyId", "companyName", "salary", "seoCompanyUrl", "seoJdUrl", "roles", "functions"

        ]

        while True:
            logging.info(f" Fetching jobs at start={start}...")

            url = self.build_url(start)
            
            # Parse JSON safely.
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                logging.error(f" Failed to fetch jobs. Status code: {response.status_code}")

                break

            try:
                data = response.json()
                # Extract jobs
                jobs = data.get("jobSearchResponse", {}).get("data", [])

                if not jobs:
                    logging.info(" No job data returned. Ending.")
                    break
                

                # Filter duplicates
                new_jobs = []
                for job in jobs:
                    job_id = str(job.get("jobId") or job.get("id"))
                    if job_id not in seen_job_ids:
                        seen_job_ids.add(job_id)
                        
                        filtered_job = {field: job.get(field) for field in desired_fields}
                        
                        if any(filtered_job.values()):  # Only add if there's at least some data
                            new_jobs.append(filtered_job)


                #Loop exit conditions
                if not new_jobs:
                    pages_without_new_jobs += 1
                    logging.info(f" No new unique jobs found on this page. Skipped pages so far: {pages_without_new_jobs}")
                    if pages_without_new_jobs >= max_pages_without_new_jobs:
                        logging.info(" Too many skipped pages. Assuming end of data. Ending.")
                        break
                else:
                    pages_without_new_jobs = 0  # reset if new jobs found

                all_jobs.extend(new_jobs)
                logging.info(f" Total unique jobs collected so far: {len(all_jobs)}")
                time.sleep(1) #Sleep between requests
                start += 15

# start >= 600:
                if start >= 300: #Hard limit to break early for safety
                    logging.info(" Reached start=600. Stopping to avoid scraping too much.")
                    break

            except Exception as e:
                logging.info(f" Error parsing response: {e}")
                break



        if all_jobs:

            unique_job_ids = {str(job.get("jobId") or job.get("id")) for job in all_jobs}
            logging.info(f" Total jobs scraped: {len(all_jobs)}")
            logging.info(f" Total unique jobs scraped: {len(unique_job_ids)}")
            
            foundit_df = pd.DataFrame(all_jobs)
            return foundit_df

        else:
            logging.info(" No jobs were scraped.")
            return pd.DataFrame()  #  Return empty DataFrame