import requests
from urllib.parse import urlencode #To safely encode query parameters in the URL.
import time  
import pandas as pd

#  Configure logger (custom filename: founditsg_YYYYMMDD_HHMMSS.log)
from utils.logger import get_module_logger
logger = get_module_logger(__name__, group='extract')

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

            "companyId", "companyName", "salary", "seoCompanyUrl", "seoJdUrl"

        ]

        while True:
            logger.info(f" Fetching jobs at start={start}...")

            url = self.build_url(start)
            
            # Parse JSON safely.
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                logger.error(f" Failed to fetch jobs. Status code: {response.status_code}")

                break

            try:
                data = response.json()
                # Extract jobs
                jobs = data.get("jobSearchResponse", {}).get("data", [])

                if not jobs:
                    logger.info(" No job data returned. Ending.")
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
                    logger.info(f" No new unique jobs found on this page. Skipped pages so far: {pages_without_new_jobs}")
                    if pages_without_new_jobs >= max_pages_without_new_jobs:
                        logger.info(" Too many skipped pages. Assuming end of data. Ending.")
                        break
                else:
                    pages_without_new_jobs = 0  # reset if new jobs found

                all_jobs.extend(new_jobs)
                logger.info(f" Total unique jobs collected so far: {len(all_jobs)}")
                time.sleep(1) #Sleep between requests
                start += 15

                if start >= 600: #Hard limit to break early for safety
                    logger.info(" Reached start=600. Stopping to avoid scraping too much.")
                    break

            except Exception as e:
                logger.info(f" Error parsing response: {e}")
                break



        if all_jobs:

            unique_job_ids = {str(job.get("jobId") or job.get("id")) for job in all_jobs}
            logger.info(f" Total jobs scraped: {len(all_jobs)}")
            logger.info(f" Total unique jobs scraped: {len(unique_job_ids)}")
            
<<<<<<< HEAD:utils/founditSG.py
=======
            # self.save_to_json(all_jobs)

>>>>>>> main:extract/founditSG.py
            foundit_df = pd.DataFrame(all_jobs)
            return foundit_df

        else:
<<<<<<< HEAD:utils/founditSG.py
            logging.info(" No jobs were scraped.")
            return pd.DataFrame()  #  Return empty DataFrame
=======
            logger.info(" No jobs were scraped.")
            return pd.DataFrame()  #  Return empty DataFrame




# # 4. Saving to File – save_to_json()
#     def save_to_json(self, jobs):

#         now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#         filename = f"foundit_jobs_{now}.json" #Creates a file with timestamp to avoid overwriting previous results.
#         output_dir = "output"
#         os.makedirs(output_dir, exist_ok=True) #to create "output" folder if not present.
#         output_path = os.path.join(output_dir, filename)

#         with open(output_path, "w", encoding="utf-8") as f:
#             json.dump(jobs, f, indent=4, ensure_ascii=False)
#             #Uses ensure_ascii=False to preserve Unicode (like company names in other languages).
#             #Uses indent=4 for human-readable formatting.

#         logger.info(f" Saved {len(jobs)} jobs: {output_path}")
>>>>>>> main:extract/founditSG.py
