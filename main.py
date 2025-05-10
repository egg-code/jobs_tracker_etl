from jobnetmm import JobNetScraper
from jobdbsg import JobsDBScraper
from foundit import FounditScraper
import os

def main():
    ## For jobnetmm
    # email = os.getenv("JOBNET_EMAIL")
    # password = os.getenv("JOBNET_PASSWORD")
    # job_function = 17  # 17 for IT jobs
    # extract = JobNetScraper(email, password)
    # jobnet_df = extract.get_jobs(job_function=job_function)
    # print(jobnet_df.head())

    # ## For jobsdb_sg
    # print("🔍 Scraping Software Developer job from JobsDB Singapore...")
    # jobsdb_scraper = JobsDBScraper(max_pages=10, headless=True)
    # jobdbsg_df = jobsdb_scraper.run()
    # print(jobdbsg_df.head())
    # print("✅ JobsDB scraping complete.")


    # # For foundit
    print("🔍 Scraping IT jobs from Foundit Singapore...")
    foundit_scraper = FounditScraper(headless=True)
    foundit_df = foundit_scraper.run()
    print(foundit_df.head())
    print("✅ Foundit scraping complete.")

if __name__ == "__main__":
    main()
