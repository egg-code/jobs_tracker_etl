from utils.jobnetmm import JobNetScraper
from utils.jobdbsg import JobsDBScraper
from utils.jobsdbth import JobsDBThScraper
from utils.jobstreetmalay import JobStreetMalaysia
from utils.founditSG import FounditScraper
import os

def main():
    # For jobnetmm
    email = os.getenv("JOBNET_EMAIL")
    password = os.getenv("JOBNET_PASSWORD")
    job_function = 17 # 17 for IT jobs
    extract = JobNetScraper(email, password)
    jobnet_df = extract.get_jobs(job_function=job_function)
    print(jobnet_df.head())
    print(f"Dataframe shape: {jobnet_df.shape}")
    jobnet_df.to_csv("jobnetmm.csv", index=False)
    
    ## For jobsdb_sg
    print("🔍 Scraping Software Developer job from JobsDB Singapore...")
    jobsdb_scraper = JobsDBScraper(headless=True)
    print("🔍 Scraping job from JobsDB Singapore...")
    jobsdb_scraper = JobsDBScraper(max_pages=1, headless=True)
    jobdbsg_df = jobsdb_scraper.run()
    print(jobdbsg_df.head())
    print(f"Dataframe shape: {jobdbsg_df.shape}")
    print(f"Duplicates found: {jobdbsg_df.duplicated().sum()}")
    print(f"Columns: {jobdbsg_df.columns.tolist()}")
    jobdbsg_df.to_csv('jobsdb_sg.csv', index=False)
    print("✅ JobsDB scraping complete.")

    # For jobsdb_th
    classification_id = '6281' # id for IT jobs
    scraper = JobsDBThScraper(classification_id)
    jobsdbth_df = scraper.scrape_jobs()
    print(jobsdbth_df.head())
    print(f"Duplicates found: {jobsdbth_df.duplicated().sum()}")
    print(f"Dataframe shape: {jobsdbth_df.shape}")
    print(f"Columns: {jobsdbth_df.columns.tolist()}")
    jobsdbth_df.to_csv('jobsdb_th.csv', index=False)

    ## For jobstreetmalay
    classification_id = '6281' # id for IT jobs
    scraper = JobStreetMalaysia()
    jobstreetmalay_df = scraper.fetch_jobs(classification_id)
    print(jobstreetmalay_df.head())
    print(f"Duplicates found: {jobstreetmalay_df.duplicated().sum()}")
    print(f"Dataframe shape: {jobstreetmalay_df.shape}")
    print(f"Columns: {jobstreetmalay_df.columns.tolist()}")

if __name__ == "__main__":
    main()