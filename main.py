from utils.jobnetmm import JobNetScraper
from utils.jobdbsg import JobsDBScraper
from utils.jobsdbth import JobsDBThScraper
import os

def main():
    ## For jobnetmm
    email = 'kaungmyatkyaw.gg@gmail.com' #os.getenv("JOBNET_EMAIL")
    password = 'mk7jd3r89f' #os.getenv("JOBNET_PASSWORD")
    job_function = 17 # 17 for IT jobs
    extract = JobNetScraper(email, password)
    jobnet_df = extract.get_jobs(job_function=job_function)
    print(jobnet_df.head())
    print(f"Dataframe shape: {jobnet_df.shape}")
    jobnet_df.to_csv('jobnetmm.csv', index=False)

    ## For jobsdb_sg
    # print("🔍 Scraping Software Developer job from JobsDB Singapore...")
    # jobsdb_scraper = JobsDBScraper(max_pages=20, headless=True)
    # jobdbsg_df = jobsdb_scraper.run()
    # print(jobdbsg_df.head())
    # print(f"Dataframe shape: {jobdbsg_df.shape}")
    # print("✅ JobsDB scraping complete.")
    # jobdbsg_df.to_csv('jobsdb_sg.csv', index=False)

    ## For jobsdb_th
    # classification_id = '6281' # id for IT jobs
    # scraper = JobsDBThScraper(classification_id)
    # jobsdbth_df = scraper.scrape_jobs()
    # print(jobsdbth_df.head())
    # print(f"Duplicates found: {jobsdbth_df.duplicated().sum()}")
    # print(f"Dataframe shape: {jobsdbth_df.shape}")
    # print(f"Columns: {jobsdbth_df.columns.tolist()}")
    # jobsdbth_df.to_csv('jobsdb_th.csv', index=False)

if __name__ == "__main__":
    main()