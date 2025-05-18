from utils.jobnetmm import JobNetScraper
from utils.jobdbsg import JobsDBScraper
from utils.jobsdbth import JobsDBThScraper
from utils.jobstreetmalay import JobStreetMalaysia
from utils.founditSG import FounditScraper
from utils.data_normalizer import JobDataNormalizer
import os


def log_site_result(site_name, df):
    print(f"{site_name}:")
    print(f"   • Jobs scraped:   {len(df)}")
    print(f"   • Duplicates:     {df.duplicated().sum()}")
    print(f"   • Columns:        {df.shape[1]}")
    print(f"   • Saved to CSV.\n")

def main():
    print("Starting multi-site job scraping...\n")
    
    results = {}

    # === JobNetMM ===
    print("Scraping: JobNet Myanmar (IT jobs)")
    email = os.getenv("JOBNET_EMAIL")
    password = os.getenv("JOBNET_PASSWORD")
    job_function = 17
    jobnetmm_raw = JobNetScraper(email, password).get_jobs(job_function=job_function)
    results["JobNetMM"] = jobnetmm_raw
    log_site_result("JobNetMM", jobnetmm_raw)
    print(jobnetmm_raw.head())
    print(jobnetmm_raw.columns)

    # === JobsDB Singapore ===
    print("Scraping: JobsDB Singapore (Software Developer)")
    jobsdbsg_raw = JobsDBScraper(max_pages=1, headless=True).run()
    results["JobsDB SG"] = jobsdbsg_raw
    log_site_result("JobsDB SG", jobsdbsg_raw)
    print(jobsdbsg_raw.head())
    print(jobsdbsg_raw.columns)

    # === JobsDB Thailand ===
    print("Scraping: JobsDB Thailand (IT jobs)")
    classification_id = '6281'
    jobsdbth_raw = JobsDBThScraper(classification_id).scrape_jobs()
    results["JobsDB TH"] = jobsdbth_raw
    log_site_result("JobsDB TH", jobsdbth_raw)
    print(jobsdbth_raw.head())
    print(jobsdbth_raw.columns)

    # === Foundit Singapore ===
    print("Scraping: Foundit Singapore (IT jobs)")
    founditsg_raw = FounditScraper(headless=True).extract_jobs()
    print(f"Foundit SG:")
    print(f"   • Jobs scraped:   {len(founditsg_raw)}")
    print(f"   • Columns:        {founditsg_raw.shape[1]}")
    print(f"   • Saved to CSV.\n")
    print(founditsg_raw.head())
    print(founditsg_raw.columns)

    # === JobStreet Malaysia ===
    print("Scraping: JobStreet Malaysia (IT jobs)")
    jobstreetmalay_raw = JobStreetMalaysia().fetch_jobs(classification_id='6281')
    results["JobStreet MY"] = jobstreetmalay_raw
    log_site_result("JobStreet MY", jobstreetmalay_raw)
    print(jobstreetmalay_raw.head())
    print(jobstreetmalay_raw.columns)

    ## Normalize Data from different sources
    print("Normalizing data...")
    normalizer = JobDataNormalizer()
    jobnetmm_df = normalizer.jobnetmm(jobnetmm_raw)
    jobsdbsg_df = normalizer.jobsdbsg(jobsdbsg_raw)
    jobsdbth_df = normalizer.jobsdbth(jobsdbth_raw)
    founditsg_df = normalizer.founditsg(founditsg_raw)
    jobstreetmalay_df = normalizer.jobstreetmalay(jobstreetmalay_raw)

    print(f"Jobnetmm : \n{jobnetmm_df.head()}")
    print(f"JobsDB SG: \n{jobsdbsg_df.head()}")
    print(f"JobsDB TH: \n{jobsdbth_df.head()}")
    print(f"Foundit SG: \n{founditsg_df.head()}")
    print(f"JobStreet MY: \n{jobstreetmalay_df.head()}")

    jobstreetmalay_df.to_csv("jobstreetmalay.csv", index=False, encoding='utf-8-sig')
    founditsg_df.to_csv("foundit.csv", index=False, encoding='utf-8-sig')
    jobsdbth_df.to_csv("jobsdbth.csv", index=False, encoding='utf-8-sig')
    jobsdbsg_df.to_csv("jobsdbsg.csv", index=False, encoding='utf-8-sig')
    jobnetmm_df.to_csv("jobnetmm.csv", index=False, encoding='utf-8-sig')
    print("Data normalization completed. Files saved.")

if __name__ == "__main__":
    main()
