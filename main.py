import argparse
from utils.jobnetmm import JobNetScraper
from utils.jobdbsg import JobsDBScraper
from utils.jobsdbth import JobsDBThScraper
from utils.jobstreetmalay import JobStreetMalaysia
from utils.founditSG import FounditScraper
from utils.data_normalizer import JobDataNormalizer
import os

def extract_jobnetmm():
    email = os.getenv("JOBNET_EMAIL")
    password = os.getenv("JOBNET_PASSWORD")
    raw = JobNetScraper(email, password).get_jobs(job_function=17)
    df = JobDataNormalizer().jobnetmm(raw)
    df.to_csv("output/jobnetmm.csv", index=False, encoding='utf-8-sig')
    return df

def extract_jobsdbsg():
    raw = JobsDBScraper(max_pages=1, headless=True).run()
    df = JobDataNormalizer().jobsdbsg(raw)
    df.to_csv("output/jobsdbsg.csv", index=False, encoding='utf-8-sig')
    return df

def extract_jobsdbth():
    raw = JobsDBThScraper(classification_id='6281').scrape_jobs()
    df = JobDataNormalizer().jobsdbth(raw)
    df.to_csv("output/jobsdbth.csv", index=False, encoding='utf-8-sig')
    return df

def extract_founditsg():
    raw = FounditScraper(headless=True).extract_jobs()
    df = JobDataNormalizer().founditsg(raw)
    df.to_csv("output/foundit.csv", index=False, encoding='utf-8-sig')

    return df


def extract_jobstreetmalay():
    raw = JobStreetMalaysia().fetch_jobs(classification_id='6281')
    df = JobDataNormalizer().jobstreetmalay(raw)
    df.to_csv("output/jobstreetmalay.csv", index=False, encoding='utf-8-sig')
    return df

def main(source):
    dispatch = {
        "jobnetmm": extract_jobnetmm,
        "jobsdbsg": extract_jobsdbsg,
        "jobsdbth": extract_jobsdbth,
        "founditsg": extract_founditsg,
        "jobstreetmalay": extract_jobstreetmalay,
    }
    if source not in dispatch:
        raise ValueError(f"Unknown source: {source}")
    
    df = dispatch[source]()
    print(f"Data extraction for {source} completed.")
    print(df.head())
    print(f"Data extraction for {source} completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    args = parser.parse_args()
    main(args.source)