import argparse
from sqlalchemy import create_engine
from extract.jobnetmm import JobNetScraper
from extract.jobdbsg import JobsDBScraper
from extract.jobsdbth import JobsDBThScraper
from extract.jobstreetmalay import JobStreetMalaysia
from extract.founditSG import FounditScraper
from utils.data_normalizer import JobDataNormalizer

from transform.founditsg_t import FounditTransform
from transform.jobnetmm_t import JobNetTransform
from transform.jobsdbsg_t import JobsDBSGTransform
from transform.jobsdbth_t import JobsDBTHTransform
from transform.jobstreetmalay_t import JobStreetMalayTransform

import pandas as pd
import os

def extract_jobnetmm():
    email = os.getenv("JOBNET_EMAIL")
    password = os.getenv("JOBNET_PASSWORD")
    raw = JobNetScraper(email, password).get_jobs(job_function=17)
    df = JobDataNormalizer().jobnetmm(raw)
    return df

def extract_jobsdbsg():
    url_pattern = "https://sg.jobsdb.com/{role}-jobs?page={page}"
    raw = JobsDBScraper(max_pages_override=50, headless=True).run(url_pattern=url_pattern)
    df = JobDataNormalizer().jobsdbsg(raw)
    return df

def extract_jobsdbth():
    params = {
                'siteKey': 'TH-Main',
                'classification': '6281',  # IT Jobs
                'pageSize': 100,
                'locale': 'en-TH'
            }
    raw = JobsDBThScraper(classification_id='6281', base_params=params).scrape_jobs()
    df = JobDataNormalizer().jobsdbth(raw)
    return df

def extract_founditsg():
    base_params = {
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
    raw = FounditScraper(base_params=base_params    ).extract_jobs()
    df = JobDataNormalizer().founditsg(raw)
    return df

def extract_jobstreetmalay():
    base_params = {
                'siteKey': 'MY-Main',
                'locale': 'en-MY',
            }
    raw = JobStreetMalaysia(classification_id="6281", base_params=base_params).fetch_jobs()
    df = JobDataNormalizer().jobstreetmalay(raw)
    return df

def main(source, log_dir="logs"):
    # Map the source to the corresponding extraction function
    extract_dispatch = {
        "jobnetmm": extract_jobnetmm,
        "jobsdbsg": extract_jobsdbsg,
        "jobsdbth": extract_jobsdbth,
        "founditsg": extract_founditsg,
        "jobstreetmalay": extract_jobstreetmalay,
    }

    # Map the source to the corresponding transformation function
    transform_dispatch = {
        "jobnetmm": JobNetTransform,
        "jobsdbsg": JobsDBSGTransform,
        "jobsdbth": JobsDBTHTransform,
        "founditsg": FounditTransform,
        "jobstreetmalay": JobStreetMalayTransform,
    }
    if source not in extract_dispatch:
        raise ValueError(f"Unknown source: {source}")
    
    extracted_df = extract_dispatch[source]()
    print(f"Data extraction for {source} completed.")
    print(extracted_df.head())

    ## Transform the data
    if source in transform_dispatch:
        transformer = transform_dispatch[source](extracted_df)
        transformed_df = transformer.transform()

        print(f"Data transformation for {source} completed.")
        print(transformed_df.head())

    ## Load the data
    database_url = os.getenv("DATABASE_URL")
    def upload_to_database(df: pd.DataFrame, table_name: str, database_url: str = database_url):
        engine = create_engine(database_url)
        try:
            with engine.connect() as connection:
                df.to_sql(table_name, con=connection, if_exists='replace', index=False)
                print(f"Data loaded into {table_name} table.")
        except Exception as e:
            print(f"Error loading data into {table_name}: {e}")


    # upload_to_database(extracted_df, f"{source}_raw")
    # upload_to_database(transformed_df, f"{source}_transformed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--log_dir", default="logs")
    args = parser.parse_args()
    main(args.source, log_dir=args.log_dir)