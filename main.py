import argparse
from sqlalchemy import create_engine
from extract.jobnetmm import JobNetScraper
from extract.jobdbsg import JobsDBScraper
from extract.jobsdbth import JobsDBThScraper
from extract.jobstreetmalay import JobStreetMalaysia
from extract.founditSG import FounditScraper
from data_normalizer import JobDataNormalizer

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
    raw = JobsDBScraper(max_pages=3, headless=True).run()
    df = JobDataNormalizer().jobsdbsg(raw)
    return df

def extract_jobsdbth():
    raw = JobsDBThScraper(classification_id='6281').scrape_jobs()
    df = JobDataNormalizer().jobsdbth(raw)
    return df

def extract_founditsg():
    raw = FounditScraper(headless=True).extract_jobs()
    df = JobDataNormalizer().founditsg(raw)

    required_fields = ['title', 'category', 'company', 'location', 'date_posted', 'job_link']
    df = df.dropna(subset=required_fields)
    
    return df

def extract_jobstreetmalay():
    raw = JobStreetMalaysia().fetch_jobs(classification_id='6281')
    df = JobDataNormalizer().jobstreetmalay(raw)
    return df

def main(source):

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
    upload_to_database(extracted_df, f"{source}_raw")


    ## Transform the data
    if source in transform_dispatch:
        transformer = transform_dispatch[source](extracted_df)
        transformed_df = transformer.transform()
        print(f"Data transformation for {source} completed.")
        transformed_df.to_csv("output/foundit.csv", index=False, encoding='utf-8-sig')
        print(transformed_df.head())


    upload_to_database(transformed_df, f"{source}_transformed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    args = parser.parse_args()
    main(args.source)