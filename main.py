import argparse
from sqlalchemy import create_engine
from utils.jobnetmm import JobNetScraper
from utils.jobdbsg import JobsDBScraper
from utils.jobsdbth import JobsDBThScraper
from utils.jobstreetmalay import JobStreetMalaysia
from utils.founditSG import FounditScraper
from utils.data_normalizer import JobDataNormalizer
from utils.transform import JobNetTransform, JobsDBSGTransform, JobsDBTHTransform, FounditTransform, JobStreetMalayTransform
import os
import pandas as pd

def extract_jobnetmm():
    email = os.getenv("JOBNET_EMAIL")
    password = os.getenv("JOBNET_PASSWORD")
    raw = JobNetScraper(email, password).get_jobs(job_function=17)
    df = JobDataNormalizer().jobnetmm(raw)
    return df

def extract_jobsdbsg():
    raw = JobsDBScraper(max_pages=50, headless=True).run()
    df = JobDataNormalizer().jobsdbsg(raw)
    df.to_csv("output/jobdbsg.csv", index=False)
    return df


def extract_jobsdbth():
    raw = JobsDBThScraper(classification_id='6281').scrape_jobs()
    df = JobDataNormalizer().jobsdbth(raw)
    return df

def extract_founditsg():
    raw = FounditScraper(headless=True).extract_jobs()
    df = JobDataNormalizer().founditsg(raw)
    return df


def extract_jobstreetmalay():
    raw = JobStreetMalaysia().fetch_jobs(classification_id='6281')
    df = JobDataNormalizer().jobstreetmalay(raw)
    df.to_csv("output/jobstreetmalay.csv", index=False)
    return df

def main(source):
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

    # Transform the data
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

    upload_to_database(extracted_df, f"{source}_raw")
    upload_to_database(transformed_df, f"{source}_transformed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    args = parser.parse_args()
    main(args.source)