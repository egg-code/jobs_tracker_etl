from sqlalchemy import create_engine
from extract.jobnetmm import JobNetScraper
from transform.jobnetmm_t import JobNetTransform
from extract.jobsdbth import JobsDBThScraper
from transform.jobsdbth_t import JobsDBTHTransform
from extract.founditSG import FounditScraper
from transform.founditsg_t import FounditTransform
from extract.jobstreetmalay import JobStreetMalaysia
from transform.jobstreetmalay_t import JobStreetMalayTransform
from extract.jobdbsg import JobsDBScraper
from transform.jobsdbsg_t import JobsDBSGTransform

from utils.data_normalizer import JobDataNormalizer
from utils.pkey_gen import custom_job_id  # Import your job ID generator
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

## Function to extract jobs from JobNetMM and transform them
def daily_jobnetmm():
    """
    Extracts jobs from JobNetMM, normalizes the data, and transforms it.
    Returns a DataFrame of transformed job data.
    """
    email = os.getenv("JOBNET_EMAIL")
    password = os.getenv("JOBNET_PASSWORD")
    
    # Scrape jobs
    raw = JobNetScraper(email, password).get_jobs(job_function=17)
    
    # Normalize job data
    raw_df = JobDataNormalizer().jobnetmm(raw)
    
    # Transform job data
    transformer = JobNetTransform(raw_df, categories_path='categories.json')
    transformed_df = transformer.transform()
    
    return transformed_df

## Function to extract daily jobs from JobsDBTH and transform them
def daily_jobsdbth():
    """
    Extracts jobs from JobsDBTH, normalizes the data, and transforms it.
    Returns a DataFrame of transformed job data.
    """
    params = {
        'siteKey': 'TH-Main',
        'classification': '6281',  # IT Jobs
        'pageSize': 100,
        'locale': 'en-TH',
        'dateRange': 1
    }
    
    # Scrape jobs
    raw = JobsDBThScraper(classification_id='6281', base_params=params).scrape_jobs()
    
    # Normalize job data
    raw_df = JobDataNormalizer().jobsdbth(raw)
    
    # Transform job data
    transformer = JobsDBTHTransform(raw_df, categories_path='categories.json')
    transformed_df = transformer.transform()
    
    return transformed_df

## Function to extract daily jobs from FounditSG and transform them
def daily_founditsg():
    """
    Extracts jobs from FounditSG, normalizes the data, and transforms it.
    Returns a DataFrame of transformed job data.
    """
    base_params = {
        "sort": 1,
        "limit": 15,
        "query": '""',
        "quickApplyJobs": "true",
        "jobFreshness": "1",  # Last 24 hours
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
    
    # Scrape jobs
    raw = FounditScraper(base_params=base_params).extract_jobs()
    
    # Normalize job data
    raw_df = JobDataNormalizer().founditsg(raw)
    
    # Transform job data
    transformer = FounditTransform(raw_df)
    transformed_df = transformer.transform()
    
    return transformed_df

## Function to extract daily jobs from JobStreet Malaysia and transform them
def daily_jobstreetmalay():
    """
    Extracts jobs from JobStreet Malaysia, normalizes the data, and transforms it.
    Returns a DataFrame of transformed job data.
    """
    base_params = {
        'siteKey': 'MY-Main',
        'locale': 'en-MY',
        'dateRange': 1  # Last 24 hours
    }
    raw = JobStreetMalaysia(classification_id="6281", base_params=base_params).fetch_jobs()
    
    # Normalize job data
    raw_df = JobDataNormalizer().jobstreetmalay(raw)
    
    # Transform job data
    transformer = JobStreetMalayTransform(raw_df)
    transformed_df = transformer.transform()
    
    return transformed_df

def daily_jobsdbsg():
    scraper = JobsDBScraper(dynamic_pages=True)
    url_pattern = "https://sg.jobsdb.com/{role}-jobs?a=24h&p={page}"
    raw = scraper.run(url_pattern=url_pattern)
    raw_df = JobDataNormalizer().jobsdbsg(raw)
    print(f"JobsDBSG: {len(raw_df)} jobs scraped")
    transformer = JobsDBSGTransform(raw_df)
    transformed_df = transformer.transform()
    return transformed_df

def check_old_or_new(df: pd.DataFrame, table_name: str = r'"IT_jobs"."IT"') -> pd.DataFrame:
    """
    Check for new jobs by comparing job_link with existing database records.
    Returns only new jobs that don't exist in the database.
    """
    if df.empty:
        print("Input DataFrame is empty.")
        return df
    
    engine = create_engine(os.getenv("DATABASE_URL"))
    
    try:
        with engine.connect() as conn:
            # Only select job_link column for efficiency
            existing_df = pd.read_sql(f'SELECT job_link FROM {table_name}', conn)
            existing_links = set(existing_df['job_link'].tolist())
            
            # Filter out existing jobs
            new_df = df[~df['job_link'].isin(existing_links)].copy()
            
            if new_df.empty:
                print("No new jobs found.")
            else:
                print(f"Found {len(new_df)} new jobs out of {len(df)} total jobs.")
    
    except Exception as e:
        print(f"Error checking for duplicates: {e}")
        print("Proceeding with all jobs...")
        new_df = df.copy()
    
    return new_df

def add_job_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add job_id to the DataFrame using the custom_job_id function.
    """
    if df.empty:
        return df
    
    print("Generating job IDs...")
    df_with_ids = custom_job_id(df)
    print(f"Generated {len(df_with_ids)} job IDs.")
    
    return df_with_ids

def save_to_database(df: pd.DataFrame, table_name: str = "IT", schema: str = "IT_jobs"):
    """
    Save the DataFrame to the database by appending to existing data.
    """
    if df.empty:
        print("No data to save to database.")
        return
    
    database_url = os.getenv("DATABASE_URL")
    engine = create_engine(database_url)
    
    try:
        # Use engine directly for auto-commit
        df.to_sql(
            table_name,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False
        )
        print(f"Successfully saved {len(df)} new jobs to database.")
        
        # Log the job IDs that were added
        print("Sample of new job IDs added:")
        print(df['job_id'].head(10).tolist())
        
    except Exception as e:
        print(f"Error saving to database: {e}")
        raise

def main():
    """
    Main function to run the daily job extraction, transformation, and loading processes.
    """
    print(f"Starting daily job scraping process at {datetime.now()}")
    
    all_dfs = []
    
    # Extract and transform jobs from each source
    try:
        print("\n=== Scraping JobNetMM ===")
        daily_jobnetmm_df = daily_jobnetmm()
        print(f"JobNetMM: {len(daily_jobnetmm_df)} jobs scraped")
        all_dfs.append(daily_jobnetmm_df)
    except Exception as e:
        print(f"Error scraping JobNetMM: {e}")

    try:
        print("\n=== Scraping JobsDB Singapore ===")
        daily_jobsdbsg_df = daily_jobsdbsg()
        print(f"JobsDB SG: {len(daily_jobsdbsg_df)} jobs scraped")
        all_dfs.append(daily_jobsdbsg_df)
    except Exception as e:
        print(f"Error scraping JobsDB Singapore: {e}")
    
    try:
        print("\n=== Scraping JobsDBTH ===")
        daily_jobsdbth_df = daily_jobsdbth()
        print(f"JobsDBTH: {len(daily_jobsdbth_df)} jobs scraped")
        all_dfs.append(daily_jobsdbth_df)
    except Exception as e:
        print(f"Error scraping JobsDBTH: {e}")
    
    try:
        print("\n=== Scraping FounditSG ===")
        daily_founditsg_df = daily_founditsg()
        print(f"FounditSG: {len(daily_founditsg_df)} jobs scraped")
        all_dfs.append(daily_founditsg_df)
    except Exception as e:
        print(f"Error scraping FounditSG: {e}")
    
    try:
        print("\n=== Scraping JobStreet Malaysia ===")
        daily_jobstreetmalay_df = daily_jobstreetmalay()
        print(f"JobStreet Malaysia: {len(daily_jobstreetmalay_df)} jobs scraped")
        all_dfs.append(daily_jobstreetmalay_df)
    except Exception as e:
        print(f"Error scraping JobStreet Malaysia: {e}")
    
    # Combine all DataFrames
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        print(f"\n=== Combined Results ===")
        print(f"Total jobs scraped: {len(combined_df)}")
    else:
        print("No data scraped from any source.")
        return pd.DataFrame()
    
    return combined_df

def run_daily_process():
    """
    Complete daily process: scrape, check duplicates, add IDs, and save to database.
    """
    try:
        # Step 1: Scrape all job sources
        combined_df = main()
        
        if combined_df.empty:
            print("No jobs to process.")
            return
        
        # Step 2: Check for new jobs (remove duplicates with existing database)
        print("\n=== Checking for New Jobs ===")
        fresh_df = check_old_or_new(combined_df)
        
        if fresh_df.empty:
            print("Daily scraping completed. No new jobs to add.")
            return
        
        # Step 3: Add job IDs to new jobs
        print("\n=== Adding Job IDs ===")
        fresh_df_with_ids = add_job_ids(fresh_df)
        
        # Step 4: Save to database
        print("\n=== Saving to Database ===")
        save_to_database(fresh_df_with_ids)
        
        print(f"\n=== Daily Process Completed Successfully ===")
        print(f"Added {len(fresh_df_with_ids)} new jobs to the database.")
        
        # Show sample of what was added
        print("\nSample of new jobs added:")
        print(fresh_df_with_ids[['job_id', 'title', 'company', 'source']].head())
        
    except Exception as e:
        print(f"Error in daily process: {e}")
        raise

if __name__ == "__main__":
    run_daily_process()