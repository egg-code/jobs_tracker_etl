from jobnetmm import JobNetScraper
import os

def main():
    ## For jobnetmm
    email = os.getenv("JOBNET_EMAIL")
    password = os.getenv("JOBNET_PASSWORD")
    job_function = 17 # 17 for IT jobs
    extract = JobNetScraper(email, password)
    jobnet_df = extract.get_jobs(job_function=job_function)
    print(jobnet_df.head())

    ## For jobsdb_sg

    ## For foundit

if __name__ == "__main__":
    main()