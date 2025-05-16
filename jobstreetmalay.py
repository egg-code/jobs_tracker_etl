import requests
import pandas as pd
import time

base_url = "https://my.jobstreet.com/api/jobsearch/v5/search"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://my.jobstreet.com/",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://my.jobstreet.com"
}

all_jobs = []
page = 1
page_size = 32

while True:
    params = {
        'siteKey': 'MY-Main',
        'page': page,
        'classification': '6281',
        'pageSize': page_size,
        'locale': 'en-MY',
        'daterange': '31'
    }

    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Failed to fetch page {page}: Status code {response.status_code}")
        break

    data = response.json()
    jobs = data.get("data", [])

    if not jobs:
        print("No more jobs found.")
        break

    all_jobs.extend(jobs)
    print(f"Fetched page {page} with {len(jobs)} jobs.")
    page += 1
    time.sleep(1)

# Convert to DataFrame
df = pd.DataFrame([
    {
        'job_id': job.get('id'),
        'title': job.get('title'),
        'company': job.get('companyName'),
        'location': job.get('locations', [{}])[0].get('label') if job.get('locations') else None,
        'posted_date': job.get('listingDate'),
        'description': job.get('teaser'),
        'work_type': ', '.join(job.get('workTypes', [])),
        'arrangement': job.get('workArrangements', {}).get('data', [{}])[0].get('label', {}).get('text')
    }
    for job in all_jobs
])

print(df.head())
