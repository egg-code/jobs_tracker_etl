import pandas as pd
from datetime import datetime, timedelta
import pytz

class JobDataNormalizer:

    def __init__(self):
        self.standard_cols = [
            'title', 'company', 'location', 'salary',
            'job_type', 'work_arrangement', 'date_posted',
            'job_link', 'country', 'source'
        ]

    # Convert "X days ago" to datetime in UTC+6:30
    def convert_to_utc_plus_630(self, text):
        yangon_tz = pytz.timezone('Asia/Yangon')
        today = datetime.now(pytz.utc)
        if isinstance(text, str) and "day" in text:
            try:
                days_ago = int(text.strip().split()[0])
                converted = today - timedelta(days=days_ago)
                return converted.astimezone(yangon_tz).strftime('%Y-%m-%d')
            except:
                return None
        return None

    # Handel missing values
    def fill_missing_foundit_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        # Replace empty strings with NaN-equivalent (so they can be filled)
        df.replace('', pd.NA, inplace=True)

        # Fill all NaN/NA values with 'N/A' regardless of type
        df.fillna('N/A', inplace=True)

        return df


    ## Foundit Singapore
    def founditsg(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={
            'title': 'title',
            'companyName': 'company',
            'locations': 'location',
            'salary': 'salary',
            'jobTypes': 'job_type',
            'postedBy': 'date_posted',
            'seoJdUrl': 'job_link'
        })

        # Convert job_type list to string
        df['job_type'] = df['job_type'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

        # Convert "X days ago" to datetime in UTC+6:30
        df['date_posted'] = df['date_posted'].apply(self.convert_to_utc_plus_630)

        # Add full URL to job_link
        df['job_link'] = df['job_link'].apply(
            lambda x: f"https://www.foundit.sg{x}" if isinstance(x, str) and not x.startswith("http") else x
        )

        # Add fixed values
        df['country'] = 'SG'
        df['work_arrangement'] = None
        df['source'] = 'founditsg'

        # Apply missing value filler
        df = self.fill_missing_foundit_fields(df)

        return df[self.standard_cols]


    ## Jobnet Myanmar
    def jobnetmm(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize the job data from JobNet Myanmar.
        """
        df = df.rename(columns={
            'Title': 'title',
            'Company': 'company',
            'Location': 'location',
            'Salary': 'salary',
            'Date_Posted': 'date_posted',
            'Job_Link': 'job_link'
        })
        df['country'] = 'MM'
        df['job_type'] = None
        df['work_arrangement'] = None
        df['source'] = 'jobnetmm'
        return df[self.standard_cols]
    
    ## JobsDB Singapore
    def jobsdbsg(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize the job data from JobsDB Singapore.
        """
        df = df.rename(columns={
            'Title': 'title',
            'Company': 'company',
            'Location': 'location',
            'Salary': 'salary',
            'Job_Type': 'job_type',
            'Work_Arrangement': 'work_arrangement',
            'Job_Link': 'job_link',
            'Date_Posted': 'date_posted'
        })
        df['country'] = 'SG'
        df['source'] = 'jobsdbsg'
        return df[self.standard_cols]
    
    ## JobsDB Thailand
    def jobsdbth(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize the job data from JobsDB Thailand.
        """
        df = df.rename(columns={
            'job_title': 'title',
            'company': 'company',
            'location': 'location',
            'salary': 'salary',
            'job_type': 'job_type',
            'work_arrangement': 'work_arrangement',
            'date_posted': 'date_posted',
            'job_link': 'job_link'
        })
        df['country'] = df['country_code']
        df['source'] = 'jobsdbth'
        return df[self.standard_cols]
    

  
    ## JobStreet Malaysia
    def jobstreetmalay(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize the job data from JobStreet Malaysia.
        """
        df = df.rename(columns={
            'job_title': 'title',
            'company': 'company',
            'location': 'location',
            'salary': 'salary',
            'job_type': 'job_type',
            'work_arrangement': 'work_arrangement',
            'date_posted': 'date_posted',
            'job_link': 'job_link'
        })
        df['country'] = df['country_code']
        df['source'] = 'jobstreetmalay'
        return df[self.standard_cols]