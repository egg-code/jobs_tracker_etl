import pandas as pd

class JobDataNormalizer:

    def __init__(self):
        self.standard_cols = [
            'title', 'company', 'location', 'salary',
            'job_type', 'work_arrangement', 'date_posted',
            'job_link', 'country', 'source'
        ]

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
        import re
        from datetime import datetime, timedelta

        # Rename columns to standard format
        df = df.rename(columns={
            'Title': 'title',
            'Category': 'category',
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

        # Fill missing salary and job_type
        df['salary'] = df['salary'].replace('', pd.NA).fillna('N/A')
        df['job_type'] = df['job_type'].replace('', pd.NA).fillna('N/A')
        df['company'] = df['job_type'].replace('', pd.NA).fillna('N/A')
        # Fill missing or empty work_arrangement
        df['work_arrangement'] = df['work_arrangement'].replace('', pd.NA)
        # df.loc[(df['work_arrangement'].isna()) & (df['job_type'] == 'Full time'), 'work_arrangement'] = 'On-site'
        df['work_arrangement'] = df['work_arrangement'].fillna('N/A')

        # Parse 'date_posted' and convert to full UTC datetime
        def parse_date_posted(text):
            try:
                match = re.search(r'(\d+)([a-z]+)', text.lower())
                if not match:
                    return None
                value, unit = int(match.group(1)), match.group(2)
                now = datetime.utcnow()
                if unit == 's':  # seconds
                    return (now - timedelta(seconds=value)).isoformat() + 'Z'
                elif unit == 'm':  # minutes
                    return (now - timedelta(minutes=value)).isoformat() + 'Z'
                elif unit == 'h':  # hours
                    return (now - timedelta(hours=value)).isoformat() + 'Z'
                elif unit == 'd':  # days
                    return (now - timedelta(days=value)).isoformat() + 'Z'
                elif unit == 'mo':  # months (approximate)
                    return (now - timedelta(days=30 * value)).isoformat() + 'Z'
                else:
                    return None
            except Exception:
                return None

        df['date_posted'] = df['date_posted'].apply(parse_date_posted)

        return df[self.standard_cols + ['category']]
    
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
    
    ## Foundit Singapore
    def founditsg(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize the job data from Foundit Singapore.
        """
        df = df.rename(columns={
            'title': 'title',
            'companyName': 'company',
            'locations': 'location',
            'salary': 'salary',
            'jobTypes': 'job_type',
            'updatedAt': 'date_posted',
            'seoJdUrl': 'job_link'
        })
        df['country'] = 'SG'
        df['work_arrangement'] = None
        df['source'] = 'founditsg'
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
    
        # Replace empty strings with NaN, then fill all NaNs with 'N/A'
        df = df.replace('', pd.NA).fillna('N/A')
    
        return df[self.standard_cols]