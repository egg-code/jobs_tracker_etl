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
            'employmentTypes': 'job_type',
            'updatedAt': 'date_posted',
            'seoJdUrl': 'job_link',
            'roles': 'category'
        })
        
        df['country'] = 'SG'
        df['work_arrangement'] = None
        df['source'] = 'founditsg'
        return df[self.standard_cols+ ['category']]

    
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