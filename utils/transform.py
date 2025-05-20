import re
import pandas as pd
from datetime import datetime, timedelta
import pytz
import logging

## Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('logs/transform_logs.log', mode='a')
fomatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(fomatter)
logger.addHandler(handler)

## Class for transforming jobnetmm df
class JobNetTransform:
    def __init__(self, df:pd.DataFrame):
        self.df = df

    def _parse_date(self):
        today =datetime.today()

        def parse_date(date_str):
            date_str = date_str.strip().lower()
            if "today" in date_str:
                return today.strftime("%Y-%m-%d")
            try:
                return datetime.strptime(date_str, "%d %b %Y").strftime("%Y-%m-%d")
            except ValueError:
                logger.error(f"Date parsing error for string: {date_str}")
                return pd.NA
        
        self.df['date_posted'] = self.df['date_posted'].apply(parse_date)

    def _fill_na(self):
        self.df = self.df.fillna(pd.NA)

    def transform(self):
        logger.info("Transforming JobNetMM DataFrame")
        self._parse_date()
        self._fill_na()
        logger.info("Transformation JobNetMM complete")
        return self.df

## Class for transforming jobsdbth df
class JobsDBTHTransform:
    def __init__(self, df:pd.DataFrame):
        self.df = df
    
    def _parse_date(self):
        def parse_date(date_str):
            try:
                return datetime.strptime(date_str.strip(), "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")
            except ValueError:
                logger.error(f"Date parsing error for string: {date_str}")
                return pd.NA
        
        self.df['date_posted'] = self.df['date_posted'].apply(parse_date)

    def _fill_na(self):
        self.df = self.df.fillna(pd.NA)

    def transform(self):
        logger.info("Transforming JobsDBTH DataFrame")
        self._parse_date()
        self._fill_na()
        logger.info("Transformation JobsDBTH complete")
        return self.df

## Class for transforming founditsg df
class FounditTransform:
    def __init__(self, df:pd.DataFrame):
        self.df = df
    
     # Convert "X days ago" to datetime in UTC+6:30
    def _convert_to_utc_plus_630(self, text):
        yangon_tz = pytz.timezone('Asia/Yangon')
        today = datetime.now(pytz.utc)
        if isinstance(text, str) and "day" in text:
            try:
                days_ago = int(text.strip().split()[0])
                converted = today - timedelta(days=days_ago)
                return converted.astimezone(yangon_tz).strftime('%Y-%m-%d')
            except Exception as e:
                logger.error(f"Date conversion error: {text} | {e}")
                return pd.NA
        return pd.NA
    
    def _convert_job_type(self):
        self.df['job_type'] = self.df['job_type'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else x
        )

    def _convert_date_posted(self):
        self.df['date_posted'] = self.df['date_posted'].apply(self._convert_to_utc_plus_630)

    def _add_full_url(self):
        self.df['job_link'] = self.df['job_link'].apply(
            lambda x: f"https://www.foundit.sg{x}" if isinstance(x, str) and not x.startswith("http") else x
        )

    def _fill_missing(self):
        self.df.replace('', pd.NA, inplace=True)
        self.df.fillna('N/A', inplace=True)

    def transform(self):
        logger.info("Transforming Foundit DataFrame")
        self._convert_job_type()
        self._convert_date_posted()
        self._add_full_url()
        self._fill_missing()
        logger.info("Transformation Foundit complete")
        return self.df

## Class for transforming jobsdbsg df
class JobsDBSGTransform:
    def __init__(self, df:pd.DataFrame):
        self.df = df

    def _parse_date_posted(self):
        def parse(text):
            try:
                match = re.search(r'(\d+)([a-z]+)', text.lower())
                if not match:
                    return pd.NA
                value, unit = int(match.group(1)), match.group(2)
                now = datetime.utcnow()
                if unit == 's':
                    return (now - timedelta(seconds=value)).isoformat() + 'Z'
                elif unit == 'm':
                    return (now - timedelta(minutes=value)).isoformat() + 'Z'
                elif unit == 'h':
                    return (now - timedelta(hours=value)).isoformat() + 'Z'
                elif unit == 'd':
                    return (now - timedelta(days=value)).isoformat() + 'Z'
                elif unit == 'mo':
                    return (now - timedelta(days=30 * value)).isoformat() + 'Z'
                else:
                    return pd.NA
            except Exception as e:
                logger.error(f"Date parsing error: {text} -> {e}")
                return pd.NA

        self.df['date_posted'] = self.df['date_posted'].apply(parse)

    def _clean_columns(self):
        # Replace empty strings with pd.NA
        cols_to_clean = ['salary', 'job_type', 'company', 'work_arrangement']
        for col in cols_to_clean:
            self.df[col] = self.df[col].replace('', pd.NA)

        # Fill missing values
        self.df['salary'] = self.df['salary'].fillna('N/A')
        self.df['job_type'] = self.df['job_type'].fillna('N/A')
        self.df['company'] = self.df['company'].fillna('N/A')
        self.df['work_arrangement'] = self.df['work_arrangement'].fillna('N/A')

    def transform(self):
        logger.info("Transforming Other Source DataFrame")
        self._parse_date_posted()
        self._clean_columns()
        logger.info("Transformation complete")
        return self.df

## Class for transforming jobstreetmalay df
class JobStreetMalayTransform:
    def __init__(self, df:pd.DataFrame):
        self.df = df
        
    def _fill_na(self):
        self.df = self.df.replace('', pd.NA).fillna('N/A')

    def transform(self):
        logger.info("Transforming JobsDBTH DataFrame")
        self._fill_na()
        logger.info("Transformation JobsDBTH complete")
        return self.df

    
if __name__ == "__main__":
   mmdf = pd.read_csv("output/jobnetmm.csv")
   jobnetmm = JobNetTransform(mmdf)
   cleaned_jobnetmm = jobnetmm.transform()
   cleaned_jobnetmm.to_csv("output/jobnetmm_cleaned.csv", index=False, encoding='utf-8-sig')
   
   thdf = pd.read_csv("output/jobsdbth.csv")
   jobsdbth = JobsDBTHTransform(thdf)
   cleaned_jobsdbth = jobsdbth.transform()
   cleaned_jobsdbth.to_csv("output/jobsdbth_cleaned.csv", index=False, encoding='utf-8-sig')

   founditdf = pd.read_csv("output/foundit.csv")
   foundit = FounditTransform(founditdf)
   cleaned_foundit = foundit.transform()
   cleaned_foundit.to_csv("output/foundit_cleaned.csv", index=False, encoding='utf-8-sig')

   sgdf = pd.read_csv("output/jobsdbsg.csv")
   jobsdbsg = JobsDBSGTransform(sgdf)
   cleaned_jobsdbsg = jobsdbsg.transform()
   cleaned_jobsdbsg.to_csv("output/jobsdbsg_cleaned.csv", index=False, encoding='utf-8-sig')

   malaydf = pd.read_csv("output/jobstreetmalay.csv")
   jobstreetmalay = JobStreetMalayTransform(malaydf)
   cleaned_jobstreetmalay = jobstreetmalay.transform()
   cleaned_jobstreetmalay.to_csv("output/jobstreetmalay_cleaned.csv", index=False, encoding='utf-8-sig')