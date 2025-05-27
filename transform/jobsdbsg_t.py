import pandas as pd
import json, re
from datetime import datetime, timedelta
import logging

## Set up logging
from utils.logger import get_module_logger
logger = get_module_logger(__name__, group='transform')

expected_columns = ['title', 'category', 'company', 'location', 'country', 'min_salary', 'max_salary', 'avg_salary', 'currency', 'job_type', 'work_arrangement', 'level', 'date_posted', 'job_link', 'source']

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
        self.df['salary'] = self.df['salary'].fillna(pd.NA)
        self.df['job_type'] = self.df['job_type'].fillna(pd.NA)
        self.df['company'] = self.df['company'].fillna(pd.NA)
        self.df['work_arrangement'] = self.df['work_arrangement'].fillna(pd.NA)

    def transform(self):
        logger.info("Transforming Other Source DataFrame")
        self._parse_date_posted()
        self._clean_columns()
        logger.info("Transformation complete")
        return self.df.reindex(columns=expected_columns)
