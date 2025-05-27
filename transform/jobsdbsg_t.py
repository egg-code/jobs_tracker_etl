import pandas as pd
import json, re
from datetime import datetime, timedelta
import logging

## Set up logging
from utils.logger import get_module_logger
logger = get_module_logger(__name__, group='transform')

expected_columns = ['title', 'category', 'company', 'location', 'country', 'min_salary', 'max_salary', 'avg_salary', 'currency', 'job_type', 'work_arrangement', 'level', 'date_posted', 'job_link', 'source']

class JobsDBSGTransform:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def parse_salary(self, s):
        if not isinstance(s, str) or not s.strip():
            return (pd.NA, pd.NA, pd.NA, pd.NA)

        try:
            s_clean = s.replace('–', '-').replace('—', '-')

            # Extract currency (e.g., $)
            currency = "SGD"

            # Extract salary range: "$5,500 - $6,500"
            range_match = re.search(r'([\d,]+)\s*-\s*([\d,]+)', s_clean)
            if range_match:
                min_salary = int(range_match.group(1).replace(',', ''))
                max_salary = int(range_match.group(2).replace(',', ''))
                avg_salary = (min_salary + max_salary) // 2
                return (avg_salary, min_salary, max_salary, currency)

            # Extract single value: "$5500"
            single_match = re.search(r'([\d,]+)', s_clean)
            if single_match:
                salary = int(single_match.group(1).replace(',', ''))
                return (salary, salary, salary, currency)

            return (pd.NA, pd.NA, pd.NA, currency)

        except Exception as e:
            logger.warning(f"Failed to parse salary: {s} -> {e}")
            return (pd.NA, pd.NA, pd.NA, pd.NA)


    def _extract_salary_columns(self):
        salary_cols = self.df['salary'].apply(self.parse_salary)
        self.df[['avg_salary', 'min_salary', 'max_salary', 'currency']] = pd.DataFrame(salary_cols.tolist(), index=self.df.index)

    def _parse_date(self):
        def parse_date(date_str):
            try:
                return datetime.strptime(date_str.strip(), "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d")
            except ValueError:
                logger.error(f"Date parsing error for string: {date_str}")
                return pd.NA

        self.df['date_posted'] = self.df['date_posted'].apply(parse_date)
    
    def _extract_job_level(self, title):
        title_lower = title.lower()
        if 'intern' in title_lower:
            return 'Intern'
        elif 'fresh' in title_lower:
            return 'Entry'
        elif 'junior' in title_lower:
            return 'Junior'
        elif 'senior' in title_lower:
            return 'Senior'
        elif 'manager' in title_lower or 'lead' in title_lower or 'head' in title_lower:
            return 'Manager'
        return pd.NA
    
    def _enrich_with_title_features(self):
        self.df['level'] = self.df['title'].apply(self._extract_job_level)

    def _fill_na(self):
        self.df = self.df.fillna(pd.NA)

    def transform(self):
        logger.info("Transforming JobsDBSG DataFrame")
        self._extract_salary_columns()
        self._parse_date()
        self._enrich_with_title_features()
        self._fill_na()
        logger.info("Transformation JobsDBSG complete")
        return self.df.reindex(columns=expected_columns)
