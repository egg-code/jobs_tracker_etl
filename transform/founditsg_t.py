import pandas as pd
import json, re
from datetime import datetime, timedelta
import pytz
import logging

## Set up logging
from utils.logger import get_module_logger
logger = get_module_logger(__name__, group='transform')

expected_columns = ['title', 'category', 'company', 'location', 'country', 'min_salary', 'max_salary', 'avg_salary', 'currency', 'job_type', 'work_arrangement', 'level', 'date_posted', 'job_link', 'source']

class FounditTransform:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def parse_salary_founditSG(self, s):
        if not isinstance(s, str) or not s.strip():
            return (pd.NA, pd.NA, pd.NA, pd.NA)

        try:
            parts = s.strip().split()
            if '-' in parts[0]:
                min_str, max_str = parts[0].split('-')
                currency = parts[1] if len(parts) > 1 else pd.NA
                min_salary = int(min_str.replace(',', '').strip())
                max_salary = int(max_str.replace(',', '').strip())
                avg_salary = (min_salary + max_salary) // 2
                return (avg_salary, min_salary, max_salary, currency)
            else:
                salary = int(parts[0].replace(',', '').strip())
                currency = parts[1] if len(parts) > 1 else pd.NA
                return (salary, pd.NA, pd.NA, currency)
        except Exception as e:
            logger.warning(f"Failed to parse salary: {s} -> {e}")
            return (pd.NA, pd.NA, pd.NA, pd.NA)

    def _convert_to_utc_plus_630(self, text):
        yangon_tz = pytz.timezone('Asia/Yangon')
        today = datetime.now(pytz.utc)
        if isinstance(text, str):
            text = text.lower().strip()
            try:
                parts = text.split()
                num = parts[0].lower()
                unit = parts[1].lower()
                num = 1 if num == 'a' or 'an' else int(num)

                if "day" in unit:
                    converted = today - timedelta(days=num)
                    return converted.astimezone(yangon_tz).strftime("%Y-%m-%d")
                elif "hour" in unit:
                    return today.astimezone(yangon_tz).strftime("%Y-%m-%d")
                elif "month" in unit:
                    converted = today - timedelta(days=num * 30)
                    return converted.astimezone(yangon_tz).strftime("%Y-%m-%d")
                else:
                    return pd.NA
            except Exception as e:
                logger.error(f"Date Conversion error: {text} -> {e}")
        return pd.NA

    def _drop_missing(self):
        self.df.dropna(subset=['job_link'], inplace=True)
        self.df.drop_duplicates(subset=['job_link'], inplace=True)

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
        self.df.fillna(pd.NA, inplace=True)

    def _extract_salary_columns(self):
        salary_cols = self.df['salary'].apply(self.parse_salary_founditSG)
        self.df[['avg_salary', 'min_salary', 'max_salary', 'currency']] = pd.DataFrame(salary_cols.tolist(), index=self.df.index)

    def transform(self):
        logger.info("Transforming Foundit DataFrame")
        self._convert_job_type()
        self._convert_date_posted()
        self._drop_missing()
        self._add_full_url()
        self._extract_salary_columns()
        self._fill_missing()
        logger.info("Transformation Foundit complete")
        return self.df.reindex(columns=expected_columns)
