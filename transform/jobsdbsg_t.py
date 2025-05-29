import pandas as pd
import json, re
from datetime import datetime
import logging
from pathlib import Path
from collections import defaultdict
from utils.logger import get_module_logger

logger = get_module_logger(__name__, group='transform')

expected_columns = ['title', 'category', 'company', 'location', 'country', 'min_salary', 'max_salary', 'avg_salary', 'currency', 'job_type', 'work_arrangement', 'level', 'date_posted', 'job_link', 'source']

# Load category data
BASE_DIR = Path(__file__).resolve().parent.parent
CATEGORY_FILE = BASE_DIR/"categories.json"

with open(CATEGORY_FILE, "r", encoding="utf-8") as f:
    category_data = json.load(f)

manual_role_lookup = category_data["manual_role_lookup"]
categories = category_data["categories"]

# Flatten manual lookup
manual_lookup_flat = {
    kw.lower(): category for category, keywords in manual_role_lookup.items() for kw in keywords
}


class JobsDBSGTransform:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def parse_salary(self, s):
        if not isinstance(s, str) or not s.strip():
            return (pd.NA, pd.NA, pd.NA, pd.NA)
        try:
            s_clean = s.replace('–', '-').replace('—', '-')
            currency = "SGD"
            range_match = re.search(r'([\d,]+)\s*-\s*([\d,]+)', s_clean)
            if range_match:
                min_salary = int(range_match.group(1).replace(',', ''))
                max_salary = int(range_match.group(2).replace(',', ''))
                avg_salary = (min_salary + max_salary) // 2
                return (avg_salary, min_salary, max_salary, currency)
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

    def _tokenize_ngrams(self, text):
        text = text.lower()
        tokens = re.findall(r'\w+', text)
        bigrams = [' '.join(pair) for pair in zip(tokens, tokens[1:])]
        return tokens + bigrams

    def _match_manual_lookup(self, text):
        if not text:
            return None
        text = text.lower()
        for keyword, category in manual_lookup_flat.items():
            if keyword in text:
                return category
        return None

    def _categorize_title_score(self, title, threshold=1.5):
        tokens = self._tokenize_ngrams(title)
        scores = defaultdict(float)
        for category, keywords in categories.items():
            for token in tokens:
                scores[category] += keywords.get(token, 0)
        best_category = max(scores, key=scores.get, default=None)
        return best_category if scores[best_category] >= threshold else "Other"

    def _categorize_job_title(self):
        def categorize(title):
            if not isinstance(title, str):
                return "Other"
            manual_result = self._match_manual_lookup(title)
            if manual_result:
                return manual_result
            return self._categorize_title_score(title)
        logger.info("Categorizing job titles into categories")
        self.df["category"] = self.df["title"].apply(categorize)

    def transform(self):
        logger.info("Transforming JobsDBSG DataFrame")
        self._extract_salary_columns()
        self._parse_date()
        self._enrich_with_title_features()
        self._categorize_job_title()
        self._fill_na()
        logger.info("Transformation JobsDBSG complete")
        return self.df.reindex(columns=expected_columns)
