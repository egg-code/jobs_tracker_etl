import pandas as pd
import json, re
from datetime import datetime
import logging

## Set up logging
from utils.logger import get_module_logger
logger = get_module_logger(__name__, group='transform')

expected_columns = ['title', 'category', 'company', 'location', 'country', 'min_salary', 'max_salary', 'avg_salary', 'currency', 'job_type', 'work_arrangement', 'level', 'date_posted', 'job_link', 'source']

class JobNetTransform:
    def __init__(self, df:pd.DataFrame, categories_path: str = 'categories.json'):
        self.df = df
        with open(categories_path, 'r') as f:
            self.categories = json.load(f)

    def _extract_job_level(self, title):
        title_lower = title.lower()
        if 'intern' in title_lower or 'internship' in title_lower:
            return 'Intern'
        elif 'fresher' in title_lower:
            return 'Entry'
        elif 'junior' in title_lower:
            return 'Junior'
        elif 'senior' in title_lower:
            return 'Senior'
        elif 'lead' in title_lower or 'manager' in title_lower or 'head' in title_lower:
            return 'Manager'

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

    def parse_salary(self, s):
        if not isinstance(s, str) or not s.strip():
            return (pd.NA, pd.NA, pd.NA, pd.NA)

        s_clean = s.strip()
        if s_clean.lower() == 'negotiable':
            return (pd.NA, pd.NA, 'Negotiable', pd.NA)

        up_to_match = re.search(r'^Up to\s+([\d,]+)\s*Ks$', s_clean, re.IGNORECASE)
        if up_to_match:
            max_salary = int(up_to_match.group(1).replace(',', ''))
            return (max_salary, pd.NA, max_salary, 'Ks')

        return (pd.NA, pd.NA, pd.NA, pd.NA)

    def _extract_salary_columns(self):
        salary_data = self.df['salary'].apply(self.parse_salary).apply(pd.Series)
        salary_data.columns = ['avg_salary', 'min_salary', 'max_salary', 'currency']
        self.df = pd.concat([self.df, salary_data], axis=1)

    def _fill_na(self):
        self.df = self.df.fillna(pd.NA)

    def _tokenize_ngrams(self, title):
        title = title.lower().replace("/", " ")
        title = re.sub(r"[^\w\s]", "", title)
        words = title.split()
        bigrams = [' '.join(pair) for pair in zip(words, words[1:])]
        return words + bigrams

    def _categorize_title_score(self, title, threshold=1.5):
        tokens = self._tokenize_ngrams(title)
        scores = {
            category: sum(keywords.get(token, 0) for token in tokens)
            for category, keywords in self.categories.items()
        }
        best_category = max(scores, key=scores.get)
        return best_category if scores[best_category] >= threshold else "Other"
    
    def _enrich_with_title_features(self):
        self.df['level'] = self.df['title'].apply(self._extract_job_level)
        self.df['category'] = self.df['title'].apply(self._categorize_title_score)

    def transform(self):
        logger.info("Transforming JobNetMM DataFrame")
        self._extract_salary_columns()
        self._parse_date()
        self._fill_na()
        self._enrich_with_title_features()
        logger.info("Transformation JobNetMM complete")
        return self.df.reindex(columns=expected_columns)
