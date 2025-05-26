import pandas as pd
import json, re
from datetime import datetime
import logging

## Set up logging
logfile_name = datetime.now().strftime('jobsdbth_t_%Y%m%d_%H%M%S.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logfile_name)
fomatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(fomatter)
logger.addHandler(handler)

expected_columns = ['title', 'category', 'company', 'location', 'country', 'min_salary', 'max_salary', 'avg_salary', 'currency', 'job_type', 'work_arrangement', 'level', 'date_posted', 'job_link', 'source']

class JobsDBTHTransform:
    def __init__(self, df: pd.DataFrame, categories_path: str = 'categories.json'):
        self.df = df

        # Load category scoring keywords
        with open(categories_path, 'r') as f:
            self.categories = json.load(f)

    def parse_salary(self, s):
        if not isinstance(s, str) or not s.strip():
            return (pd.NA, pd.NA, pd.NA, pd.NA)

        try:
            # Clean the salary string
            s_clean = s.replace('–', '-').replace('—', '-').replace('฿', '').strip()
            currency = "THB"

            # Extract numeric salary range (e.g., 25000-35000)
            range_match = re.search(r'([\d,]+)\s*-\s*([\d,]+)', s_clean)
            if range_match:
                min_str, max_str = range_match.groups()
                min_salary = int(min_str.replace(',', ''))
                max_salary = int(max_str.replace(',', ''))
                avg_salary = (min_salary + max_salary) // 2
                return (avg_salary, min_salary, max_salary, currency)
            
            # Handle single value salary
            single_match = re.search(r'([\d,]+)', s_clean)
            if single_match:
                salary = int(single_match.group(1).replace(',', ''))
                return (salary, pd.NA, pd.NA, currency)

            return (pd.NA, pd.NA, pd.NA, pd.NA)

        except Exception as e:
            logger.warning(f"Failed to parse salary: {s} -> {e}")
            return (pd.NA, pd.NA, pd.NA, pd.NA)

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

    def _extract_salary_columns(self):
        salary_data = self.df['salary'].apply(self.parse_salary).apply(pd.Series)
        salary_data.columns = ['avg_salary', 'min_salary', 'max_salary', 'currency']
        self.df = pd.concat([self.df, salary_data], axis=1)

    def _clean_job_title(self, title):
        title = title.lower()
        noise_keywords = [
            'bangkok', 'thailand', 'buri', 'chiang', 'phuket', 'nonthaburi', 'rayong', 'chonburi',
            'based', 'relocation', 'provided', 'remote', 'onsite', 'visa', 'package', 'renewal',
            'thai', 'speakers', 'speaking', 'japanese', 'english', 'malaysia', 'national', 'communication required',
            'contract', 'months', 'month', 'term', 'fix', 'year', '1year', 'years', '6month', '10',
            'asoke', 'asok', 'ekkamai', 'punnawithi', 'phrom', 'phong', 'khlong', 'toei', 'chongnonsri'
        ]
        for kw in noise_keywords:
            title = re.sub(rf'\b{re.escape(kw)}\b', '', title)
        title = re.sub(r'\s+', ' ', title)
        title = re.sub(r'[^\w\s/.-]', '', title)
        title = re.sub(r'\b(and|or|with|in|of|the|to|for|by|on|at|from|as|is|are|be|an|a)\b', '', title)
        return re.sub(r'\s+', ' ', title).strip()

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
        self.df['cleaned_title'] = self.df['title'].apply(self._clean_job_title)
        self.df['level'] = self.df['cleaned_title'].apply(self._extract_job_level)
        self.df['category'] = self.df['cleaned_title'].apply(self._categorize_title_score)

    def transform(self):
        logger.info("Transforming JobsDBTH DataFrame")
        self._extract_salary_columns()
        self._parse_date()
        self._fill_na()
        self._enrich_with_title_features()
        logger.info("Transformation JobsDBTH complete")
        return self.df.reindex(columns=expected_columns)
