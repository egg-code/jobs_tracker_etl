import re
import json
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

expected_columns = ['title', 'category', 'company', 'location', 'country', 'min_salary', 'max_salary', 'avg_salary', 'currency', 'job_type', 'work_arrangement', 'level', 'date_posted', 'job_link', 'source']
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
        return self.df.reindex(columns=expected_columns)

## Class for transforming jobsdbth df
class JobsDBTHTransform:
    def __init__(self, df: pd.DataFrame, categories_path: str = 'categories_th.json'):
        self.df = df

        # Load category scoring keywords
        with open(categories_path, 'r') as f:
            self.categories = json.load(f)

    def parse_salary(self, s):
        if not isinstance(s, str) or not s.strip():
            return (pd.NA, pd.NA, pd.NA, pd.NA)

        try:
            # Clean the salary string
            s_clean = s.replace('–', '-').replace('—', '-')
            # Extract currency symbol (if any)
            currency_match = re.search(r'[^\d,\-\s]+', s)
            currency = currency_match.group() if currency_match else pd.NA

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
        salary_cols = self.df['salary'].apply(self.parse_salary)
        self.df[['avg_salary', 'min_salary', 'max_salary', 'currency']] = pd.DataFrame(salary_cols.tolist(), index=self.df.index)

    def _clean_job_title(self, title):
        title = title.lower()
        noise_keywords = [
            'bangko k', 'thailand', 'buri', 'chiang', 'phuket', 'nonthaburi', 'rayong', 'chonburi',
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

## Class for transforming founditsg df
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
    
## Class for transforming jobsdbsg df
# class JobsDBSGTransform:
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
        logger.info("Transforming Jobsdb SG DataFrame")
        self._parse_date_posted()
        self._clean_columns()
        logger.info("Transformation Jobsdb SG complete")
        return self.df.reindex(columns=expected_columns)
    
class JobsDBSGTransform:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def parse_salary(self, s):
        if not isinstance(s, str) or not s.strip():
            return (pd.NA, pd.NA, pd.NA, pd.NA)

        try:
            s_clean = s.replace('–', '-').replace('—', '-')

            # Extract currency (e.g., $)
            currency_match = re.search(r'([^\d\s,.-]+)', s_clean)
            currency = currency_match.group(1) if currency_match else pd.NA

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

    def _fill_na(self):
        self.df = self.df.fillna(pd.NA)

    def transform(self):
        logger.info("Transforming JobsDBSG DataFrame")
        self._extract_salary_columns()
        self._parse_date()
        self._fill_na()
        logger.info("Transformation JobsDBSG complete")
        # self.df.to_csv("output/jobsdbsg_transform.csv", index=False)
        return self.df.reindex(columns=expected_columns)
    
## Class for transforming jobstreetmalay df
class JobStreetMalayTransform:
    def __init__(self, df: pd.DataFrame, categories_path: str = 'categories_th.json'):
        self.df = df
        with open(categories_path, 'r') as f:
            self.categories = json.load(f)

    def parse_salary(self, s):
        if not isinstance(s, str) or not s.strip() or 'N/A' in s:
            return (pd.NA, pd.NA, pd.NA, pd.NA)
        try:
            s_clean = s.replace('–', '-').replace('—', '-').replace('\xa0', ' ')
            currency_match = re.match(r'^([A-Za-z]+)', s_clean.strip())
            currency = currency_match.group(1) if currency_match else pd.NA

            # Match range like "RM 3,388 – RM 4,752 per month"
            range_match = re.search(r'([\d,]+)\s*-\s*([\d,]+)', s_clean)
            if range_match:
                min_str, max_str = range_match.groups()
                min_salary = int(min_str.replace(',', ''))
                max_salary = int(max_str.replace(',', ''))
                avg_salary = (min_salary + max_salary) // 2
                return (avg_salary, min_salary, max_salary, currency)

            # Match single value like "RM 4,500 per month"
            single_match = re.search(r'([\d,]+)', s_clean)
            if single_match:
                salary = int(single_match.group(1).replace(',', ''))
                return (salary, salary, salary, currency)

        except Exception as e:
            logger.warning(f"Failed to parse salary: {s} -> {e}")

        return (pd.NA, pd.NA, pd.NA, pd.NA)


    def _parse_date(self):
        def parse(date_str):
            try:
                return datetime.strptime(date_str.strip(), "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")
            except Exception as e:
                logger.error(f"Date parsing error: {date_str} -> {e}")
                return pd.NA
        self.df['date_posted'] = self.df['date_posted'].apply(parse)

    def _extract_salary_columns(self):
        salary_cols = self.df['salary'].apply(self.parse_salary)
        self.df[['avg_salary', 'min_salary', 'max_salary', 'currency']] = pd.DataFrame(salary_cols.tolist(), index=self.df.index)

    def _clean_job_title(self, title):
        title = title.lower()
        noise_keywords = [
            'malaysia', 'kuala lumpur', 'selangor', 'pahang', 'johor', 'penang', 'based',
            'relocation', 'provided', 'remote', 'onsite', 'contract', 'internship', 'months',
            'year', 'full time', 'part time', 'temp', 'permanent', 'staff', 'job', 'post'
        ]
        for kw in noise_keywords:
            title = re.sub(rf'\b{re.escape(kw)}\b', '', title)
        title = re.sub(r'\s+', ' ', title)
        title = re.sub(r'[^\w\s/.-]', '', title)
        title = re.sub(r'\b(and|or|with|in|of|the|to|for|by|on|at|from|as|is|are|be|an|a)\b', '', title)
        return title.strip()

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

    def _fill_na(self):
        self.df.replace('', pd.NA, inplace=True)
        self.df.fillna(pd.NA, inplace=True)

    def transform(self):
        logger.info("Transforming JobStreet MY DataFrame")
        self._parse_date()
        self._extract_salary_columns()
        self._fill_na()
        self._enrich_with_title_features()
        logger.info("Transformation JobStreet MY complete")
        # self.df.to_csv("output/jobstreet_transform.csv", index=False)
        
        return self.df.reindex(columns=expected_columns)
