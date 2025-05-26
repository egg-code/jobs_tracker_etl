import pandas as pd
import json, re
from datetime import datetime
import logging

## Set up logging

logfile_name = datetime.now().strftime('jobstreetmalay_t_%Y%m%d_%H%M%S.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logfile_name)
fomatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(fomatter)
logger.addHandler(handler)

expected_columns = ['title', 'category', 'company', 'location', 'country', 'min_salary', 'max_salary', 'avg_salary', 'currency', 'job_type', 'work_arrangement', 'level', 'date_posted', 'job_link', 'source']

class JobStreetMalayTransform:
    def __init__(self, df:pd.DataFrame):
        self.df = df
        
    def _fill_na(self):
        self.df = self.df.replace('', pd.NA)

    def transform(self):
        logger.info("Transforming JobsDBTH DataFrame")
        self._fill_na()
        logger.info("Transformation JobsDBTH complete")
        return self.df.reindex(columns=expected_columns)