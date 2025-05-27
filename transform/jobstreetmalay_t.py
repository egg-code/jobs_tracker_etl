import pandas as pd
import json, re
from datetime import datetime
import logging

## Set up logging
from utils.logger import get_module_logger
logger = get_module_logger(__name__, group='transform')

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