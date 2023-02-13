import requests
import pandas as pd

from pathlib import Path  

import clinical_trials.config as config
from clinical_trials.db import Database

class Extract():
  BASE_URL = 'http://ClinicalTrials.gov/api'
  def __init__(self, format='json', min_rank=1, max_rank=20):
    self.format = format
    self.min_rank = min_rank
    self.max_rank = max_rank

  def get_data_version(self):
    url = f'{self.BASE_URL}/info/data_vrs?fmt={self.format}'
    response = requests.get(url)
    return response.json()  

  def get_api_version(self):
    url = f'{self.BASE_URL}/info/api_vrs?fmt={self.format}'
    response = requests.get(url)
    return response.json()  

  def get_api_definitions(self):
    url = f'{self.BASE_URL}/info/api_defs?fmt={self.format}'
    response = requests.get(url)
    return response.json()

  def get_full_studies(self):
    url = f'{self.BASE_URL}/query/full_studies?fmt={self.format}'
    response = requests.get(url)
    return response.json()

  def get_study_fields(self):
    url = f"""
    {self.BASE_URL}/query/study_fields?fields=NCTId,Condition,BriefTitle&fmt={self.format}&
    min_rnk={self.min_rank}&max_rnk={self.max_rank}
    """
    response = requests.get(url)
    return response.json()



def transform_study_fields(json_dict):
  data = json_dict['StudyFieldsResponse']
  cols = data['FieldList']
  df = pd.json_normalize(data['StudyFields'])
  for col in cols:
    df = df.assign(**{col : df[col]}).explode(col)

  df = df.reset_index(drop=True)
  df = df.drop(columns=['Rank'])
  return df

class Load():
  def __init__(self):
    self.db = Database(config)
    
  
  def load_study_fields(self, df):
    table, schema = 'study', 'public'
    if self.db._check_table_exists(schema, table):
      self.db.insert_many(df, table)
    else:
      raise Exception(f'Table {schema}.{table} does not exist. Please check the pipeline process.')

  def write_study_fields(self, df, path):
    filepath = Path(path)  
    filepath.parent.mkdir(parents=True, exist_ok=True)  
    df.to_csv(filepath)  