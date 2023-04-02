
import config
import requests
import pandas as pd

from datetime import datetime
from glom import glom
from pathlib import Path  

from db import Database



class Extract():
  BASE_URL = 'http://ClinicalTrials.gov/api'
  def __init__(self, format='json'):
    self.format = format

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
    url = f'{self.BASE_URL}/query/study_fields?fields=NCTId,Condition,BriefTitle&fmt={self.format}'
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
    self.db.insert_many(df, 'study')

  def write_study_fields(self, df):
    filepath = Path(f'data/study_fields_{datetime.now()}.csv')  
    filepath.parent.mkdir(parents=True, exist_ok=True)  
    df.to_csv(filepath)  