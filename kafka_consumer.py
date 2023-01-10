import config
import json
import pandas as pd

from datetime import datetime
from kafka import KafkaConsumer
from pathlib import Path  

def extract_from_kafka(topic, file_path):
  consumer = KafkaConsumer(topic, bootstrap_servers=config.AWS_MSK_BS)

  df = pd.DataFrame(columns=['NCTId','Condition','BriefTitle'])

  for message in consumer:
      json_data = json.loads(message.value)
      df = df.append(json_data, ignore_index=True)

  df.to_csv(file_path, index=False)


if __name__ == '__main__':
  topic, key = 'ct_studies'
  # csv_filename_base = f'kafka_study_fields_{datetime.now()}.csv'
  file_path = Path(f'data/kafka_study_fields_{datetime.now()}.csv')  
  file_path.parent.mkdir(parents=True, exist_ok=True)  
  extract_from_kafka(topic, file_path)