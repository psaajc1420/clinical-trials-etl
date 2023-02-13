import pandas as pd
from kafka import KafkaConsumer

import clinical_trials.config as config

class KafkaToCSVOperator:
    def __init__(self,
                 kafka_conn_id,
                 topic,
                 csv_file,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.kafka_conn_id = kafka_conn_id
        self.topic = topic
        self.csv_file = csv_file

    def execute(self):
      consumer = KafkaConsumer(
        self.topic,
        group_id='ct-studies-grp',
        bootstrap_servers=self.kafka_conn_id)

      records = [record.value for record in consumer]
      df = pd.DataFrame.from_records(records)
      print(df)

      df.to_csv(self.csv_file)



def extract_from_kafka():
  
  topic, key = 'ct_studies', 'ct_studies_key'
  csv_file = f'data/kafka/study_fields'
  kafka_conn_id = config.BS
  op = KafkaToCSVOperator(kafka_conn_id, topic, csv_file)
  op.execute()

if __name__ == '__main__':
  extract_from_kafka()