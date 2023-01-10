import pandas as pd
import config

from kafka import KafkaProducer
from pathlib import Path  

def send_to_kafka(topic, key, file_path):
    producer = KafkaProducer(bootstrap_servers=config.AWS_MSK_BS)
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        producer.send(topic, key=key, value=row.to_json())
    producer.flush()


if __name__ == '__main__':
    topic, key = 'ct_studies', 'ct_studies_key'
    # filepath = Path(f'data/study_fields_{datetime.now()}.csv')  
    file_path = Path(f'data/study_fields_2023-01-10 11:37:28.363179.csv')  
    file_path.parent.mkdir(parents=True, exist_ok=True)  
    send_to_kafka(topic, key, file_path)