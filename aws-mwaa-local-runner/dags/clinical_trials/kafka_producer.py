from kafka import KafkaProducer

import clinical_trials.config as config
from clinical_trials.clinical_trials import Extract
from clinical_trials.clinical_trials import Load
from clinical_trials.clinical_trials import transform_study_fields


class CSVToKafkaOperator:
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

        producer = KafkaProducer(bootstrap_servers=self.kafka_conn_id)
        
        with open(self.csv_file, 'r') as file:
            # Read the CSV file line by line
            for line in file:
                # Convert the line to a byte array and send it as a message to the specified Kafka topic
                producer.send(self.topic, line.encode())

        # Flush and close the Kafka producer
        producer.flush()
        producer.close()


def send_to_kafka():
    topic, key = 'ct_studies', 'ct_studies_key'
    csv_file = f'data/kafka/study_fields'
    # extract
    ct_extract = Extract(min_rank=1, max_rank=1)
    data = ct_extract.get_study_fields()

    # transform
    df = transform_study_fields(data)

    # load
    ct_load = Load()
    ct_load.write_study_fields(df, csv_file)

    # file_path = Path(f'data/study_fields_2023-01-30 07:05:02.703708.csv')  
    # file_path.parent.mkdir(parents=True, exist_ok=True)  
    kafka_conn_id = config.BS
    op = CSVToKafkaOperator(kafka_conn_id, topic, csv_file)
    op.execute()

if __name__ == '__main__':
    send_to_kafka()

