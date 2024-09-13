# kafka_consumer.py

import os
import boto3
from s3fs import S3FileSystem
from kafka import KafkaConsumer
from json import loads
import sys, types, json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) 
from helpers.helpers import CodeHelper
from helpers.config import get_settings
# if you got this issue while importing from kafka: ModuleNotFoundError: No module named 'kafka.vendor.six.moves'
# try this code:
import sys, types
m = types.ModuleType('kafka.vendor.six.moves', 'Mock module')
setattr(m, 'range', range)
sys.modules['kafka.vendor.six.moves'] = m

class S3KafkaConsumer:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, s3_bucket,
                kafka_bootstrap_servers, kafka_topic, log_file_path):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.s3_bucket = s3_bucket
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.logger = CodeHelper(log_file_path)
        self.logger.log_info("S3KafkaConsumer initialized")

        # Create a session
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )

        # Create an S3 client
        self.s3_client = self.session.client('s3')

        # Create an S3FileSystem instance
        self.s3 = S3FileSystem(key=self.aws_access_key_id, secret=self.aws_secret_access_key, client_kwargs={'region_name': self.region_name})

        # Create Kafka consumer
        self.consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=[self.kafka_bootstrap_servers],  
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )

    def process_messages(self):
        self.logger.log_info(f"Starting to consume messages from {self.kafka_topic}")
        for count, message in enumerate(self.consumer):
            # try:
            s3_path = f"s3://{self.s3_bucket}/rental_data{count}.json"
            with self.s3.open(s3_path, 'w') as file:
                json.dump(message.value, file)
            self.logger.log_info(f"Saved message {count} to {s3_path}")
            # except Exception as e:
            #     self.logger.log_error(f"Error processing message {count}: {str(e)}")

    def run(self):
        try:
            self.process_messages()
        except Exception as e:
            self.logger.log_error(f"An error occurred: {str(e)}")
        finally:
            self.consumer.close()
            self.logger.log_info("Kafka consumer closed")

def main():
    settings = get_settings()
    log_file_path = os.path.join(os.getcwd(), 'src/log.txt')
    
    s3_kafka_consumer = S3KafkaConsumer(
        aws_access_key_id = settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY,
        region_name = settings.REGION_NAME,
        s3_bucket = settings.AWS_S3_BUCKET,
        kafka_bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic = settings.KAFKA_TOPIC,
        log_file_path = log_file_path
    )
    s3_kafka_consumer.run()

if __name__ == "__main__":
    main()