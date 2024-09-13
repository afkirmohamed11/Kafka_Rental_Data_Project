# kafka_producer.py

import os
import pandas as pd
from kafka import KafkaProducer
import psycopg2
from psycopg2 import sql
import json
from decimal import Decimal
from helpers.helpers import CodeHelper
from helpers.config import get_settings
# if you got this issue while importing from kafka: ModuleNotFoundError: No module named 'kafka.vendor.six.moves'
# try this code:
import sys, types
m = types.ModuleType('kafka.vendor.six.moves', 'Mock module')
setattr(m, 'range', range)
sys.modules['kafka.vendor.six.moves'] = m



class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)
    
class DataFetcher:
    def __init__(self, host, database, user, password, schema_name, table_name, kafka_bootstrap_servers, kafka_topic, log_file_path):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.schema_name = schema_name
        self.table_name = table_name
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.logger = CodeHelper(log_file_path)
        self.logger.log_info("DataFetcher initialized")

    def fetch_data(self):
        conn_params = {
            "host": self.host,
            "database": self.database,
            "user": self.user,
            "password": self.password
        }
        
        try:
            conn = psycopg2.connect(**conn_params)
            cur = conn.cursor()

            query = sql.SQL("SELECT * FROM {}.{}").format(
                sql.Identifier(self.schema_name),
                sql.Identifier(self.table_name)
            )
            cur.execute(query)

            rows = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]

            df = pd.DataFrame(rows, columns=column_names)
            records = df.to_dict(orient="records")

            self.logger.log_info(f"Fetched {len(records)} records from the database")
            return records

        except (Exception, psycopg2.Error) as error:
            self.logger.log_error(f"Error fetching data: {error}")
            return None

        finally:
            if conn:
                cur.close()
                conn.close()
                self.logger.log_info("PostgreSQL connection is closed")

    def send_data_to_kafka(self, records):
        if not records:
            self.logger.log_error("No records to send to Kafka")
            return

        try:
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, cls=DecimalEncoder).encode('utf-8')
            )

            for record in records:
                producer.send(self.kafka_topic, value=record)

            producer.flush()
            self.logger.log_info(f"All {len(records)} records sent to Kafka topic: {self.kafka_topic}")

        except Exception as error:
            self.logger.log_error(f"Error sending data to Kafka: {error}")

        finally:
            if 'producer' in locals():
                producer.close()
                self.logger.log_info("Kafka producer is closed")

    def fetch_and_send_data(self):
        records = self.fetch_data()
        if records:
            self.send_data_to_kafka(records)

def main():
    settings = get_settings()
    data_fetcher = DataFetcher(
        host = settings.DATABASE_HOST,
        database = settings.DATABASE_NAME,
        user = settings.DATABASE_USER,
        password = settings.DATABASE_PASSWORD,
        schema_name = settings.DATABASE_SCHEMA,
        table_name = settings.DATABASE_TABLE,
        kafka_bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic = settings.KAFKA_TOPIC,
        log_file_path = os.path.join(os.getcwd(), 'src/log.txt')
    )

    data_fetcher.fetch_and_send_data()

if __name__ == "__main__":
    main()