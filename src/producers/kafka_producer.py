from kafka import KafkaProducer
import json
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

class RedditKafkaProducer:
    def __init__(self):
        load_dotenv()
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        self.topic = os.getenv('KAFKA_TOPIC')
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )

    def send_message(self, message):
        """
        Send a message to Kafka topic
        """
        try:
            future = self.producer.send(self.topic, message)
            self.producer.flush()  # Synchronous send
            future.get(timeout=60)  # Wait for message to be delivered
            self.logger.info(f"Message sent successfully to topic {self.topic}")
        except Exception as e:
            self.logger.error(f"Error sending message to Kafka: {str(e)}")

    def close(self):
        """
        Close the Kafka producer
        """
        self.producer.close() 