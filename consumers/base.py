import json
from abc import ABC, abstractmethod

from kafka import KafkaConsumer
from mongo import MongoClient

class BaseConsumer(ABC):
    def __init__(self, topic: str = None):
        self.consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        if topic: self.consumer.subscribe([topic])

        self.mongo_client = MongoClient()
        self.db = self.mongo_client.db

    def consume(self):
        for message in self.consumer:
            try:
                data = message.value
                topic = message.topic
                self.process(data)
                print(f"Consumed data from: {topic}")
            except Exception as e:
                print(f"Error processing data: {e}")

    @abstractmethod
    def process(self, data: dict):
        pass