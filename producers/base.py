import time
from abc import ABC, abstractmethod
import json
from kafka import KafkaProducer

class BaseProducer(ABC):
    def __init__(self, interval_in_sec: int = 4 * 60 * 60):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.interval = interval_in_sec

    @abstractmethod
    def fetch(self, subcategory: str) -> dict or None:
        pass

    def produce(self, topic: str, subcategory: str = None):
        try:
            data = self.fetch(subcategory)
            if data:
                self.producer.send(topic, value=data)
                print(f"Sent '{topic} ({'r/' if topic == 'reddit' else ''}{subcategory if subcategory else topic})' data to Kafka!")
        except Exception as e:
            print(f"Error producing data to {topic}: {e}")

    def run(self, topic: str, subcategories: list = None):
        while True:
            if subcategories:
                for subcategory in subcategories:
                    self.produce(topic, subcategory)
            else:
                self.produce(topic)
            time.sleep(self.interval)