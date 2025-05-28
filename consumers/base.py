import json
from abc import ABC, abstractmethod

from kafka import KafkaConsumer


class BaseConsumer(ABC):
    def __init__(self, topic: str = None):
        self.consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        if topic: self.consumer.subscribe([topic])

    @abstractmethod
    def consume(self):
        pass

    @abstractmethod
    def process(self, data: dict):
        pass