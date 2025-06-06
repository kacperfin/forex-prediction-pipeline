from producers.base import BaseProducer
from datetime import datetime


class DataProcessorProducer(BaseProducer):
    def __init__(self):
        # Run every hour
        super().__init__(interval_in_sec=60 * 60)

    def fetch(self, subcategory=None) -> dict:
        """Just create a trigger message with timestamp"""
        return {
            'timestamp': datetime.now().isoformat(),
            'message': 'Process data now'
        }