from consumers.base import BaseConsumer


class CNNConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(topic='cnn')
        self.collection = self.mongo_client.get_collection('cnn')

    def process(self, data: dict):
        self.collection.insert_one({
            'timestamp': data['timestamp'],
            'fear_greed': data['fear_greed'],
            # 'classification': data['classification'],
            # 'previous_close': data['previous_close'],
            # 'week_ago': data['week_ago'],
            # 'month_ago': data['month_ago']
        })

        print(f"Processed Fear & Greed data: {data['fear_greed']}")
