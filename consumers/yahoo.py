from consumers.base import BaseConsumer


class YahooConsumer(BaseConsumer):
    def __init__(self):
        super().__init__()
        self.consumer.subscribe(pattern='yahoo.*')
        self.collection = self.mongo_client.get_collection('yahoo')

    def process(self, data: dict):
        ticker = data['ticker']
        ticker_data = data['data']

        for item in ticker_data:
            item['ticker'] = ticker

        if ticker_data:
            self.collection.insert_many(ticker_data)
            print(f"Processed Yahoo Finance data for {ticker}")
