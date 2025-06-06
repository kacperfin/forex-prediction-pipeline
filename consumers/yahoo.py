from consumers.base import BaseConsumer


class YahooConsumer(BaseConsumer):
    def __init__(self):
        super().__init__()
        self.consumer.subscribe(pattern='yahoo.*')
        self.collection = self.mongo_client.get_collection('yahoo')

    def process(self, data: dict):
        tickers = data.get('tickers', [data.get('ticker')])
        ticker_data = data['data']

        if ticker_data:
            # Get datetime values to check for duplicates
            datetimes = []
            for record in ticker_data:
                for key in record:
                    if 'Datetime' in key:
                        datetimes.append(record[key])
                        break

            if datetimes:
                # Remove any existing records for the same timeframes
                self.collection.delete_many({"Datetime": {"$in": datetimes}})

            self.collection.insert_many(ticker_data)
            print(f"Processed Yahoo Finance data for {', '.join(tickers)}")
