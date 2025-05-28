import os

import pandas as pd

from consumers.base import BaseConsumer


class YahooConsumer(BaseConsumer):
    def __init__(self):
        super().__init__()
        self.consumer.subscribe(pattern='yahoo.*')

    def process(self, data: dict):
        ticker = data['ticker']
        df = pd.DataFrame(data['data'])

        # Convert ISO format strings to datetime objects
        if 'Datetime' in df.columns:
            df['Datetime'] = pd.to_datetime(df['Datetime'])

        # Create output directory if it doesn't exist
        os.makedirs('data/yahoo', exist_ok=True)

        # Save to CSV
        output_file = f"data/yahoo/{ticker.replace('=', '')}.csv"
        df.to_csv(output_file, index=False)
        print(f"Processed Yahoo Finance data for {ticker}")

    def consume(self):
        for message in self.consumer:
            try:
                topic = message.topic
                data = message.value
                self.process(data)
                print(f"Consumed Yahoo Finance data from topic: {topic}")
            except Exception as e:
                print(f"Error processing Yahoo Finance data: {e}")