import json
import time
from datetime import datetime, timedelta

import yfinance as yf

from producers.base import BaseProducer


tickers = [
    'EURPLN=X',
    'USDPLN=X',
    'GBPPLN=X'
]

class YahooProducer(BaseProducer):
    def __init__(self):
        super().__init__(interval_in_sec=4 * 60 * 60)
        self.tickers = tickers

    def fetch(self, subcategory: str = None) -> dict:
        """Fetch data for all tickers at once"""
        end_date = datetime.now().replace(minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(hours=int(self.interval / 60 / 60))

        # Download all tickers at once
        data = yf.download(
            self.tickers,
            start=start_date,
            end=end_date,
            interval='1h'
        )

        # Reset index to make Datetime a column
        data = data.reset_index()

        # Convert to JSON string and back to dictionary
        json_data = data.to_json(orient='records', date_format='iso')
        records = json.loads(json_data)

        return {
            'timestamp': datetime.now().isoformat(),
            'tickers': self.tickers,
            'data': records
        }

    def run(self, topic: str, subcategories: list = None):
        """Override run to fetch all tickers in one go"""
        while True:
            # Don't iterate through subcategories, just produce once
            self.produce(topic)
            time.sleep(self.interval)