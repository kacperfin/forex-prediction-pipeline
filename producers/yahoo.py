import json
from datetime import datetime, timedelta

import yfinance as yf

from producers.base import BaseProducer

tickers = ['EURPLN=X', 'USDPLN=X', 'GBPPLN=X']


class YahooProducer(BaseProducer):
    def __init__(self):
        super().__init__(interval_in_sec=4 * 60 * 60)

    def fetch(self, subcategory: str = None) -> dict or None:
        ticker_symbol = subcategory
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        data = yf.download(
            ticker_symbol,
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d'),
            interval=f'{int(self.interval / 60 / 60)}h'
        )

        # Reset index to make sure Datetime becomes a column
        data = data.reset_index()

        # Convert to JSON string and back to dictionary to ensure serializable data
        json_data = data.to_json(orient='records', date_format='iso')
        records = json.loads(json_data)

        # Format for Kafka
        data_dict = {
            'timestamp': datetime.now().isoformat(),
            'ticker': ticker_symbol,
            'data': records
        }

        return data_dict