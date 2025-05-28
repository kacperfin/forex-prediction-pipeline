import random
import time

import requests
from datetime import datetime

from producers.base import BaseProducer


class CNNProducer(BaseProducer):
    def __init__(self):
        super().__init__(interval_in_sec=24 * 60 * 60)
        # User agent to mimic a browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.cnn.com/markets/fear-and-greed',
            'Origin': 'https://www.cnn.com'
        }

    def fetch(self, subcategory=None) -> dict or None:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"

        try:
            response = requests.get(url, headers=self.headers, timeout=10)

            if response.status_code == 200:
                data = response.json()

                # Format for Kafka
                fear_greed_data = {
                    'timestamp': datetime.now().isoformat(),
                    'score': data.get('fear_and_greed', {}).get('score'),
                    'classification': data.get('fear_and_greed', {}).get('rating'),
                    'previous_close': data.get('fear_and_greed', {}).get('previous_close'),
                    'week_ago': data.get('fear_and_greed', {}).get('previous_1_week'),
                    'month_ago': data.get('fear_and_greed', {}).get('previous_1_month')
                }
                print(f"Fetched CNN Fear & Greed data: {fear_greed_data['score']} ({fear_greed_data['classification']})")
                return fear_greed_data
            else:
                raise Exception(f"HTTP error | {response.status_code}")
        except Exception as e:
            print(f"Error fetching CNN Fear & Greed data: {e}")
            return None
