import requests

from producers.base import BaseProducer


class CNNProducer(BaseProducer):
    def __init__(self):
        super().__init__(interval_in_sec=4 * 60 * 60)
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
                    'timestamp': data.get('fear_and_greed', {}).get('timestamp'),
                    'fear_greed': data.get('fear_and_greed', {}).get('score'),
                }
                print(f"Fetched CNN Fear & Greed data: {fear_greed_data['fear_greed']}")
                return fear_greed_data
            else:
                raise Exception(f"HTTP error | {response.status_code}")
        except Exception as e:
            print(f"Error fetching CNN Fear & Greed data: {e}")
            return None
