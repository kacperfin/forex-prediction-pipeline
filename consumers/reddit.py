import os

import pandas as pd

from consumers.base import BaseConsumer


class RedditConsumer(BaseConsumer):
    def __init__(self):
        super().__init__()
        self.consumer.subscribe(pattern='reddit.*')

    def process(self, data: dict):
        timestamp = data['timestamp']
        subreddit = data['subreddit']
        posts = data['posts']

        # Create DataFrame from posts
        df = pd.DataFrame(posts)

        # Convert UNIX timestamp to datetime
        df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')

        # Add collection timestamp
        df['collection_timestamp'] = timestamp

        # Create output directory if it doesn't exist
        os.makedirs('data/reddit', exist_ok=True)

        # Save to subreddit-specific file
        output_file = f"data/reddit/{subreddit.lower()}.csv"

        # Append or create file
        mode = 'a' if os.path.exists(output_file) else 'w'
        header = not os.path.exists(output_file)

        df.to_csv(output_file, mode=mode, header=header, index=False)
        print(f"Processed and saved Reddit data from r/{subreddit} with {len(posts)} posts")

    def consume(self):
        for message in self.consumer:
            try:
                topic = message.topic
                data = message.value
                self.process(data)
                print(f"Consumed Reddit data from: r/{data['subreddit']}")
            except Exception as e:
                print(f"Error processing Reddit data: {e}")
