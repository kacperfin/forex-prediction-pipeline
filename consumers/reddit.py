import pandas as pd

from consumers.base import BaseConsumer


class RedditConsumer(BaseConsumer):
    def __init__(self):
        super().__init__()
        self.consumer.subscribe(pattern='reddit.*')
        self.collection = self.mongo_client.get_collection('reddit')

    def process(self, data: dict):
        timestamp = data['timestamp']
        subreddit = data['subreddit']
        posts = data['posts']

        for post in posts:
            post['collection_timestamp'] = timestamp
            post['created_utc'] = pd.to_datetime(post['created_utc'], unit='s')

        if posts:
            self.collection.insert_many(posts)
            print(f"Processed and saved Reddit data from r/{subreddit} with {len(posts)} posts")
