import os
from datetime import datetime

import praw
from dotenv import load_dotenv

from producers.base import BaseProducer

SUBREDDITS = [
    'WallStreetBets',
    'Macroeconomics',
    'Economics',
    'PersonalFinance',
    'EUpersonalfinance',
    'UKPersonalFinance',
    'Poland',
    'Polska',
    'Europe',
    'EuropeanUnion',
    'UKPolitics',
    'Geopolitics',
    'Energy',
    'MonetaryPolicy',
    'AskEconomics'
]

class RedditProducer(BaseProducer):
    def __init__(self):
        super().__init__(interval_in_sec=60 * 60)
        self.reddit = self.init_reddit()
        self.limit = 1000

    def fetch(self, subcategory: str) -> dict or None:
        subreddit_name = subcategory
        try:
            subreddit = self.reddit.subreddit(subreddit_name)

            posts_data = []
            for post in subreddit.hot(limit=self.limit):
                post_data = {
                    'id': post.id,
                    'title': post.title,
                    'score': post.score,
                    'upvote_ratio': post.upvote_ratio,
                    'url': post.url,
                    'created_utc': post.created_utc,
                    'num_comments': post.num_comments,
                    'subreddit': subreddit_name
                }
                posts_data.append(post_data)

            # Format for Kafka
            reddit_data = {
                'timestamp': datetime.now().isoformat(),
                'subreddit': subreddit_name,
                'posts': posts_data
            }

            return reddit_data
        except Exception as e:
            print(f"Error fetching data from r/{subreddit_name}: {e}")
            return None

    @staticmethod
    def init_reddit() -> praw.Reddit:
        # Calculate and display the absolute path of .env file
        dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../.env'))
        if not os.path.exists(dotenv_path):
            raise FileNotFoundError(f".env file not found!")

        load_dotenv(dotenv_path=dotenv_path)

        # Reddit API setup
        return praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT'),
        )