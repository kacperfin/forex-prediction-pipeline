import os

import pandas as pd
import pymongo
from dotenv import load_dotenv


class MongoClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoClient, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            usr, passwd = self.get_credentials()
            self.client = pymongo.MongoClient(f"mongodb://{usr}:{passwd}@localhost:27017/")
            self.client.server_info()
            self.db = self.client['forex_prediction']
            self._initialized = True

    @staticmethod
    def get_credentials() -> tuple[str, str]:
        dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '.env'))
        load_dotenv(dotenv_path=dotenv_path)

        username = os.getenv('MONGO_USERNAME')
        password = os.getenv('MONGO_PASSWORD')

        return username, password

    def get_collection(self, collection_name: str):
        return self.db[collection_name]

    def print_all_data(self, limit: int = 5):
        collection_names = self.db.list_collection_names()
        print(f"\nFound {len(collection_names)} collections: {', '.join(collection_names)}")

        for collection_name in collection_names:
            collection = self.db[collection_name]
            count = collection.count_documents({})
            print(f"\n{'=' * 50}")
            print(f"Collection: {collection_name} ({count} documents)")
            print(f"{'=' * 50}")

            for i, doc in enumerate(collection.find().limit(limit)):
                print(f"\nDocument #{i + 1}:")
                print(doc)

            if count > limit:
                print(f"\n... and {count - limit} more documents")

    def drop_database(self):
        db_name = 'forex_prediction'

        self.client.drop_database(db_name)
        print(f"Database '{db_name}' has been dropped")
        print("Remaining databases:", self.client.list_database_names())

        self.db = self.client[db_name]

    def get_processed_data(self) -> pd.DataFrame:
        processed_collection = self.get_collection('processed_data')

        cursor = processed_collection.find({})
        df = pd.DataFrame(list(cursor))

        if '_id' in df.columns:
            df.drop('_id', axis=1, inplace=True)

        if 'Datetime' in df.columns:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            df.set_index('Datetime', inplace=True)

        return df
