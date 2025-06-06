from consumers.base import BaseConsumer
from preprocessor import Preprocessor
import pandas as pd
import time


class DataProcessorConsumer(BaseConsumer):
    def __init__(self):
        # Subscribe to a dedicated topic for processing triggers
        super().__init__(topic='data_processor')
        self.preprocessor = Preprocessor()
        self.processed_collection = self.mongo_client.get_collection('processed_data')

    def process(self, data: dict):
        """Process data when a message is received on the data_processor topic"""
        print("Running data processor triggered by Kafka message...")
        self.process_all_data()

    def process_all_data(self):
        """Process data from all collections and store in processed_data collection"""
        # Get data from collections
        forex_data = self._get_yahoo_data()
        reddit_data = self._get_reddit_data()
        fear_greed_data = self._get_fear_greed_data()

        if forex_data.empty:
            print("No forex data available for processing")
            return

        # Process data using preprocessor methods
        tickers = [
            'EURPLN=X',
            'USDPLN=X',
            'GBPPLN=X'
        ]
        for ticker in tickers:
            # Dodanie wskaźników technicznych dla każdego tickera
            forex_data = self.preprocessor._add_technical_features(forex_data, ticker)

        if not reddit_data.empty:
            reddit_sentiment = self._prepare_reddit_sentiment(reddit_data)
            forex_data = self.preprocessor._merge_reddit_sentiment(forex_data, reddit_sentiment)

        if not fear_greed_data.empty:
            forex_data = self._merge_fear_greed_direct(forex_data, fear_greed_data)

        # Fill missing values
        forex_data.fillna(method='ffill', inplace=True)
        forex_data.fillna(method='bfill', inplace=True)

        rename_list = [
            {"('Datetime', '')": "Datetime"},
            {"('Close', 'EURPLN=X')": 'EURPLN=X_Close'},
            {"('High', 'EURPLN=X')": 'EURPLN=X_High'},
            {"('Low', 'EURPLN=X')": 'EURPLN=X_Low'},
            {"('Open', 'EURPLN=X')": 'EURPLN=X_Open'},
            {"('Volume', 'EURPLN=X')": 'EURPLN=X_Volume'},
            {"('Close', 'USDPLN=X')": 'USDPLN=X_Close'},
            {"('High', 'USDPLN=X')": 'USDPLN=X_High'},
            {"('Low', 'USDPLN=X')": 'USDPLN=X_Low'},
            {"('Open', 'USDPLN=X')": 'USDPLN=X_Open'},
            {"('Volume', 'USDPLN=X')": 'USDPLN=X_Volume'},
            {"('Close', 'GBPPLN=X')": 'GBPPLN=X_Close'},
            {"('High', 'GBPPLN=X')": 'GBPPLN=X_High'},
            {"('Low', 'GBPPLN=X')": 'GBPPLN=X_Low'},
            {"('Open', 'GBPPLN=X')": 'GBPPLN=X_Open'},
            {"('Volume', 'GBPPLN=X')": 'GBPPLN=X_Volume'},
        ]

        for name in rename_list:
            forex_data.rename(columns=name, inplace=True)

        # Store processed data in MongoDB
        records = forex_data.reset_index().to_dict('records')
        if records:
            # Remove existing data for the same timeframe
            start_time = min(forex_data.index)
            end_time = max(forex_data.index)
            self.processed_collection.delete_many({
                'Datetime': {'$gte': start_time, '$lte': end_time}
            })

            # Insert new processed data
            self.processed_collection.insert_many(records)
            print(f"Processed and stored {len(records)} records in processed_data collection")

    def _get_yahoo_data(self):
        """Retrieve forex data from MongoDB and convert to DataFrame"""
        yahoo_collection = self.mongo_client.get_collection('yahoo')
        cursor = yahoo_collection.find({})
        df = pd.DataFrame(list(cursor))

        if df.empty:
            return pd.DataFrame()

        if '_id' in df.columns:
            df.drop('_id', axis=1, inplace=True)

        df["('Datetime', '')"] = pd.to_datetime(df["('Datetime', '')"])
        df.set_index("('Datetime', '')", inplace=True)
        df.sort_index(inplace=True)

        return df

    def _get_reddit_data(self):
        """Retrieve reddit data from MongoDB"""
        reddit_collection = self.mongo_client.get_collection('reddit')
        cursor = reddit_collection.find({})
        df = pd.DataFrame(list(cursor))

        if df.empty:
            return pd.DataFrame()

        if '_id' in df.columns:
            df.drop('_id', axis=1, inplace=True)

        return df

    def _prepare_reddit_sentiment(self, df_reddit):
        """Similar to _process_reddit_data but takes DataFrame directly"""
        # Apply sentiment analysis if not already done
        if 'title_sentiment' not in df_reddit.columns:
            from textblob import TextBlob

            def get_sentiment(text):
                if pd.isna(text):
                    return 0
                try:
                    blob = TextBlob(str(text))
                    return blob.sentiment.polarity
                except:
                    return 0

            df_reddit['title_sentiment'] = df_reddit['title'].apply(get_sentiment)

        # Create datetime column if it doesn't exist
        if 'datetime_1h' not in df_reddit.columns:
            df_reddit['datetime_1h'] = pd.to_datetime(df_reddit['created_utc']).dt.floor('1H')

        # Aggregate data
        sentiment_agg = df_reddit.groupby('datetime_1h').agg({
            'title_sentiment': ['mean', 'std', 'count'],
            'score': ['mean', 'sum', 'max'],
            'num_comments': ['mean', 'sum'],
            'upvote_ratio': 'mean'
        }).reset_index()

        sentiment_agg.columns = ['_'.join(col).strip() if col[1] else col[0]
                                 for col in sentiment_agg.columns.values]
        sentiment_agg.rename(columns={'datetime_1h': 'datetime'}, inplace=True)

        return sentiment_agg

    def _get_fear_greed_data(self):
        """Retrieve fear & greed data from CSV file"""
        try:
            cnn_collection = self.mongo_client.get_collection('cnn')
            cursor = cnn_collection.find({})
            df = pd.DataFrame(list(cursor))

            if df.empty:
                print("Fear and greed CSV file is empty")
                return pd.DataFrame()

            # Convert datetime column to proper datetime format
            df['datetime'] = pd.to_datetime(df['timestamp'])

            return df
        except Exception as e:
            print(f"Error reading fear and greed data: {e}")
            return pd.DataFrame()

    def _merge_fear_greed_direct(self, df_forex, fg_df):
        """Merge fear & greed data with forex data"""
        if fg_df.empty:
            print("No fear and greed data to merge")
            return df_forex

        # Create a copy to avoid modifying the original
        fg_expanded = pd.DataFrame()

        # Expand daily data to hourly - each day's value applies to all hours of that day
        days = fg_df['datetime'].dt.date.unique()
        rows = []

        for day in days:
            # Get the fear_greed value for this day
            day_data = fg_df[fg_df['datetime'].dt.date == day].iloc[0]
            fear_greed_value = day_data['fear_greed']

            # Create a row for each hour of the day
            for hour in range(24):
                timestamp = pd.Timestamp(year=day.year, month=day.month, day=day.day, hour=hour)
                rows.append({'timestamp': timestamp, 'fear_greed': fear_greed_value})

        fg_expanded = pd.DataFrame(rows)
        fg_expanded.set_index('timestamp', inplace=True)

        # Make sure both dataframes have compatible timezone settings
        df_forex.index = df_forex.index.tz_localize(None)
        fg_expanded.index = fg_expanded.index.tz_localize(None)

        # Merge the dataframes on their indices
        df_merged = df_forex.merge(fg_expanded, left_index=True, right_index=True, how='left')
        return df_merged

    def consume(self, processing_interval=3600):
        """Run the processor periodically"""
        while True:
            print("Running data processor...")
            self.process_all_data()
            time.sleep(processing_interval)
