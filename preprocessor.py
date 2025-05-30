import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from textblob import TextBlob
import warnings

warnings.filterwarnings('ignore')


class Preprocessor:
    def load_and_preprocess_data(self, eurpln_file, reddit_file=None, fear_greed_file=None):
        """
        Ładowanie i preprocessing wszystkich źródeł danych
        """
        print("Ładowanie danych EUR/PLN...")
        # Ładowanie danych forex
        df_forex = pd.read_csv(eurpln_file)
        df_forex['Datetime'] = pd.to_datetime(df_forex['Datetime'])
        df_forex.set_index('Datetime', inplace=True)
        df_forex.sort_index(inplace=True)

        # Podstawowe cechy techniczne
        df_forex = self._add_technical_features(df_forex)

        # Dodanie danych Reddit
        if reddit_file:
            print("Przetwarzanie danych Reddit...")
            reddit_sentiment = self._process_reddit_data(reddit_file)
            df_forex = self._merge_reddit_sentiment(df_forex, reddit_sentiment)

        # Dodanie Fear & Greed Index
        if fear_greed_file:
            print("Dodawanie Fear & Greed Index...")
            df_forex = self._merge_fear_greed(df_forex, fear_greed_file)

        df_forex.fillna(method='ffill', inplace=True)
        df_forex.fillna(method='bfill', inplace=True)

        return df_forex

    def _add_technical_features(self, df):
        """
        Dodawanie wskaźników technicznych
        """
        # Podstawowe cechy cenowe
        df['price_change'] = df['Close'].pct_change().fillna(0)
        df['price_change_abs'] = df['price_change'].abs()
        df['high_low_ratio'] = df['High'] / df['Low']
        df['close_open_ratio'] = df['Close'] / df['Open']

        hours_per_day = 24

        # SMA
        sma_periods_days = [5, 10, 25]
        for period_days in sma_periods_days:
            period_hours = period_days * hours_per_day
            df[f'SMA_{period_days}D'] = df['Close'].rolling(window=period_hours).mean().fillna(0)

        # RSI
        rsi_window_days = 14
        rsi_window_hours = rsi_window_days * hours_per_day
        delta = df['Close'].diff(1)

        gain = (delta.where(delta > 0, 0)).rolling(window=rsi_window_hours).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=rsi_window_hours).mean()

        rs = gain / loss
        df[f'RSI_{rsi_window_days}D'] = 100 - (100 / (1 + rs))
        df[f'RSI_{rsi_window_days}D'].fillna(0, inplace=True)

        # Wstęgi Bollingera
        bb_periods_days = 20
        bb_periods_hours = bb_periods_days * hours_per_day
        bb_num_std_dev = 2

        df[f'BB_Middle_{bb_periods_days}D'] = df['Close'].rolling(window=bb_periods_hours).mean()
        std_dev = df['Close'].rolling(window=bb_periods_hours).std()

        df[f'BB_Upper_{bb_periods_days}D'] = df[f'BB_Middle_{bb_periods_days}D'] + (std_dev * bb_num_std_dev)
        df[f'BB_Lower_{bb_periods_days}D'] = df[f'BB_Middle_{bb_periods_days}D'] - (std_dev * bb_num_std_dev)

        df[f'BB_Middle_{bb_periods_days}D'].fillna(0, inplace=True)
        df[f'BB_Upper_{bb_periods_days}D'].fillna(0, inplace=True)
        df[f'BB_Lower_{bb_periods_days}D'].fillna(0, inplace=True)

        # Cechy czasowe
        df['hour'] = df.index.hour
        df['day_of_week'] = df.index.dayofweek
        df['is_weekend'] = (df.index.dayofweek >= 5).astype(int)

        return df

    def _process_reddit_data(self, reddit_file):
        """
        Przetwarzanie danych Reddit do analizy sentymentu
        """
        df_reddit = pd.read_csv(reddit_file)
        df_reddit['created_utc'] = pd.to_datetime(df_reddit['created_utc'])

        # Analiza sentymentu
        def get_sentiment(text):
            if pd.isna(text):
                return 0
            try:
                blob = TextBlob(str(text))
                return blob.sentiment.polarity
            except:
                return 0

        df_reddit['title_sentiment'] = df_reddit['title'].apply(get_sentiment)

        # Agregacja sentymentów na interwały 1h
        df_reddit['datetime_1h'] = df_reddit['created_utc'].dt.floor('1H')

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

    def _merge_reddit_sentiment(self, df_forex, reddit_sentiment):
        """
        Łączenie danych forex z sentymentem Reddit
        """
        reddit_sentiment['datetime'] = pd.to_datetime(reddit_sentiment['datetime'])
        reddit_sentiment.set_index('datetime', inplace=True)

        df_forex.index = df_forex.index.tz_localize(None)
        reddit_sentiment.index = reddit_sentiment.index.tz_localize(None)

        reddit_sentiment.columns = ['reddit_' + col for col in reddit_sentiment.columns]
        df_forex = df_forex.merge(reddit_sentiment, left_index=True, right_index=True, how='left')
        return df_forex

    def _merge_fear_greed(self, df_forex, fear_greed_file):
        """
        Łączenie z Fear & Greed Index
        """
        fg_df = pd.read_csv(fear_greed_file)
        fg_df['datetime'] = pd.to_datetime(fg_df['datetime'])

        hours = range(24)

        rows = []
        for _, row in fg_df.iterrows():
            date = row['datetime'].date()
            for h in hours:
                new_datetime = pd.Timestamp(year=date.year, month=date.month, day=date.day, hour=h)
                new_row = row.copy()
                new_row['datetime'] = new_datetime
                rows.append(new_row)

        fg_expanded = pd.DataFrame(rows)
        fg_expanded.set_index('datetime', inplace=True)

        df_forex.index = df_forex.index.tz_localize(None)
        fg_expanded.index = fg_expanded.index.tz_localize(None)

        df_forex = df_forex.merge(fg_expanded, left_index=True, right_index=True, how='left')
        return df_forex


def main():
    """
    Główny pipeline trenowania modelu
    """
    preprocessor = Preprocessor()

    # Ładowanie danych
    df = preprocessor.load_and_preprocess_data(
        eurpln_file='EURPLNX_1h.csv',
        reddit_file='reddit_posts.csv',
        fear_greed_file='fear_and_greed_historical.csv'
    )

    print(f"Załadowano {len(df)} wierszy danych.")

    df.to_csv("data.csv")

    return preprocessor


if __name__ == "__main__":
    preprocessor = main()