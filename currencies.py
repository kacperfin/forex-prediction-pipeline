import yfinance as yf
from datetime import datetime, timedelta

ticker_symbol = 'EURPLN=X'

end_date = datetime.now()
start_date = end_date - timedelta(days=30)

data = yf.download(
    ticker_symbol,
    start=start_date.strftime('%Y-%m-%d'),
    end=end_date.strftime('%Y-%m-%d'),
    interval='1h'
)

# Zapisz do pliku CSV
csv_filename = f"{ticker_symbol.replace('=','')}_4h_1.csv"
data.to_csv(csv_filename)

print(f"Dane zapisano do pliku: {csv_filename}")