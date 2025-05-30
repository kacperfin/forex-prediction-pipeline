import requests
import csv
from datetime import datetime as dt

url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/2024-11-29"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

response = requests.get(url, headers=headers)

if response.status_code == 200 and response.text.strip():
    data = response.json()
    historical_data = data.get("fear_and_greed_historical", {}).get("data", [])

    with open("fear_and_greed_historical11.csv", mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["datetime", "fear_greed"])

        for entry in historical_data:
            datetime = dt.utcfromtimestamp(entry["x"] / 1000).strftime('%Y-%m-%d')
            fear_greed = entry["y"]
            writer.writerow([datetime, fear_greed])

    print("Zapisano dane historyczne do pliku fear_and_greed_historical.csv.")
else:
    print(f"Błąd: status {response.status_code}")
    print(response.text[:500])
