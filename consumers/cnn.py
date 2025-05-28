import os

from consumers.base import BaseConsumer


class CNNConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(topic='cnn')

    def process(self, data: dict):
        # Create output directory if it doesn't exist
        os.makedirs('data/cnn', exist_ok=True)

        # Create or append to CSV
        output_file = "data/cnn/fear_and_greed.csv"

        # Create header if file doesn't exist
        if not os.path.exists(output_file):
            with open(output_file, "w") as f:
                f.write("timestamp,score,classification,previous_close,week_ago,month_ago\n")

        # Append data
        with open(output_file, "a") as f:
            f.write(
                f"{data['timestamp']},{data['score']},{data['classification']},{data['previous_close']},{data['week_ago']},{data['month_ago']}\n")

        print(f"Processed Fear & Greed data: {data['score']} ({data['classification']})")

    def consume(self):
        for message in self.consumer:
            try:
                self.process(message.value)
                print(f"Consumed Fear & Greed data (score: {message.value['score']})")
            except Exception as e:
                print(f"Error processing Fear & Greed data: {e}")