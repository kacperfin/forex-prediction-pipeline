import threading
from typing import List

from consumers.cnn import CNNConsumer
from consumers.yahoo import YahooConsumer
from consumers.reddit import RedditConsumer

# List of all available consumer classes
CONSUMERS = [CNNConsumer, YahooConsumer, RedditConsumer]


def run_consumer(consumer_class, barrier: threading.Barrier = None):
    consumer = consumer_class()
    try:
        print(f"Starting {consumer_class.__name__}...")
        if barrier:
            barrier.wait()
        consumer.consume()
    except Exception as e:
        print(f"Error in {consumer_class.__name__}: {e}")


def run_all_consumers(barrier: threading.Barrier = None) -> List[threading.Thread]:
    threads = []

    for consumer_class in CONSUMERS:
        thread = threading.Thread(
            target=run_consumer,
            args=(consumer_class, barrier),
            name=f"{consumer_class.__name__}Thread",
            daemon=True
        )
        thread.start()
        threads.append(thread)

    return threads


if __name__ == "__main__":
    print("Starting all consumers in parallel")
    consumer_threads = run_all_consumers()

    try:
        for t in consumer_threads:
            t.join()
    except KeyboardInterrupt:
        print("Shutting down consumers")