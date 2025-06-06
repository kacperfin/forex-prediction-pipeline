import threading
import traceback
from typing import List

from consumers.cnn import CNNConsumer
from consumers.processor import DataProcessorConsumer
from consumers.yahoo import YahooConsumer
from consumers.reddit import RedditConsumer

# List of all available consumer classes
CONSUMERS = [CNNConsumer, YahooConsumer, RedditConsumer, DataProcessorConsumer]


def run_consumer(consumer_class, barrier: threading.Barrier = None):
    consumer = consumer_class()
    try:
        print(f"Starting {consumer_class.__name__}...")
        if barrier and consumer_class.__name__ != "DataProcessorConsumer":
            barrier.wait()
        consumer.consume()
    except Exception as e:
        print(f"Error in {consumer_class.__name__}: {e}")
        traceback.print_exc()


def run_all_consumers(barrier: threading.Barrier = None) -> List[threading.Thread]:
    threads = []

    for consumer_class in CONSUMERS:
        # Skip DataProcessorConsumer for now
        if consumer_class.__name__ == "DataProcessorConsumer":
            continue

        thread = threading.Thread(
            target=run_consumer,
            args=(consumer_class, barrier),
            name=f"{consumer_class.__name__}Thread",
            daemon=True
        )
        thread.start()
        threads.append(thread)
        print(f"Started {thread.name}")

    # Start DataProcessorConsumer with delay
    for consumer_class in CONSUMERS:
        if consumer_class.__name__ == "DataProcessorConsumer":
            print(f"Scheduling {consumer_class.__name__} to start in 15 seconds...")

            # Create Timer for delayed start
            delayed_thread = threading.Timer(
                60.0,
                run_consumer,
                args=(consumer_class, barrier)
            )
            # Set name after creation
            delayed_thread.name = f"{consumer_class.__name__}Thread"
            delayed_thread.daemon = True
            delayed_thread.start()
            threads.append(delayed_thread)
            print(f"Scheduled {delayed_thread.name}")

    return threads


if __name__ == "__main__":
    print("Starting all consumers in parallel")
    consumer_threads = run_all_consumers()

    try:
        for t in consumer_threads:
            t.join()
    except KeyboardInterrupt:
        print("Shutting down consumers")
