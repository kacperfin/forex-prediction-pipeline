import threading
from typing import List

from producers.cnn import CNNProducer
from producers.yahoo import YahooProducer, tickers
from producers.reddit import RedditProducer, SUBREDDITS
from producers.processor import DataProcessorProducer

# Producer configurations
PRODUCERS = [
    {"class": CNNProducer, "topic": "cnn", "subcategories": None},
    {"class": YahooProducer, "topic": "yahoo", "subcategories": tickers},
    {"class": RedditProducer, "topic": "reddit", "subcategories": SUBREDDITS},
    {"class": DataProcessorProducer, "topic": "data_processor", "subcategories": None},
]


def run_producer(producer_class, topic, subcategories=None, barrier: threading.Barrier = None):
    try:
        producer = producer_class()
        print(f"Starting {producer_class.__name__}...")
        if barrier and producer_class.__name__ != "DataProcessorProducer":
            barrier.wait()
        producer.run(topic=topic, subcategories=subcategories)
    except Exception as e:
        print(f"Error in {producer_class.__name__}: {e}")


def run_all_producers(barrier: threading.Barrier = None) -> List[threading.Thread]:
    threads = []

    for producer_config in PRODUCERS:
        # Skip DataProcessorProducer for now
        if producer_config["class"].__name__ == "DataProcessorProducer":
            continue

        thread = threading.Thread(
            target=run_producer,
            args=(producer_config["class"], producer_config["topic"], producer_config["subcategories"], barrier),
            name=f"{producer_config['class'].__name__}Thread",
            daemon=True
        )
        thread.start()
        threads.append(thread)
        print(f"Started {thread.name}")

    # Start DataProcessorProducer with delay
    for producer_config in PRODUCERS:
        if producer_config["class"].__name__ == "DataProcessorProducer":
            print(f"Scheduling {producer_config['class'].__name__} to start in 15 seconds...")

            # Create Timer without name parameter
            delayed_thread = threading.Timer(
                60.0,
                run_producer,
                args=(producer_config["class"], producer_config["topic"],
                      producer_config["subcategories"], barrier)
            )
            # Set name after creation
            delayed_thread.name = f"{producer_config['class'].__name__}Thread"
            delayed_thread.daemon = True
            delayed_thread.start()
            threads.append(delayed_thread)
            print(f"Scheduled {delayed_thread.name}")

    return threads


if __name__ == "__main__":
    print("Starting all producers in parallel")
    producer_threads = run_all_producers()

    try:
        for t in producer_threads:
            t.join()
    except KeyboardInterrupt:
        print("Shutting down producers")