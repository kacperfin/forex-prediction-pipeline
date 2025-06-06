import time
import threading

from kafka.errors import NoBrokersAvailable

from consumers import CONSUMERS, run_all_consumers
from producers import PRODUCERS, run_all_producers


def main():
    print("Starting data pipeline...")
    total_threads = len(PRODUCERS) + len(CONSUMERS)

    # Include main thread, exclude DataProcessorProducer and DataProcessorConsumer
    startup_barrier = threading.Barrier(total_threads + 1 - 2)

    try:
        print("Starting all producers in parallel...")
        producer_threads = run_all_producers(startup_barrier)

        print("Starting all consumers in parallel...")
        consumer_threads = run_all_consumers(startup_barrier)

        print("Waiting for all threads to initialize...")
        startup_barrier.wait()
        print("All threads initialized successfully! Processing data...")
    except NoBrokersAvailable as e:
        print(f"Kafka brokers not available, did you launch Docker containers?\n{e}")
        return
    except Exception as e:
        print(f"Error during startup: {e}")
        return

    all_threads = producer_threads + consumer_threads
    try:
        while True:
            active_count = sum(1 for t in all_threads if t.is_alive())
            print(f"Running {active_count}/{len(all_threads)} threads. Press Ctrl+C to exit.")
            time.sleep(60)
    except KeyboardInterrupt:
        print("Shutting down data pipeline...")


if __name__ == "__main__":
    main()
