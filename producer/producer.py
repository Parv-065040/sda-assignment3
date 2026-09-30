"""
producer.py

Assignment 2 — Streaming Data Analytics

Reads the sample JSON-lines files created by generate_sample_data.py and
streams each record to its corresponding Kafka topic, one event at a time,
with a short delay — so the flow of messages can be observed live in a
kafka-console-consumer window (for the required screenshot) rather than
dumping everything instantly.

Run generate_sample_data.py first, then make sure Zookeeper + Kafka are
running locally before running this script:

    # Terminal 1
    bin/zookeeper-server-start.sh config/zookeeper.properties

    # Terminal 2
    bin/kafka-server-start.sh config/server.properties

    # Terminal 3 (create topics once)
    bin/kafka-topics.sh --create --topic clickstream      --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    bin/kafka-topics.sh --create --topic cart-events      --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    bin/kafka-topics.sh --create --topic transactions     --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    bin/kafka-topics.sh --create --topic ad-clicks        --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

    # Terminal 4 (watch messages arrive -- this is your screenshot)
    bin/kafka-console-consumer.sh --topic clickstream --bootstrap-server localhost:9092 --from-beginning

    # Terminal 5
    python3 producer.py
"""

import json
import time
import logging
import os
import argparse
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError, KafkaConnectionError, KafkaError

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] producer: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.environ.get("LOG_DIR", "."), "producer.log")),
    ],
)
logger = logging.getLogger("producer")

SAMPLE_DIR = Path("sample_data")

# Maps each sample data file to the Kafka topic it should be streamed to
# (topic names match the pipeline design from Assignment 1)
SOURCE_TOPIC_MAP = {
    "clickstream.jsonl": "clickstream",
    "cart_events.jsonl": "cart-events",
    "transactions.jsonl": "transactions",
    "ad_events.jsonl": "ad-clicks",
}


def build_producer(bootstrap_servers, max_retries=8, base_delay=1.0):
    """
    Connects to Kafka with exponential backoff. In a Dockerized setup,
    the producer container can start before the Kafka broker is fully
    ready to accept connections -- retrying here (instead of crashing
    immediately) means docker-compose's startup order doesn't have to
    be perfect, and a brief Kafka restart won't kill the producer.
    """
    attempt = 0
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
            )
            logger.info(f"Connected to Kafka at {bootstrap_servers}")
            return producer
        except (KafkaTimeoutError, KafkaConnectionError) as e:
            attempt += 1
            if attempt > max_retries:
                logger.error(f"Giving up connecting to Kafka after {max_retries} attempts: {e}")
                raise
            delay = min(base_delay * (2 ** (attempt - 1)), 30)
            logger.warning(
                f"Kafka not available yet (attempt {attempt}/{max_retries}). "
                f"Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)


def load_records(filename):
    """Load all records from a sample file into a list (small files, fine in memory)."""
    path = SAMPLE_DIR / filename
    if not path.exists():
        logger.warning(f"{path} not found -- run generate_sample_data.py first")
        return []
    records = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def safe_send(producer, topic, key, value):
    """Send with error handling so one bad send doesn't crash the whole run."""
    try:
        producer.send(topic, key=key, value=value)
        return True
    except KafkaError as e:
        logger.error(f"Failed to send to topic '{topic}': {e}")
        return False


def stream_file(producer, filename, topic, delay_seconds):
    """Kept for --source (single-file) mode: streams one file start to finish."""
    records = load_records(filename)
    count = 0
    for record in records:
        key = record.get("user_id")
        if safe_send(producer, topic, key, record):
            count += 1
            logger.info(f"[{topic}] sent record {count}: "
                        f"user_id={record.get('user_id')} "
                        f"event_type={record.get('event_type')}")
        time.sleep(delay_seconds)
    producer.flush()
    return count


def stream_interleaved(producer, source_topic_map, delay_seconds):
    """
    Streams all sample files at once, using PROPORTIONAL interleaving so
    the instantaneous mix of event types reflects each source's real
    share of total volume (e.g. views arrive ~3x as often as purchases,
    matching the 300:90 record counts), instead of a naive 1-for-1
    round-robin which would flatten every source to an equal, unrealistic
    arrival rate.

    Algorithm: at each step, send from whichever active source has made
    the LEAST progress (sent/total) so far. This is the same idea as
    weighted round-robin schedulers -- a source with more total records
    gets picked more often, but everything still finishes at roughly the
    same time and stays interleaved throughout, not in sequential blocks.
    """
    queues = []
    for filename, topic in source_topic_map.items():
        records = load_records(filename)
        if records:
            queues.append({"topic": topic, "records": records, "index": 0})
        else:
            logger.warning(f"no records loaded for topic '{topic}'")

    total_sent = 0
    active = list(queues)

    while active:
        # Pick the queue with the lowest completion fraction so far.
        next_queue = min(active, key=lambda q: q["index"] / len(q["records"]))

        record = next_queue["records"][next_queue["index"]]
        key = record.get("user_id")
        if safe_send(producer, next_queue["topic"], key, record):
            total_sent += 1
            logger.info(f"[{next_queue['topic']}] sent record {next_queue['index'] + 1}: "
                        f"user_id={record.get('user_id')} "
                        f"event_type={record.get('event_type')}")
        next_queue["index"] += 1

        if next_queue["index"] >= len(next_queue["records"]):
            active.remove(next_queue)

        time.sleep(delay_seconds)

    producer.flush()
    return total_sent


def main():
    parser = argparse.ArgumentParser(description="Stream sample e-commerce events to Kafka.")
    parser.add_argument("--bootstrap-servers",
                         default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                         help="Kafka bootstrap server address (env: KAFKA_BOOTSTRAP_SERVERS)")
    parser.add_argument("--delay", type=float,
                         default=float(os.environ.get("PRODUCER_DELAY_SECONDS", "0.5")),
                         help="Seconds to wait between messages (env: PRODUCER_DELAY_SECONDS)")
    parser.add_argument("--source", choices=list(SOURCE_TOPIC_MAP.keys()) + ["all"],
                         default="all", help="Which sample file to stream (default: all)")
    parser.add_argument("--mode", choices=["interleaved", "sequential"], default="interleaved",
                         help="'interleaved' (default) sends all 4 sources proportionally so every "
                              "topic gets live data together -- best for dashboard demos. "
                              "'sequential' streams one file fully before starting the next.")
    args = parser.parse_args()

    logger.info(f"Connecting to Kafka at {args.bootstrap_servers} ...")
    producer = build_producer(args.bootstrap_servers)
    logger.info("Connected. Streaming events...")

    if args.source != "all":
        topic = SOURCE_TOPIC_MAP[args.source]
        logger.info(f"Streaming {args.source} -> topic '{topic}'")
        total = stream_file(producer, args.source, topic, args.delay)
    elif args.mode == "interleaved":
        logger.info("Streaming all 4 sources interleaved (proportional)...")
        total = stream_interleaved(producer, SOURCE_TOPIC_MAP, args.delay)
    else:
        total = 0
        for filename, topic in SOURCE_TOPIC_MAP.items():
            logger.info(f"Streaming {filename} -> topic '{topic}'")
            total += stream_file(producer, filename, topic, args.delay)

    producer.close()
    logger.info(f"Done. {total} total records streamed to Kafka.")


if __name__ == "__main__":
    main()
