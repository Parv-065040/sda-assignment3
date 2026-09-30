"""
consumer.py

Assignment 3 -- Streaming Data Analytics

Reads events from all 4 Kafka topics (clickstream, cart-events, transactions,
ad-clicks), aggregates them into 15-second tumbling windows, computes KPIs
(conversion rate, cart abandonment rate, revenue, ad spend, etc.) plus
5-window moving averages, and writes the results into MySQL tables that
Grafana will read from.

Tables written to (see Assignment 3 plan for schema):
    window_metrics        -- one row per window: funnel counts, rates, revenue, ad spend
    category_revenue      -- one row per (window, category): revenue/orders by category
    campaign_performance  -- one row per (window, campaign): clicks/impressions/cost/ctr

Run this AFTER producer.py is running (or at the same time), so there is
data flowing into the topics.
"""

import json
import time
import logging
import os
import socket
import statistics
import argparse
from collections import defaultdict
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaTimeoutError, KafkaConnectionError, KafkaError
import mysql.connector
from mysql.connector.errors import Error as MySQLError

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] consumer: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.environ.get("LOG_DIR", "."), "consumer.log")),
    ],
)
logger = logging.getLogger("consumer")

TOPICS = ["clickstream", "cart-events", "transactions", "ad-clicks"]
WINDOW_SECONDS = 15
MOVING_AVG_WINDOWS = 5

# ---- Phase 2: anomaly detection ----
# Z-score based: for each monitored metric, compare this window's value
# against the mean/stdev of that SAME consumer instance's own recent
# history. "direction" controls which side of the distribution is
# actually bad news for that metric -- a conversion rate SPIKE isn't
# something to alert on, but a DROP is; the reverse is true for cart
# abandonment and ad spend.
ANOMALY_Z_THRESHOLD = float(os.environ.get("ANOMALY_Z_THRESHOLD", "2.0"))
ANOMALY_LOOKBACK_WINDOWS = int(os.environ.get("ANOMALY_LOOKBACK_WINDOWS", "20"))
ANOMALY_MIN_HISTORY = 5  # don't judge "normal" from fewer than this many prior windows

MONITORED_METRICS = {
    # metric_name: (column_in_window_metrics, direction)
    # direction: 'low' = alert only on an unusual DROP
    #            'high' = alert only on an unusual SPIKE
    "conversion_rate": ("conversion_rate", "low"),
    "cart_abandonment_rate": ("cart_abandonment_rate", "high"),
    "revenue": ("revenue", "low"),
    "ad_spend": ("ad_spend", "high"),
}

# Identifies which consumer instance produced a given row. Docker gives each
# scaled replica a unique hostname (the container ID) automatically, so
# running `docker-compose up --scale consumer=2` naturally produces two
# distinct IDs here with no extra config needed. This is what makes
# multi-instance scaling safe: every row in MySQL is traceable to the
# instance that wrote it, so Grafana can correctly SUM across instances
# per time bucket instead of one instance's rows being indistinguishable
# from another's.
CONSUMER_ID = f"{socket.gethostname()}-{os.getpid()}"


def get_mysql_connection(host, user, password, database, max_retries=8, base_delay=1.0):
    """Connects to MySQL with exponential backoff -- see build_consumer for why."""
    attempt = 0
    while True:
        try:
            conn = mysql.connector.connect(
                host=host, user=user, password=password, database=database
            )
            logger.info(f"Connected to MySQL at {host}")
            return conn
        except MySQLError as e:
            attempt += 1
            if attempt > max_retries:
                logger.error(f"Giving up connecting to MySQL after {max_retries} attempts: {e}")
                raise
            delay = min(base_delay * (2 ** (attempt - 1)), 30)
            logger.warning(
                f"MySQL not available yet (attempt {attempt}/{max_retries}): {e}. "
                f"Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)


def build_consumer(bootstrap_servers, max_retries=8, base_delay=1.0):
    """
    Connects to Kafka with exponential backoff. In Docker, the consumer
    container can start before Kafka is ready to accept connections --
    retrying here means startup order in docker-compose doesn't have to
    be perfect, and a brief Kafka restart won't kill the consumer.

    enable_auto_commit is OFF: offsets are committed manually, only after
    a window's data has been durably written to MySQL (see
    flush_window_safe). This means a crash between "received message" and
    "wrote it to MySQL" causes those messages to be re-delivered on
    restart (at-least-once), which is the correct trade-off here --
    re-aggregating a few extra events into the next window is much
    cheaper than silently losing data.
    """
    attempt = 0
    while True:
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=bootstrap_servers,
                value_deserializer=lambda v: v,  # deserialize manually per-message (see main loop) so a single malformed message can be caught and sent to the DLQ instead of crashing consumer.__next__
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=1000,  # lets us break out of poll loop to check window timer
                group_id="dashboard-consumer-group",
            )
            logger.info(f"Connected to Kafka at {bootstrap_servers}, subscribed to {TOPICS} "
                        f"(consumer_id={CONSUMER_ID})")
            return consumer
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


def build_dlq_producer(bootstrap_servers, max_retries=8, base_delay=1.0):
    """Same retry pattern as the main consumer connection, for the producer
    used to route unprocessable messages to their dead-letter topic."""
    attempt = 0
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            logger.info("Dead-letter producer connected")
            return producer
        except (KafkaTimeoutError, KafkaConnectionError) as e:
            attempt += 1
            if attempt > max_retries:
                logger.error(f"Giving up connecting DLQ producer after {max_retries} attempts: {e}")
                raise
            delay = min(base_delay * (2 ** (attempt - 1)), 30)
            logger.warning(f"DLQ producer: Kafka not available yet (attempt {attempt}/{max_retries}). "
                           f"Retrying in {delay:.1f}s...")
            time.sleep(delay)


class WindowAggregator:
    """Accumulates raw events for the current window, then resets."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.views = 0
        self.cart_adds = 0
        self.purchases = 0
        self.revenue = 0.0
        self.orders = 0
        self.ad_clicks = 0
        self.ad_impressions = 0
        self.ad_spend = 0.0
        self.category_revenue = defaultdict(lambda: {"revenue": 0.0, "orders": 0})
        self.campaign_stats = defaultdict(lambda: {
            "clicks": 0, "impressions": 0, "cost": 0.0, "utm_source": ""
        })
        # Phase 3 (RFM segmentation) needs per-customer purchase history,
        # not just window-level aggregates -- so raw transaction records
        # are captured here and bulk-inserted in flush_window, separate
        # from the aggregated counters above.
        self.transactions = []
        # Phase 5 (dashboard): raw search terms for the "Top Search
        # Queries" chart -- same pattern as transactions above.
        self.search_queries = []
        # Phase 6 (Campaign -> Segment chart): per-user click/email-click
        # attribution, needed to later join against customer_segments and
        # see which segments each campaign's clickers belong to. NOT
        # capturing impressions here -- too voluminous, and an impression
        # alone isn't real engagement the way a click is.
        self.ad_interactions = []

    def process(self, topic, record):
        event_type = record.get("event_type")

        if topic == "clickstream":
            if event_type == "view":
                self.views += 1
            elif event_type == "search":
                query = record.get("search_query")
                if query:
                    self.search_queries.append({
                        "user_id": record.get("user_id"),
                        "query": query,
                        # same reasoning as purchased_at: real wall-clock
                        # time set at insert, not the synthetic event_time
                    })

        elif topic == "cart-events":
            if event_type == "cart":
                self.cart_adds += 1

        elif topic == "transactions":
            if event_type == "purchase":
                self.purchases += 1
                order_value = record.get("order_value", 0.0)
                self.revenue += order_value
                self.orders += 1
                category = record.get("category_code", "unknown")
                self.category_revenue[category]["revenue"] += order_value
                self.category_revenue[category]["orders"] += 1
                self.transactions.append({
                    "user_id": record.get("user_id"),
                    "order_id": record.get("order_id"),
                    "category_code": category,
                    "order_value": order_value,
                    # NOTE: deliberately NOT using record.get("event_time") here --
                    # the sample data's event_time is a fixed synthetic date range
                    # (Aug 20-30, 2026) baked in at generation time, so it never
                    # reflects when a message actually streamed through Kafka.
                    # purchased_at is set to the real wall-clock window_end time
                    # in flush_window instead, so recency stays meaningful as
                    # this runs live.
                })

        elif topic == "ad-clicks":
            campaign = record.get("campaign", "unknown")
            cost = record.get("cost", 0.0)
            stats = self.campaign_stats[campaign]
            stats["utm_source"] = record.get("utm_source", "")
            if event_type == "ad_impression":
                self.ad_impressions += 1
                stats["impressions"] += 1
            else:  # ad_click or email_click
                self.ad_clicks += 1
                stats["clicks"] += 1
                stats["cost"] += cost
                self.ad_spend += cost
                self.ad_interactions.append({
                    "user_id": record.get("user_id"),
                    "campaign": campaign,
                    "event_type": event_type,
                })


def fetch_recent_rates(cursor, n, consumer_id):
    """
    Get the last n conversion_rate / cart_abandonment_rate values for THIS
    consumer instance only, most recent first. Scoping by consumer_id
    matters once you scale to multiple consumers: each instance only ever
    sees a subset of partitions/traffic, so its own moving average should
    smooth its own history, not get mixed with another instance's
    unrelated window boundaries and volumes.
    """
    cursor.execute(
        "SELECT conversion_rate, cart_abandonment_rate FROM window_metrics "
        "WHERE consumer_id = %s ORDER BY id DESC LIMIT %s", (consumer_id, n)
    )
    return cursor.fetchall()


def compute_moving_average(cursor, new_conversion_rate, new_abandonment_rate, consumer_id):
    recent = fetch_recent_rates(cursor, MOVING_AVG_WINDOWS - 1, consumer_id)
    conv_values = [new_conversion_rate] + [r[0] for r in recent]
    aband_values = [new_abandonment_rate] + [r[1] for r in recent]
    conv_ma = sum(conv_values) / len(conv_values)
    aband_ma = sum(aband_values) / len(aband_values)
    return conv_ma, aband_ma


def check_anomalies(cursor, consumer_id, current_values):
    """
    Compares each monitored metric's value THIS window against the mean
    and standard deviation of that same consumer instance's own recent
    history (ANOMALY_LOOKBACK_WINDOWS prior rows). Returns a list of
    alert dicts for anything that crosses ANOMALY_Z_THRESHOLD standard
    deviations in the "bad" direction for that metric.

    Must be called BEFORE this window's row is inserted into
    window_metrics, or the current value would be included in its own
    baseline.
    """
    alerts = []
    for metric_name, (column, direction) in MONITORED_METRICS.items():
        current_value = current_values[metric_name]

        cursor.execute(
            f"SELECT {column} FROM window_metrics WHERE consumer_id = %s "
            f"ORDER BY id DESC LIMIT %s",
            (consumer_id, ANOMALY_LOOKBACK_WINDOWS),
        )
        history = [r[0] for r in cursor.fetchall()]
        if len(history) < ANOMALY_MIN_HISTORY:
            continue  # not enough history yet to know what's "normal"

        mean = statistics.mean(history)
        stdev = statistics.pstdev(history)
        if stdev == 0:
            continue  # no variation in recent history to compare against

        z = (current_value - mean) / stdev

        triggered = (
            (direction == "low" and z <= -ANOMALY_Z_THRESHOLD) or
            (direction == "high" and z >= ANOMALY_Z_THRESHOLD)
        )
        if not triggered:
            continue

        severity = "critical" if abs(z) >= ANOMALY_Z_THRESHOLD * 1.5 else "warning"
        direction_word = "dropped" if z < 0 else "spiked"
        alerts.append({
            "metric_name": metric_name,
            "metric_value": current_value,
            "expected_value": mean,
            "deviation_score": z,
            "severity": severity,
            "message": (
                f"{metric_name} {direction_word} to {current_value:.4f} "
                f"(recent average {mean:.4f}, {abs(z):.2f} std deviations away)"
            ),
        })
    return alerts


def write_alerts(cursor, alerts, triggered_at, consumer_id):
    for alert in alerts:
        cursor.execute(
            """
            INSERT INTO alerts
                (triggered_at, consumer_id, metric_name, metric_value,
                 expected_value, deviation_score, severity, message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                triggered_at, consumer_id, alert["metric_name"], alert["metric_value"],
                alert["expected_value"], alert["deviation_score"],
                alert["severity"], alert["message"],
            ),
        )
        logger.warning(f"ALERT [{alert['severity']}] {alert['message']}")


def flush_window(conn, agg: WindowAggregator, window_start, window_end):
    cursor = conn.cursor()

    conversion_rate = (agg.purchases / agg.views) if agg.views > 0 else 0.0
    cart_abandonment_rate = (
        1 - (agg.purchases / agg.cart_adds) if agg.cart_adds > 0 else 0.0
    )
    # Clamp to a sane 0-1 range. With independently-sampled demo data, a
    # window can occasionally see more purchases than cart-adds/views (they
    # aren't causally linked event-to-event), which would otherwise produce
    # a nonsensical negative abandonment rate or a conversion rate over
    # 100%. Real production data derived from an actual session funnel
    # wouldn't need this, but it keeps the dashboard numbers meaningful for
    # this simulated dataset.
    conversion_rate = max(0.0, min(1.0, conversion_rate))
    cart_abandonment_rate = max(0.0, min(1.0, cart_abandonment_rate))

    conversion_rate_ma, cart_abandonment_rate_ma = compute_moving_average(
        cursor, conversion_rate, cart_abandonment_rate, CONSUMER_ID
    )

    # Anomaly check runs BEFORE inserting this window's row, comparing
    # against history that doesn't yet include it.
    alerts = check_anomalies(cursor, CONSUMER_ID, {
        "conversion_rate": conversion_rate,
        "cart_abandonment_rate": cart_abandonment_rate,
        "revenue": agg.revenue,
        "ad_spend": agg.ad_spend,
    })

    cursor.execute(
        """
        INSERT INTO window_metrics
            (window_start, window_end, consumer_id, views, cart_adds, purchases,
             conversion_rate, conversion_rate_ma,
             cart_abandonment_rate, cart_abandonment_rate_ma,
             revenue, orders, ad_clicks, ad_impressions, ad_spend)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            window_start, window_end, CONSUMER_ID, agg.views, agg.cart_adds, agg.purchases,
            conversion_rate, conversion_rate_ma,
            cart_abandonment_rate, cart_abandonment_rate_ma,
            agg.revenue, agg.orders, agg.ad_clicks, agg.ad_impressions, agg.ad_spend,
        ),
    )

    if alerts:
        write_alerts(cursor, alerts, window_end, CONSUMER_ID)

    for category, stats in agg.category_revenue.items():
        cursor.execute(
            """
            INSERT INTO category_revenue (window_start, consumer_id, category_code, revenue, orders)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (window_start, CONSUMER_ID, category, stats["revenue"], stats["orders"]),
        )

    for campaign, stats in agg.campaign_stats.items():
        impressions = stats["impressions"]
        clicks = stats["clicks"]
        ctr = (clicks / impressions) if impressions > 0 else 0.0
        cursor.execute(
            """
            INSERT INTO campaign_performance
                (window_start, consumer_id, campaign, utm_source, clicks, impressions, cost, ctr)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (window_start, CONSUMER_ID, campaign, stats["utm_source"], clicks, impressions,
             stats["cost"], ctr),
        )

    if agg.transactions:
        cursor.executemany(
            """
            INSERT INTO customer_transactions
                (user_id, order_id, category_code, order_value, purchased_at, consumer_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [
                (t["user_id"], t["order_id"], t["category_code"], t["order_value"],
                 window_end, CONSUMER_ID)
                for t in agg.transactions
            ],
        )

    if agg.search_queries:
        cursor.executemany(
            """
            INSERT INTO search_queries (user_id, query, searched_at, consumer_id)
            VALUES (%s, %s, %s, %s)
            """,
            [
                (s["user_id"], s["query"], window_end, CONSUMER_ID)
                for s in agg.search_queries
            ],
        )

    if agg.ad_interactions:
        cursor.executemany(
            """
            INSERT INTO customer_ad_interactions
                (user_id, campaign, event_type, interacted_at, consumer_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            [
                (a["user_id"], a["campaign"], a["event_type"], window_end, CONSUMER_ID)
                for a in agg.ad_interactions
            ],
        )

    conn.commit()
    cursor.close()

    logger.info(
        f"[{window_end.strftime('%H:%M:%S')}] window flushed (consumer_id={CONSUMER_ID}) | "
        f"views={agg.views} cart_adds={agg.cart_adds} purchases={agg.purchases} | "
        f"conv_rate={conversion_rate:.2%} (ma {conversion_rate_ma:.2%}) | "
        f"abandon_rate={cart_abandonment_rate:.2%} (ma {cart_abandonment_rate_ma:.2%}) | "
        f"revenue={agg.revenue:.2f} | ad_spend={agg.ad_spend:.2f}"
    )


def send_to_dlq(dlq_producer, original_topic, raw_value, error):
    """Routes an unprocessable message to its dead-letter topic instead of
    crashing or silently dropping it. The DLQ payload wraps the original
    raw bytes plus error context, so someone can inspect *why* it failed
    later without needing to reproduce the failure."""
    dlq_topic = f"{original_topic}-dlq"
    try:
        payload = {
            "original_topic": original_topic,
            "error": str(error),
            "failed_at": datetime.now().isoformat(),
            "consumer_id": CONSUMER_ID,
            "raw_value": raw_value.decode("utf-8", errors="replace") if isinstance(raw_value, bytes) else str(raw_value),
        }
        dlq_producer.send(dlq_topic, value=json.dumps(payload).encode("utf-8"))
        dlq_producer.flush()
        logger.warning(f"Routed unprocessable message from '{original_topic}' to '{dlq_topic}': {error}")
    except KafkaError as e:
        logger.error(f"Failed to route message to DLQ topic '{dlq_topic}' (message dropped): {e}")


def main():
    parser = argparse.ArgumentParser(description="Consume Kafka topics and write windowed metrics to MySQL.")
    parser.add_argument("--bootstrap-servers",
                         default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--mysql-host", default=os.environ.get("MYSQL_HOST", "localhost"))
    parser.add_argument("--mysql-user", default=os.environ.get("MYSQL_USER", "root"))
    parser.add_argument("--mysql-password",
                         default=os.environ.get("MYSQL_PASSWORD"),
                         required="MYSQL_PASSWORD" not in os.environ,
                         help="Required unless MYSQL_PASSWORD env var is set")
    parser.add_argument("--mysql-database",
                         default=os.environ.get("MYSQL_DATABASE", "streaming_analytics"))
    parser.add_argument("--window-seconds", type=int,
                         default=int(os.environ.get("WINDOW_SECONDS", WINDOW_SECONDS)))
    args = parser.parse_args()

    logger.info(f"Connecting to Kafka at {args.bootstrap_servers} ...")
    consumer = build_consumer(args.bootstrap_servers)
    dlq_producer = build_dlq_producer(args.bootstrap_servers)

    logger.info(f"Connecting to MySQL at {args.mysql_host} ...")
    conn = get_mysql_connection(
        args.mysql_host, args.mysql_user, args.mysql_password, args.mysql_database
    )
    logger.info(f"Connected. Starting windowed aggregation (consumer_id={CONSUMER_ID})...")

    agg = WindowAggregator()
    window_start = datetime.now()

    def process_message(message):
        """Parses and aggregates one message, routing to DLQ on any failure.
        Returns True if the message was successfully aggregated."""
        try:
            record = json.loads(message.value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            send_to_dlq(dlq_producer, message.topic, message.value, f"JSON decode failed: {e}")
            return False
        try:
            agg.process(message.topic, record)
            return True
        except Exception as e:
            # Broad catch is intentional here: this is the pipeline's last
            # line of defense against ANY malformed-but-valid-JSON record
            # (wrong types, unexpected structure) crashing the consumer.
            send_to_dlq(dlq_producer, message.topic, message.value, f"Processing failed: {e}")
            return False

    try:
        while True:
            for message in consumer:
                process_message(message)

                if (datetime.now() - window_start).total_seconds() >= args.window_seconds:
                    window_end = datetime.now()
                    conn, success = flush_window_safe(conn, agg, window_start, window_end, args)
                    if success:
                        consumer.commit()  # only advance offsets after a successful durable write
                        agg.reset()
                        window_start = window_end
                    # else: keep accumulating in the SAME agg/window rather than
                    # resetting -- MySQL being down shouldn't mean this window's
                    # events vanish. The next successful flush will just cover a
                    # longer-than-usual window. Offsets stay uncommitted too, so
                    # if the process restarts before MySQL recovers, Kafka will
                    # redeliver these messages rather than skipping them.

            # consumer_timeout_ms triggers this branch when no messages arrive;
            # still check whether the window has elapsed so windows flush even
            # during quiet periods (agg will just be all zeros).
            if (datetime.now() - window_start).total_seconds() >= args.window_seconds:
                window_end = datetime.now()
                conn, success = flush_window_safe(conn, agg, window_start, window_end, args)
                if success:
                    consumer.commit()
                    agg.reset()
                    window_start = window_end

    except KeyboardInterrupt:
        logger.info("Stopping consumer (KeyboardInterrupt)...")
    finally:
        consumer.close()
        dlq_producer.close()
        conn.close()


def flush_window_safe(conn, agg, window_start, window_end, args):
    """
    Wraps flush_window with a single reconnect-and-retry attempt. A dropped
    MySQL connection (container restart, brief network blip) would otherwise
    kill the whole consumer -- this keeps it running through a transient
    outage instead of losing the process and every window after it.

    Returns (conn, success). success=False means BOTH the original write
    and the post-reconnect retry failed -- the caller must NOT commit
    Kafka offsets in that case, or this window's messages would be marked
    as processed despite never being durably written anywhere, which is a
    silent, permanent data loss rather than the intended "at most a little
    reprocessing" trade-off.
    """
    try:
        flush_window(conn, agg, window_start, window_end)
        return conn, True
    except MySQLError as e:
        logger.warning(f"MySQL write failed ({e}), attempting to reconnect...")
        try:
            conn.close()
        except Exception:
            pass
        conn = get_mysql_connection(
            args.mysql_host, args.mysql_user, args.mysql_password, args.mysql_database
        )
        try:
            flush_window(conn, agg, window_start, window_end)
            return conn, True
        except MySQLError as e2:
            logger.error(
                f"Retry after reconnect also failed -- NOT committing Kafka offsets for this "
                f"window, so these messages will be redelivered and retried on the next poll "
                f"or after restart instead of being permanently lost: {e2}"
            )
            return conn, False


if __name__ == "__main__":
    main()  