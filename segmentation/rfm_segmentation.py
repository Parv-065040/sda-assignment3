"""
rfm_segmentation.py

Phase 3 -- Streaming Data Analytics (production upgrade)

Standalone service (no Kafka involved) that periodically reads
`customer_transactions` and computes an RFM (Recency, Frequency,
Monetary) score for every customer, classifies them into a marketing
segment, and upserts the result into `customer_segments`.

RFM methodology:
    - Recency:   days since the customer's most recent purchase (lower = better)
    - Frequency: total number of purchases (higher = better)
    - Monetary:  total amount spent (higher = better)

Each is scored 1-5 via quintiles (NTILE(5)) computed across the current
customer base, then combined into a segment using standard RFM rule
thresholds (Champions, Loyal Customers, Big Spenders, At Risk, New
Customers, Lost, Needs Attention).

Runs forever, re-scoring on a fixed interval (SEGMENTATION_INTERVAL_SECONDS).
"""

import os
import time
import logging
from datetime import datetime

import mysql.connector
from mysql.connector.errors import Error as MySQLError

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] segmentation: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.environ.get("LOG_DIR", "."), "segmentation.log")),
    ],
)
logger = logging.getLogger("segmentation")

SEGMENTATION_INTERVAL_SECONDS = int(os.environ.get("SEGMENTATION_INTERVAL_SECONDS", "60"))
MIN_TRANSACTIONS_TO_SCORE = int(os.environ.get("MIN_TRANSACTIONS_TO_SCORE", "5"))


def get_mysql_connection(host, user, password, database, max_retries=8, base_delay=1.0):
    """Same retry-with-backoff pattern as consumer.py."""
    attempt = 0
    while True:
        try:
            conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
            logger.info(f"Connected to MySQL at {host}")
            return conn
        except MySQLError as e:
            attempt += 1
            if attempt > max_retries:
                logger.error(f"Giving up connecting to MySQL after {max_retries} attempts: {e}")
                raise
            delay = min(base_delay * (2 ** (attempt - 1)), 30)
            logger.warning(f"MySQL not available yet (attempt {attempt}/{max_retries}): {e}. "
                           f"Retrying in {delay:.1f}s...")
            time.sleep(delay)


def classify_segment(r_score, f_score, m_score):
    """
    Standard RFM rule-based classification. Recency-driven churn signals
    (At Risk / Can't Lose Them / Lost) are checked BEFORE the frequency/
    monetary-based "currently good" labels (Loyal Customers, Big
    Spenders) -- a customer with terrible recency should never be
    labeled "Loyal" just because they used to buy a lot, since that's
    precisely the profile RFM segmentation exists to catch as churning.
    """
    if r_score >= 4 and f_score >= 4 and m_score >= 4:
        return "Champions"
    if r_score <= 2 and f_score >= 4 and m_score >= 4:
        return "Can't Lose Them"  # were high-value, now inactive -- most urgent win-back
    if r_score <= 2 and f_score >= 3:
        return "At Risk"
    if r_score <= 2 and f_score <= 2:
        return "Lost"
    if f_score >= 4 and m_score >= 3:
        return "Loyal Customers"
    if m_score >= 4:
        return "Big Spenders"
    if f_score == 1 and r_score >= 4:
        return "New Customers"
    return "Needs Attention"


def compute_and_write_segments(conn):
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT COUNT(DISTINCT user_id) FROM customer_transactions
        """
    )
    (distinct_customers,) = cursor.fetchone()
    if distinct_customers < MIN_TRANSACTIONS_TO_SCORE:
        logger.info(f"Only {distinct_customers} customers with purchase history so far "
                    f"(need {MIN_TRANSACTIONS_TO_SCORE}) -- skipping this cycle.")
        cursor.close()
        return 0

    # NOTE: recency is measured in MINUTES, not days, deliberately. A demo
    # run spans minutes/hours of real time, so DATEDIFF-in-days would put
    # nearly every customer at "0 days" -- a massive tie cluster with no
    # real signal to rank on. NTILE breaks ties using arbitrary internal
    # row order in that case, which was producing customers who purchased
    # moments ago getting randomly assigned a BAD recency score. Minute
    # granularity gives real variance to rank on even within a short demo
    # window. (In a real production system running over weeks/months,
    # day-granularity would be the right choice again.)
    cursor.execute(
        """
        SELECT
            user_id,
            TIMESTAMPDIFF(MINUTE, MAX(purchased_at), NOW()) AS recency_minutes,
            COUNT(*) AS frequency,
            SUM(order_value) AS monetary,
            NTILE(5) OVER (ORDER BY TIMESTAMPDIFF(MINUTE, MAX(purchased_at), NOW()) DESC) AS r_score,
            NTILE(5) OVER (ORDER BY COUNT(*) ASC) AS f_score,
            NTILE(5) OVER (ORDER BY SUM(order_value) ASC) AS m_score
        FROM customer_transactions
        GROUP BY user_id
        """
    )
    rows = cursor.fetchall()

    now = datetime.now()
    upserts = []
    for user_id, recency_minutes, frequency, monetary, r_score, f_score, m_score in rows:
        segment = classify_segment(r_score, f_score, m_score)
        upserts.append((user_id, recency_minutes, frequency, monetary, r_score, f_score, m_score,
                         segment, now))

    cursor.executemany(
        """
        INSERT INTO customer_segments
            (user_id, recency_minutes, frequency, monetary, r_score, f_score, m_score, segment, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            recency_minutes = VALUES(recency_minutes),
            frequency = VALUES(frequency),
            monetary = VALUES(monetary),
            r_score = VALUES(r_score),
            f_score = VALUES(f_score),
            m_score = VALUES(m_score),
            segment = VALUES(segment),
            updated_at = VALUES(updated_at)
        """,
        upserts,
    )

    # Phase 6 (dashboard): snapshot the segment distribution at this point
    # in time, so a trend chart can show segments growing/shrinking over
    # multiple cycles -- customer_segments above only ever holds the
    # LATEST state per customer (upserted), so without this separate
    # history table there would be no way to see change over time.
    segment_counts = {}
    for row in upserts:
        segment_counts[row[7]] = segment_counts.get(row[7], 0) + 1

    if segment_counts:
        cursor.executemany(
            """
            INSERT INTO segment_history (snapshot_at, segment, customer_count)
            VALUES (%s, %s, %s)
            """,
            [(now, segment, count) for segment, count in segment_counts.items()],
        )

    conn.commit()
    cursor.close()

    logger.info(f"Scored {len(upserts)} customers | " +
                ", ".join(f"{seg}={count}" for seg, count in sorted(segment_counts.items())))
    return len(upserts)


def main():
    mysql_host = os.environ.get("MYSQL_HOST", "localhost")
    mysql_user = os.environ.get("MYSQL_USER", "root")
    mysql_password = os.environ.get("MYSQL_PASSWORD")
    mysql_database = os.environ.get("MYSQL_DATABASE", "streaming_analytics")

    if not mysql_password:
        raise SystemExit("MYSQL_PASSWORD environment variable is required")

    logger.info(f"Connecting to MySQL at {mysql_host} ...")
    conn = get_mysql_connection(mysql_host, mysql_user, mysql_password, mysql_database)
    logger.info(f"Connected. Re-scoring customer segments every {SEGMENTATION_INTERVAL_SECONDS}s...")

    try:
        while True:
            try:
                compute_and_write_segments(conn)
            except MySQLError as e:
                logger.warning(f"Segmentation pass failed ({e}), reconnecting...")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_mysql_connection(mysql_host, mysql_user, mysql_password, mysql_database)
            time.sleep(SEGMENTATION_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Stopping segmentation service (KeyboardInterrupt)...")
    finally:
        conn.close()


if __name__ == "__main__":
    main()