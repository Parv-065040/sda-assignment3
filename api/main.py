"""
main.py

Phase 4 -- Streaming Data Analytics (production upgrade)

A REST API layer over the pipeline's MySQL data, demonstrating the
architecturally correct pattern: Grafana is ONE consumer of this data,
but any other system (a mobile app, another team's internal tool, a
scheduled report generator) can pull the same metrics over HTTP without
needing direct database access or knowledge of the schema.

Auto-generated interactive docs are available at /docs once running
(FastAPI/Swagger UI -- no extra work required).
"""

import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import mysql.connector
from mysql.connector import pooling
from mysql.connector.errors import Error as MySQLError

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] api: %(message)s",
)
logger = logging.getLogger("api")

MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "streaming_analytics")

ALLOWED_TREND_METRICS = {
    "conversion_rate", "conversion_rate_ma",
    "cart_abandonment_rate", "cart_abandonment_rate_ma",
    "revenue", "ad_spend", "views", "cart_adds", "purchases",
}

pool: Optional[pooling.MySQLConnectionPool] = None


def build_pool(max_retries=8, base_delay=1.0):
    """Same retry-with-backoff pattern used in consumer.py / rfm_segmentation.py."""
    import time
    attempt = 0
    while True:
        try:
            p = pooling.MySQLConnectionPool(
                pool_name="api_pool",
                pool_size=5,
                host=MYSQL_HOST, user=MYSQL_USER,
                password=MYSQL_PASSWORD, database=MYSQL_DATABASE,
            )
            logger.info(f"Connected to MySQL at {MYSQL_HOST} (pool of 5)")
            return p
        except MySQLError as e:
            attempt += 1
            if attempt > max_retries:
                logger.error(f"Giving up connecting to MySQL after {max_retries} attempts: {e}")
                raise
            delay = min(base_delay * (2 ** (attempt - 1)), 30)
            logger.warning(f"MySQL not available yet (attempt {attempt}/{max_retries}). "
                           f"Retrying in {delay:.1f}s...")
            time.sleep(delay)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    if not MYSQL_PASSWORD:
        raise RuntimeError("MYSQL_PASSWORD environment variable is required")
    pool = build_pool()
    yield
    # nothing to explicitly close -- the pool's connections close with the process


app = FastAPI(
    title="Streaming Analytics API",
    description="Read-only REST API over the e-commerce streaming pipeline's metrics, alerts, and customer segments.",
    version="1.0.0",
    lifespan=lifespan,
)


def get_connection():
    if pool is None:
        raise HTTPException(status_code=503, detail="Database pool not initialized yet")
    try:
        return pool.get_connection()
    except MySQLError as e:
        logger.error(f"Failed to get a pooled connection: {e}")
        raise HTTPException(status_code=503, detail="Database temporarily unavailable")


def run_query(sql, params=None, fetch_one=False):
    conn = get_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, params or ())
        result = cursor.fetchone() if fetch_one else cursor.fetchall()
        cursor.close()
        return result
    except MySQLError as e:
        logger.error(f"Query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")
    finally:
        conn.close()  # returns the connection to the pool, does not actually disconnect


# ---------------------------------------------------------------------------
# Response models (pydantic) -- gives the auto-generated docs real schemas
# instead of generic "any JSON" placeholders.
# ---------------------------------------------------------------------------

class MetricsSummary(BaseModel):
    total_revenue: float
    total_orders: int
    total_ad_spend: float
    overall_conversion_rate_pct: float
    overall_cart_abandonment_rate_pct: float


class FunnelStage(BaseModel):
    stage: str
    count: int


class AlertOut(BaseModel):
    triggered_at: datetime
    severity: str
    metric_name: str
    metric_value: float
    expected_value: Optional[float]
    deviation_score: Optional[float]
    message: Optional[str]


class SegmentSummary(BaseModel):
    segment: str
    customers: int
    avg_lifetime_value: float
    total_segment_value: float


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    """Basic liveness check -- does not touch the database."""
    return {"status": "ok"}


@app.get("/metrics/summary", response_model=MetricsSummary)
def metrics_summary():
    """Headline KPIs, aggregated across all consumer instances and windows."""
    row = run_query(
        """
        SELECT
            COALESCE(SUM(revenue), 0) AS total_revenue,
            COALESCE(SUM(orders), 0) AS total_orders,
            COALESCE(SUM(ad_spend), 0) AS total_ad_spend,
            COALESCE(SUM(conversion_rate * views) / NULLIF(SUM(views), 0), 0) * 100 AS conv_rate_pct,
            COALESCE(SUM(cart_abandonment_rate * cart_adds) / NULLIF(SUM(cart_adds), 0), 0) * 100 AS abandon_rate_pct
        FROM window_metrics
        """,
        fetch_one=True,
    )
    return MetricsSummary(
        total_revenue=row["total_revenue"],
        total_orders=row["total_orders"],
        total_ad_spend=row["total_ad_spend"],
        overall_conversion_rate_pct=row["conv_rate_pct"],
        overall_cart_abandonment_rate_pct=row["abandon_rate_pct"],
    )


@app.get("/metrics/funnel", response_model=list[FunnelStage])
def metrics_funnel():
    """Views -> Cart Adds -> Purchases, summed across all windows/instances."""
    row = run_query(
        "SELECT COALESCE(SUM(views),0) AS views, COALESCE(SUM(cart_adds),0) AS cart_adds, "
        "COALESCE(SUM(purchases),0) AS purchases FROM window_metrics",
        fetch_one=True,
    )
    return [
        FunnelStage(stage="Views", count=row["views"]),
        FunnelStage(stage="Cart Adds", count=row["cart_adds"]),
        FunnelStage(stage="Purchases", count=row["purchases"]),
    ]


@app.get("/metrics/trend")
def metrics_trend(
    metric: str = Query(..., description=f"One of: {', '.join(sorted(ALLOWED_TREND_METRICS))}"),
    limit: int = Query(50, ge=1, le=500),
):
    """Recent windowed values for a single metric, most recent last (chart-ready order)."""
    if metric not in ALLOWED_TREND_METRICS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown metric '{metric}'. Allowed: {sorted(ALLOWED_TREND_METRICS)}",
        )
    # metric name is validated against an allow-list above, so it's safe to
    # interpolate directly -- it is NEVER taken from raw, unvalidated input.
    rows = run_query(
        f"SELECT window_start, {metric} AS value FROM window_metrics "
        f"ORDER BY id DESC LIMIT %s",
        (limit,),
    )
    return list(reversed(rows))


@app.get("/alerts/recent", response_model=list[AlertOut])
def alerts_recent(
    limit: int = Query(20, ge=1, le=200),
    severity: Optional[str] = Query(None, description="Filter: info, warning, or critical"),
):
    if severity and severity not in {"info", "warning", "critical"}:
        raise HTTPException(status_code=400, detail="severity must be one of: info, warning, critical")

    if severity:
        rows = run_query(
            "SELECT triggered_at, severity, metric_name, metric_value, expected_value, "
            "deviation_score, message FROM alerts WHERE severity = %s "
            "ORDER BY triggered_at DESC LIMIT %s",
            (severity, limit),
        )
    else:
        rows = run_query(
            "SELECT triggered_at, severity, metric_name, metric_value, expected_value, "
            "deviation_score, message FROM alerts ORDER BY triggered_at DESC LIMIT %s",
            (limit,),
        )
    return rows


@app.get("/segments", response_model=list[SegmentSummary])
def segments():
    """Customer segment breakdown with aggregate value -- answers 'where should marketing focus'."""
    rows = run_query(
        """
        SELECT segment,
               COUNT(*) AS customers,
               ROUND(AVG(monetary), 2) AS avg_lifetime_value,
               ROUND(SUM(monetary), 2) AS total_segment_value
        FROM customer_segments
        GROUP BY segment
        ORDER BY total_segment_value DESC
        """
    )
    return rows


@app.get("/segments/at-risk")
def segments_at_risk(limit: int = Query(20, ge=1, le=200)):
    """Concrete, actionable list of customers a retention campaign should target first."""
    rows = run_query(
        """
        SELECT user_id, recency_days, frequency, monetary, segment, updated_at
        FROM customer_segments
        WHERE segment IN ('At Risk', %s)
        ORDER BY monetary DESC
        LIMIT %s
        """,
        ("Can't Lose Them", limit),
    )
    return rows


@app.get("/campaigns")
def campaigns():
    """Campaign performance summed across all windows -- clicks, impressions, spend, CTR."""
    rows = run_query(
        """
        SELECT campaign, utm_source,
               SUM(clicks) AS clicks,
               SUM(impressions) AS impressions,
               ROUND(SUM(cost), 2) AS total_cost,
               ROUND(SUM(clicks) / NULLIF(SUM(impressions), 0) * 100, 2) AS ctr_pct
        FROM campaign_performance
        GROUP BY campaign, utm_source
        ORDER BY total_cost DESC
        """
    )
    return rows