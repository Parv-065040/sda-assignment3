# Phase 1 — Reliability & Scalability

## What changed in `consumer.py`

### 1. Dead-letter queue (DLQ) handling
Every message is now processed through `process_message()`, which:
- Catches JSON decode failures (malformed bytes) and routes the raw
  message to `<topic>-dlq` instead of crashing.
- Catches any other processing error (e.g. valid JSON but the wrong
  shape — a list instead of an object, missing fields your code didn't
  expect) with a broad exception handler, also routing to the DLQ.
- The DLQ payload includes the original topic, the error message, a
  timestamp, which consumer instance saw it, and the raw original value
  -- enough to actually debug *why* something failed later.

Tested with: a valid message (processes normally, no DLQ), malformed
JSON (routed to DLQ, doesn't crash), and valid-JSON-wrong-type (also
routed to DLQ, doesn't crash) -- all three confirmed working before
this was handed over.

### 2. `consumer_id` on every row
Every row written to `window_metrics`, `category_revenue`, and
`campaign_performance` is now tagged with a `consumer_id`
(`hostname-pid`). Docker gives each scaled replica a unique hostname
automatically, so `docker-compose up --scale consumer=2` produces two
distinct IDs with no extra configuration.

This matters because the 5-window moving average is now scoped to
**only that instance's own history** (`WHERE consumer_id = %s`). Without
this, scaling to 2 consumers would have silently mixed two unrelated
instances' window sequences into one meaningless "moving average."

**Dashboard nuance to know before you scale:** your existing KPI tile
queries (`SELECT SUM(revenue) FROM window_metrics`, etc.) will
continue to work correctly when scaled, since they already sum across
every row with no `consumer_id` filter. But the **trend charts**
(Conversion Rate Trend, Cart Abandonment Trend) currently plot the raw
per-row `conversion_rate` / `conversion_rate_ma` values directly — with
2 consumer instances, you'll see two overlapping, independently-timed
lines on the same chart instead of one clean line. If you want a single
unified trend line once you actually scale, the fix is a small query
change (bucket by time and aggregate across `consumer_id`), not a code
change — flag this for the "final Grafana redesign" step in your plan.

### 3. Manual offset commits tied to successful writes
`enable_auto_commit` is now `False`. Kafka offsets are only committed
after `flush_window_safe()` confirms the window's data was durably
written to MySQL. Three cases, all tested:
- Write succeeds first try → commit, reset, move to next window.
- Write fails, reconnect succeeds, retry succeeds → commit, reset,
  move to next window.
- Write fails AND the retry after reconnecting also fails → **do NOT
  commit**, and do NOT reset the in-memory aggregate either. The
  consumer keeps accumulating into the same window and will retry on
  the next flush check, so a MySQL outage extends the current window
  instead of silently dropping it. If the process crashes before MySQL
  recovers, Kafka redelivers those messages on restart since the
  offset was never committed.

This was tightened once during testing: an earlier version of this
logic would have committed offsets even after a fully failed write,
which is a real data-loss bug (offsets say "processed", but nothing was
ever saved). Caught and fixed before delivery, not after.

## Applying this to your running system

Your database already has real data in it (9 / 41 / 43 rows per your
progress report), so we're not wiping it. Run the migration instead:

```powershell
docker exec -i <your-mysql-container-name> mysql -u root -p streaming_analytics < db/migrations/001_add_consumer_id.sql
```
(or open an Exec session and paste the file's contents at the `mysql>`
prompt, same as your earlier table-creation steps). Existing rows will
show `consumer_id = 'unknown'` — that's expected and correct, since
they were written before this concept existed.

Then replace `consumer/consumer.py` with the updated version and
rebuild:
```powershell
docker-compose up -d --build consumer
```

## Testing the DLQ

Publish a deliberately malformed message to any topic and confirm it
lands in the matching `-dlq` topic instead of crashing the consumer:
```powershell
docker exec -it <kafka-container> kafka-console-producer.sh --topic clickstream --bootstrap-server localhost:9092
```
Type `not valid json` and press Enter, then check:
```powershell
docker exec -it <kafka-container> kafka-console-consumer.sh --topic clickstream-dlq --bootstrap-server localhost:9092 --from-beginning
```
You should see a JSON payload wrapping your bad message plus the error
reason. The consumer's own log should show a `WARNING` line, not a
crash.

## Testing consumer-group scaling

```powershell
docker-compose up -d --scale consumer=2
docker-compose ps
docker-compose logs --tail=50 consumer
```
You should see two consumer containers, each logging a different
`consumer_id`, and Kafka's partition assignment splitting the 3
partitions per topic between them (not all partitions going to one
instance).

## Files changed / added
```
consumer/consumer.py              # updated (DLQ, consumer_id, manual commits)
db/mysql-init.sql                 # updated (consumer_id in fresh installs)
db/migrations/001_add_consumer_id.sql   # new (non-destructive migration for your existing DB)
```
