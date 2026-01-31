# Distributed Clickstream Analytics Lakehouse (HDFS + Spark + Hive)

This project demonstrates clustered/distributed data processing by building an end-to-end clickstream analytics pipeline:
- **HDFS**: store raw clickstream logs partitioned by date
- **Spark**: distributed ETL (schema normalization, dedup, sessionization, funnel metrics)
- **Hive**: external tables over curated Parquet outputs for interactive SQL analytics

## Architecture (high level)
1. Generate or ingest clickstream events (JSON lines)
2. Upload raw events to HDFS partitioned by date: `dt=YYYY-MM-DD`
3. Run Spark ETL:
   - read raw JSON from HDFS
   - clean & enforce schema
   - deduplicate events
   - create sessions (30-min inactivity)
   - compute daily aggregates + funnel metrics
   - write partitioned Parquet to curated zone
4. Create Hive external tables pointing to curated Parquet
5. Run HiveQL queries for DAU/MAU, sources, conversions

## Data model (raw events)
Each event is JSON with:
- `event_id`, `user_id`, `event_time`, `event_type` (`view`, `add_to_cart`, `checkout`, `purchase`)
- `page_url`, `referrer`, `device`, `country`

Example (JSONL):
```json
{"event_id":"ev_000000001","user_id":"user_000123","event_time":"2026-01-01T09:10:11Z","event_type":"view","page_url":"/product/sku_00042","referrer":"google","device":"mobile","country":"US"}
