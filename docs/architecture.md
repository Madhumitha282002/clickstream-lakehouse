## Pipeline zones
- **Raw Zone (HDFS)**: JSON clickstream logs partitioned by `dt=YYYY-MM-DD`
- **Curated Zone (HDFS)**: Parquet outputs partitioned by dt

## Jobs
1. `etl_clickstream.py`
   - schema enforcement + cleaning
   - dedup by event_id
   - sessionization (30-minute gap rule)
   - daily metrics + funnel metrics

## Tables (Hive external)
- clickstream.events_clean
- clickstream.sessions
- clickstream.daily_metrics
- clickstream.funnel_metrics
