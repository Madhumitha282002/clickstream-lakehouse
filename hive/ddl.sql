-- Update these paths to match your environment:
-- Example:
--   hdfs:///data/clickstream/curated
-- or a local path if using local mode (Hive typically expects HDFS in real setups)

CREATE DATABASE IF NOT EXISTS clickstream;

-- Clean events
DROP TABLE IF EXISTS clickstream.events_clean;
CREATE EXTERNAL TABLE clickstream.events_clean (
  event_id   STRING,
  user_id    STRING,
  event_type STRING,
  page_url   STRING,
  referrer   STRING,
  device     STRING,
  country    STRING,
  event_ts   TIMESTAMP
)
PARTITIONED BY (dt DATE)
STORED AS PARQUET
LOCATION '/data/clickstream/curated/events_clean';

-- Sessions
DROP TABLE IF EXISTS clickstream.sessions;
CREATE EXTERNAL TABLE clickstream.sessions (
  user_id STRING,
  session_id STRING,
  session_start TIMESTAMP,
  session_end TIMESTAMP,
  events_in_session BIGINT,
  made_purchase INT,
  reached_checkout INT,
  added_to_cart INT,
  session_duration_sec BIGINT
)
PARTITIONED BY (dt DATE)
STORED AS PARQUET
LOCATION '/data/clickstream/curated/sessions';

-- Daily metrics
DROP TABLE IF EXISTS clickstream.daily_metrics;
CREATE EXTERNAL TABLE clickstream.daily_metrics (
  dau BIGINT,
  events BIGINT,
  purchases BIGINT,
  checkouts BIGINT,
  add_to_carts BIGINT,
  views BIGINT,
  checkout_rate DOUBLE,
  purchase_rate DOUBLE
)
PARTITIONED BY (dt DATE)
STORED AS PARQUET
LOCATION '/data/clickstream/curated/daily_metrics';

-- Funnel metrics by referrer
DROP TABLE IF EXISTS clickstream.funnel_metrics;
CREATE EXTERNAL TABLE clickstream.funnel_metrics (
  referrer STRING,
  users_view BIGINT,
  users_add_to_cart BIGINT,
  users_checkout BIGINT,
  users_purchase BIGINT,
  view_to_cart DOUBLE,
  cart_to_checkout DOUBLE,
  checkout_to_purchase DOUBLE
)
PARTITIONED BY (dt DATE)
STORED AS PARQUET
LOCATION '/data/clickstream/curated/funnel_metrics';

-- If partitions don't appear automatically, you can run:
-- MSCK REPAIR TABLE clickstream.events_clean;
-- MSCK REPAIR TABLE clickstream.sessions;
-- MSCK REPAIR TABLE clickstream.daily_metrics;
-- MSCK REPAIR TABLE clickstream.funnel_metrics;
