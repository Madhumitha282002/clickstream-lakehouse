import argparse
from pyspark.sql import SparkSession, functions as F, Window

def build_spark(app_name: str) -> SparkSession:
    return (SparkSession.builder
            .appName(app_name)
            .enableHiveSupport()  # useful when running on cluster with Hive metastore
            .getOrCreate())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input path (local or hdfs:// or hdfs:///)")
    ap.add_argument("--output", required=True, help="Output base path for curated zone")
    args = ap.parse_args()

    spark = build_spark("ClickstreamLakehouseETL")

    # Read JSONL
    df = spark.read.json(args.input)

    # --- Clean & normalize ---
    # enforce required columns, drop bad rows
    required = ["event_id", "user_id", "event_time", "event_type", "page_url", "referrer", "device", "country"]
    for c in required:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    df = (df
          .withColumn("event_ts", F.to_timestamp("event_time"))  # expects ISO strings
          .drop("event_time")
          .filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull() & F.col("event_ts").isNotNull())
          .filter(F.col("event_type").isin(["view", "add_to_cart", "checkout", "purchase"]))
          )

    # partition date
    df = df.withColumn("dt", F.to_date("event_ts"))

    # de-dup by event_id (keep first)
    df = df.dropDuplicates(["event_id"])

    # Write cleaned events
    events_out = f"{args.output.rstrip('/')}/events_clean"
    (df
     .repartition("dt")
     .write.mode("overwrite")
     .partitionBy("dt")
     .parquet(events_out))

    # --- Sessionization (30-minute inactivity window) ---
    w = Window.partitionBy("user_id").orderBy("event_ts")

    df_sess = (df
               .withColumn("prev_ts", F.lag("event_ts").over(w))
               .withColumn("gap_min", (F.col("event_ts").cast("long") - F.col("prev_ts").cast("long")) / 60.0)
               .withColumn("new_session", F.when(F.col("prev_ts").isNull() | (F.col("gap_min") > 30), 1).otherwise(0))
               .withColumn("session_idx", F.sum("new_session").over(w))
               .withColumn("session_id", F.concat_ws("-", F.col("user_id"), F.format_string("%06d", F.col("session_idx"))))
               )

    # Per-session metrics
    sess_metrics = (df_sess
                    .groupBy("dt", "user_id", "session_id")
                    .agg(
                        F.min("event_ts").alias("session_start"),
                        F.max("event_ts").alias("session_end"),
                        F.count("*").alias("events_in_session"),
                        F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("made_purchase"),
                        F.max(F.when(F.col("event_type") == "checkout", 1).otherwise(0)).alias("reached_checkout"),
                        F.max(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("added_to_cart"),
                    )
                    .withColumn("session_duration_sec", F.col("session_end").cast("long") - F.col("session_start").cast("long"))
                    )

    sessions_out = f"{args.output.rstrip('/')}/sessions"
    (sess_metrics
     .repartition("dt")
     .write.mode("overwrite")
     .partitionBy("dt")
     .parquet(sessions_out))

    # --- Daily metrics (DAU + conversion) ---
    daily = (df
             .groupBy("dt")
             .agg(
                 F.countDistinct("user_id").alias("dau"),
                 F.count("*").alias("events"),
                 F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
                 F.sum(F.when(F.col("event_type") == "checkout", 1).otherwise(0)).alias("checkouts"),
                 F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_carts"),
                 F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
             )
             .withColumn("checkout_rate", F.col("checkouts") / F.when(F.col("views") > 0, F.col("views")).otherwise(F.lit(None)))
             .withColumn("purchase_rate", F.col("purchases") / F.when(F.col("views") > 0, F.col("views")).otherwise(F.lit(None)))
             )

    daily_out = f"{args.output.rstrip('/')}/daily_metrics"
    (daily
     .repartition("dt")
     .write.mode("overwrite")
     .partitionBy("dt")
     .parquet(daily_out))

    # --- Funnel metrics by referrer (view -> add_to_cart -> checkout -> purchase) ---
    funnel = (df
              .groupBy("dt", "referrer")
              .agg(
                  F.countDistinct(F.when(F.col("event_type") == "view", F.col("user_id"))).alias("users_view"),
                  F.countDistinct(F.when(F.col("event_type") == "add_to_cart", F.col("user_id"))).alias("users_add_to_cart"),
                  F.countDistinct(F.when(F.col("event_type") == "checkout", F.col("user_id"))).alias("users_checkout"),
                  F.countDistinct(F.when(F.col("event_type") == "purchase", F.col("user_id"))).alias("users_purchase"),
              )
              .withColumn("view_to_cart", F.col("users_add_to_cart") / F.when(F.col("users_view") > 0, F.col("users_view")).otherwise(F.lit(None)))
              .withColumn("cart_to_checkout", F.col("users_checkout") / F.when(F.col("users_add_to_cart") > 0, F.col("users_add_to_cart")).otherwise(F.lit(None)))
              .withColumn("checkout_to_purchase", F.col("users_purchase") / F.when(F.col("users_checkout") > 0, F.col("users_checkout")).otherwise(F.lit(None)))
              )

    funnel_out = f"{args.output.rstrip('/')}/funnel_metrics"
    (funnel
     .repartition("dt")
     .write.mode("overwrite")
     .partitionBy("dt")
     .parquet(funnel_out))

    print("ETL complete.")
    print(f"events_clean:   {events_out}")
    print(f"sessions:       {sessions_out}")
    print(f"daily_metrics:  {daily_out}")
    print(f"funnel_metrics: {funnel_out}")

    spark.stop()

if __name__ == "__main__":
    main()
