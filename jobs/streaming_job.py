from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count, window, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import json
import os

OUTPUT_PATH = "/opt/spark/dashboard_data/latest_snapshot.json"
HISTORY_PATH = "/opt/spark/dashboard_data/history.jsonl"

def write_results(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    rows = batch_df.toJSON().collect()
    parsed = [json.loads(r) for r in rows]

    with open(OUTPUT_PATH, "w") as f:
        json.dump(parsed, f)

    with open(HISTORY_PATH, "a") as f:
        for row in parsed:
            f.write(json.dumps(row) + "\n")

    print(f"[Batch {batch_id}] Written {len(parsed)} rows to dashboard")

def main():
    spark = SparkSession.builder \
        .appName("StockStreamingJob") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    os.makedirs("/opt/spark/dashboard_data", exist_ok=True)

    schema = StructType([
        StructField("date", StringType()),
        StructField("ticker", StringType()),
        StructField("company", StringType()),
        StructField("sector", StringType()),
        StructField("industry", StringType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("adj_close", DoubleType()),
        StructField("volume", LongType()),
        StructField("event_time", StringType()),
    ])

    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "stock-events") \
        .option("startingOffsets", "latest") \
        .load()

    parsed = raw.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_time")
    ).select("data.*", "kafka_time")

    aggregated = parsed \
        .withWatermark("kafka_time", "10 seconds") \
        .groupBy(
            window(col("kafka_time"), "30 seconds"),
            col("sector")
        ) \
        .agg(
            round(avg("close"), 2).alias("avg_close"),
            count("*").alias("event_count")
        )

    console_query = aggregated.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="10 seconds") \
        .start()

    file_query = aggregated.writeStream \
        .outputMode("update") \
        .foreachBatch(write_results) \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/opt/spark/dashboard_data/checkpoint") \
        .start()

    file_query.awaitTermination()

if __name__ == "__main__":
    main()