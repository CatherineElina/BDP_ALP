from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, round, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import json
import os

HISTORY_PATH = "/opt/spark/dashboard_data/history.jsonl"

def append_to_history(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    
    # Collect calculated records from the micro-batch
    rows = batch_df.toJSON().collect()
    parsed = [json.loads(r) for r in rows]

    # Append records directly to the running historical transaction log
    with open(HISTORY_PATH, "a") as f:
        for row in parsed:
            f.write(json.dumps(row) + "\n")

    print(f"[Batch {batch_id}] Successfully appended {len(parsed)} engineered records to historical log")

def main():
    spark = SparkSession.builder \
        .appName("StockFeatureEngineeringJob") \
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

    # Ingest from Kafka topic with failOnDataLoss set to false for stability
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "stock-events") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed = raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Real-Time Feature Engineering using Spark expressions
    enriched = parsed \
        .withColumn("daily_return", round((col("close") - col("open")) / col("open"), 4)) \
        .withColumn("price_range", round(col("high") - col("low"), 2))

    # Stream using append mode to preserve structural sequence logs
    file_query = enriched.writeStream \
        .outputMode("append") \
        .foreachBatch(append_to_history) \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", "/opt/spark/dashboard_data/checkpoint") \
        .start()

    file_query.awaitTermination()

if __name__ == "__main__":
    main()