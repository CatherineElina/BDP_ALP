from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, stddev, col, round

def main():
    spark = SparkSession.builder \
        .appName("StockBatchAnalysis") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.csv(
        "/opt/spark/data/stock_prices_daily.csv",
        header=True,
        inferSchema=True
    )

    print("\n=== Schema ===")
    df.printSchema()
    print(f"Total rows: {df.count()}")

    print("\n=== Average Closing Price by Sector ===")
    df.groupBy("Sector") \
        .agg(round(avg("Close"), 2).alias("avg_close")) \
        .orderBy(col("avg_close").desc()) \
        .show()

    print("\n=== Top 5 Most Volatile Stocks ===")
    df.groupBy("Ticker", "Company_Name", "Sector") \
        .agg(round(stddev("Close"), 2).alias("price_stddev")) \
        .orderBy(col("price_stddev").desc()) \
        .limit(5) \
        .show()

    print("\n=== Average Daily Volume by Sector ===")
    df.groupBy("Sector") \
        .agg(round(avg("Volume"), 0).alias("avg_volume")) \
        .orderBy(col("avg_volume").desc()) \
        .show()

    spark.stop()

if __name__ == "__main__":
    main()