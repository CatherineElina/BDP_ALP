# BDP_ALP — US Stock Market Historical OHLCV Analytics Pipeline

## Big Data Processing Final Project

## Project Status

Current progress:

- [x] Public GitHub repository initialized
- [x] Final dataset selected
- [x] Dataset downloaded from Kaggle
- [x] Dataset size validated
- [x] Dataset schema validated
- [x] Normalized sample dataset generated
- [x] Docker Compose infrastructure setup
- [ ] HDFS data storage setup
- [x] Spark batch processing
- [x] Kafka producer simulation
- [x] Spark Structured Streaming
- [x] Streamlit dashboard
- [ ] Final findings and documentation

<!-- - [x] Public GitHub repository initialized
- [x] Final dataset selected
- [x] Dataset downloaded from Kaggle
- [x] Dataset size validated
- [x] Dataset schema validated
- [x] Normalized sample dataset generated
- [ ] Docker Compose infrastructure setup
- [ ] HDFS data storage setup
- [ ] Spark batch processing
- [ ] Kafka producer simulation
- [ ] Spark Structured Streaming
- [ ] Streamlit dashboard
- [ ] Final findings and documentation -->

---

## Final Dataset

This project uses the following final dataset:

**US Stock Market Historical OHLCV**  
Source: Kaggle  
Dataset file: `stock_prices_daily.csv`

### Dataset Validation Result

| Item | Result |
|---|---:|
| Raw CSV size | 33.03 MB |
| Minimum required dataset size | 10 MB |
| Size requirement status | Passed |
| Total records | 184,138 rows |
| Original number of columns | 11 |

### Original Dataset Columns

```text
date
ticker
company_name
sector
industry
open
high
low
close
adj_close
volume
```

The original Kaggle dataset uses the column `company_name`. For consistency with the project design, this column is normalized into `company` during preprocessing.

### Normalized Sample Data Columns

```text
date
ticker
company
sector
industry
open
high
low
close
adj_close
volume
daily_return_pct
price_range
```

Additional derived columns:

| Column | Description |
|---|---|
| `daily_return_pct` | Percentage change from open price to close price |
| `price_range` | Difference between daily high and daily low |

A normalized sample containing 1,000 rows is included in:

```text
data/sample/sample_stock_data.csv
```

The full raw dataset is not stored in GitHub. It can be downloaded using:

```bash
./scripts/download_dataset.sh
```

---

## Domain

Finance / Stock Market Analytics

## Project Description

This project builds an end-to-end big data pipeline for analyzing historical United States stock market data. Historical OHLCV records are processed through batch analytics, while the same records are replayed sequentially to simulate incoming stock market events for streaming analysis.

## Problem Statement

How can a big data pipeline analyze historical stock performance by company and sector while also monitoring simulated stock market activity in real time?

## Planned Batch Insights

1. Identify the sector with the highest average trading volume.
2. Identify companies with the highest average daily return.
3. Identify companies with the highest volatility based on daily return variation.

## Planned Real-Time Metrics

1. Number of stock events received through Kafka.
2. Latest close price per company during replay.
3. Top companies based on accumulated streaming volume.

## Planned Technology Stack

- Docker Compose
- Apache Hadoop HDFS
- Apache Kafka
- Apache Spark
- Spark Structured Streaming
- Streamlit
- Python

## Planned Pipeline Architecture

```text
Kaggle CSV Dataset
        |
        +----> HDFS Storage ----> Spark Batch Processing ----> Batch Output
        |
        +----> Python Producer ----> Kafka ----> Spark Structured Streaming
                                                     |
                                                     v
                                             Streamlit Dashboard
```

## Dataset Download

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install kaggle pandas
```

Login to Kaggle:

```bash
kaggle auth login
```

Download the final dataset:

```bash
chmod +x scripts/download_dataset.sh
./scripts/download_dataset.sh
```

The downloaded raw CSV file will be stored locally in:

```text
data/raw/stock_prices_daily.csv
```

The raw CSV file is ignored by Git and is not uploaded to this public repository.

## How to Run
### 1. Clone the Repository
```bash
git clone https://github.com/CatherineElina/BDP_ALP.git
cd BDP_ALP
```
### 2. Start Docker Services
Run all services using Docker Compose:
```bash
docker compose up
```
### 3. Run Kafka Producer
Open a new terminal:
```bash
python producer/producer.py
```
Example producer output:

Sent: AAPL @ 192.45
Sent: MSFT @ 415.22
Sent: NVDA @ 120.31
The producer simulates live stock market events by replaying historical OHLCV records into the Kafka topic stock-events.
### 4. Run Spark Structured Streaming
Open another terminal:
```bash
docker exec -it bdp-alp-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 /opt/spark/jobs/streaming_job.py
```
Example Spark output:

Batch: 17
+--------------------+----------+---------+-----------+
|window              |sector    |avg_close|event_count|
+--------------------+----------+---------+-----------+
This streaming job consumes Kafka events and continuously aggregates stock market data by sector.
### 5. Run Batch Analysis
```bash
docker exec -it bdp-alp-spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/batch_analysis.py
```
### 6. Open the Dashboards
Service	URL
Kafka UI:               http://localhost:8080
Spark Master UI:	http://localhost:8082
Streamlit Dashboard:	http://localhost:8501
The Streamlit dashboard will automatically update as streaming data is processed.