# Big Data Processing Final Project — US Stock Market Historical OHLCV Analytics Pipeline

## Table of Contents
* [Final Dataset](#final-dataset)
* [Architecture Diagram](#architecture-diagram)
* [Domain](#domain)
* [Project Description](#project-description)
* [Problem Statement](#problem-statement)
* [Project Objectives](#project-objectives)
* [Final Dataset](#final-dataset)
* [Architecture Diagram](#architecture-diagram)
* [Technology Stack](#technology-stack)
* [Pipeline Architecture](#pipeline-architecture)
* [Planned Batch Insights](#planned-batch-insights)
* [Planned Real-Time Metrics](#planned-real-time-metrics)
* [How to Run](#how-to-run)
* [Streamlit Dashboard](#streamlit-dashboard)
* [Findings & Conclusion](#findings--conclusion)
* [Known Limitations](#known-limitations)
* [Contributors](#contributors)

---

## Final Dataset

This project uses the following final dataset:

**US Stock Market Historical OHLCV**
Source: Kaggle
Dataset file: `stock_prices_daily.csv`

### Dataset Validation Result

The dataset was validated prior to processing. Validation checks included missing value detection, duplicate record detection, data type verification, and dataset size verification. No significant data quality issues were identified.

| Validation Metric | Target Baseline | Actual Result | Verification Status |
| --- | --- | --- | --- |
| Dataset File Size | Minimum 10 MB | 33.03 MB | ✅ Passed |
| Total Row Count | Minimum 10,000 rows | 184,138 rows | ✅ Passed |
| Missing Values | Zero missing fields | 0% (0 out of 184,138 rows) | ✅ Clean |
| Duplicate Records | Zero duplicate rows | 0 duplicate records | ✅ Clean |
| Structural Columns | Match expected schema | 11 data columns | ✅ Verified |
| Schema Data Types | Match transactional types | Verification successful | ✅ Verified |

### Dataset Columns

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

### Dataset Description

The dataset contains historical daily OHLCV (Open, High, Low, Close, Volume) stock market records from major publicly traded companies in the United States.

Several key fields are heavily used throughout the analytics pipeline:

| Column   | Description                                                 |
| -------- | ----------------------------------------------------------- |
| `ticker` | Unique stock symbol used to identify each company           |
| `sector` | Company business sector used for aggregation and comparison |
| `open`   | Stock opening price for a trading day                       |
| `close`  | Stock closing price for a trading day                       |
| `high`   | Highest trading price during the day                        |
| `low`    | Lowest trading price during the day                         |
| `volume` | Total traded shares during the day                          |

---

## Architecture Diagram

```text
                           +----------------------+
                           | Kaggle CSV Dataset   |
                           | stock_prices_daily   |
                           +----------+-----------+
                                      |
                                      v
                        +---------------------------+
                        | Docker Volume / Local CSV |
                        +-------------+-------------+
                                      |
                                      v
                     +--------------------------------+
                     | Hadoop HDFS (NameNode/DataNode)|
                     | hdfs://namenode:9000/data/... |
                     +---------------+----------------+
                                     |
                    +----------------+----------------+
                    |                                 |
                    v                                 v

       +--------------------------+     +--------------------------+
       | Spark Batch Processing   |     | Kafka Producer Simulator |
       | batch_analysis.py        |     | producer.py              |
       +-------------+------------+     +-------------+------------+
                     |                                |
                     v                                v
          +---------------------+         +----------------------+
          | Batch Analytics     |         | Apache Kafka         |
          | Sector Aggregation  |         | stock-events topic   |
          +---------------------+         +-----------+----------+
                                                      |
                                                      v
                                   +----------------------------------+
                                   | Spark Structured Streaming       |
                                   | streaming_job.py                 |
                                   +----------------+-----------------+
                                                    |
                                                    v
                                   +----------------------------------+
                                   | Streamlit Dashboard              |
                                   | Real-Time Visualization          |
                                   +----------------------------------+
```

---

## Domain

Finance / Stock Market Analytics

## Project Description

This project builds an end-to-end big data pipeline for analyzing historical United States stock market data. Historical OHLCV records are processed through distributed batch analytics using Apache Spark and Hadoop HDFS, while the same records are replayed sequentially to simulate incoming stock market events for streaming analysis using Apache Kafka and Spark Structured Streaming.

The project demonstrates the integration of modern big data technologies for scalable data ingestion, distributed storage, batch processing, real-time streaming analytics, and interactive dashboard visualization.

## Problem Statement

How can a scalable big data pipeline leverage distributed storage and parallel computing to analyze historical stock performance across companies and sectors, while simultaneously processing real-time transaction event streams to detect liquidity anomalies and monitor capital rotation?

## Project Objectives

1. Implement distributed data storage to ingest and store large-scale US stock market historical OHLCV data using Hadoop Distributed File System (HDFS) to ensure fault tolerance and high availability.
2. Conduct distributed historical analytics by performing scalable batch analysis with Apache Spark to compute long-term sector averages, historical price standard deviations, and company return benchmarks.
3. Create a real-time event stream simulation using an event-driven message pipeline with Apache Kafka to replay historical trading records sequentially as simulated live stock market transactions.
4. Execute near real-time stream processing using Spark Structured Streaming to consume the simulated Kafka events, perform real-time feature engineering (daily return calculation), and identify trading volume anomalies.
5. Develop interactive analytical visualization by building a high-performance single-page Streamlit dashboard to visualize macro-level market pulses, real-time sector capital rotation, leaderboard momentum, and micro-level ticker indicators.

## Planned Batch Insights
The batch processing layer utilizes Apache Spark to extract four key dimensional insights from the historical market records stored in HDFS

1. Sector Liquidity Concentrations by identifying which business sectors attract the highest average daily trading volume to map where long term market liquidity and activity are historically clustered.
2. Historical Sector Valuations by calculating average price levels per sector based on historical closing prices to understand the valuation baselines across different industries.
3. Market Risk Map or Volatility Classification by measuring historical price fluctuations using the sample standard deviation of closing prices over time to identify and categorize equities by their risk characteristics.
4. Long-Term Performance Leaders by computing the average daily returns of individual companies to discover which equities consistently outperformed their peer groups over the observed period.

## Planned Real-Time Metrics
The Streamlit dashboard transforms the active Spark streaming logs into five structured live analytical workspaces

1. Market Pulse Matrix or Macro KPIs which calculates key session metrics to monitor general market health
2. Sector Performance and Capital Flow Heatmap using an interactive Plotly Treemap that sizes squares by volume and color grades by return percentage alongside horizontal bar charts to dynamically track sector rotation and institutional capital migration.
3. Momentum Leaders and Anomaly Detection
4. Interactive Ticker Technical Explorer which displays a micro-analysis workbench visualizing synchronized Plotly Candlestick charts integrated with 5-day and 20-day Moving Averages alongside an overlaid Bollinger Bands volatility channel with all axis controls locked using fixedrange=True to maintain scale stability.
5. Streaming Telemetry Log Layer displaying a transparent tabular view of the 20 most recent raw JSON records parsed and enriched by the Spark Structured Streaming engine to prove pipeline telemetry and processing validity.

## Technology Stack

* Docker Compose
* Apache Hadoop HDFS
* Apache Kafka
* Apache Spark
* Spark Structured Streaming
* Streamlit
* Python

## Pipeline Architecture

```text
Kaggle CSV Dataset
        |
        v
Hadoop HDFS Storage
        |
        +----> Spark Batch Processing ----> Batch Analytics Output
        |
        +----> Python Kafka Producer ----> Apache Kafka
                                                 |
                                                 v
                                  Spark Structured Streaming
                                                 |
                                                 v
                                      Streamlit Dashboard
```

---

<!-- # Dataset Download

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

---

# How to Run

## 1. Clone the Repository

```bash
git clone https://github.com/CatherineElina/BDP_ALP.git
cd BDP_ALP
```

---

## 2. Start Docker Services

Run all services using Docker Compose:

```bash
docker compose up
```

This will start:

* Hadoop NameNode
* Hadoop DataNode
* Apache Kafka
* Spark Master
* Spark Worker
* Streamlit Dashboard

---

## 3. Upload Dataset to HDFS

### Create HDFS directory

```bash
docker exec -it bdp-alp-namenode hdfs dfs -mkdir -p /data/stock
```

### Copy dataset into NameNode container

```bash
docker cp data/stock_prices_daily.csv bdp-alp-namenode:/tmp/stock_prices_daily.csv
```

### Upload dataset from container into HDFS

```bash
docker exec -it bdp-alp-namenode hdfs dfs -put /tmp/stock_prices_daily.csv /data/stock/
```

### Verify dataset in HDFS

```bash
docker exec -it bdp-alp-namenode hdfs dfs -ls /data/stock/
```

Expected output:

```text
Found 1 items
-rw-r--r--   1 root supergroup ... /data/stock/stock_prices_daily.csv
```

---

## 4. Run Spark Batch Analysis

Open a new terminal:

```bash
docker exec -it bdp-alp-spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/batch_analysis.py
```

The batch analysis job reads the dataset directly from Hadoop HDFS:

```python
hdfs://namenode:9000/data/stock/stock_prices_daily.csv
```

---

## 5. Run Kafka Producer

Open another terminal:

```bash
python producer/producer.py
```

Example producer output:

```text
Sent: AAPL @ 192.45
Sent: MSFT @ 415.22
Sent: NVDA @ 120.31
```

The producer simulates live stock market events by replaying historical OHLCV records into the Kafka topic `stock-events`.

---

## 6. Run Spark Structured Streaming

Open another terminal:

```bash
docker exec -it bdp-alp-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 /opt/spark/jobs/streaming_job.py
```

Example Spark output:

```text
[Batch 1] Successfully appended 752 engineered records to historical log
[Batch 2] Successfully appended 183 engineered records to historical log
[Batch 3] Successfully appended 564 engineered records to historical log
```

This streaming job consumes Kafka events and continuously aggregates stock market data by sector.

---

## 7. Open the Dashboards

| Service             | URL                   |
| ------------------- | --------------------- |
| Hadoop NameNode UI  | http://localhost:9870 |
| Kafka UI            | http://localhost:8080 |
| Spark Master UI     | http://localhost:8082 |
| Streamlit Dashboard | http://localhost:8501 |

The Streamlit dashboard will automatically update as streaming data is processed. -->

# How to Run

Silakan ikuti langkah-langkah di bawah ini secara berurutan untuk menjalankan seluruh pipeline dari awal.

## 1. Clone the Repository

```bash
git clone https://github.com/CatherineElina/BDP_ALP.git
cd BDP_ALP
```

---

## 2. Environment Setup & Dependency Installation

> ⚠️ **PENTING:** Karena setiap sistem memiliki path Python yang berbeda, **jangan gunakan folder `.venv` bawaan repository** jika ter-clone. Anda wajib membuat Virtual Environment baru di laptop sendiri untuk menghindari error interpreter path (*"executable not found"*).

### Langkah Setup Environment:

```bash
# 1. Hapus folder .venv lama jika ada (opsional)
# Windows (PowerShell): Remove-Item -Recurse -Force .venv
# Linux/Mac: rm -rf .venv

# 2. Buat virtual environment baru 
python3 -m venv .venv

# 3. Aktifkan virtual environment
# Di Windows (PowerShell):
.venv\Scripts\Activate.ps1
# Di Linux/Mac:
source .venv/bin/activate

# 4. Install seluruh dependencies proyek
pip install -r producer/requirements.txt
pip install kaggle pandas
```

---

## 3. Download Dataset

Sebelum menjalankan service, unduh dataset resmi terlebih dahulu:

1. Pastikan Anda sudah login atau memiliki kredensial Kaggle di laptop Anda:

```bash
kaggle auth login
```

2. Jalankan script unduhan:


**Bagi Pengguna Linux / Mac / Windows Git Bash:**
```bash
chmod +x scripts/download_dataset.sh
./scripts/download_dataset.sh
```

**Bagi Pengguna Windows (PowerShell):**

```bash
kaggle datasets download -d asadullahcreative/us-stock-market-historical-ohlcv-dataset -p data/raw --unzip
```

Dataset mentah berbentuk CSV akan tersimpan secara lokal di folder `data/raw/stock_prices_daily.csv`. File ini sudah otomatis diabaikan oleh `.gitignore` agar tidak mengotori repositori GitHub.

---

## 4. Start Docker Services

Sebelum menjalankan perintah di bawah ini, **pastikan aplikasi Docker Desktop sudah dibuka dan sedang berjalan (running)** di laptop Anda. Jika Docker Desktop belum aktif, perintah di bawah ini akan memicu error *"docker daemon is not running"*.

Setelah Docker Desktop dipastikan aktif, jalankan seluruh infrastruktur big data menggunakan Docker Compose:
```bash
docker compose up -d
```

>💡 Informasi: Parameter -d (detached mode) digunakan agar seluruh service berjalan di latar belakang (background), sehingga terminal ini tidak terkunci dan tetap bisa Anda gunakan untuk langkah berikutnya.

Perintah ini akan menyalakan service berikut di latar belakang:

* Hadoop NameNode & DataNode
* Apache Kafka
* Apache Spark Master & Worker
* Streamlit Dashboard

### Troubleshooting: Port Already in Use & Checkpoint Inconsistency

Saat menjalankan docker compose up -d, Anda mungkin mengalami error port bentrok atau situasi di mana Spark Structured Streaming job tiba-tiba berhenti memperbarui dashboard dengan benar. 

Masalah visualisasi yang macet ini biasanya dipicu karena direktori checkpoint dan berkas log historis mengandung metadata usang dari sesi sebelumnya, yang menyebabkan Spark mengalami kendala berikut
* Skip incoming records
* Continue from outdated offsets
* Produce duplicated records
* Stop updating dashboard outputs

Berikut adalah opsi solusi yang dapat dilakukan untuk menangani kendala-kendala di atas

**Opsi A: Mematikan Proses yang Menggunakan Port**

Anda dapat menghentikan proses yang memonopoli port target (misal port 8501 untuk Streamlit, atau 8080, 8082, 9870).

- Bagi Pengguna Linux / Mac / Windows Git Bash:
```bash
# Bunuh proses yang menduduki port 8501 seketika
sudo kill -9 $(sudo lsof -t -i:8501)
```
- Bagi Pengguna Powershell:
```bash
# 1. Cari PID dari proses yang memonopoli port 8501
Get-Process -Id (Get-NetTCPConnection -LocalPort 8501).OwningProcess

# 2. Hentikan proses tersebut berdasarkan PID yang didapat (misal PID: 1234)
Stop-Process -Id 1234 -Force
```

**Opsi B: Mengubah Konfigurasi Port Binding di Docker Compose**

ika port tersebut memang mutlak harus digunakan oleh aplikasi lain, Anda bisa mengalihkan binding port internal Docker ke port eksternal baru di komputer Anda.

1. Buka file docker-compose.yaml.
2. Cari service yang bermasalah (misalnya streamlit-dashboard pada port 8501).
3. Ubah kolom konfigurasi ports dari "8501:8501" menjadi "8502:8501".
4. Sekarang dashboard Anda dapat diakses melalui URL baru: http://localhost:8502.

**Opsi C: Force Clean Gantung Container Docker Lama**

Terkadang container dari sesi pengujian sebelumnya belum mati dengan sempurna di Docker daemon. Selesaikan dengan siklus pembersihan paksa ini:

```bash
# 1. Matikan dan hapus semua container yang terhubung ke compose ini
docker compose down --remove-orphans

# 2. Hapus container sisa yang menggantung (jika ada)
docker system prune -f

# 3. Jalankan kembali service
docker compose up -d
```
---

## 5. Upload Dataset to Hadoop HDFS

Salin dataset lokal yang baru diunduh ke dalam ekosistem penyimpanan terdistribusi HDFS:

### Create HDFS directory

```bash
docker exec -it bdp-alp-namenode hdfs dfs -mkdir -p /data/stock
```

### Copy dataset into NameNode container

```bash
docker cp data/raw/stock_prices_daily.csv bdp-alp-namenode:/tmp/stock_prices_daily.csv
```
### Upload dari container ke Hadoop HDFS

```bash
docker exec -it bdp-alp-namenode hdfs dfs -put /tmp/stock_prices_daily.csv /data/stock/
```

### Verify dataset in HDFS

```bash
docker exec -it bdp-alp-namenode hdfs dfs -ls /data/stock/
```

**Expected Output:**

```text
Found 1 items
-rw-r--r--   1 root supergroup ... /data/stock/stock_prices_daily.csv
```

---

## 6. Run Spark Batch Analysis

Buka terminal baru, **aktifkan virtual environment Anda terlebih dahulu**, lalu jalankan job analisis batch historis:

```bash
# Aktifkan .venv di terminal baru ini sebelum menjalankan command (jika belum aktif)
# Windows (PowerShell): .venv\Scripts\Activate.ps1
# Linux/Mac: source .venv/bin/activate

# Jalankan Spark Batch Job
docker exec -it bdp-alp-spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/batch_analysis.py
```

>💡 Informasi: Perintah docker exec di atas berjalan langsung di dalam isolated container, sehingga proses internal Docker tidak membutuhkan aktivasi .venv lokal Anda. Namun, pastikan virtual environment lokal Anda tetap aktif di terminal ini untuk menjaga konsistensi environment project Anda.

Job ini akan memproses data langsung dari HDFS (hdfs://namenode:9000/data/stock/stock_prices_daily.csv) dan mencetak metrik agregasi di konsol.

**Expected Output (Spark Batch Analytics Console Logs):**

```bash
=== Schema ===
root
 |-- Date: timestamp (nullable = true)
 |-- Ticker: string (nullable = true)
 |-- Company_Name: string (nullable = true)
 |-- Sector: string (nullable = true)
 |-- Industry: string (nullable = true)
 |-- Open: double (nullable = true)
 |-- High: double (nullable = true)
 |-- Low: double (nullable = true)
 |-- Close: double (nullable = true)
 |-- Adj_Close: double (nullable = true)
 |-- Volume: integer (nullable = true)
 |-- daily_return_pct: double (nullable = true)

Total rows: 184138

=== Average Closing Price by Sector ===
+--------------------+---------+
|              Sector|avg_close|
+--------------------+---------+
|          Healthcare|   247.28|
|  Financial Services|   197.98|
|         Industrials|   194.14|
|          Technology|   177.51|
|   Consumer Cyclical|   177.08|
|  Consumer Defensive|   163.57|
|     Basic Materials|   153.75|
|Communication Ser...|   110.05|
|              Energy|    77.97|
+--------------------+---------+

=== Top 5 Most Volatile Stocks ===
+------+--------------------+--------------------+------------+
|Ticker|        Company_Name|              Sector|price_stddev|
+------+--------------------+--------------------+------------+
|   LLY|Eli Lilly and Com...|          Healthcare|       288.8|
|  COST|Costco Wholesale ...|  Consumer Defensive|      240.16|
|   BLK|     BlackRock, Inc.|  Financial Services|      191.29|
|  META|Meta Platforms, Inc.|Communication Ser...|      183.22|
|    GS|The Goldman Sachs...|  Financial Services|      181.65|
+------+--------------------+--------------------+------------+

=== Average Daily Volume by Sector ===
+--------------------+-----------+
|              Sector| avg_volume|
+--------------------+-----------+
|          Technology|4.5870624E7|
|Communication Ser...|3.0883702E7|
|   Consumer Cyclical|2.7396078E7|
|              Energy|  9726925.0|
|  Financial Services|  8848670.0|
|  Consumer Defensive|  7819863.0|
|          Healthcare|  6171474.0|
|     Basic Materials|  5080293.0|
|         Industrials|  4141391.0|
+--------------------+-----------+

=== Top 5 Companies by Average Daily Return ===
+------+--------------------+--------------+
|Ticker|        Company_Name|avg_return_pct|
+------+--------------------+--------------+
|  AAPL|          Apple Inc.|          0.11|
|  CARR|Carrier Global Co...|           0.1|
|  NVDA|  NVIDIA Corporation|           0.1|
|    GS|The Goldman Sachs...|          0.08|
|   NEM| Newmont Corporation|          0.08|
+------+--------------------+--------------+
```

---

## 7. Run Kafka Producer (Stream Simulator)

Buka terminal baru lainnya untuk mulai mensimulasikan data pasar saham secara real-time. Anda wajib mengaktifkan virtual environment pada terminal baru ini karena script produsen berjalan langsung menggunakan Python interpreter lokal di laptop Anda:

```bash
# Wajib aktifkan .venv di terminal baru ini agar library 'kafka-python' / 'pandas' terdeteksi (jika belum aktif)
# Windows (PowerShell): .venv\Scripts\Activate.ps1
# Linux/Mac: source .venv/bin/activate

# Jalankan Python Kafka Producer
python producer/producer.py
```

**Example Producer Output:**

```text
Sent: AAPL @ 192.45
Sent: MSFT @ 415.22
Sent: NVDA @ 120.31
```

Script ini membaca data historis dan mengirimkannya baris demi baris ke Kafka topic `stock-events`.

---

## 8. Run Spark Structured Streaming

Buka terminal baru satu lagi untuk memproses aliran data dari Kafka secara real-time:

```bash
# Aktifkan .venv di terminal baru ini (opsional/untuk konsistensi)
# Windows (PowerShell): .venv\Scripts\Activate.ps1
# Linux/Mac: source .venv/bin/activate

# Jalankan Spark Streaming Job
docker exec -it bdp-alp-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 /opt/spark/jobs/streaming_job.py
```

>⚠️ Catatan Penting: Sama seperti langkah Batch Analysis, perintah docker exec ini mengeksekusi Spark-Submit langsung di dalam Docker container master. Oleh karena itu, perintah ini tidak membutuhkan .venv lokal laptop Anda untuk bekerja, karena Spark akan menggunakan dependensi Java/Scala/Python yang sudah terisolasi di dalam kontainer Docker tersebut.

**Example Spark Output:**

```text
[Batch 1] Successfully appended 752 engineered records to historical log
[Batch 2] Successfully appended 183 engineered records to historical log
```

---

## 9. Open the Dashboards

Sekarang Anda dapat memantau jalannya pipeline dan visualisasi data melalui URL berikut:

| Service | URL |
| --- | --- |
| **Streamlit Dashboard** | http://localhost:8501 |
| Hadoop NameNode UI | http://localhost:9870 |
| Kafka UI | http://localhost:8080 |
| Spark Master UI | http://localhost:8082 |

Dashboard Streamlit akan melakukan *hot-reload* dan memperbarui grafiknya secara otomatis seiring data streaming diproses.


---

## Streamlit Dashboard

The US Stock Market Intelligence Dashboard serves as the final visualization layer of the streaming analytics pipeline, transforming raw stock transactions into actionable market insights through an integrated Macro-to-Micro Analytical Framework. The dashboard is designed as a single-page monitoring console that supports both real-time observation and historical exploration of market behavior.

- Section 1 (Global Controller): Provides a centralized control panel featuring a global Date Range Filter and Sector Filter, allowing analysts to dynamically adjust the analysis horizon and focus on specific market segments without affecting dashboard consistency.
- Section 2 (Market Pulse): Presents key market-wide indicators including Total Trading Volume, Average Market Return, Market Breadth (Advance-Decline Ratio), and Market Volatility. These KPIs offer an immediate snapshot of overall market conditions and sentiment during the selected trading session.
- Section 3 (Sector Performance & Capital Flow): Visualizes capital movement across sectors using interactive charts and a sector heatmap. The dashboard highlights sector-level performance by comparing average returns and transaction volumes, enabling quick identification of outperforming and underperforming industries.
- Section 4 (Market Leaders & Anomaly Detection): Identifies dominant market participants through a ranked leaderboard of actively traded companies and provides anomaly detection capabilities via Top Gainers, Top Losers, and Volume Spike Analysis, helping analysts discover unusual market activities and potential trading opportunities.
- Section 5 (Interactive Company Explorer): Offers a detailed company-level investigation environment through an interactive Plotly Candlestick (OHLC) Visualization enhanced with Moving Average (MA5 and MA20) overlays. Users can explore historical price behavior for any company available in the dataset. To ensure a stable user experience on desktop and tablet devices, chart axes are configured with fixedrange=True, preventing accidental zooming or scrolling interactions.
- Section 6 (Streaming Monitoring Layer): Displays the latest records generated by the Kafka–Spark streaming pipeline, providing transparency into real-time data ingestion and enabling validation of the end-to-end processing workflow.

The dashboard also incorporates an optional Auto Refresh Controller, allowing users to enable or disable automatic updates and customize refresh intervals according to monitoring requirements. This feature ensures efficient real-time observation while avoiding unnecessary interface refreshes during analytical investigations.  
![Streamlit Dashboard](assets/dashboard1.png)
![Streamlit Dashboard](assets/dashboard2.png)
![Streamlit Dashboard](assets/dashboard3.png)

### Streamlit Dashboard Visual Guide

This guide provides a structural breakdown and verification baseline for the live system interface. Graders can utilize these checkpoints to validate that all streaming and visualization mechanisms are performing according to the technical requirements.

#### Visual Breakdown of the 6 Interface Components
1. Section 1 (Global Controller) located within the left sidebar containing the central date range boundaries and sector segment filters. Graders should see all dashboard main panel charts dynamically recalculate without processing delays when these filters are adjusted.
2. Section 2 (Market Pulse) displaying a 4-column balanced grid of live macro KPIs for total market volume, session average return, market breadth, and internal standard deviation volatility risk.
3. Section 3 (Sector Performance and Capital Flow) showcasing an interactive multi-color Plotly Treemap and secondary vertical bar charts to trace active sector rotation. Sectors with dominant liquidation values will dynamically command larger geometric space on the screen.
4. Section 4 (Market Leaders and Anomaly Detection) features split tabular leaderboards indexing the top 10 absolute active market leaders, top session gainers, top session losers, and immediate flags for volume spikes exceeding twice the baseline asset history.
5. Section 5 (Interactive Company Explorer) presenting a detailed technical analysis environment containing synchronized Plotly Candlestick price metrics overlaid with MA5 and MA20 moving averages alongside a transparent Bollinger Bands channel.
6. Section 6 (Streaming Monitoring Layer) displaying a running tabular log at the bottom of the interface containing the 20 most recent raw JSON transactions processed by the streaming infrastructure to prove data pipeline validity.

#### System Expected Behavior
* Update Frequency and Smooth Transitions: Visual components must refresh dynamically every 5 seconds without triggering aggressive screen blanking or layout shifting.
* Interface Stability and Navigation: Search queries and slider parameter shifts should interact instantly without disrupting the active backend Spark Structured Streaming connection.
* Fixed Axis Range Control: The interactive Plotly Candlestick axes must maintain scale lock using fixedrange=True to ensure that automated session updates do not cause unexpected zoom drift while an analyst is inspecting historical intervals.
* Exception Handling and Failsafe Metrics: In the event that the Kafka stream is intentionally paused or stopped, the metric counters must not display Python traceback error messages, but instead hold the last known transaction state with perfect visual stability.

---

# Findings & Conclusion

## Batch Analytics Findings

The Spark batch analytics pipeline successfully processed **184,138 historical US stock market records** stored within Hadoop HDFS and generated several valuable insights regarding sector performance, trading activity, stock volatility, and company-level returns. By leveraging Apache Spark's distributed processing capabilities, large-scale historical market data was analyzed efficiently across multiple dimensions.

The analysis revealed that the **Technology sector** generated the highest average daily trading volume at approximately **45.9 million shares**, significantly exceeding all other sectors. Communication Services and Consumer Cyclical followed with average volumes of approximately **30.9 million** and **27.4 million shares**, respectively. These findings indicate that technology-related companies consistently attracted the highest level of investor participation and market liquidity throughout the observed period.

From a valuation perspective, the **Healthcare sector** recorded the highest average closing stock price at approximately **$247.28**, followed by Financial Services (**$197.98**) and Industrials (**$194.14**). This suggests that companies operating within these sectors generally maintained higher market valuations and stronger price levels compared to other sectors represented in the dataset.

The volatility analysis identified several companies exhibiting substantial price fluctuations over time. **Eli Lilly and Company (LLY)** emerged as the most volatile stock with a price standard deviation of approximately **288.8**, followed by **Costco Wholesale Corporation (COST)**, **BlackRock (BLK)**, **Meta Platforms (META)**, and **Goldman Sachs (GS)**. Elevated volatility levels indicate larger price movements and potentially higher investment risk, while also presenting greater opportunities for short-term trading strategies.

The company performance analysis further showed that **Apple Inc. (AAPL)** achieved the highest average daily return at approximately **0.11%**, closely followed by **Carrier Global Corporation (CARR)** and **NVIDIA Corporation (NVDA)** at approximately **0.10%**. These companies demonstrated relatively consistent positive price appreciation throughout the historical observation period, suggesting strong long-term performance compared to their peers.

Several findings from the batch analytics stage were also consistent with observations from the streaming analytics dashboard. In particular, the dominance of the Technology sector in trading activity and the strong performance of companies such as NVIDIA highlight recurring market patterns observed across both historical and near real-time analyses.

Overall, these batch analytics results demonstrate how distributed big data processing using Apache Spark and Hadoop HDFS can effectively uncover historical market trends, sector dynamics, risk characteristics, and company performance. The findings directly support the project objective of extracting meaningful business insights from large-scale stock market datasets through scalable big data technologies.

## Key Performance Indicators (KPI Summary)

The historical metrics extracted by the Apache Spark batch processing layer establish the baseline performance thresholds for the overall market profile

| Metric Category | Target Indicator Dimension | Top Performing Baseline Value |
| --- | --- | --- |
| Highest Sector Liquidity | Average Daily Trading Volume | Technology Sector with 45.9M Shares |
| Top Sector Price Level | Average Closing Stock Price | Healthcare Sector at 247.28 USD |
| Maximum Market Volatility | Closing Price Standard Deviation | Eli Lilly and Company at 288.8 |
| Top Company Return Performance | Average Daily Return Percentage | Apple Inc at 0.11% |

## Cross-Validation: Batch vs. Streaming Results

The real-time streaming infrastructure provides a mechanism to cross-validate the long term analytical insights extracted during the historical batch processing phase. When the transactional data stream is replayed through Apache Kafka and processed by Spark Structured Streaming, the near real-time telemetry metrics directly confirm the systemic patterns discovered in the batch analysis.

The structural dominance of the technology sector is fully verified as it consistently maintains the highest share transaction counts and dynamic capital accumulation logs on the active live dashboard. Furthermore, individual company momentum tracking shows that high performing equities such as NVIDIA and Apple experience recurring intraday volume expansions and steady price appreciation during stream processing. 

This behavioral consistency across both standalone distributed paradigms proves the computational accuracy of the pipeline feature engineering logic. The cross-validation demonstrates that replaying aggregated big data stores yields consistent structural insights whether evaluated as static historical files in Hadoop HDFS or as live sequential event message queues.

---

## Streaming Analytics Findings

The streaming analytics pipeline successfully simulated real-time stock market activity by replaying historical OHLCV (Open, High, Low, Close, Volume) records through Apache Kafka. Each stock transaction was continuously published as an event stream and consumed by Spark Structured Streaming for real-time processing and feature engineering.

Spark Structured Streaming transformed incoming events into analytical indicators such as daily return and price range before storing the enriched records for downstream visualization. This enabled the dashboard to continuously update market intelligence metrics while preserving the historical transaction sequence for further analysis.

The Streamlit dashboard successfully visualized multiple analytical perspectives, including:

* Market-wide indicators such as Total Trading Volume, Average Market Return, Market Breadth, and Market Volatility.
* Sector-level performance through return and transaction volume analysis.
* Market leadership rankings based on trading activity and performance metrics.
* Top Gainers and Top Losers for identifying the strongest and weakest performing assets.
* Volume Spike detection to identify unusual trading activity compared to historical averages.
* Interactive company-level candlestick analysis enhanced with Moving Average (MA5 and MA20) indicators.
* Live monitoring of the most recent records processed by the streaming pipeline.

During the simulation, the dashboard revealed several meaningful market patterns. Technology and Consumer Cyclical sectors consistently generated the highest trading volumes, indicating strong investor participation. Meanwhile, sector performance varied over time, allowing analysts to observe capital rotation across industries and identify sectors experiencing relative strength or weakness.

The anomaly detection component successfully highlighted stocks experiencing abnormal trading volume compared to their historical behavior. This provided an effective mechanism for identifying unusual market events and potential investment opportunities.

The integration between Apache Kafka, Spark Structured Streaming, Docker Compose, Streamlit, and Hadoop HDFS successfully established an end-to-end big data ecosystem capable of supporting both real-time analytics and historical exploration. The architecture demonstrated how streaming technologies can continuously process, enrich, and visualize stock market information with minimal latency.

These streaming analytics results directly address the project's objective by demonstrating how stock market activity can be monitored, analyzed, and interpreted in near real time using a scalable big data streaming architecture.


---

## Conclusion

This project successfully implemented an end-to-end big data analytics pipeline using Apache Hadoop HDFS, Apache Kafka, Apache Spark, Spark Structured Streaming, Docker Compose, and Streamlit.

The system supports both distributed batch analytics and real-time streaming analytics workflows while processing historical US stock market OHLCV data.

The project successfully answered the main problem statement by demonstrating:

* How historical stock market data can be processed using distributed batch analytics
* How simulated stock market events can be monitored in near real time
* How batch and streaming pipelines can be integrated within one scalable architecture

The final pipeline successfully achieved the main project objectives:

* Distributed data storage using Hadoop HDFS
* Historical stock market batch analysis
* Real-time event streaming simulation
* Streaming aggregation using Spark Structured Streaming
* Interactive live dashboard visualization
* Reproducible deployment using Docker Compose

---

# Known Limitations

* The system uses historical CSV data replay instead of a live stock market API.
* Streaming throughput and scalability were not benchmarked under high-load production conditions.
* The dashboard currently focuses on sector-level aggregation and does not include advanced forecasting or machine learning models.
* Fault tolerance and multi-node distributed deployment were not fully implemented.
* The Kafka producer replays historical records sequentially and does not simulate actual market timing or irregular trading activity.
* The Hadoop HDFS cluster currently uses a single-node replication configuration (`dfs.replication=1`) intended for development and educational purposes rather than production-scale distributed storage.

---

# 👥 Contributors

| Name | NIM |
|------|-----|
| Ruby Arthalia Golden | 0706022310035 |
| Amanda Renata Go | 0706022310010 |
| Catherine Eline Santoso | 0706022310009 |
| Deborah Michelle Kwandinata | 0706022310014 |
| Feylin Christelia | 0706022310012 |

--- 

<!-- 
## Troubleshooting
During the development and testing phase of the streaming pipeline, an issue occasionally occurred where the Spark Structured Streaming job stopped updating the dashboard correctly. This happened because the existing **checkpoint directory** and **historical output files** contained metadata from previous streaming sessions, causing Spark to reuse outdated offsets and state information.

When this situation occurs, the following cleanup procedure can be performed before restarting the streaming job:

### Step 1 — Stop Running Containers

```bash
docker compose down
```

### Step 2 — Remove Previous Checkpoint Data

```bash
docker exec -it bdp-alp-spark-master rm -rf /opt/spark/dashboard_data/checkpoint
```

Or remove it from the mounted host directory:

```bash
rm -rf dashboard_data/checkpoint
```

### Step 3 — Remove Historical Streaming Outputs

```bash
rm dashboard_data/history.jsonl
```

or

```bash
docker exec -it bdp-alp-spark-master rm -f /opt/spark/dashboard_data/history.jsonl
```

### Step 4 — Restart Infrastructure

```bash
docker compose up -d
```

### Step 5 — Restart Streaming Job

```bash
docker exec -it bdp-alp-spark-master \
/opt/spark/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
/opt/spark/jobs/streaming_job.py
```

### Step 6 — Restart Kafka Producer

```bash
python producer/producer.py
```

--- -->

