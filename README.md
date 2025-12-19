# Real-Time Streaming Analytics Pipeline (Kafka + Spark Structured Streaming)

## Overview

This project implements an **end-to-end real-time data streaming pipeline** using **Apache Kafka** and **Apache Spark Structured Streaming**, with analytical results persisted into **PostgreSQL**.

The pipeline simulates user activity events (product views, purchases, add_to_cart.), processes them in real time using **event-time windowing and watermarking**, and generates **business-ready analytics**.

This project demonstrates **production-oriented data engineering practices**, including:
- Stream ingestion
- Event-time processing
- Windowed aggregations
- Parallel streaming queries
- Structured data persistence

---

## Architecture Diagram

![Architecture Diagram](https://github.com/mkmasudrana806/End-to-End-Real-Time-Streaming-Data-Pipeline/blob/main/Real%20Time%20Streaming%20Architecture%20Diagram.png)
---

## Tech Stack

- **Event Producer**: kafka-python
- **Message Broker**: Apache Kafka (KRaft mode)
- **Stream Processing**: Apache Spark Structured Streaming
- **Database**: PostgreSQL
- **Containerization**: Docker & Docker Compose

---

## Project Structure

```text
RealTimeStreamingPipeline/
│
├── producer/
│ ├── Dockerfile
│ ├── requirements.txt
│ └── event_producer.py
│
├── spark/
│ ├── Dockerfile
│ └── streaming_jobs/
│       └── read_kafka_stream.py
│
├── sql/
│ └── create_tables.sql
│
└── docker-compose.yml
```



---

## Data Flow Explanation

### 1. Event Production
- A Python producer continuously generates user events:
  - `product_view`
  - `add_to_cart`
  - `purchase`
- Events are serialized as JSON and sent to Kafka topic `user_events`.

---

### 2. Kafka
- Kafka runs in **KRaft mode** (no ZooKeeper).

---

### 3. Spark Structured Streaming
- Spark reads Kafka events in real time.
- JSON payloads are parsed into structured columns.
- Event-time is extracted and converted to timestamp.
- Watermarking handles late-arriving events.

---

### 4. Analytics Computed

#### A. Product View Window (2 minutes)
**Question answered:**  
Which products are getting the most views?

**Output columns:**
- window_start
- window_end
- product_id
- view_count

---

#### B. Category Activity Window (2 minutes)
**Question answered:**  
Which categories are most active?

**Output columns:**
- window_start
- window_end
- category
- event_count

---

#### C. Category Revenue Window (2 minutes)
**Question answered:**  
How much revenue is generated per category?

**Output columns:**
- window_start
- window_end
- category
- total_revenue

---

### 5. PostgreSQL Sink
- Spark uses `foreachBatch` to write aggregated results.
- Each metric is stored in a **separate analytics table**.
- This design is **warehouse-friendly** and BI-ready.

---

## SQL Schema (Run Once in Postgres Database)

- Please find /sql/create_tables.sql file

---

## How to Run the Project

This section explains how to run the complete real-time streaming pipeline locally using Docker.

---

### Prerequisites

Make sure the following are installed on your system:

- Docker 
- Docker Compose 
- Git

---

### Step 1: Clone the Repository

- Open Command Prompt
  
```bash
git clone [https://github.com/mkmasudrana806/End-to-End-Real-Time-Streaming-Data-Pipeline.git
cd End-to-End-Real-Time-Streaming-Data-Pipeline
```

### Step 2: Start All Services

- Start Kafka, Spark, PostgreSQL, and the producer using Docker Compose:

```bash
docker compose up -d
```
- Verify all services are running

```bash
docker compose ps
```
You should see 'kafka-producer', 'spark', 'postgres', 'broker'

### Step 3: Verify Kafka Producer

```bash
docker compose logs -f producer
```

You should see 'Sent event: {'event_id': '...', 'event_type': 'product_view', ...}' and so on


### Step 4: Run Spark Structured Streaming Job

```bash
docker exec -it spark /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.postgresql:postgresql:42.7.3 \
  /opt/spark-apps/read_kafka_stream.py
```

### Step 6: Verify Data in PostgreSQL

- at least run this command in postgres client
- or using docker 'docker exec -it postgres psql -U airflow -d airflow'
  
```bash
select * from product_view_window order by window_start desc limit 10;
```


### Step 7: Stop the Project

```bash
docker compose down
```

---

## Key Engineering Concepts Demonstrated

- Kafka-based event streaming
- Spark Structured Streaming
- Event-time windowing
- Watermark handling
- Parallel streaming queries
- Idempotent database writes
- Dockerized data infrastructure
