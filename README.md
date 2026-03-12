# Kafka + Spark Structured Streaming (Docker) Setup Guide

This guide explains how to set up a **local streaming pipeline** using:

* Apache Kafka (message broker)
* Apache Spark Structured Streaming (stream processing)
* Delta Lake (data storage)
* Docker containers

The system runs Kafka and Spark locally in Docker and supports:

* Producers running on the **host machine**
* Spark consumers running **inside Docker**

---

# Architecture

```
Python Producer (Host)
        ↓
Kafka Broker (Docker)
        ↓
Spark Structured Streaming
        ↓
Delta Lake Storage
```

Kafka uses **two listeners**:

```
Host → localhost:29092
Docker containers → kafka:9092
```

---

# Prerequisites

## Install Docker

Verify installation:

```
docker --version
docker compose version
```

Start Docker Desktop before continuing.

---

## Install OpenJDK (Recommended)

Spark requires Java.

```
brew install openjdk
```

Add Java to PATH:

```
echo 'export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

Verify:

```
java -version
```

---

# Project Structure

```
kafka-spark-docker/
│
├── docker-compose.yml
└── README.md
```

---

# Start the Cluster

Start all services:

```
docker compose up -d
```

Verify containers:

```
docker ps
```

Expected containers:

```
zookeeper
kafka
spark-master
spark-worker
```

---

# Access Spark UI

Open in browser:

```
http://localhost:8080
```

You should see the Spark master dashboard.

---

# Create Kafka Topic

Create the streaming topic:

```
docker exec -it kafka kafka-topics \
--create \
--topic beauty_events \
--bootstrap-server localhost:29092 \
--partitions 1 \
--replication-factor 1
```

Verify topics:

```
docker exec -it kafka kafka-topics \
--list \
--bootstrap-server localhost:29092
```

Expected output:

```
beauty_events
```

---

# Verify Kafka Producer (Messages arriving)

Check offsets increasing:

```
docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell \
--broker-list localhost:29092 \
--topic beauty_events
```

Example output:

```
beauty_events:0:25
```

This means **25 events were produced**.

---

# Verify Kafka Consumer

Check messages manually:

```
docker exec -it kafka kafka-console-consumer \
--topic beauty_events \
--bootstrap-server localhost:29092 \
--from-beginning
```

You should see JSON events printed to the terminal.

---

# Login to Spark Container

Open Spark container shell:

```
docker exec -it spark-master bash
```

---

# Reset Spark Streaming Checkpoint (Important)

If restarting the streaming job, remove the checkpoint:

```
rm -rf /tmp/delta/events_checkpoint
```

This prevents Spark from resuming a previous failed stream.

---

# Start PySpark with Kafka + Delta Support

Inside the Spark container run:

```
/opt/spark/bin/pyspark \
--packages io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.jars.ivy=/tmp/.ivy2
```

Explanation:

| Configuration        | Purpose                     |
| -------------------- | --------------------------- |
| delta-spark          | Delta Lake support          |
| spark-sql-kafka      | Kafka connector             |
| spark.sql.extensions | Enable Delta features       |
| spark.sql.catalog    | Delta catalog               |
| spark.jars.ivy       | Fix Docker permission issue |

You should see:

```
SparkSession available as 'spark'
```

---

# Run the Streaming Job

Once PySpark starts, run your streaming code to:

1. Read from Kafka topic `beauty_events`
2. Parse JSON events
3. Print decoded records
4. Write events to Delta Lake

Delta storage path:

```
/tmp/delta/events
```

Checkpoint directory:

```
/tmp/delta/events_checkpoint
```

---

# Stop the Cluster

Stop all containers:

```
docker compose down
```

---

# Useful Commands

### List running containers

```
docker ps
```

---

### View Kafka logs

```
docker logs kafka
```

---

### Enter Kafka container

```
docker exec -it kafka bash
```

---

### Enter Spark container

```
docker exec -it spark-master bash
```

---

# Real-World Streaming Architecture

Typical production pipeline:

```
Applications / APIs
        ↓
Kafka
        ↓
Spark Structured Streaming
        ↓
Delta Lake
        ↓
Analytics / ML / BI
```

Common use cases:

* clickstream analytics
* fraud detection
* monitoring pipelines
* recommendation systems
