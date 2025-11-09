# Substation Meter Data Pipeline

A real-time IoT data engineering pipeline simulating electricity meter data from Tamil Nadu substations. Built with MQTT, Kafka, PySpark, MongoDB, and Docker for ingestion, streaming, processing, and analytics. Ideal for showcasing Data Engineering skills in interviews.

## Project Overview

- **Domain:** IoT Electricity Meters (e.g., substation monitoring with 15-min data pushes).

- **Tech Stack:** Python, MQTT (Mosquitto), Kafka, PySpark (Structured Streaming), MongoDB, Docker Compose, Looker Studio.

- **Features:**

  - Simulate meter data generation and MQTT publishing.

  - Bridge MQTT to Kafka with retry logic.

  - PySpark ELT: Aggregate/filter streaming data, sink to MongoDB.

  - Dashboards for energy trends and alerts.

- **Why?** Demonstrates end-to-end streaming ETL for scalable IoT analytics.

## Architecture

```
┌─────────────────┐
│  Meter Simulator│
│   (Python)      │
└────────┬────────┘
         │ MQTT
         ▼
┌─────────────────┐
│  MQTT Broker    │
│  (Mosquitto)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ MQTT→Kafka      │
│    Bridge       │
│   (Python)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Kafka Broker   │
│  (Apache Kafka) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ PySpark Stream  │
│   Processor     │
│ (Structured     │
│  Streaming)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    MongoDB      │
│   (Database)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Looker Studio  │
│  (Dashboards)   │
└─────────────────┘
```

## Setup and Run (Local)

1. Clone repo: `git clone https://github.com/yourusername/substation-meter-stream.git`

2. Navigate: `cd substation-meter-stream`

3. Start infra: `docker-compose up -d` (starts MQTT, Kafka, MongoDB).

4. Run bridge: `python src/mqtt_to_kafka_bridge.py`

5. Run simulator: `python src/meter_data_simulator.py`

6. Run PySpark: `spark-submit src/pyspark_processor.py` (add jars as needed).

7. Connect Looker Studio to MongoDB (localhost:27017).

8. View dashboards in Looker.

## Data Flow

- Generator → MQTT → Kafka → PySpark → MongoDB → Looker.

## Demo

[Embed video or GIF of pipeline running.]

## Learnings

- Handled streaming with fault tolerance.

- Optimized for interviews: PySpark for big data, Docker for reproducibility.

## License

MIT | Built by [Your Name], Data Analyst transitioning to DE.

