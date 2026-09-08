# Real-Time Taxi Data Streaming Pipeline with Kafka, Apache Flink, and PostgreSQL

## Overview

This project demonstrates how a real-time data engineering pipeline can be built using **Kafka-compatible streaming, Apache Flink, Python, PostgreSQL, and Docker**.

The project uses historical **New York City taxi trip data** to simulate live events. Each taxi trip is treated as if it were generated in real time by an application or external system.

The events are published to a Kafka stream, processed continuously with Apache Flink, and stored in PostgreSQL for further analytics.

The main goal of the project is to understand and implement the core concepts behind modern event-driven and streaming data architectures.

---

## Business Problem

Transportation platforms generate large volumes of trip events continuously. To make operational decisions, these events need to be processed as they arrive rather than waiting for periodic batch jobs.

For example, a taxi platform may want to understand:

* which pickup locations are currently experiencing high demand
* how pickup activity changes throughout the day
* how many trips occur within a specific time window
* how to handle events that arrive late because of network delays

The challenge is to build a pipeline that can reliably ingest continuous events, process them in real time, handle delayed or out-of-order records, and make the processed data available for analytics.

This project addresses that problem by building an event-driven streaming pipeline using Kafka-compatible infrastructure, Apache Flink, and PostgreSQL.


## Architecture

```text
NYC Taxi Dataset
       |
       v
Python Producer
       |
       v
Kafka / Redpanda
       |
       v
Apache Flink
       |
       v
PostgreSQL
       |
       v
Real-Time Analytics / Dashboard
```

### How the pipeline works

1. Historical taxi trip data is loaded with Python.
2. Each row is converted into a structured taxi ride event.
3. A Python producer publishes the events to a Kafka topic.
4. Redpanda is used as a lightweight Kafka-compatible event streaming platform.
5. Apache Flink continuously consumes the events from Kafka.
6. Flink performs real-time processing and time-based aggregations.
7. Processed results are stored in PostgreSQL.
8. The stored data can later be used for dashboards, monitoring, or analytics.

---

## Tech Stack

| Technology              | Purpose                                   |
| ----------------------- | ----------------------------------------- |
| Python                  | Data ingestion and event producer         |
| Apache Kafka / Redpanda | Event streaming and message transport     |
| Apache Flink / PyFlink  | Real-time stream processing               |
| PostgreSQL              | Storage for processed streaming results   |
| Docker & Docker Compose | Running the complete local infrastructure |
| Pandas                  | Reading and preparing the taxi dataset    |
| JSON                    | Event serialization                       |
| Jupyter Notebook        | Development and experimentation           |

---

## Project Workflow

### 1. Data Source

The project uses New York City Yellow Taxi trip data.

Instead of connecting to a real live application, historical taxi records are replayed gradually so that they behave like a continuous stream of real-world events.

Each event contains information such as:

```text
pickup_location_id
dropoff_location_id
trip_distance
total_amount
pickup_datetime
```

This makes it possible to reproduce a realistic streaming scenario without requiring a paid or external live API.

---

### 2. Event Modeling

Each taxi record is converted into a structured Python object before being published.

Conceptually, an event looks like:

```python
Ride(
    pickup_location_id=10,
    dropoff_location_id=25,
    trip_distance=4.2,
    total_amount=18.50,
    pickup_datetime="2025-11-01T12:30:00"
)
```

Using a defined event model makes the data structure predictable and easier to maintain.

---

### 3. Kafka Producer

A Python Kafka producer sends taxi events into a Kafka topic.

Before sending an event, the Python object is serialized into JSON.

```text
Python Object
     |
     v
Dictionary
     |
     v
JSON
     |
     v
Bytes
     |
     v
Kafka Topic
```

The producer sends records continuously with a small delay between events to simulate real-time traffic.

---

### 4. Kafka / Redpanda

The project uses **Redpanda** as the Kafka-compatible streaming platform.

Redpanda implements the Kafka protocol, meaning standard Kafka producers and consumers can communicate with it.

Kafka acts as the central event stream between the producer and downstream processing system.

You can think of it as a continuously running data pipe:

```text
Producer ---> Kafka Topic ---> Consumer
```

This decouples the system generating the data from the systems that process it.

---

### 5. Basic Python Consumer

Before introducing Apache Flink, a standard Python Kafka consumer is implemented.

The purpose of this step is to understand the basic producer-consumer architecture.

```text
Python Producer
      |
      v
Kafka / Redpanda
      |
      v
Python Consumer
```

The consumer listens continuously for newly arriving taxi events.

Initially, the received messages are simply printed for validation.

---

### 6. Streaming Data into PostgreSQL

The consumer is then extended to write incoming taxi events into PostgreSQL.

The architecture becomes:

```text
Python Producer
      |
      v
Kafka / Redpanda
      |
      v
Python Consumer
      |
      v
PostgreSQL
```

This demonstrates a basic event-driven ingestion pipeline where events are stored immediately after arriving.

---

## Why Apache Flink?

A basic Python consumer works well for simple tasks, but real-time processing becomes more difficult when the system needs to handle:

* aggregations
* event-time processing
* failures and retries
* state management
* out-of-order events
* late-arriving events
* time windows
* continuous calculations

Implementing these features manually would require significantly more custom code.

Apache Flink is therefore introduced as the stream-processing engine.

---

## Apache Flink Processing

The Python consumer is replaced with an Apache Flink job.

The producer remains unchanged.

```text
Python Producer
      |
      v
Kafka / Redpanda
      |
      v
Apache Flink
      |
      v
PostgreSQL
```

Flink continuously listens to the Kafka topic and processes events as they arrive.

---

## Flink Architecture

The local Flink environment contains two main components:

### Job Manager

The Job Manager coordinates Flink jobs and decides how tasks should be executed.

### Task Manager

Task Managers perform the actual data processing.

Conceptually:

```text
             Job Manager
                  |
                  v
          Task Manager(s)
                  |
                  v
         Stream Processing
```

The Flink cluster is deployed locally using Docker Compose.

---

## Flink Source and Sink

The Flink pipeline contains two main connections.

### Source

Kafka / Redpanda is used as the event source.

```text
Kafka --> Flink
```

### Sink

PostgreSQL is used as the destination for processed results.

```text
Flink --> PostgreSQL
```

Flink connectors are configured so the processing job can communicate with both systems.

---

## First Flink Job: Pass-Through Pipeline

The first Flink job reproduces the behavior of the original Python consumer.

It reads taxi events from Kafka and writes them directly into PostgreSQL.

```text
Kafka
  |
  v
Flink
  |
  v
PostgreSQL
```

This job does not perform complex transformations and is mainly used to validate the end-to-end Flink pipeline.

---

## Real-Time Aggregation

The next step introduces actual stream processing.

The pipeline calculates how many taxi pickups occur at a particular location within a given time period.

For example:

```text
Pickup Location: 42
Time Window: 10:00 - 11:00
Number of Pickups: 153
```

This type of calculation is useful for scenarios such as:

* demand monitoring
* transportation analytics
* operational dashboards
* traffic analysis
* fraud monitoring
* real-time business intelligence

---

## Window Processing

Streaming data does not have a natural end, so Flink divides the stream into time windows.

In this project, events are grouped into hourly windows.

Example:

```text
10:00 -------- 11:00
      Window 1

11:00 -------- 12:00
      Window 2

12:00 -------- 13:00
      Window 3
```

Flink can then calculate metrics for each location within each window.

Example output:

| Window Start | Pickup Location | Pickup Count |
| ------------ | --------------: | -----------: |
| 10:00        |              42 |          153 |
| 10:00        |              75 |           88 |
| 11:00        |              42 |          174 |

---

## Handling Late Events with Watermarks

One of the most important concepts implemented in the project is **event-time processing**.

In real systems, events do not always arrive immediately.

For example:

```text
Taxi event occurred:
12:00:03

Network delay occurs

Event arrives:
12:00:08
```

The event belongs to an earlier time period even though it arrived later.

Apache Flink uses **watermarks** to handle this situation.

A watermark defines how much delay the system is willing to tolerate before considering a time window complete.

This allows the pipeline to process delayed or out-of-order events more reliably.

---

## Example Use Case

A taxi platform could use this pipeline to monitor pickup activity across the city.

For example:

```text
Location 42
10:00 - 11:00
153 pickups

Location 75
10:00 - 11:00
88 pickups

Location 42
11:00 - 12:00
174 pickups
```

These metrics could be displayed on a real-time dashboard to help identify:

* high-demand locations
* changes in passenger demand
* busy time periods
* unusual activity patterns

---

## Dockerized Infrastructure

The main infrastructure components run using Docker Compose.

The environment includes:

```text
Redpanda
PostgreSQL
Flink Job Manager
Flink Task Manager
```

Docker makes the project reproducible and allows the complete streaming environment to run locally without installing every service directly on the host machine.

---

## Key Concepts Demonstrated

This project covers several important data engineering concepts:

* Event-driven architecture
* Real-time data ingestion
* Kafka producers and consumers
* Kafka topics
* JSON serialization and deserialization
* Stream processing with Apache Flink
* Event-time processing
* Window-based aggregations
* Watermarks
* Handling late-arriving events
* Stateful stream processing
* PostgreSQL integration
* Dockerized data infrastructure

---

## What I Learned

Through this project, I gained hands-on experience with the complete lifecycle of a streaming data pipeline.

I learned how to:

* design structured streaming events
* publish events using a Python Kafka producer
* work with Kafka-compatible streaming infrastructure
* build and test Kafka consumers
* connect streaming pipelines to PostgreSQL
* process continuous events using Apache Flink
* create time-window-based aggregations
* understand event time versus processing time
* handle delayed and out-of-order events using watermarks
* run a multi-service data engineering environment using Docker Compose

One of the most valuable lessons from the project was understanding the difference between **event streaming** and **stream processing**.

Kafka is responsible for transporting and storing the stream of events, while Apache Flink is responsible for continuously processing, transforming, and aggregating those events.

---

## Potential Improvements

The current project provides the core streaming architecture, but it can be extended further.

Possible improvements include:

* adding a real-time dashboard with Power BI, Grafana, or Streamlit
* replacing simulated taxi data with a real public streaming API
* storing raw streaming events in an S3-based data lake
* adding Apache Airflow for orchestration
* implementing data quality checks
* adding schema validation
* introducing monitoring and logging
* deploying the infrastructure to AWS, Azure, or GCP
* implementing checkpointing and fault recovery
* adding a dead-letter queue for invalid events

A more production-oriented architecture could look like:

```text
Real-Time API
      |
      v
Kafka / Redpanda
      |
      v
Apache Flink
   /       \
  v         v
S3       PostgreSQL
           |
           v
       Dashboard
```

---

## Project Summary

This project demonstrates an end-to-end real-time data engineering pipeline where taxi events are produced in Python, streamed through Kafka-compatible infrastructure, processed continuously with Apache Flink, and stored in PostgreSQL.

The main focus is not only moving data between systems, but also understanding how real streaming systems handle **continuous events, time windows, state, delayed records, and real-time aggregations**.

It provides a practical foundation for building larger event-driven architectures used in modern data engineering environments.
