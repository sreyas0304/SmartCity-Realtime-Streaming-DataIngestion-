# SmartCity-Realtime-Streaming-Data-Ingestion

The pipeline simulates car travel between two cities in the U.S., capturing telemetry, GPS, weather, camera, and emergency data. Designed for high throughput and low-latency processing, the architecture ensures scalable, real-time analytics using Apache Kafka and Spark, with final insights visualized through Power BI. It covers the entire flow—from data generation and ingestion to real-time processing, transformation, storage, and visualization—leveraging modern cloud and big data tools.


# Architecture Diagram

![image](https://github.com/user-attachments/assets/e77b1b0e-40dd-4995-a3c3-821fbe8d0b25)

# Technologies Used:

Python, Apache Kafka, Apache Zookeeper, Apache Spark (Structured Streaming), Docker, AWS S3, AWS Glue, AWS IAM, AWS Athena, AWS Redshift, Power BI, DBeaver

# Project Overview:

The real-time data ingestion pipeline comprises the following major components:

1. Data Simulation & Ingestion
Synthetic data for GPS, weather, vehicle diagnostics, and emergency events is generated using a Python script.

Apache Kafka serves as the ingestion layer, efficiently capturing high-velocity data using multiple Kafka topics for each data stream.

2. Real-Time Processing
Apache Spark Structured Streaming consumes Kafka streams in real time.

Spark processes and transforms the data using in-memory computation to ensure high speed and scalability.

Processed data is written to Amazon S3 in a structured format for further ETL operations.

3. Data Transformation & Storage
AWS Glue Catalog is used to catalog data stored in S3 and apply schema-on-read.

Transformed data is written to AWS Redshift, using DBeaver as the interface for managing and querying Redshift tables.

4. Data Visualization
The final dataset in Redshift is connected to Power BI for visualization.

Dashboards provide insights into real-time car movement, emergency alerts, weather impact, and route optimization.

# Key Components:

## Kafka & Zookeeper
Kafka handles real-time data ingestion.

Zookeeper maintains configuration and synchronization across distributed components.

## Spark Streaming
Consumes data from Kafka.

Applies real-time transformations, filtering, and joins across different data streams.

Writes processed output to AWS S3.

## AWS Components
S3: Stores the processed streaming data as a landing zone.

Glue: Catalogs and transforms data for Redshift.

IAM: Manages access to AWS services.

Athena: Allows ad-hoc querying of S3 data.

Redshift: Acts as the final data warehouse for analytical querying.

DBeaver: Interface used to manage Redshift schema and validate data flow.

## Visualization Layer
Power BI: Connected to Redshift for real-time data visualization and interactive dashboards.


# Key Features:

- Real-time data ingestion and processing with high throughput and low latency.

- Modular pipeline with independent Kafka topics per data stream.

- In-memory processing with Spark for faster data transformation.

- Cloud-based storage and querying using AWS S3, Glue, and Redshift.

- Actionable dashboards in Power BI for insights into Smart City traffic and safety.
