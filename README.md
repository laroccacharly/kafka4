# Kafka 4.0 with PySpark Consumer

This project demonstrates how to use Apache Kafka 4.0 with a PySpark consumer for high-performance data processing.

## Architecture

- **Producer**: Generates location data and sends it to a Kafka topic using the Confluent Kafka Python client
- **Consumer**: Uses PySpark Streaming to consume data from Kafka and write to Parquet files
- **Kafka**: Runs in a Docker container using the official Apache Kafka 4.0 image

![Architecture Diagram](https://d1qlp37w2ygoqn.cloudfront.net/diagram.png)

## Usage
Build the images: 
```bash
DOCKER_DEFAULT_PLATFORM=linux/amd64 docker compose build
```
Start the containers: 
```bash
docker compose up
```
Produce data via the CLI: 
```bash
uv run cli.py produce 
```
