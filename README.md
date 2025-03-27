# Kafka 4.0 with PySpark Consumer

Kafka 4.0 no longer requires Zookeeper. This significantly reduces its overhead and improves its usability. 
Here is a demo project to help you get started with this powerful tool. We use docker-compose to set up the infrastructure and PySpark to convert the data into Parquet files. 

[![Kafka 4.0 Demo](https://img.youtube.com/vi/GhwDNVH8joE/0.jpg)](https://youtu.be/GhwDNVH8joE)

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
