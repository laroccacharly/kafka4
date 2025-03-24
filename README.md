# Kafka 4.0 with PySpark Consumer

This project demonstrates how to use Apache Kafka 4.0 with a PySpark consumer for high-performance data processing.

## Architecture

- **Producer**: Generates location data and sends it to a Kafka topic using the Confluent Kafka Python client
- **Consumer**: Uses PySpark Structured Streaming to consume data from Kafka and write to Parquet files
- **Kafka**: Runs in a Docker container using the official Apache Kafka 4.0 image

## Running the Application

```bash
# Start the services
docker-compose up -d

# Check logs from the producer
docker logs -f producer

# Check logs from the consumer
docker logs -f consumer

# Stop the services
docker-compose down
```

## Implementation Details

### Producer
The producer generates random location events and sends them to the Kafka topic 'locations'. It uses the Confluent Kafka Python client.

### Consumer
The consumer uses PySpark Structured Streaming to read from Kafka and write to Parquet files. This approach offers several advantages:

1. **Parallelism**: PySpark automatically parallelizes the processing across available cores
2. **Fault Tolerance**: Checkpointing ensures that the consumer can recover from failures
3. **Optimized Parquet Writing**: PySpark efficiently writes to Parquet format with optimized compression
4. **Scalability**: Can be scaled up by increasing resources allocated to the container

## Configuration

The application can be configured through environment variables:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: broker:9092)
- `PYSPARK_DRIVER_MEMORY`: Memory allocated to the PySpark driver (default: 1g)
- `PYSPARK_EXECUTOR_MEMORY`: Memory allocated to PySpark executors (default: 1g)

## Performance Tuning

To further improve performance:

1. Increase the number of partitions in the Kafka topic
2. Adjust Spark configurations (`spark.sql.shuffle.partitions`, `spark.default.parallelism`)
3. Allocate more resources to the consumer container

## Dependencies

- Python 3.11
- PySpark 3.5.1
- Confluent Kafka 2.3.0
- Apache Kafka 4.0 