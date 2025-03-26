# Gets the data from kafka and prints it to the console
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Configuration
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'broker:9092')
TOPIC_NAME = 'locations'
GROUP_ID = 'location-consumer'
OUTPUT_DIR = './data'
CHECKPOINT_DIR = './checkpoints'
TIMEOUT = 5  # seconds to wait for new messages before terminating

# Define the schema for the location events
location_schema = StructType([
    StructField("id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("latitude", FloatType(), False),
    StructField("longitude", FloatType(), False),
    StructField("elevation", FloatType(), False),
    StructField("speed", FloatType(), False),
    StructField("direction", FloatType(), False),
    StructField("device_id", StringType(), False),
    StructField("vehicle_type", StringType(), False)
])

def create_spark_session():
    """Create and return a Spark session."""
    return (SparkSession.builder
            .appName("Kafka Location Consumer")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.shuffle.partitions", "8")  # Adjust based on your needs
            .config("spark.default.parallelism", "8")    # Adjust based on your needs
            .getOrCreate())

def main():
    """Main function to consume messages from Kafka using PySpark."""
    # Create directories if they don't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Starting to consume from topic '{TOPIC_NAME}' using PySpark")
    
    # Read from Kafka
    df = (spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
          .option("subscribe", TOPIC_NAME)
          .option("startingOffsets", "earliest")
          .option("kafka.group.id", GROUP_ID)
          .option("kafka.group.protocol", "consumer")  # Use new consumer protocol from Kafka 4.0
          .option("failOnDataLoss", "false")
          .load())
    
    # Parse the JSON value
    parsed_df = (df
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), location_schema).alias("data"))
                .select("data.*"))
    
    # Write the data to Parquet using foreachBatch
    def write_batch(batch_df, batch_id):
        if not batch_df.isEmpty():
            # Generate a timestamp for the batch
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"{OUTPUT_DIR}/locations_{timestamp}_{batch_id}.parquet"
            
            # Write the batch to Parquet
            batch_df.write.parquet(output_path)
            print(f"Batch {batch_id}: Wrote {batch_df.count()} records to {output_path}")
    
    # Execute the streaming query with timeout
    stream_query = (parsed_df
                   .writeStream
                   .foreachBatch(write_batch)
                   .option("checkpointLocation", CHECKPOINT_DIR)
                   .trigger(processingTime="5 seconds")  # Process in 5-second batches
                   .start())
    
    # Wait for either data to be processed or timeout
    start_time = time.time()
    last_progress_time = start_time
    
    try:
        while True:
            if stream_query.lastProgress is not None:
                last_progress_time = time.time()
            
            # Check if we've exceeded the timeout with no new data
            if time.time() - last_progress_time > TIMEOUT:
                print(f"No new data received for {TIMEOUT} seconds. Stopping consumer.")
                break
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("Consumer interrupted by user")
    finally:
        stream_query.stop()
        print("Consumer stopped")
        spark.stop()

if __name__ == "__main__":
    # Start consuming
    main()