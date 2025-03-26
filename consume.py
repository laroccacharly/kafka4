# Gets the data from kafka and prints it to the console
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import logging 
from flask import Flask, jsonify
import glob
import shutil
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
app = Flask(__name__)

# Configuration
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'broker:9092')
TOPIC_NAME = 'locations'
OUTPUT_DIR = './data'
CHECKPOINT_DIR = './checkpoints'
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
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.default.parallelism", "8")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.task.maxFailures", "1")
            .getOrCreate())

def process_batch(batch_df, batch_id):
    """Process each batch of data."""
    try:
        # Generate timestamp for the batch
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_output_path = f"{OUTPUT_DIR}/locations_{timestamp}_{batch_id}_temp"
        final_output_path = f"{OUTPUT_DIR}/locations_{timestamp}_{batch_id}.parquet"
        count = batch_df.count()
        if count > 0:  # Only write if we have data
            # Ensure single partition and write as a single file
            logger.info(f"Batch {batch_id}: Writing {count} rows to {temp_output_path}")
            (batch_df
             .coalesce(1)  # Ensure single partition
             .write
             .mode("overwrite")
             .option("compression", "snappy")
             .format("parquet")
             .save(temp_output_path))
            
            # Get the actual parquet file and rename it

            
            # Find the parquet file in the directory
            parquet_files = glob.glob(f"{temp_output_path}/part-*.parquet")
            if parquet_files:
                # Move the actual parquet file to the desired location
                actual_file = parquet_files[0]
                shutil.move(actual_file, final_output_path)
                
                # Clean up the temporary directory
                shutil.rmtree(temp_output_path)
                
                logger.info(f"Batch {batch_id}: Successfully wrote data to {final_output_path}")
            else:
                logger.warning(f"Batch {batch_id}: No parquet file found in {temp_output_path}")
        else:
            logger.info(f"Batch {batch_id}: Skipped writing as batch was empty")
            
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")
        if os.path.exists(temp_output_path):
            shutil.rmtree(temp_output_path)

def main():
    """Main function to consume messages from Kafka using PySpark."""
    # Create directories if they don't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Starting to consume from topic '{TOPIC_NAME}' using PySpark")
    
    try:
        # Read from Kafka
        df = (spark
              .readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
              .option("subscribe", TOPIC_NAME)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "false")
              .load())

        # Parse the JSON value
        parsed_df = (df
                    .selectExpr("CAST(value AS STRING)")
                    .select(from_json(col("value"), location_schema).alias("data"))
                    .select("data.*"))

        # Execute the streaming query using foreachBatch
        stream_query = (parsed_df
                       .writeStream
                       .foreachBatch(process_batch)
                       .option("checkpointLocation", CHECKPOINT_DIR)
                       .trigger(processingTime="5 seconds")  # Process every 5 seconds
                       .start())

        # Wait for the query to terminate
        stream_query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in streaming query: {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Consumer stopped")

@app.route('/', methods=['POST'])
def consume():
    """Endpoint to trigger data consumption from Kafka"""
    try:
        logger.info("Received request to consume data from Kafka")
        main()
        return jsonify({"status": "success", "message": "Consumer completed"}), 200
    except Exception as e:
        logger.error(f"Error in consumer: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    logger.info("Starting consumer service")
    # app.run(host='0.0.0.0', port=5000)
    main()