from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, udf, explode, split, when, lit,
    to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, FloatType, TimestampType, BooleanType
)
from textblob import TextBlob
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

class RedditSparkProcessor:
    def __init__(self):
        load_dotenv()
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Create required directories
        os.makedirs("data/processed_posts", exist_ok=True)
        os.makedirs("data/checkpoints", exist_ok=True)
        os.makedirs("data/spark-events", exist_ok=True)
        
        # Initialize Spark Session with detailed configuration
        self.spark = SparkSession.builder \
            .appName("RedditStreamProcessor") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.streaming.checkpointLocation", "data/checkpoints") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "data/spark-events") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .getOrCreate()
            
        # Set log level
        self.spark.sparkContext.setLogLevel("INFO")
        
        self.logger.info("Spark session initialized")
            
        # Define schema for Reddit posts
        self.schema = StructType([
            StructField("post_id", StringType(), True),
            StructField("subreddit", StringType(), True),
            StructField("title", StringType(), True),
            StructField("author", StringType(), True),
            StructField("created_utc", TimestampType(), True),
            StructField("score", IntegerType(), True),
            StructField("upvote_ratio", FloatType(), True),
            StructField("num_comments", IntegerType(), True),
            StructField("permalink", StringType(), True),
            StructField("url", StringType(), True),
            StructField("is_self", BooleanType(), True),
            StructField("selftext", StringType(), True),
            StructField("collected_at", TimestampType(), True)
        ])
        
    @staticmethod
    def get_sentiment(text):
        """Calculate sentiment score using TextBlob"""
        try:
            if not text or text.isspace():
                return 0.0
            return TextBlob(text).sentiment.polarity
        except Exception as e:
            logging.error(f"Error in sentiment analysis: {str(e)}")
            return 0.0
        
    def calculate_engagement_score(self, df):
        """Calculate engagement score based on various metrics"""
        return df.withColumn(
            "engagement_score",
            (col("score") * 0.4 + 
             col("num_comments") * 0.3 + 
             col("upvote_ratio") * 100 * 0.3)
        )
    
    def process_stream(self):
        """Process streaming data from Kafka"""
        try:
            self.logger.info("Starting stream processing")
            
            # Register UDF for sentiment analysis
            sentiment_udf = udf(self.get_sentiment, FloatType())
            
            # Read from Kafka
            self.logger.info(f"Connecting to Kafka at {os.getenv('KAFKA_BOOTSTRAP_SERVERS')}")
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", os.getenv('KAFKA_BOOTSTRAP_SERVERS')) \
                .option("subscribe", os.getenv('KAFKA_TOPIC')) \
                .option("startingOffsets", "latest") \
                .load()
            
            self.logger.info("Successfully connected to Kafka")
            
            # Parse JSON data
            parsed_df = df.select(
                from_json(col("value").cast("string"), self.schema).alias("data")
            ).select("data.*")
            
            # Calculate sentiment scores
            processed_df = parsed_df \
                .withColumn("title_sentiment", sentiment_udf(col("title"))) \
                .withColumn(
                    "content_sentiment", 
                    when(col("is_self"), sentiment_udf(col("selftext")))
                    .otherwise(0.0)
                )
            
            # Calculate engagement metrics
            processed_df = self.calculate_engagement_score(processed_df)
            
            # Add processing timestamp
            processed_df = processed_df.withColumn("processed_at", current_timestamp())
            
            # Write to console for debugging
            console_query = processed_df \
                .writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", False) \
                .start()
            
            self.logger.info("Started console output stream")
            
            # Write to Parquet files
            parquet_query = processed_df \
                .writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", "data/processed_posts") \
                .option("checkpointLocation", "data/checkpoints") \
                .partitionBy("subreddit") \
                .start()
            
            self.logger.info("Started Parquet output stream")
            
            # Wait for termination
            console_query.awaitTermination()
            parquet_query.awaitTermination()
            
        except Exception as e:
            self.logger.error(f"Error in stream processing: {str(e)}")
            raise e

if __name__ == "__main__":
    processor = RedditSparkProcessor()
    processor.process_stream() 