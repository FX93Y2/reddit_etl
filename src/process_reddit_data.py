from src.processors.spark_processor import RedditSparkProcessor
import logging

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    logger.info("Starting Reddit Spark Processor...")
    processor = RedditSparkProcessor()
    
    try:
        processor.process_stream()
    except KeyboardInterrupt:
        logger.info("Stopping Reddit Spark Processor...")
    except Exception as e:
        logger.error(f"Error in Spark Processor: {str(e)}")

if __name__ == "__main__":
    main() 