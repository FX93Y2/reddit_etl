from collectors.reddit_collector import RedditCollector
import time
import logging

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    collector = RedditCollector()
    
    try:
        while True:
            collector.collect_and_stream_posts(limit=50)
            logger.info("Waiting 5 minutes before next collection...")
            time.sleep(300)  # Wait 5 minutes between collections
    except KeyboardInterrupt:
        logger.info("Stopping Reddit collector...")

if __name__ == "__main__":
    main() 