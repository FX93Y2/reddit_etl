from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, desc, col
import logging

class RedditAnalyzer:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("RedditAnalyzer") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("INFO")
        
    def load_processed_data(self, path="data/processed_posts"):
        """Load processed posts from Parquet files"""
        try:
            df = self.spark.read.parquet(path)
            self.logger.info(f"Loaded {df.count()} posts from {path}")
            return df
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            return None
    
    def get_top_posts(self, df, n=10):
        """Get top n posts by engagement score"""
        return df.orderBy(desc("engagement_score")).limit(n)
    
    def get_subreddit_stats(self, df):
        """Get statistics by subreddit"""
        return df.groupBy("subreddit").agg(
            count("*").alias("post_count"),
            avg("engagement_score").alias("avg_engagement"),
            avg("title_sentiment").alias("avg_title_sentiment"),
            avg("content_sentiment").alias("avg_content_sentiment")
        ).orderBy(desc("post_count"))
    
    def get_sentiment_distribution(self, df):
        """Analyze sentiment distribution"""
        return df.select(
            "title_sentiment",
            "content_sentiment"
        ).summary("count", "mean", "stddev", "min", "max")

def main():
    analyzer = RedditAnalyzer()
    
    # Load processed data
    df = analyzer.load_processed_data()
    if df is None:
        return
    
    try:
        # Get and display top posts
        print("\n=== Top 10 Posts by Engagement ===")
        top_posts = analyzer.get_top_posts(df)
        top_posts.select("subreddit", "title", "engagement_score").show(truncate=False)
        
        # Get and display subreddit statistics
        print("\n=== Subreddit Statistics ===")
        subreddit_stats = analyzer.get_subreddit_stats(df)
        subreddit_stats.show()
        
        # Get and display sentiment analysis
        print("\n=== Sentiment Distribution ===")
        sentiment_stats = analyzer.get_sentiment_distribution(df)
        sentiment_stats.show()
        
    except Exception as e:
        logging.error(f"Error during analysis: {str(e)}")
    finally:
        analyzer.spark.stop()

if __name__ == "__main__":
    main() 