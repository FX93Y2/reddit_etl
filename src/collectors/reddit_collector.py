import os
import praw
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv
from producers.kafka_producer import RedditKafkaProducer

class RedditCollector:
    def __init__(self):
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Load environment variables
        load_dotenv()
        
        # Initialize Reddit API client
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        # Get subreddits from environment
        self.subreddits = os.getenv('REDDIT_SUBREDDITS').replace(' ', '').split(',')
        
        # Initialize Kafka producer
        self.kafka_producer = RedditKafkaProducer()
        
    def collect_and_stream_posts(self, limit=100, timeframe='day'):
        """
        Collect posts and stream them to Kafka
        """
        try:
            for subreddit_name in self.subreddits:
                self.logger.info(f"Collecting posts from r/{subreddit_name}")
                subreddit = self.reddit.subreddit(subreddit_name)
                
                for post in subreddit.top(time_filter=timeframe, limit=limit):
                    post_data = {
                        'post_id': post.id,
                        'subreddit': subreddit_name,
                        'title': post.title,
                        'author': str(post.author),
                        'created_utc': datetime.fromtimestamp(post.created_utc),
                        'score': post.score,
                        'upvote_ratio': post.upvote_ratio,
                        'num_comments': post.num_comments,
                        'permalink': post.permalink,
                        'url': post.url,
                        'is_self': post.is_self,
                        'selftext': post.selftext if post.is_self else '',
                        'collected_at': datetime.now()
                    }
                    
                    # Stream to Kafka
                    self.kafka_producer.send_message(post_data)
                    
                    # Optional: Save to local storage
                    if os.getenv('USE_LOCAL_STORAGE', 'false').lower() == 'true':
                        self.save_to_local([post_data])
                        
        except Exception as e:
            self.logger.error(f"Error in collect_and_stream_posts: {str(e)}")
        finally:
            self.kafka_producer.close()
    
    def save_to_local(self, posts):
        """
        Save collected posts to local storage if configured
        """
        if os.getenv('USE_LOCAL_STORAGE', 'false').lower() == 'true':
            try:
                data_path = os.getenv('LOCAL_DATA_PATH', 'data')
                os.makedirs(data_path, exist_ok=True)
                
                df = pd.DataFrame(posts)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"{data_path}/reddit_posts_{timestamp}.csv"
                
                df.to_csv(filename, index=False)
                self.logger.info(f"Saved {len(posts)} posts to {filename}")
                
            except Exception as e:
                self.logger.error(f"Error saving posts to local storage: {str(e)}") 