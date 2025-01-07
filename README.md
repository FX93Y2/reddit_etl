# Reddit Social Media ETL Pipeline

A real-time data pipeline that collects, processes, and analyzes Reddit posts from gaming-related subreddits using Apache Kafka and PySpark.

## Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline that:
- Collects posts from gaming subreddits (r/gaming, r/steam, r/buildapc, r/ps5)
- Streams data through Apache Kafka
- Processes data using PySpark for sentiment analysis and engagement metrics
- Stores processed data in Parquet format for analytics

## Architecture

```
Reddit API → Reddit Collector → Kafka → PySpark Processor → Parquet Files
```

### Components
- **Reddit Collector**: Fetches posts using PRAW (Python Reddit API Wrapper)
- **Kafka**: Message broker for real-time data streaming
- **PySpark Processor**: 
  - Performs sentiment analysis on post titles and content
  - Calculates engagement metrics
  - Structures data for analytics

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Reddit API credentials
- Java 8+ (for PySpark)

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd social_media_etl
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -e .
```

4. Set up environment variables in `.env`:
```
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=your_user_agent
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=reddit_posts
```

## Usage

1. Start Kafka:
```bash
sudo docker-compose up -d
```

2. Create Kafka topic:
```bash
sudo docker exec -it social_media_etl_kafka_1 kafka-topics.sh --create --topic reddit_posts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

3. Run the Reddit collector:
```bash
python src/collect_reddit_data.py
```

4. Run the Spark processor:
```bash
python src/process_reddit_data.py
```

## Data Processing

The pipeline performs the following transformations:
- Sentiment analysis on post titles and content using TextBlob
- Engagement score calculation based on:
  - Post score (40% weight)
  - Number of comments (30% weight)
  - Upvote ratio (30% weight)
- Data enrichment with processing timestamps
- Partitioning by subreddit

## Output

Processed data is saved in Parquet format with the following structure:
```
data/processed_posts/
├── subreddit=gaming/
├── subreddit=steam/
├── subreddit=buildapc/
└── subreddit=ps5/
```

Each partition contains:
- Post metadata (ID, author, timestamps)
- Content analysis (title/content sentiment)
- Engagement metrics
- Processing metadata

## Project Structure

```
social_media_etl/
├── data/
│   ├── checkpoints/
│   ├── processed_posts/
│   └── spark-events/
├── src/
│   ├── collectors/
│   │   └── reddit_collector.py
│   ├── processors/
│   │   └── spark_processor.py
│   ├── collect_reddit_data.py
│   └── process_reddit_data.py
├── .env
├── .gitignore
├── docker-compose.yml
├── README.md
├── requirements.txt
└── setup.py
```

## Monitoring

- Kafka: Monitor topic lag and consumer group health
- Spark: Track processing metrics via Spark UI

## License

This project is licensed under the MIT License - see the LICENSE file for details. 
