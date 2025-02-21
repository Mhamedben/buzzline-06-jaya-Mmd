import pandas as pd

df = pd.read_csv('/Users/jaya/Documents/MS_Analytics/StreamingData/buzzline-06-jaya/data/amazon.csv')
new_df = df["category"].str.split('|', expand=True)
max_columns = new_df.shape[1]
new_df.columns = [f'Level_{i+1}' for i in range(max_columns)]
new_df['count'] = 1
new_df_gb = new_df.groupby('Level_1')['count'].sum().reset_index()
#new_df_gb = new_df_gb.sort_values(by='count', ascending=False)
df['category'] = new_df['Level_1']
breakpoint()


############################
import os
import sys
import pathlib
import json
import random
import pandas as pd
import numpy as np
import time
from datetime import datetime
from dotenv import load_dotenv

# Import Kafka only if available
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# Import logging utility
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

data = pd.read_csv('/Users/jaya/Documents/MS_Analytics/StreamingData/buzzline-06-jaya/data/amazon_clean.csv')

# Sample product categories
CATEGORIES = ['Computers&Accessories', 'Electronics', 'MusicalInstruments',
       'OfficeProducts', 'Home&Kitchen', 'HomeImprovement', 'Toys&Games',
       'Car&Motorbike', 'Health&PersonalCare']
RATING = np.array(data['rating'])
PRODUCT_NAME = np.array(data['product_name'])

#####################################
# Getter Functions for Environment Variables
#####################################

def get_message_interval() -> int:
    return int(os.getenv("PROJECT_INTERVAL_SECONDS", 1))

def get_kafka_topic() -> str:
    return os.getenv("PROJECT_TOPIC", "buzzline-topic")

def get_kafka_server() -> str:
    return os.getenv("KAFKA_SERVER", "localhost:9092")

#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("project_live.json")

def generate_transaction():
    """create a rating record."""
    transaction = {
        "product_id": f"T{random.randint(1000, 9999)}",
        "timestamp": datetime.utcnow().isoformat(),
        "rating": f"C{random.choice(RATING)}",
        "category": random.choice(CATEGORIES),
        "product_name": random.choice(PRODUCT_NAME)}
        
    return transaction

def write_transactions_to_file():
    """Continuously generate transactions and write to a JSON file."""
    while True:
        transaction = generate_transaction()
        print(f"Generated Transaction: {transaction}")
        
        # Append transaction to a JSON file
        with open("data/transactions.json", "a") as f:
            f.write(json.dumps(transaction) + "\n")

        time.sleep(3)  # Generates a transaction every 3 seconds

def main():
    logger.info("START producer...")
    interval_secs = get_message_interval()
    topic = get_kafka_topic()
    kafka_server = get_kafka_server()
    
    # Attempt to create Kafka producer
    producer = None
    if KAFKA_AVAILABLE:
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_server,
                value_serializer=lambda x: json.dumps(x).encode("utf-8")
            )
            logger.info(f"Kafka producer connected to {kafka_server}")
        except Exception as e:
            logger.error(f"Kafka connection failed: {e}")
            producer = None
    
    try:
        for message in generate_transaction():
            logger.info(message)
            
            # Write to file
            with DATA_FILE.open("a") as f:
                f.write(json.dumps(message) + "\n")
            
            # Send to Kafka if available
            if producer:
                producer.send(topic, value=message)
                logger.info(f"Sent message to Kafka topic '{topic}': {message}")
            
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("Producer shutting down.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()