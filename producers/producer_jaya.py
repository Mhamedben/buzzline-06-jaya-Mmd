import os
import sys
import pathlib
import json
import random
import pandas as pd
import numpy as np
import time
from datetime import datetime

# Import logging utility
from utils.utils_logger import logger

# Load data
DATA_FILE =  r'\Users\khalo\DSMM\buzzline-06-jaya-Mmd\data\amazon_clean.csv'
try:
    data = pd.read_csv(DATA_FILE)
    logger.info(f"Successfully loaded data from {DATA_FILE}")
except Exception as e:
    logger.error(f"Error loading data file: {e}")
    sys.exit(1)

# Sample product categories
CATEGORIES = ['Computers&Accessories', 'Electronics', 'MusicalInstruments',
       'OfficeProducts', 'Home&Kitchen', 'HomeImprovement', 'Toys&Games',
       'Car&Motorbike', 'Health&PersonalCare']
RATING = np.array(data['rating'])
PRODUCT_NAME = np.array(data['product_name'])

def generate_transaction():
    """Create a rating record."""
    transaction = {
        "product_id": f"T{random.randint(1000, 9999)}",
        "timestamp": datetime.utcnow().isoformat(),
        "rating": round(float(random.choice(RATING)), 1),  # Ensure float rating
        "category": random.choice(CATEGORIES),
        "product_name": random.choice(PRODUCT_NAME)
    }
    logger.info(f"Generated transaction: {transaction}")
    return transaction

def write_transactions_to_file():
    """Continuously generate transactions and write to a JSON file."""
    TRANSACTION_FILE = "data/transactions.json"
    logger.info(f"Starting transaction writing to {TRANSACTION_FILE}")
    while True:
        try:
            transaction = generate_transaction()
            with open(TRANSACTION_FILE, "a") as f:
                f.write(json.dumps(transaction) + "\n")
            logger.info(f"Transaction written to {TRANSACTION_FILE}")
        except Exception as e:
            logger.error(f"Error writing transaction: {e}")
        time.sleep(3)  # Generates a transaction every 3 seconds

if __name__ == "__main__":
    try:
        write_transactions_to_file()
    except KeyboardInterrupt:
        logger.warning("Transaction generation interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
