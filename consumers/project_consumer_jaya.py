import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
import time
from utils.utils_logger import logger

# SQLite Database Path
DB_PATH = "transactions_data.sqlite"

# Connect to SQLite Database
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create Transactions Table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        product_name TEXT,
        category TEXT,
        rating REAL
    )
''')
conn.commit()

TRANSACTION_FILE = "data/transactions.json"

# Initialize Matplotlib interactive plot with two subplots
plt.ion()  # Interactive mode on
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 5))  # Create two subplots for top products and top categories

def process_transaction(transaction):
    """Insert a transaction into the database."""
    cursor.execute('''
        INSERT INTO transactions (timestamp, product_name, category, rating)
        VALUES (?, ?, ?, ?)
    ''', (transaction["timestamp"], transaction["product_name"], transaction["category"], transaction["rating"]))
    conn.commit()
    logger.info(f"Inserted transaction: {transaction}")

def read_transactions():
    """Read transactions from the JSON file and store them in the database."""
    try:
        with open(TRANSACTION_FILE, "r") as f:
            for line in f:
                transaction = json.loads(line.strip())
                process_transaction(transaction)
        # Clear the file after reading
        open(TRANSACTION_FILE, "w").close()
    except Exception as e:
        logger.error(f"Error reading transactions: {e}")

def plot_top_products():
    """Plot top products by average rating."""
    query = """
        SELECT product_name, AVG(rating) as avg_rating
        FROM transactions
        GROUP BY product_name
        ORDER BY avg_rating DESC
        LIMIT 10
    """
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        logger.warning("Not enough data for product visualization.")
        return

    ax1.clear()  # Clear previous plot

    # Update the bar chart with new data
    sns.barplot(x="product_name", y="avg_rating", data=df, palette="coolwarm", ax=ax1)
    ax1.set_title("Top Products by Average Rating")
    ax1.set_xlabel("Product Name")
    ax1.set_ylabel("Average Rating")
    ax1.set_xticklabels(df["product_name"], rotation=45)

def plot_top_categories():
    """Plot top categories by transaction count."""
    query = """
        SELECT category, COUNT(*) as count
        FROM transactions
        GROUP BY category
        ORDER BY count DESC
    """
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        logger.warning("Not enough data for category visualization.")
        return

    ax2.clear()  # Clear previous plot

    # Update the pie chart with new data
    ax2.pie(df["count"], labels=df["category"], autopct='%1.1f%%', colors=sns.color_palette("pastel"))
    ax2.set_title("Top Categories by Transactions")
    
    plt.tight_layout()  # Ensure the layout fits well
    plt.draw()  # Update the plot

# Main Consumer Loop (Continuously reads transactions and updates charts)
while True:
    read_transactions()  # Read and process new transactions
    plot_top_products()  # Plot the updated products
    plot_top_categories()  # Plot the updated categories
    logger.info("Waiting for new transactions...")
    plt.pause(2)  # Wait 2 seconds before updating again
