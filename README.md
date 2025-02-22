# buzzline-06-jaya-Final project

# Amazon Streaming Data Pipeline

Overview

This project sets up a real-time streaming data pipeline for processing Amazon product review data. It consists of a Producer that generates and writes transactions to a JSON file and a Consumer that reads the transactions, stores them in an SQLite database, and visualizes insights dynamically using Matplotlib.

## To start 
1️. Create and Activate Virtual Environment
Before running the scripts, set up a virtual environment:

### Create virtual environment
```python3 -m venv .venv```

### Activate virtual environment (Mac/Linux)
 ```source .venv/bin/activate```

### Start kafka and zookeeper
```cd ~/kafka```
```chmod +x bin/zookeeper-server-start.sh```
```bin/zookeeper-server-start.sh config/zookeeper.properties```

```cd ~/kafka```
```chmod +x binkafka-server-start.sh```
```bin/kafka-server-start.sh config/server.properties```



## Project Components

1. Producer

The Producer simulates transactions by selecting random Amazon product reviews, assigning categories, and writing the data to a JSON file (transactions.json).

Key Features:

Loads Amazon product data from a CSV file.

Generates random product transactions with timestamps, categories, ratings, and product names.

Writes transactions to a JSON file every 3 seconds.

Technologies Used: Python, Pandas, NumPy, JSON, Logging

2. Consumer

The Consumer reads the generated transactions from the JSON file, inserts them into an SQLite database, and dynamically updates two real-time charts:

Top Products by Average Rating (Bar Chart)

Top Categories by Transaction Count (Pie Chart)

### Key Features:

Reads transactions from the JSON file and inserts them into an SQLite database.

Clears the JSON file after processing to avoid duplicate entries.

Dynamically updates visualization using Matplotlib's interactive mode.

Technologies Used: Python, SQLite, Pandas, Matplotlib, Seaborn

## Setup Instructions

### Prerequisites

Ensure you have Python installed along with the required dependencies:

pip install pandas numpy matplotlib seaborn sqlite3 kafka zookeeper

## Running the Producer

To start generating transactions, run:

```python3 -m producers.producer_jaya```

This will continuously generate and write new transactions to transactions.json every 3 seconds.

Running the Consumer

To process transactions and visualize the data, run:

```python3 -m consumers.project_consumer_jaya.py```

This will read the transactions, update the SQLite database, and dynamically refresh the visualizations.


