
"""
project_consumer_jaya.py

Read a JSON-formatted file as it is being written. 

Example JSON message:
{"message": "I just saw a movie! It was amazing.", "author": "Eve"}
"""

#####################################
# Import Modules
#####################################
import json
import os
import sys
import time
import pathlib
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from utils.utils_logger import logger

# Load environment variables
load_dotenv()

#####################################
# Set up Paths - read from the file the producer writes
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("project_live.json")

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Data folder: {DATA_FOLDER}")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Set up data structures
#####################################

# Sentiment count dictionary
sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}

#####################################
# Set up live visuals
#####################################

fig, ax = plt.subplots(figsize=(10, 8))  # Single plot for sentiment
plt.ion() 

# # Set a background color for the plot for a more modern look
# fig.patch.set_facecolor('#f1f1f1')

#####################################
# Define an update chart function for live plotting
#####################################

def update_chart():
    """Update the live chart with the latest sentiment counts."""
    
    # Clear the previous chart
    ax.clear()

    # Update the chart with sentiment counts
    sentiment_labels = ['Positive', 'Neutral', 'Negative']
    sentiment_values = [sentiment_counts['positive'], sentiment_counts['neutral'], sentiment_counts['negative']]
    
    # Change color palette
    colors = ['#90A4AE', '#F44336','#4CAF50'] 

    # Create the bar chart with updated colors and aesthetic
    ax.bar(sentiment_labels, sentiment_values, color=colors, edgecolor='blue', linewidth=1.2)

    # Add gridlines for better readability
    ax.grid(True, which='both', axis='y', linestyle='--', color='gray', alpha=0.5)

    # Set labels and title
    ax.set_xlabel("Sentiment", fontsize=14, fontweight='bold')
    ax.set_ylabel("Count of messages", fontsize=14, fontweight='bold')
    ax.set_title("Sentiment distribution", fontsize=18, fontweight='bold')

    # Enhance x-axis and y-axis labels with larger font and rotation
    ax.set_xticklabels(sentiment_labels, rotation=45, ha="right", fontsize=12)
    ax.set_yticklabels(ax.get_yticks(), fontsize=12)

    # Adjust layout
    plt.tight_layout()
    
    # Redraw the chart
    plt.draw()
    plt.pause(0.01)

#####################################
# Process Message Function
#####################################

def process_message(message: dict) -> None:
    """
    Process a single JSON message, update sentiment counts, and update the chart.
    Use the sentiment from the producer directly.
    """
    try:
        # Extract the 'message' field and sentiment value from the producer
        message_text = message.get("message", "No message text found")
        sentiment_value = message.get("sentiment", None)

        # Display the actual message
        logger.info(f"Received message: {message_text}")

        if sentiment_value is not None:
            # Determine the sentiment category based on the sentiment value
            if sentiment_value > 0.5:
                sentiment = "positive"
            elif sentiment_value < 0.5:
                sentiment = "negative"
            else:
                sentiment = "neutral"

            # Update the sentiment counts
            sentiment_counts[sentiment] += 1

            # Log the updated sentiment counts
            logger.info(f"Updated sentiment counts: {sentiment_counts}")

            # Update the chart
            update_chart()

        else:
            logger.error("Sentiment value missing in the message.")

    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Main Function
#####################################

def main() -> None:
    """
    Main entry point for the consumer.
    - Monitors the file for new messages and updates a live chart for sentiment distribution.
    """

    logger.info("START consumer.")

    # Verify the file we're monitoring exists if not, exit early
    if not DATA_FILE.exists():
        logger.error(f"Data file {DATA_FILE} does not exist. Exiting.")
        sys.exit(1)

    try:
        # Try to open the file and read from it
        with open(DATA_FILE, "r") as file:

            # Move the cursor to the end of the file
            file.seek(0, os.SEEK_END)
            print("Consumer is ready and waiting for new JSON messages...")

            while True:
                # Read the next line from the file
                line = file.readline()

                # If we strip whitespace from the line and it's not empty
                if line.strip():  
                    # Process this new message
                    try:
                        message_dict = json.loads(line)
                        process_message(message_dict)
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON message: {line}")
                else:
                    # Otherwise, wait a brief moment before checking again
                    logger.debug("No new messages. Waiting...")
                    time.sleep(0.5)
                    continue 

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Final cleanup
        plt.ioff()
        plt.show()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    main()
