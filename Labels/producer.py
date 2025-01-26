from google.cloud import pubsub_v1
import os
import csv
import json
from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import json
import os 

# Set up the environment for Google Cloud credentials
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set up project and topic
project_id = ""  # Replace with your GCP project ID
topic_name = ""  # Replace with your desired topic name

# Initialize Pub/Sub Publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}...")

# Read the CSV file and publish records
csv_file_path = "Labels.csv"  # Update this path to where your CSV file is located

try:
    with open(csv_file_path, mode="r") as csv_file:
        reader = csv.DictReader(csv_file)
        
        for row in reader:
            # Serialize the dictionary into a JSON message
            message = json.dumps(row).encode("utf-8")
            
            # Publish the message to the topic
            future = publisher.publish(topic_path, message)
            
            # Ensure publishing is completed
            print(f"Published message ID: {future.result()}")
            print(f"Published record: {row}")

except FileNotFoundError:
    print(f"Error: The file {csv_file_path} does not exist.")
except Exception as e:
    print(f"An error occurred: {e}")
