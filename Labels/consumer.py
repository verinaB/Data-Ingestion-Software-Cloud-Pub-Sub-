from google.cloud import pubsub_v1
import os
import json
from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import json
import os 

# Set up the environment for Google Cloud credentials
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set up project and subscription
project_id = ""  # Replace with your GCP project ID
subscription_name = ""  # Replace with your desired subscription name

# Initialize Pub/Sub Subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)
print(f"Listening for messages on {subscription_path}...")

# Callback function to process received messages
def callback(message):
    try:
        # Deserialize the message
        record = json.loads(message.data.decode("utf-8"))
        print(f"Consumed record: {record}")
        
        # Acknowledge the message after processing
        message.ack()
    except Exception as e:
        print(f"Failed to process message: {e}")
        message.nack()

# Subscribe and start listening for messages
with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("\nStopped listening for messages.")
