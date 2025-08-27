import os
import time
import json
from pymongo import MongoClient
from confluent_kafka import Producer

def create_producer(broker, retries=10, delay=5):
    """
    Try to connect to Kafka with retries.
    """
    for i in range(retries):
        try:
            p = Producer({"bootstrap.servers": broker})
            # quick test produce (to make sure broker is reachable)
            p.produce("healthcheck", b"ping")
            p.flush(1)
            print("Connected to Kafka")
            return p
        except Exception as e:
            print(f"Failed to connect to Kafka (attempt {i+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after retries")

class Retriever:
    """
    This class connects to mongodb Atlas, fetches documents,
    and publishes them into kafka topics based on classification
    """

    def __init__(self):
        # read environment variables (no hardcoding!)
        mongo_uri = os.getenv("MONGO_URI")
        kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")

        if not mongo_uri:
            raise ValueError("Missing environment variable: MONGO_URI")

        # setup mongodb connection
        self.client = MongoClient(mongo_uri)
        self.db = self.client.get_database("IranMalDB")
        self.collection = self.db.get_collection("tweets")

        # setup kafka producer with retry
        self.producer = create_producer(kafka_broker)

        self.last_timestamp = None
        print("Retriever initialized successfully")

    def fetch_documents(self, limit=100):
        """
        fetch oldest documents from mongodb based on timestamp
        """
        if self.last_timestamp:
            cursor = self.collection.find(
                {"timestamp": {"$gt": self.last_timestamp}}
            ).sort("timestamp", 1).limit(limit)
        else:
            cursor = self.collection.find().sort("timestamp", 1).limit(limit)

        docs = list(cursor)
        if docs:
            self.last_timestamp = docs[-1]["timestamp"]
        return docs
    
    def publish_to_kafka(self, documents):
        """
        publish documents to kafka based on antisemitic flag
        """
        for doc in documents:
            topic = (
                "raw_tweets_antisemitic"
                if doc.get("antisemitic") == 1
                else "raw_tweets_not_antisemitic"
            )
            try:
                self.producer.produce(topic, json.dumps(doc).encode("utf-8"))
                print(f"Published to {topic}: {doc.get('text')[:50]}")
            except Exception as e:
                print(f"Failed to publish message: {e}")

        self.producer.flush()

    def run(self):
        """
        run the retriever loop (fetch every 60 seconds)
        """
        while True:
            try:
                docs = self.fetch_documents(limit=100)
                if docs:
                    self.publish_to_kafka(docs)
                else:
                    print("No new documents found")
            except Exception as e:
                print(f"Error in run loop: {e}")

            # wait 60 seconds before next fetch
            time.sleep(60)