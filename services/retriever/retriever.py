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
    This class connects to MongoDB Atlas, fetches documents,
    and publishes them into Kafka topics based on classification.
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
        Fetch oldest documents from MongoDB based on CreateDate.
        Map Atlas fields into standard schema.
        """
        if self.last_timestamp:
            cursor = self.collection.find(
                {"CreateDate": {"$gt": self.last_timestamp}}
            ).sort("CreateDate", 1).limit(limit)
        else:
            cursor = self.collection.find().sort("CreateDate", 1).limit(limit)

        docs = list(cursor)
        if docs:
            self.last_timestamp = docs[-1]["CreateDate"]

        # Map Atlas fields into our schema
        mapped_docs = []
        for d in docs:
            mapped_docs.append({
                "id": str(d.get("_id")),
                "text": d.get("text", ""),
                "antisemitic": d.get("Antisemitic", 0),
                "timestamp": d.get("CreateDate"),
            })

        print(f"Fetched {len(mapped_docs)} documents from Atlas")
        return mapped_docs
    
    def publish_to_kafka(self, documents):
        """
        Publish documents to Kafka based on antisemitic flag.
        """
        for doc in documents:
            topic = (
                "raw_tweets_antisemitic"
                if doc.get("antisemitic") == 1
                else "raw_tweets_not_antisemitic"
            )
            try:
                self.producer.produce(topic, json.dumps(doc, default=str).encode("utf-8"))
                print(f"Published to {topic}: {doc.get('text')[:50]}")
            except Exception as e:
                print(f"Failed to publish message: {e}")

        self.producer.flush()

    def run(self):
        """
        Run the retriever loop (fetch every 60 seconds).
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