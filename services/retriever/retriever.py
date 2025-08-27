import os
import time
from pymongo import MongoClient
from confluent_kafka import Producer

class Retriever:
    """
    This class connects to mongodb Atlas, fetches documents,

    and publishes them into kafka topics based on classification
    """

    def __init__(self):
        # are i read environment variables (no hardcoding!)
        mongo_uri = os.getenv("MONGO_URI")
        kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")

        if not mongo_uri:
            raise ValueError("Missing environment variable: MONGO_URI")

        # and setup mongodb connection
        self.client = MongoClient(mongo_uri)
        self.db = self.client.get_database("IranMalDB")
        self.collection = self.db.get_collection("tweets")

        # setup kafka producer
        self.producer = Producer({"bootstrap.servers": kafka_broker})

        self.last_timestamp = None

        print("Retriever initialized successfully")

    def fetch_documents(self, limit=100):
        """
        now fetch oldest documents from mongodb based on timestamp
        """
        if self.last_timestamp:
            cursor = self.collection.find({"timestamp": {"$gt": self.last_timestamp}}).sort("timestamp", 1).limit(limit)
        else:
            cursor = self.collection.find().sort("timestamp", 1).limit(limit)
        docs = list(cursor)
        if docs:
            self.last_timestamp = docs[-1]["timestamp"]
        return docs
    
    def publish_to_kafka(self, documents):
        """
        and publish documents to kafka based on antisemitic flag
        """
        for doc in documents:
            topic = (
                "raw_tweets_antisemitic"
                if doc.get("antisemitic") == 1
                else "raw_tweets_not_antisemitic"
            )
            try:
                self.producer.produce(topic, str(doc).encode("utf-8"))
                print(f"Published to {topic}: {doc.get('text')[:50]}")
            except Exception as e:
                print(f"Failed to publish message: {e}")

        self.producer.flush()

    def run(self):
        """
        now i run the retriever loop (fetch every 60 seconds).
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

            # and wait 60 seconds before next fetch
            time.sleep(60)