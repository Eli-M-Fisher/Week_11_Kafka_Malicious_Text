import os
import json
from pymongo import MongoClient
from confluent_kafka import Consumer

class Persister:
    """
    This service consumes enriched tweets from Kafka
    and saves them into a local MongoDB.
    """

    def __init__(self):
        # Kafka setup
        kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        self.consumer = Consumer({
            "bootstrap.servers": kafka_broker,
            "group.id": "persister-group",
            "auto.offset.reset": "earliest"
        })
        self.consumer.subscribe([
            "enriched_preprocessed_tweets_antisemitic",
            "enriched_preprocessed_tweets_not_antisemitic"
        ])

        # MongoDB setup
        mongo_uri = os.getenv("MONGO_LOCAL_URI", "mongodb://localhost:27017")
        self.client = MongoClient(mongo_uri)
        self.db = self.client.get_database("LocalTweetsDB")
        self.collection_antisemitic = self.db.get_collection("tweets_antisemitic")
        self.collection_not_antisemitic = self.db.get_collection("tweets_not_antisemitic")

        print("Persister initialized successfully")

    def save_to_db(self, doc: dict):
        """
        now save the document to the proper mongoDB collection.
        """
        try:
            if doc.get("antisemitic") == 1:
                self.collection_antisemitic.insert_one(doc)
                print(f"Saved to tweets_antisemitic: {doc.get('clean_text', '')[:50]}")
            else:
                self.collection_not_antisemitic.insert_one(doc)
                print(f"Saved to tweets_not_antisemitic: {doc.get('clean_text', '')[:50]}")
        except Exception as e:
            print(f"Failed to save document: {e}")

    def process_message(self, message):
        """
        i convert kafka message to dict and save it
        """
        try:
            doc = json.loads(message.value().decode("utf-8"))
            self.save_to_db(doc)
        except Exception as e:
            print(f"Failed to process message: {e}")

    def run(self):
        """
        and keep consuming from Kafka and persist documents
        """
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue

            self.process_message(msg)