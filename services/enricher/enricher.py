import os
import re
import json
from textblob import TextBlob
from confluent_kafka import Consumer, Producer

class Enricher:
    """
    This service consumes preprocessed messages from Kafka,
    adds extra features (sentiment, weapons detection, timestamps),
    and publishes enriched messages to new topics
    """

    def __init__(self):
        # the kafka setup
        kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")

        self.consumer = Consumer({
            "bootstrap.servers": kafka_broker,
            "group.id": "enricher-group",
            "auto.offset.reset": "earliest"
        })

        self.producer = Producer({"bootstrap.servers": kafka_broker})

        # and subscribe to preprocessor topics
        self.consumer.subscribe([
            "preprocessed_tweets_antisemitic",
            "preprocessed_tweets_not_antisemitic"
        ])

        # laestly load weapons set
        self.weapons = self._load_weapons()

        print("Enricher initialized successfully")

    def detect_sentiment(self, text: str) -> str:
        """
        are i detect sentiment: positive or negative or neutral
        """
        if not text:
            return "neutral"
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        if polarity > 0.1:
            return "positive"
        elif polarity < -0.1:
            return "negative"
        return "neutral"

    def detect_weapons(self, text: str):
        """
        detect weapon keywords from the blacklist file.
        """
        words = {word.strip().lower() for word in text.split()}
        found = words & self.weapons
        return list(found)

    def detect_timestamp(self, text: str):
        """
        are we try to find a timestamp inside the text (very simple regex).
        Example formats: 25/03/2020 09:30 or -
        """
        match = re.search(r"\d{2}[-/]\d{2}[-/]\d{4} \d{2}:\d{2}", text)
        return match.group(0) if match else ""

    def process_message(self, message):
        """
        process one Kafka message: add features and publish enriched doc.
        """
        try:
            doc = json.loads(message.value().decode("utf-8"))
            clean_text = doc.get("clean_text", "")

            doc["sentiment"] = self.detect_sentiment(clean_text)
            doc["weapons_detected"] = self.detect_weapons(clean_text)
            doc["relevant_timestamp"] = self.detect_timestamp(doc.get("text", ""))

            topic = (
                "enriched_preprocessed_tweets_antisemitic"
                if doc.get("antisemitic") == 1
                else "enriched_preprocessed_tweets_not_antisemitic"
            )

            self.producer.produce(topic, str(doc).encode("utf-8"))
            print(f"Published to {topic}: {clean_text[:50]}")
        except Exception as e:
            print(f"Failed to enrich message: {e}")

    def run(self):
        """
        Keep consuming, enrich, and republish.
        """
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            self.process_message(msg)
            self.producer.flush()
    
    def _load_weapons(self):
        weapons_file = os.getenv("WEAPONS_FILE", "../../data/weapons.txt")
        try:
            with open(weapons_file, "r") as f:
                return set([w.strip().lower() for w in f.readlines()])
        except FileNotFoundError:
            print("Weapons file not found, continuing without it.")
            return set([])
