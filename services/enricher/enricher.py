import os
import re
import json
import time
from textblob import TextBlob
from confluent_kafka import Consumer, Producer


def create_consumer(broker, group_id, topics, retries=10, delay=5):
    for i in range(retries):
        try:
            c = Consumer({
                "bootstrap.servers": broker,
                "group.id": group_id,
                "auto.offset.reset": "earliest"
            })
            c.subscribe(topics)
            print(f"Connected Kafka Consumer (topics={topics})")
            return c
        except Exception as e:
            print(f"Consumer connection failed (attempt {i+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka Consumer after retries")


def create_producer(broker, retries=10, delay=5):
    for i in range(retries):
        try:
            p = Producer({"bootstrap.servers": broker})
            p.produce("healthcheck", b"ping")
            p.flush(1)
            print("Connected Kafka Producer")
            return p
        except Exception as e:
            print(f"Producer connection failed (attempt {i+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka Producer after retries")


class Enricher:
    """
    This service consumes preprocessed messages from Kafka,
    adds extra features (sentiment, weapons detection, timestamps),
    and publishes enriched messages to new topics.
    """

    def __init__(self):
        # the kafka setup
        kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")

        # use retry-enabled helpers
        self.consumer = create_consumer(
            kafka_broker,
            "enricher-group",
            ["preprocessed_tweets_antisemitic", "preprocessed_tweets_not_antisemitic"]
        )
        self.producer = create_producer(kafka_broker)

        # load weapons list
        weapons_file = os.getenv("WEAPONS_FILE", "../../data/weapons.txt")
        try:
            with open(weapons_file, "r") as f:
                self.weapons = [w.strip().lower() for w in f.readlines()]
        except FileNotFoundError:
            self.weapons = []
            print("Weapons file not found, continuing without it.")

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
        and detect weapon keywords from the blacklist file.
        """
        return [w for w in self.weapons if w in text.lower()]

    def detect_timestamp(self, text: str):
        """
        are we try to find a timestamp inside the text (very simple regex).
        Example formats: 25/03/2020 09:30 or -
        """
        match = re.search(r"\d{2}[-/]\d{2}[-/]\d{4} \d{2}:\d{2}", text)
        return match.group(0) if match else ""

    def process_message(self, message):
        """
        and process one Kafka message: add features and publish enriched doc.
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