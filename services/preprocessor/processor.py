import os
import re
import json
import time
from confluent_kafka import Consumer, Producer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer


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


class Preprocessor:
    """
    this service consumes messages from Kafka,

    cleans the text, and publishes processed messages to new topics
    """

    def __init__(self):
        # Kafka setup
        kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")

        # use retry-enabled helpers
        self.consumer = create_consumer(
            kafka_broker,
            "preprocessor-group",
            ["raw_tweets_antisemitic", "raw_tweets_not_antisemitic"]
        )
        self.producer = create_producer(kafka_broker)

        # NLP tools
        self.stop_words = set(stopwords.words("english"))
        self.lemmatizer = WordNetLemmatizer()

        print("Preprocessor initialized successfully")

    def clean_text(self, text: str) -> str:
        """
        ans clean the input text step by step:
        - remove punctuation
        - remove special characters
        - remove extra spaces and tabs and newlines
        - lowercasing
        - remove stop words
        - lemmatization
        """
        # are remove punctuation and special characters
        text = re.sub(r"[^\w\s]", " ", text)

        # are remove multiple spaces and newlines
        text = re.sub(r"\s+", " ", text).strip()

        # lowercasing
        words = text.lower().split()

        # are i remove stopwords and lemmatize
        words = [self.lemmatizer.lemmatize(w) for w in words if w not in self.stop_words]

        return " ".join(words)

    def process_message(self, message):
        """
        and process a single kafka message and publish cleaned output
        """
        try:
            doc = json.loads(message.value().decode("utf-8"))
            clean = self.clean_text(doc.get("text", ""))
            
            # Add clean_text to document
            doc["clean_text"] = clean

            # Publish to the right topic
            topic = (
                "preprocessed_tweets_antisemitic"
                if doc.get("antisemitic") == 1
                else "preprocessed_tweets_not_antisemitic"
            )

            self.producer.produce(topic, str(doc).encode("utf-8"))
            print(f"Published to {topic}: {clean[:50]}")
        except Exception as e:
            print(f"Failed to process message: {e}")

    def run(self):
        """
        here we keep consuming from Kafka, process, and re-publish...
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