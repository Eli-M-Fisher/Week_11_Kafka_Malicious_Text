import os
import re
import json
from confluent_kafka import Consumer, Producer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

class Preprocessor:
    """
    are is service consumes messages from Kafka

    cleans the text, and publishes processed messages to new topics
    """

    def __init__(self):
        # Kafka setup
        kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")

        self.consumer = Consumer({
            "bootstrap.servers": kafka_broker,
            "group.id": "preprocessor-group",
            "auto.offset.reset": "earliest"
        })

        self.producer = Producer({"bootstrap.servers": kafka_broker})

        # Subscribe to retriever topics
        self.consumer.subscribe(["raw_tweets_antisemitic", "raw_tweets_not_antisemitic"])

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
        are we keep consuming from Kafka, process, and re-publish...
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