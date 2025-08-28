# Malicious Text Feature Engineering System

## The system itself

We have built a system for **retrieving, processing and storing hostile tweets** from a data source (mongoDB Atlas), and it goes through a **pipeline** route using **Kafka**.
At each stage of the text, we did additional processing, until the text was stored in a local database and for external retrieval via API

---

## Structure from above

The system is essentially **Microservices**, and each service runs separately, then it connects to Kafka and Mongo, and does everything under the **Single Responsibility** principle:

1. **Retriever**

* Retrieves texts from mongoDb Atlas.
* Then it divides them into two topics in Kafka (`raw_tweets_antisemitic`, `raw_tweets_not_antisemitic`) according to the specified classification

2. **Preprocessor**

* Now it listens to the raw messages.
* Cleans text (punctuation, special characters, stopwords, lowercase, Lemmatization).
* Then sends back to Kafka in new topics (`preprocessed_*`).

3. **Enricher**

* At this stage we receive cleaned texts
* Adds features:

* **Sentiment** analysis (positive/negative/neutral).
* **Weapon identification** from a blacklist
* **Extracting dates/times** from the text.
* Finally publishes to Kafka in `enriched_preprocessed_*`.

4. **Persister**

* At this stage, we listen to enriched messages.
* and save to a local database (MongoDB container).
* and create two collections:

* `tweets_antisemitic`
* `tweets_not_antisemitic`

5. **DataRetrieval (API)**

* A simple FastAPI server.
* Provides two endpoints:

* `/antisemitic` All antisemitic tweets (including processing results).
* `/not-antisemitic` All non-antisemitic tweets.

---

## Data Flow

1. **Atlas** - Retriever - Kafka (raw).
2. Kafka (raw) - Preprocessor - Kafka (preprocessed).
3. Kafka (preprocessed) - Enricher - Kafka (enriched).
4. Kafka (enriched) - Persister - Mongo Local.
5. Mongo Local - DataRetrieval API - Client (browser / Postman).

---

## Local usage

1. Run:

```bat
scripts/local_commands.bat
```

I made the script initialize everything (Kafka, Mongo, services, API).

2. If you want to access the API:

* [http://localhost:8000](http://localhost:8000)
* [http://localhost:8000/antisemitic](http://localhost:8000/antisemitic)
* [http://localhost:8000/not-antisemitic](http://localhost:8000/not-antisemitic)

---