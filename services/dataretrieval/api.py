import os
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient

app = FastAPI(title="Data Retrieval Service")

client = None
db = None
collection_antisemitic = None
collection_not_antisemitic = None

@app.on_event("startup")
def startup_db():
    """
    first we connect to MongoDB when the service starts.
    """
    global client, db, collection_antisemitic, collection_not_antisemitic
    mongo_uri = os.getenv("MONGO_LOCAL_URI", "mongodb://localhost:27017")
    client = MongoClient(mongo_uri)
    db = client.get_database("LocalTweetsDB")
    collection_antisemitic = db.get_collection("tweets_antisemitic")
    collection_not_antisemitic = db.get_collection("tweets_not_antisemitic")
    print("Connected to MongoDB (startup)")

@app.on_event("shutdown")
def shutdown_db():
    """
    close mongodb connection when the service stops.
    """
    global client
    if client:
        client.close()
        print("MongoDB connection closed")

@app.get("/")
def root():
    return {"message": "Data Retrieval Service is running!"}

@app.get("/antisemitic")
def get_antisemitic_tweets(limit: int = 100):
    """
    now i return antisemitic tweets (default limit = 100).
    """
    try:
        docs = list(collection_antisemitic.find().limit(limit))
        for d in docs:
            d["_id"] = str(d["_id"])   # Convert ObjectId to string
        return docs
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {e}")

@app.get("/not-antisemitic")
def get_not_antisemitic_tweets(limit: int = 100):
    """
    and return non-antisemitic tweets (default limit = 100).
    """
    try:
        docs = list(collection_not_antisemitic.find().limit(limit))
        for d in docs:
            d["_id"] = str(d["_id"])  # Convert ObjectId to string
        return docs
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {e}")