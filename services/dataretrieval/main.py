import uvicorn
from api import app

if __name__ == "__main__":
    print("DataRetrieval service is starting...")
    uvicorn.run(app, host="0.0.0.0", port=8000)