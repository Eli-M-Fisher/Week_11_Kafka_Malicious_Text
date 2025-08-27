from fastapi.testclient import TestClient
from services.dataretrieval.api import app

client = TestClient(app)

def test_root_endpoint():
    """
    test root endpoint returns success message.
    """
    response = client.get("/")
    assert response.status_code == 200
    assert "Data Retrieval Service" in response.json()["message"]

def test_antisemitic_endpoint(monkeypatch):
    """
    and test /antisemitic endpoint with mocked db
    """
    from services.dataretrieval import api

    # mock collection
    api.collection_antisemitic = [{"_id": "123", "clean_text": "test"}]

    response = client.get("/antisemitic")
    assert response.status_code == 200