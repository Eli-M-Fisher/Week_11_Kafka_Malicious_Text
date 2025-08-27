import pytest
from services.retriever.retriever import Retriever

def test_retriever_init(monkeypatch):
    """
    now i test Retriever initialization with missing env var
    """
    monkeypatch.delenv("MONGO_URI", raising=False)
    with pytest.raises(ValueError):
        Retriever()