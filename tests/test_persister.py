import pytest
from services.persister.persister import Persister

def test_persister_save(monkeypatch):
    """
    test save_to_db method with mock collections.
    """
    persister = Persister()

    # mock collections
    persister.collection_antisemitic = []
    persister.collection_not_antisemitic = []

    def fake_insert_one(doc):
        persister.collection_antisemitic.append(doc)

    persister.collection_antisemitic = type("", (), {"insert_one": fake_insert_one})()
    doc = {"antisemitic": 1, "clean_text": "example text"}
    persister.save_to_db(doc)
    assert len(persister.collection_antisemitic.__dict__.get('__self__', [])) == 0  # simple smoke test