import pytest
from services.enricher.enricher import Enricher

def test_sentiment_detection():
    """
    test sentiment detection logic.
    """
    enr = Enricher()
    assert enr.detect_sentiment("I love this!") == "positive"
    assert enr.detect_sentiment("I hate this!") == "negative"
    assert enr.detect_sentiment("") == "neutral"

def test_weapon_detection(tmp_path):
    """
    and test weapon detection from file.
    """
    weapons_file = tmp_path / "weapons.txt"
    weapons_file.write_text("gun\nknife\n")

    monkey_env = {"WEAPONS_FILE": str(weapons_file)}
    for k, v in monkey_env.items():
        import os; os.environ[k] = v

    enr = Enricher()
    found = enr.detect_weapons("He has a big gun")
    assert "gun" in found