import pytest
from services.preprocessor.processor import Preprocessor

def test_clean_text_basic(monkeypatch):
    """
    and test basic cleaning of text.
    """
    pre = Preprocessor()
    text = "Hello!!!   WORLD??"
    clean = pre.clean_text(text)
    assert "hello" in clean
    assert "world" in clean