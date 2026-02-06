"""Test load pairs functionality."""

from dataclasses import FrozenInstanceError
from typing import Final

import pytest

from src.cnt_collector_node import load_pairs


def test_load_pairs_from_file(tmp_path):
    """Test successful load of test pairs. Make sure that the
    dataclass ensures the pairs remain readonly.
    """

    pairs: Final[str] = 'DEX_PAIRS = {"hello": "world"}'
    pairs_file = tmp_path / "pairs.py"
    pairs_file.write_text(pairs, encoding="utf-8")
    dex_pairs = load_pairs.load(path=str(pairs_file))
    assert dex_pairs.DEX_PAIRS["hello"] == "world"
    with pytest.raises(FrozenInstanceError):
        # Attempt to modify the dataclass. This should fail with
        # FrozenInstanceError.
        dex_pairs.pairs = []
    pycache = tmp_path / "__pycache__"
    assert not pycache.exists()


def test_load_pairs_no_file():
    """Test `load_pairs` functionality when the file doesn't exit."""

    with pytest.raises(SystemExit):
        load_pairs.load(path="")


def test_load_pairs_no_attribute(tmp_path):
    """Test `load_pairs` functionality when `DEX_PAIRS` doesn't exist"""

    pairs_file = tmp_path / "pairs.py"
    pairs_file.write_text("", encoding="utf-8")
    with pytest.raises(SystemExit):
        load_pairs.load(path=str(pairs_file))
