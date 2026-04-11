"""Test fixtures and configuration."""

import json
from pathlib import Path

import pytest

SAMPLE_DIR = Path(__file__).parent / "sample_responses"


@pytest.fixture
def drug_enforcement_response():
    """Load sample drug enforcement API response."""
    with open(SAMPLE_DIR / "drug_enforcement.json") as f:
        return json.load(f)


@pytest.fixture
def device_recall_response():
    """Load sample device recall API response."""
    with open(SAMPLE_DIR / "device_recall.json") as f:
        return json.load(f)


@pytest.fixture
def food_enforcement_response():
    """Load sample food enforcement API response."""
    with open(SAMPLE_DIR / "food_enforcement.json") as f:
        return json.load(f)


@pytest.fixture
def drug_record(drug_enforcement_response):
    """Single drug enforcement record."""
    return drug_enforcement_response["results"][0]


@pytest.fixture
def device_record(device_recall_response):
    """Single device recall record."""
    return device_recall_response["results"][0]


@pytest.fixture
def food_record(food_enforcement_response):
    """Single food enforcement record."""
    return food_enforcement_response["results"][0]
