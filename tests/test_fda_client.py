"""Tests for the OpenFDA API client."""

import pytest

from src.ingestion.fda_client import FDAClient, ENDPOINTS


class TestFDAClientUnit:
    """Unit tests that don't hit the real API."""

    def test_endpoints_defined(self):
        assert "Drugs" in ENDPOINTS
        assert "Devices" in ENDPOINTS
        assert "Food" in ENDPOINTS

    def test_client_init_defaults(self):
        client = FDAClient(api_key="", base_url="https://api.fda.gov")
        assert client.base_url == "https://api.fda.gov"
        assert client.rpm == 200
        client.close()

    def test_client_context_manager(self):
        with FDAClient() as client:
            assert client is not None

    def test_get_records_invalid_type(self):
        with FDAClient() as client:
            with pytest.raises(ValueError, match="Unknown product_type"):
                list(client.get_records("InvalidType", "20260101", "20260401"))


class TestFDAClientIntegration:
    """Integration tests that hit the real OpenFDA API.

    These are marked as slow and skipped by default.
    Run with: pytest -m integration
    """

    @pytest.mark.integration
    def test_fetch_drug_enforcements(self):
        with FDAClient() as client:
            batches = list(client.get_drug_enforcements("20250101", "20250131"))
            assert len(batches) > 0
            first_batch = batches[0]
            assert len(first_batch) > 0
            record = first_batch[0]
            assert "recall_number" in record
            assert "recalling_firm" in record

    @pytest.mark.integration
    def test_fetch_device_recalls(self):
        with FDAClient() as client:
            batches = list(client.get_device_recalls("20250101", "20250131"))
            assert len(batches) > 0

    @pytest.mark.integration
    def test_fetch_food_enforcements(self):
        with FDAClient() as client:
            batches = list(client.get_food_enforcements("20250101", "20250131"))
            assert len(batches) > 0

    @pytest.mark.integration
    def test_fetch_page_single(self):
        with FDAClient() as client:
            data = client.fetch_page("drug/enforcement.json", limit=5)
            assert "meta" in data
            assert "results" in data
            assert len(data["results"]) <= 5
