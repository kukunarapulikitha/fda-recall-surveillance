"""Tests for the pipeline orchestrator."""

from src.ingestion.pipeline import RecallPipeline


class TestDeduplicate:
    def test_dedup_by_recall_number(self):
        records = [
            {"recall_number": "D-001", "source": "openfda_api", "report_date": None},
            {"recall_number": "D-001", "source": "openfda_api", "report_date": None},
            {"recall_number": "D-002", "source": "openfda_api", "report_date": None},
        ]
        result = RecallPipeline._deduplicate(records)
        assert len(result) == 2
        recall_numbers = {r["recall_number"] for r in result}
        assert recall_numbers == {"D-001", "D-002"}

    def test_dedup_api_wins_over_website(self):
        records = [
            {"recall_number": "D-001", "source": "fda_website", "report_date": None},
            {"recall_number": "D-001", "source": "openfda_api", "report_date": None},
        ]
        result = RecallPipeline._deduplicate(records)
        assert len(result) == 1
        assert result[0]["source"] == "openfda_api"

    def test_dedup_keeps_more_recent(self):
        from datetime import date

        records = [
            {"recall_number": "D-001", "source": "openfda_api", "report_date": date(2026, 1, 1)},
            {"recall_number": "D-001", "source": "openfda_api", "report_date": date(2026, 3, 1)},
        ]
        result = RecallPipeline._deduplicate(records)
        assert len(result) == 1
        assert result[0]["report_date"] == date(2026, 3, 1)

    def test_dedup_empty_recall_number(self):
        records = [
            {"recall_number": "", "source": "openfda_api", "report_date": None},
            {"recall_number": "D-001", "source": "openfda_api", "report_date": None},
        ]
        result = RecallPipeline._deduplicate(records)
        assert len(result) == 1
        assert result[0]["recall_number"] == "D-001"
