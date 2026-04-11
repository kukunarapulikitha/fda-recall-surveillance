"""Tests for the FDA website scraper."""

from src.ingestion.fda_scraper import FDAScraper


class TestFDAScraperUnit:
    def test_parse_website_date_formats(self):
        assert FDAScraper._parse_website_date("01/15/2026") is not None
        assert FDAScraper._parse_website_date("January 15, 2026") is not None
        assert FDAScraper._parse_website_date("2026-01-15") is not None
        assert FDAScraper._parse_website_date("invalid") is None

    def test_infer_product_type_drug(self):
        assert FDAScraper._infer_product_type("Metformin tablets 500mg", "") == "Drugs"
        assert FDAScraper._infer_product_type("Oral injection solution", "") == "Drugs"

    def test_infer_product_type_device(self):
        assert FDAScraper._infer_product_type("Surgical catheter system", "") == "Devices"
        assert FDAScraper._infer_product_type("Blood glucose monitor device", "") == "Devices"

    def test_infer_product_type_food(self):
        assert FDAScraper._infer_product_type("Organic peanut butter", "") == "Food"
        assert FDAScraper._infer_product_type("Frozen chicken nuggets", "") == "Food"

    def test_generate_pseudo_id(self):
        pid = FDAScraper._generate_pseudo_id("01/15/2026", "Test Corp", "Widget")
        assert pid.startswith("WEB-")
        assert len(pid) <= 44  # WEB- + up to 40 chars

    def test_generate_pseudo_id_deterministic(self):
        a = FDAScraper._generate_pseudo_id("01/15/2026", "Corp", "Prod")
        b = FDAScraper._generate_pseudo_id("01/15/2026", "Corp", "Prod")
        assert a == b
