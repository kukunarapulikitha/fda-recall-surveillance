"""Normalizer to transform raw API/scraper responses into a uniform recall format."""

import logging
from datetime import date, datetime

logger = logging.getLogger(__name__)


class RecallNormalizer:
    """Transforms raw recall data from different sources into a unified format."""

    def normalize(self, raw: dict, product_type: str, source: str = "openfda_api") -> dict:
        """Route to the correct normalizer based on product type and source."""
        if source == "fda_website":
            return self.normalize_website_entry(raw)
        dispatch = {
            "Drugs": self.normalize_drug_enforcement,
            "Devices": self.normalize_device_recall,
            "Food": self.normalize_food_enforcement,
        }
        normalizer = dispatch.get(product_type)
        if normalizer is None:
            raise ValueError(f"Unknown product_type: {product_type}")
        return normalizer(raw)

    def normalize_drug_enforcement(self, raw: dict) -> dict:
        """Normalize a drug enforcement record from OpenFDA."""
        openfda = self._flatten_openfda(raw.get("openfda", {}))
        return {
            "recall_number": raw.get("recall_number", ""),
            "event_id": raw.get("event_id"),
            "source": "openfda_api",
            "product_type": "Drugs",
            "classification": self._clean_text(raw.get("classification", "")),
            "status": self._clean_text(raw.get("status", "")),
            "recalling_firm": self._clean_text(raw.get("recalling_firm", "")),
            "reason_for_recall": self._clean_text(raw.get("reason_for_recall", "")),
            "product_description": self._clean_text(raw.get("product_description", "")),
            "product_quantity": raw.get("product_quantity"),
            "code_info": raw.get("code_info"),
            "distribution_pattern": raw.get("distribution_pattern"),
            "voluntary_mandated": raw.get("voluntary_mandated"),
            "initial_firm_notification": raw.get("initial_firm_notification"),
            "recall_initiation_date": self._parse_fda_date(
                raw.get("recall_initiation_date")
            ),
            "report_date": self._parse_fda_date(raw.get("report_date")),
            "center_classification_date": self._parse_fda_date(
                raw.get("center_classification_date")
            ),
            "termination_date": self._parse_fda_date(raw.get("termination_date")),
            "city": raw.get("city"),
            "state": raw.get("state"),
            "country": raw.get("country"),
            "postal_code": raw.get("postal_code"),
            **openfda,
            "raw_json": raw,
        }

    def normalize_device_recall(self, raw: dict) -> dict:
        """Normalize a device recall record from OpenFDA.

        Device API uses different field names than drug/food enforcement.
        """
        openfda = self._flatten_openfda(raw.get("openfda", {}))
        return {
            "recall_number": raw.get("res_event_number", raw.get("cfres_id", "")),
            "event_id": raw.get("cfres_id"),
            "source": "openfda_api",
            "product_type": "Devices",
            "classification": self._map_device_classification(
                raw.get("event_type", "")
            ),
            "status": self._clean_text(raw.get("recall_status", "")),
            "recalling_firm": self._clean_text(raw.get("recalling_firm", "")),
            "reason_for_recall": self._clean_text(raw.get("reason_for_recall", "")),
            "product_description": self._clean_text(
                raw.get("product_description", raw.get("code_info", ""))
            ),
            "product_quantity": raw.get("product_quantity"),
            "code_info": raw.get("code_info"),
            "distribution_pattern": raw.get("distribution_pattern"),
            "voluntary_mandated": raw.get("voluntary_mandated"),
            "initial_firm_notification": raw.get("initial_firm_notification"),
            "recall_initiation_date": self._parse_fda_date(
                raw.get("event_date_initiated")
            ),
            "report_date": self._parse_fda_date(
                raw.get("event_date_posted", raw.get("event_date_created"))
            ),
            "center_classification_date": self._parse_fda_date(
                raw.get("center_classification_date")
            ),
            "termination_date": self._parse_fda_date(
                raw.get("event_date_terminated")
            ),
            "city": raw.get("city"),
            "state": raw.get("state"),
            "country": raw.get("country"),
            "postal_code": raw.get("postal_code"),
            **openfda,
            "raw_json": raw,
        }

    def normalize_food_enforcement(self, raw: dict) -> dict:
        """Normalize a food enforcement record from OpenFDA."""
        openfda = self._flatten_openfda(raw.get("openfda", {}))
        return {
            "recall_number": raw.get("recall_number", ""),
            "event_id": raw.get("event_id"),
            "source": "openfda_api",
            "product_type": "Food",
            "classification": self._clean_text(raw.get("classification", "")),
            "status": self._clean_text(raw.get("status", "")),
            "recalling_firm": self._clean_text(raw.get("recalling_firm", "")),
            "reason_for_recall": self._clean_text(raw.get("reason_for_recall", "")),
            "product_description": self._clean_text(raw.get("product_description", "")),
            "product_quantity": raw.get("product_quantity"),
            "code_info": raw.get("code_info"),
            "distribution_pattern": raw.get("distribution_pattern"),
            "voluntary_mandated": raw.get("voluntary_mandated"),
            "initial_firm_notification": raw.get("initial_firm_notification"),
            "recall_initiation_date": self._parse_fda_date(
                raw.get("recall_initiation_date")
            ),
            "report_date": self._parse_fda_date(raw.get("report_date")),
            "center_classification_date": self._parse_fda_date(
                raw.get("center_classification_date")
            ),
            "termination_date": self._parse_fda_date(raw.get("termination_date")),
            "city": raw.get("city"),
            "state": raw.get("state"),
            "country": raw.get("country"),
            "postal_code": raw.get("postal_code"),
            **openfda,
            "raw_json": raw,
        }

    def normalize_website_entry(self, raw: dict) -> dict:
        """Normalize a record scraped from the FDA website."""
        raw_for_storage = {k: v for k, v in raw.items() if k != "raw_html"}
        raw_for_storage["_scrape_html"] = raw.get("raw_html", "")
        return {
            "recall_number": raw.get("recall_number", ""),
            "event_id": None,
            "source": "fda_website",
            "product_type": raw.get("product_type", "Food"),
            "classification": None,
            "status": "Ongoing",
            "recalling_firm": raw.get("recalling_firm", "Unknown"),
            "reason_for_recall": raw.get("reason_for_recall", ""),
            "product_description": raw.get("product_description"),
            "product_quantity": None,
            "code_info": None,
            "distribution_pattern": None,
            "voluntary_mandated": None,
            "initial_firm_notification": None,
            "recall_initiation_date": raw.get("recall_initiation_date"),
            "report_date": raw.get("report_date"),
            "center_classification_date": None,
            "termination_date": None,
            "city": None,
            "state": None,
            "country": "United States",
            "postal_code": None,
            "brand_name": None,
            "generic_name": None,
            "manufacturer_name": None,
            "product_ndc": None,
            "substance_name": None,
            "route": None,
            "application_number": None,
            "raw_json": raw_for_storage,
        }

    @staticmethod
    def _parse_fda_date(date_str: str | None) -> date | None:
        """Parse FDA date format YYYYMMDD to a Python date object."""
        if not date_str:
            return None
        date_str = date_str.strip()
        if len(date_str) == 8 and date_str.isdigit():
            try:
                return datetime.strptime(date_str, "%Y%m%d").date()
            except ValueError:
                return None
        # Try ISO format
        try:
            return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        except (ValueError, IndexError):
            return None

    @staticmethod
    def _flatten_openfda(openfda: dict) -> dict:
        """Flatten the nested openfda object into top-level fields."""
        def first_or_join(value):
            if isinstance(value, list):
                return ", ".join(str(v) for v in value) if value else None
            return value

        return {
            "brand_name": first_or_join(openfda.get("brand_name")),
            "generic_name": first_or_join(openfda.get("generic_name")),
            "manufacturer_name": first_or_join(openfda.get("manufacturer_name")),
            "product_ndc": first_or_join(openfda.get("product_ndc")),
            "substance_name": first_or_join(openfda.get("substance_name")),
            "route": first_or_join(openfda.get("route")),
            "application_number": first_or_join(openfda.get("application_number")),
        }

    @staticmethod
    def _clean_text(text: str | None) -> str:
        """Strip whitespace and normalize encoding."""
        if not text:
            return ""
        return " ".join(text.split())

    @staticmethod
    def _map_device_classification(event_type: str) -> str | None:
        """Map device event_type to recall classification."""
        mapping = {
            "Recall": "Class II",  # Default for generic recalls
            "Class I": "Class I",
            "Class II": "Class II",
            "Class III": "Class III",
        }
        return mapping.get(event_type, event_type if event_type else None)
