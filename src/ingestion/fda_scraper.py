"""FDA website scraper for recall alerts using BeautifulSoup."""

import logging
import re
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

from src.config import settings

logger = logging.getLogger(__name__)


class FDAScraper:
    """Scrapes the FDA Recalls, Market Withdrawals & Safety Alerts page."""

    def __init__(self, base_url: str | None = None):
        self.base_url = base_url or settings.FDA_WEBSITE_URL
        self._client = httpx.Client(
            timeout=30.0,
            headers={
                "User-Agent": (
                    "FDA-Recall-Surveillance/1.0 "
                    "(research pipeline; contact: admin@example.com)"
                )
            },
            follow_redirects=True,
        )

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def fetch_recent_alerts(self, max_pages: int = 3) -> list[dict]:
        """Fetch recent recall alerts from the FDA website.

        Args:
            max_pages: Maximum number of pages to scrape.

        Returns:
            List of raw recall dicts in a common intermediate format.
        """
        all_entries = []
        for page_num in range(max_pages):
            url = self.base_url
            if page_num > 0:
                url = f"{self.base_url}?page={page_num}"

            try:
                response = self._client.get(url)
                response.raise_for_status()
            except httpx.HTTPError as e:
                logger.error("Failed to fetch FDA page %d: %s", page_num, e)
                break

            soup = BeautifulSoup(response.text, "lxml")
            entries = self._parse_page(soup)
            if not entries:
                break

            all_entries.extend(entries)
            logger.info("Scraped %d entries from page %d", len(entries), page_num)

        logger.info("Total scraped entries: %d", len(all_entries))
        return all_entries

    def _parse_page(self, soup: BeautifulSoup) -> list[dict]:
        """Parse recall entries from a single page of HTML."""
        entries = []
        # The FDA recalls page lists items in a view-content container
        recall_rows = soup.select("table.cols-4 tbody tr")
        if not recall_rows:
            # Try alternative structure — the page layout may vary
            recall_rows = soup.select(".view-content .views-row")

        if not recall_rows:
            # Fall back to any table rows in the main content area
            recall_rows = soup.select("main table tbody tr")

        for row in recall_rows:
            entry = self._parse_recall_entry(row)
            if entry:
                entries.append(entry)

        return entries

    def _parse_recall_entry(self, element) -> dict | None:
        """Parse a single recall entry from an HTML element.

        Returns a dict in the common intermediate format, or None if parsing fails.
        """
        try:
            cells = element.find_all("td")
            if len(cells) < 3:
                # Try parsing as a views-row div
                return self._parse_views_row(element)

            # Table format: Date | Brand Name(s) | Product Description | Company Name
            date_text = cells[0].get_text(strip=True) if len(cells) > 0 else ""
            brand = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            description = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            company = cells[3].get_text(strip=True) if len(cells) > 3 else ""

            # Try to extract a link for more details
            link = cells[1].find("a") if len(cells) > 1 else None
            detail_url = link["href"] if link and link.get("href") else None

            recall_date = self._parse_website_date(date_text)

            # Generate a pseudo recall number from the date and company
            pseudo_id = self._generate_pseudo_id(date_text, company, brand)

            return {
                "recall_number": pseudo_id,
                "source": "fda_website",
                "product_type": self._infer_product_type(description, brand),
                "recalling_firm": company or "Unknown",
                "reason_for_recall": description or "See FDA website for details",
                "product_description": brand,
                "report_date": recall_date,
                "recall_initiation_date": recall_date,
                "detail_url": detail_url,
                "raw_html": str(element),
            }
        except Exception as e:
            logger.warning("Failed to parse recall entry: %s", e)
            return None

    def _parse_views_row(self, element) -> dict | None:
        """Parse a views-row div format."""
        title_el = element.select_one(".views-field-title a, h3 a, .field-content a")
        date_el = element.select_one(".views-field-created, .date-display-single, time")
        company_el = element.select_one(
            ".views-field-field-company, .views-field-field-recall-firm"
        )

        if not title_el:
            return None

        title = title_el.get_text(strip=True)
        date_text = date_el.get_text(strip=True) if date_el else ""
        company = company_el.get_text(strip=True) if company_el else "Unknown"
        detail_url = title_el.get("href", "")

        recall_date = self._parse_website_date(date_text)
        pseudo_id = self._generate_pseudo_id(date_text, company, title)

        return {
            "recall_number": pseudo_id,
            "source": "fda_website",
            "product_type": self._infer_product_type(title, ""),
            "recalling_firm": company,
            "reason_for_recall": title,
            "product_description": title,
            "report_date": recall_date,
            "recall_initiation_date": recall_date,
            "detail_url": detail_url,
            "raw_html": str(element),
        }

    @staticmethod
    def _parse_website_date(date_str: str):
        """Try to parse various date formats from the FDA website."""
        date_str = date_str.strip()
        formats = [
            "%m/%d/%Y",
            "%B %d, %Y",
            "%b %d, %Y",
            "%Y-%m-%d",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _infer_product_type(description: str, brand: str) -> str:
        """Infer product type from description text."""
        text = f"{description} {brand}".lower()
        drug_keywords = [
            "tablet", "capsule", "injection", "mg", "drug", "medication",
            "pharmaceutical", "rx", "prescription", "oral", "syrup",
        ]
        device_keywords = [
            "device", "implant", "catheter", "pump", "monitor", "surgical",
            "diagnostic", "ventilator", "stent", "prosthes",
        ]
        if any(kw in text for kw in drug_keywords):
            return "Drugs"
        if any(kw in text for kw in device_keywords):
            return "Devices"
        return "Food"

    @staticmethod
    def _generate_pseudo_id(date_text: str, company: str, product: str) -> str:
        """Generate a deterministic pseudo recall number for website entries."""
        clean = re.sub(r"[^a-zA-Z0-9]", "", f"{date_text}{company}{product}")[:40]
        return f"WEB-{clean}"
