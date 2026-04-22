"""Capture full-page screenshots of each Task 2 analytics page via Playwright.

Usage:
    python scripts/capture_screenshots.py

Assumes the monitoring dashboard is running at http://localhost:8501.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from playwright.async_api import Page, async_playwright

DASHBOARD_URL = "http://localhost:8501"
OUT_DIR = Path(__file__).resolve().parent.parent / "docs" / "images" / "task2"

# Wait for Plotly charts to fully render before capturing.
PLOT_WAIT_MS = 3500


async def _pick_page(page: Page, label: str) -> None:
    """Click the sidebar radio option with the given label."""
    # Streamlit renders radio options as <label> with inner text.
    await page.get_by_text(label, exact=True).first.click()
    await page.wait_for_timeout(PLOT_WAIT_MS)


async def _shot(page: Page, filename: str) -> None:
    path = OUT_DIR / filename
    await page.screenshot(path=str(path), full_page=True)
    print(f"  wrote {path.relative_to(OUT_DIR.parent.parent.parent)}")


async def capture() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1600, "height": 1000})
        page = await context.new_page()

        print("Loading dashboard...")
        await page.goto(DASHBOARD_URL, wait_until="networkidle")
        await page.wait_for_timeout(PLOT_WAIT_MS)

        print("Recall Pattern Analysis (Categorization tab)...")
        await _pick_page(page, "Recall Pattern Analysis")
        await _shot(page, "01-recall-patterns-categorization.png")

        print("Recall Pattern Analysis (Temporal tab)...")
        await page.get_by_text("Temporal Patterns", exact=True).first.click()
        await page.wait_for_timeout(PLOT_WAIT_MS)
        await _shot(page, "02-recall-patterns-temporal.png")

        print("Recall Pattern Analysis (Severity tab)...")
        await page.get_by_text("Severity & Risk", exact=True).first.click()
        await page.wait_for_timeout(PLOT_WAIT_MS)
        await _shot(page, "03-recall-patterns-severity.png")

        print("High-Risk Rankings...")
        await _pick_page(page, "High-Risk Rankings")
        await _shot(page, "04-high-risk-rankings.png")

        print("Executive Summary...")
        await _pick_page(page, "Executive Summary")
        await _shot(page, "05-executive-summary.png")

        await browser.close()
        print("Done.")


if __name__ == "__main__":
    asyncio.run(capture())
