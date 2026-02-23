"""
Tests for Apify-based scraper (AI extractor and mapping to Truth rows).
"""

import pytest
import pandas as pd

from scraper import scraped_items_to_truth_rows, ScraperError, _normalize_item


class TestNormalizeItem:
    """Tests for _normalize_item."""

    def test_normalize_first_last_organization(self):
        """Normalize firstName, lastName, organization."""
        item = {"firstName": "John", "lastName": "Doe", "organization": "Acme"}
        out = _normalize_item(item)
        assert out["firstName"] == "John"
        assert out["lastName"] == "Doe"
        assert out["organization"] == "Acme"

    def test_normalize_from_full_name(self):
        """Split full name into firstName and lastName."""
        item = {"name": "Jane Smith", "company": "Tech Corp"}
        out = _normalize_item(item)
        assert out["firstName"] == "Jane"
        assert out["lastName"] == "Smith"
        assert out["organization"] == "Tech Corp"

    def test_normalize_email_preserved(self):
        """Email is preserved when present."""
        item = {"firstName": "A", "lastName": "B", "email": "a@b.com"}
        out = _normalize_item(item)
        assert out["email"] == "a@b.com"


class TestScrapedItemsToTruthRows:
    """Tests for scraped_items_to_truth_rows."""

    def test_maps_to_truth_columns(self):
        """Map firstName, lastName, organization to Truth columns."""
        items = [
            {"firstName": "John", "lastName": "Doe", "organization": "Acme Inc"},
            {"firstName": "Jane", "lastName": "Smith", "organization": ""},
        ]
        df = scraped_items_to_truth_rows(items)
        assert len(df) == 2
        assert df.iloc[0]["First Name"] == "John"
        assert df.iloc[0]["Last Name"] == "Doe"
        assert df.iloc[0]["Company Name (Based on Website Domain)"] == "Acme Inc"
        assert df.iloc[0]["Lead Source"] == "Website Scrape"
        assert df.iloc[1]["First Name"] == "Jane"
        assert df.iloc[1]["Company Name (Based on Website Domain)"] == ""

    def test_no_email_column_when_none_provided(self):
        """Email ID (unique) column is not added when no item has email."""
        items = [{"firstName": "John", "lastName": "Doe", "organization": "Acme"}]
        df = scraped_items_to_truth_rows(items)
        assert "Email ID (unique)" not in df.columns

    def test_email_column_when_provided(self):
        """Email ID (unique) column is present when at least one item has email."""
        items = [
            {"firstName": "John", "lastName": "Doe", "organization": "Acme", "email": "j@acme.com"},
        ]
        df = scraped_items_to_truth_rows(items)
        assert "Email ID (unique)" in df.columns
        assert df.iloc[0]["Email ID (unique)"] == "j@acme.com"

    def test_drops_rows_with_no_name_or_org(self):
        """Rows with neither name nor organization are dropped."""
        items = [
            {"firstName": "John", "lastName": "Doe", "organization": "Acme"},
            {"firstName": "", "lastName": "", "organization": ""},
        ]
        df = scraped_items_to_truth_rows(items)
        assert len(df) == 1
        assert df.iloc[0]["First Name"] == "John"

    def test_empty_list_returns_empty_dataframe(self):
        """Empty items list returns empty DataFrame."""
        df = scraped_items_to_truth_rows([])
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_lead_source_website_scrape(self):
        """Lead Source is set to Website Scrape."""
        items = [{"firstName": "A", "lastName": "B", "organization": "C"}]
        df = scraped_items_to_truth_rows(items)
        assert (df["Lead Source"] == "Website Scrape").all()
