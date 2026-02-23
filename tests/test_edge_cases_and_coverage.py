"""
Comprehensive edge-case and coverage tests for scraper, Apollo client, ingest pipeline, and utils.
"""

import json
from unittest.mock import patch, MagicMock
import pytest
import pandas as pd
import responses

from scraper import (
    _normalize_item,
    _truncate_html,
    _fetch_html,
    _extract_from_html_with_llm,
    scraped_items_to_truth_rows,
    run_ai_extractor,
    ScraperError,
)
from apollo import ApolloClient
from config import config
from ingest import process_scrape, ExcelIngestor
from utils import extract_domain


class TestNormalizeItemEdgeCases:
    def test_organisation_british_spelling(self):
        item = {"firstName": "J", "lastName": "D", "organisation": "Acme"}
        out = _normalize_item(item)
        assert out["organization"] == "Acme"

    def test_company_name_key(self):
        item = {"first_name": "J", "last_name": "D", "company_name": "Corp"}
        out = _normalize_item(item)
        assert out["organization"] == "Corp"

    def test_single_word_name_becomes_first_name_only(self):
        item = {"name": "Madonna", "organization": "Music"}
        out = _normalize_item(item)
        assert out["firstName"] == "Madonna"
        assert out["lastName"] == ""

    def test_empty_item_returns_empty_normalized(self):
        item = {}
        out = _normalize_item(item)
        assert out["firstName"] == ""
        assert out["lastName"] == ""
        assert out["organization"] == ""

    def test_only_organization_no_name(self):
        item = {"organization": "Acme Inc"}
        out = _normalize_item(item)
        assert out["organization"] == "Acme Inc"
        assert out["firstName"] == ""


class TestTruncateHtml:
    def test_short_html_unchanged(self):
        html = "<p>hello</p>"
        assert _truncate_html(html, 100) == html

    def test_long_html_truncated_with_suffix(self):
        html = "x" * 200
        result = _truncate_html(html, 50)
        assert len(result) == 50 + len("\n[... truncated ...]")
        assert result.endswith("\n[... truncated ...]")


class TestFetchHtml:
    @patch("scraper.requests.get")
    def test_success_returns_text(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            text="<html></html>",
            encoding="utf-8",
            raise_for_status=MagicMock(),
        )
        result = _fetch_html("https://example.com")
        assert result == "<html></html>"

    @patch("scraper.requests.get")
    def test_http_error_returns_none(self, mock_get):
        mock_get.return_value = MagicMock(
            raise_for_status=MagicMock(side_effect=Exception("404")),
        )
        result = _fetch_html("https://example.com")
        assert result is None


class TestExtractFromHtmlWithLlm:
    @patch("scraper.config")
    @patch("openai.OpenAI")
    def test_valid_json_array_returns_normalized_list(self, mock_openai_cls, mock_config):
        mock_config.OPENAI_API_KEY = "key"
        mock_config.MAX_HTML_CHARS = 10000
        mock_config.OPENAI_EXTRACTION_MODEL = "gpt-4o-mini"
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client
        mock_client.chat.completions.create.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content='[{"firstName":"J","lastName":"D","organization":"Acme"}]'))]
        )
        result = _extract_from_html_with_llm("<html>...</html>", "https://example.com")
        assert len(result) == 1
        assert result[0]["firstName"] == "J"

    @patch("scraper.config")
    def test_no_openai_key_returns_empty(self, mock_config):
        mock_config.OPENAI_API_KEY = ""
        result = _extract_from_html_with_llm("<html></html>", "https://example.com")
        assert result == []


class TestScrapedItemsToTruthRowsEdgeCases:
    def test_source_url_set_when_provided(self):
        items = [{"firstName": "J", "lastName": "D", "organization": "Acme"}]
        df = scraped_items_to_truth_rows(items, source_url="https://example.com")
        assert len(df) == 1
        assert df.iloc[0]["First Name"] == "J"

    def test_item_with_only_organization(self):
        items = [{"firstName": "", "lastName": "", "organization": "Acme Only"}]
        df = scraped_items_to_truth_rows(items)
        assert len(df) == 1
        assert df.iloc[0]["Company Name (Based on Website Domain)"] == "Acme Only"


class TestRunAiExtractorFlow:
    @patch("scraper.config")
    def test_missing_openai_key_raises_before_fetch(self, mock_config):
        mock_config.OPENAI_API_KEY = ""
        with pytest.raises(ScraperError, match="OPENAI_API_KEY"):
            run_ai_extractor("https://example.com")

    @patch("scraper.config")
    @patch("scraper._extract_from_html_with_llm")
    @patch("scraper._fetch_html")
    def test_html_success_with_items_returns_them(self, mock_fetch, mock_extract, mock_config):
        mock_config.OPENAI_API_KEY = "key"
        mock_config.ENABLE_HTML_FIRST_EXTRACTION = True
        mock_fetch.return_value = "<html></html>"
        mock_extract.return_value = [{"firstName": "J", "lastName": "D", "organization": "Acme"}]
        result = run_ai_extractor("https://example.com")
        assert len(result) == 1
        assert result[0]["firstName"] == "J"


class TestApolloBatchAndOrgEdgeCases:
    @responses.activate
    def test_people_batch_capped_at_10(self, mock_apollo_people_response):
        responses.add(
            responses.POST,
            f"{config.APOLLO_BASE_URL}/people/bulk_match",
            json=mock_apollo_people_response,
            status=200,
        )
        responses.add(
            responses.POST,
            f"{config.APOLLO_BASE_URL}/people/bulk_match",
            json={"matches": [mock_apollo_people_response["matches"][0]]},
            status=200,
        )
        client = ApolloClient(api_key="test_key")
        records = [
            {"First Name": f"F{i}", "Last Name": f"L{i}", "Company Name (Based on Website Domain)": "Acme"}
            for i in range(11)
        ]
        results = client.enrich_people_bulk(records, batch_size=25)
        assert len(results) == 11
        assert len(responses.calls) == 2
        first_body = json.loads(responses.calls[0].request.body)
        assert len(first_body["details"]) == 10

    def test_prepare_org_payload_domains_only(self):
        client = ApolloClient(api_key="test_key")
        records = [
            {"Website URLs": "https://example.com"},
            {"Website URLs": "acme.org"},
            {"Company Name (Based on Website Domain)": "No URL Corp"},
        ]
        payload = client._prepare_org_payload(records)
        assert "domains" in payload
        assert payload["domains"] == ["example.com", "acme.org"]

    def test_prepare_org_payload_empty_when_no_domains(self):
        client = ApolloClient(api_key="test_key")
        records = [{"Company Name (Based on Website Domain)": "Acme"}]
        payload = client._prepare_org_payload(records)
        assert payload == {"domains": []}

    @responses.activate
    def test_org_enrich_skips_api_when_no_domains(self, mock_apollo_org_response):
        client = ApolloClient(api_key="test_key")
        records = [
            {"Company Name (Based on Website Domain)": "Acme", "First Name": "J", "Last Name": "D"}
        ]
        results = client.enrich_organizations_bulk(records)
        assert len(results) == 1
        assert len(responses.calls) == 0


class TestProcessScrapeEdgeCases:
    @patch("ingest.run_ai_extractor", None)
    @patch("ingest.scraped_items_to_truth_rows", None)
    def test_scraper_unavailable_returns_error(self):
        result = process_scrape("https://example.com")
        assert result["total_processed"] == 0
        assert any("Scraper" in e.get("message", "") for e in result["errors"])

    @patch("ingest.run_ai_extractor")
    def test_scraper_error_returns_with_message(self, mock_extractor):
        mock_extractor.side_effect = ScraperError("Token missing")
        result = process_scrape("https://example.com")
        assert result["total_processed"] == 0
        assert any("Token" in e["message"] for e in result["errors"])

    @patch("ingest.run_ai_extractor")
    def test_zero_items_returns_early(self, mock_extractor):
        mock_extractor.return_value = []
        result = process_scrape("https://example.com")
        assert result["total_processed"] == 0


class TestExtractDomainEdgeCases:
    def test_https_with_www_and_path(self):
        assert extract_domain("https://www.example.com/path?q=1") == "example.com"

    def test_http_subdomain(self):
        assert extract_domain("http://subdomain.example.co.uk") == "subdomain.example.co.uk"

    def test_no_scheme(self):
        assert extract_domain("example.com") == "example.com"

    def test_empty_string(self):
        assert extract_domain("") is None
