"""
Tests for Apollo API client.
"""

import pytest
import responses
from apollo import (
    ApolloClient, ApolloAPIError,
    map_apollo_person_response, map_apollo_company_response
)
from config import config


class TestApolloClientInitialization:
    """Tests for Apollo client initialization."""

    def test_init_with_api_key(self):
        """Test initialization with API key."""
        client = ApolloClient(api_key="test_key")
        assert client.api_key == "test_key"

    def test_init_without_api_key_fails(self, monkeypatch):
        """Test that initialization without API key raises error."""
        monkeypatch.delenv("APOLLO_API_KEY", raising=False)
        monkeypatch.setattr(config, "APOLLO_API_KEY", "")

        with pytest.raises(ValueError, match="API key required"):
            ApolloClient()


class TestPeopleEnrichment:
    """Tests for people enrichment."""

    @responses.activate
    def test_enrich_people_success(self, mock_apollo_people_response):
        """Test successful people enrichment."""
        # Mock API response
        responses.add(
            responses.POST,
            f"{config.APOLLO_BASE_URL}/people/bulk_match",
            json=mock_apollo_people_response,
            status=200
        )

        client = ApolloClient(api_key="test_key")
        records = [{
            "First Name": "John",
            "Last Name": "Doe",
            "Email ID (unique)": "john.doe@example.com"
        }]

        results = client.enrich_people_bulk(records)

        assert len(results) == 1
        assert results[0]["First Name"] == "John"
        assert "Email ID (unique)" in results[0]

    @responses.activate
    def test_enrich_people_with_rate_limit(self, mock_apollo_people_response):
        """Test retry on rate limit."""
        # First request returns 429, second succeeds
        responses.add(
            responses.POST,
            f"{config.APOLLO_BASE_URL}/people/bulk_match",
            json={"error": "Rate limited"},
            status=429
        )
        responses.add(
            responses.POST,
            f"{config.APOLLO_BASE_URL}/people/bulk_match",
            json=mock_apollo_people_response,
            status=200
        )

        client = ApolloClient(api_key="test_key")
        records = [{"First Name": "John", "Last Name": "Doe"}]

        results = client.enrich_people_bulk(records)

        assert len(results) == 1
        assert len(responses.calls) == 2  # Verify retry occurred


class TestOrganizationEnrichment:
    """Tests for organization enrichment."""

    @responses.activate
    def test_enrich_orgs_success(self, mock_apollo_org_response):
        """Test successful organization enrichment."""
        responses.add(
            responses.POST,
            f"{config.APOLLO_BASE_URL}/organizations/bulk_enrich",
            json=mock_apollo_org_response,
            status=200
        )

        client = ApolloClient(api_key="test_key")
        records = [{
            "Company Name (Based on Website Domain)": "Example Inc",
            "Website URLs": "example.com"
        }]

        results = client.enrich_organizations_bulk(records)

        assert len(results) == 1
        assert "Company Name (Based on Website Domain)" in results[0]


class TestRetryLogic:
    """Tests for retry logic."""

    @responses.activate
    def test_retry_on_server_error(self, mock_apollo_people_response):
        """Test retry on 500 server error."""
        # First two requests fail, third succeeds
        responses.add(
            responses.POST,
            f"{config.APOLLO_BASE_URL}/people/bulk_match",
            json={"error": "Server error"},
            status=500
        )
        responses.add(
            responses.POST,
            f"{config.APOLLO_BASE_URL}/people/bulk_match",
            json={"error": "Server error"},
            status=500
        )
        responses.add(
            responses.POST,
            f"{config.APOLLO_BASE_URL}/people/bulk_match",
            json=mock_apollo_people_response,
            status=200
        )

        client = ApolloClient(api_key="test_key")
        records = [{"First Name": "John", "Last Name": "Doe"}]

        results = client.enrich_people_bulk(records)

        assert len(results) == 1
        assert len(responses.calls) == 3

    @responses.activate
    def test_fail_after_max_retries(self):
        """Test failure after max retries."""
        # All requests fail
        for _ in range(config.APOLLO_MAX_RETRIES + 1):
            responses.add(
                responses.POST,
                f"{config.APOLLO_BASE_URL}/people/bulk_match",
                json={"error": "Server error"},
                status=500
            )

        client = ApolloClient(api_key="test_key")
        records = [{"First Name": "John", "Last Name": "Doe"}]

        # Should capture error in results
        results = client.enrich_people_bulk(records)

        assert len(results) == 1
        assert "_enrichment_error" in results[0]

    @responses.activate
    def test_client_error_no_retry(self):
        """Test that client errors (4xx) don't retry."""
        responses.add(
            responses.POST,
            f"{config.APOLLO_BASE_URL}/people/bulk_match",
            json={"error": "Bad request"},
            status=400
        )

        client = ApolloClient(api_key="test_key")
        records = [{"First Name": "John", "Last Name": "Doe"}]

        results = client.enrich_people_bulk(records)

        # Should only make one request (no retry)
        assert len(responses.calls) == 1
        assert "_enrichment_error" in results[0]


class TestResponseMapping:
    """Tests for response mapping functions."""

    def test_map_person_response(self, mock_apollo_people_response):
        """Test mapping person API response."""
        person_data = mock_apollo_people_response["matches"][0]
        mapped = map_apollo_person_response(person_data)

        # Check base columns
        assert mapped["First Name"] == "John"
        assert mapped["Last Name"] == "Doe"
        assert mapped["Job Title"] == "Software Engineer"
        assert mapped["Email ID (unique)"] == "john.doe@example.com"
        assert mapped["Country"] == "United States"

        # Check Apollo columns
        assert mapped["Apollo Person: Email Status"] == "verified"
        assert mapped["Apollo Person: Seniority"] == "senior"
        assert "Apollo Person: Departments" in mapped

    def test_map_company_response(self, mock_apollo_org_response):
        """Test mapping company API response."""
        org_data = mock_apollo_org_response["matches"][0]
        mapped = map_apollo_company_response(org_data)

        # Check base columns
        assert mapped["Company Name (Based on Website Domain)"] == "Example Inc"
        assert mapped["Industry"] == "Technology"
        assert mapped["Website URLs"] == "https://example.com"
        assert mapped["# Employees"] == "500"
        assert mapped["Listed Company"] == "Yes"

        # Check Apollo columns
        assert mapped["Apollo Company: Primary Domain"] == "example.com"
        assert mapped["Apollo Company: Founded Year"] == "2010"
        assert mapped["Apollo Company: Public Ticker"] == "EXMP"

    def test_map_company_not_listed(self):
        """Test mapping company without public trading symbol."""
        org_data = {
            "name": "Private Corp",
            "industry": "Finance"
        }
        mapped = map_apollo_company_response(org_data)

        assert mapped["Listed Company"] == "No"


class TestBackoffCalculation:
    """Tests for exponential backoff calculation."""

    def test_calculate_backoff(self):
        """Test backoff calculation."""
        client = ApolloClient(api_key="test_key")

        # First attempt
        backoff1 = client._calculate_backoff(0)
        assert backoff1 == config.APOLLO_INITIAL_BACKOFF

        # Second attempt
        backoff2 = client._calculate_backoff(1)
        assert backoff2 == config.APOLLO_INITIAL_BACKOFF * 2

        # Third attempt
        backoff3 = client._calculate_backoff(2)
        assert backoff3 == config.APOLLO_INITIAL_BACKOFF * 4

        # Should cap at max backoff
        backoff_max = client._calculate_backoff(10)
        assert backoff_max == config.APOLLO_MAX_BACKOFF
