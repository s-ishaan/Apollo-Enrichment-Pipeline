"""
Apollo API client for people and organization enrichment.
Includes retry logic, exponential backoff, and response mapping.
"""

import time
import logging
from typing import Dict, List, Any, Optional
import requests

from config import config, load_config
from utils import (
    setup_logging, chunk_list, flatten_list_to_string,
    safe_str, safe_int, mask_pii, extract_domain
)

logger = setup_logging(config.LOG_LEVEL, config.LOG_FILE,
                       config.LOG_TO_CONSOLE)


class ApolloAPIError(Exception):
    """Custom exception for Apollo API errors."""
    pass


class ApolloClient:
    """
    Client for Apollo.io API with retry logic and rate limiting.
    """

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Apollo API client.

        Args:
            api_key: Apollo API key (defaults to config)

        Raises:
            ValueError: If API key not provided
        """
        self.api_key = api_key or config.APOLLO_API_KEY
        if not self.api_key:
            raise ValueError(
                "Apollo API key required. Set APOLLO_API_KEY environment variable."
            )

        self.base_url = config.APOLLO_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "X-Api-Key": self.api_key
        })

        logger.info("Apollo API client initialized")

    def enrich_people_bulk(
        self,
        records: List[Dict[str, Any]],
        batch_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Enrich people data via bulk match endpoint.

        Args:
            records: List of records with first_name, last_name, and company info
            batch_size: Batch size (defaults to config)

        Returns:
            List of enriched records (same order as input)
        """
        batch_size = batch_size or config.APOLLO_BATCH_SIZE
        batch_size = min(batch_size, 10)  # Apollo allows max 10 per request
        all_results = []

        logger.info(
            f"Enriching {len(records)} people in batches of {batch_size}")

        for i, batch in enumerate(chunk_list(records, batch_size)):
            logger.info(
                f"Processing people batch {i + 1} ({len(batch)} records)")

            try:
                payload = self._prepare_people_payload(batch)
                response = self._make_request("/people/bulk_match", payload)
                enriched = self._parse_people_response(response, batch)
                all_results.extend(enriched)

            except Exception as e:
                logger.error(f"People batch {i + 1} failed: {e}")
                # Add failed records with error marker
                for record in batch:
                    all_results.append({
                        **record,
                        "_enrichment_error": str(e)
                    })

        logger.info(f"People enrichment complete: {len(all_results)} records")
        return all_results

    def enrich_organizations_bulk(
        self,
        records: List[Dict[str, Any]],
        batch_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Enrich organization data via bulk enrich endpoint.

        Args:
            records: List of records with domain or company name
            batch_size: Batch size (defaults to config)

        Returns:
            List of enriched organization records
        """
        batch_size = batch_size or config.APOLLO_BATCH_SIZE
        batch_size = min(batch_size, 10)  # Apollo allows max 10 per request
        all_results = []

        logger.info(
            f"Enriching {len(records)} organizations in batches of {batch_size}")

        for i, batch in enumerate(chunk_list(records, batch_size)):
            logger.info(f"Processing org batch {i + 1} ({len(batch)} records)")

            # Separate records with domain vs without (API requires "domains" array)
            records_with_org_info = []
            records_without_org_info = []

            for record in batch:
                has_domain = record.get("Website URLs") and str(
                    record["Website URLs"]).strip()
                has_company = record.get("Company Name (Based on Website Domain)") and str(
                    record["Company Name (Based on Website Domain)"]).strip()

                if has_domain or has_company:
                    records_with_org_info.append(record)
                else:
                    records_without_org_info.append(record)

            # Build domains list (API expects "domains"); only records with Website URLs can be enriched
            records_with_domain = []
            domains = []
            for record in records_with_org_info:
                if record.get("Website URLs") and str(record["Website URLs"]).strip():
                    domain = extract_domain(
                        str(record["Website URLs"]).strip())
                    if domain:
                        records_with_domain.append(record)
                        domains.append(domain)
            records_with_only_company = [
                r for r in records_with_org_info if r not in records_with_domain]

            # Skip API call if no domains (e.g. scraped data has only company names)
            if not domains:
                if records_with_org_info:
                    logger.debug(
                        "Batch %s: No domains (only company names); skipping org enrichment",
                        i + 1,
                    )
                all_results.extend(records_with_org_info)
                all_results.extend(records_without_org_info)
                continue

            try:
                payload = {"domains": domains}
                response = self._make_request(
                    "/organizations/bulk_enrich", payload)
                enriched = self._parse_org_response(
                    response, records_with_domain)
                all_results.extend(enriched)
                all_results.extend(records_with_only_company)
                all_results.extend(records_without_org_info)

            except Exception as e:
                logger.error(f"Organization batch {i + 1} failed: {e}")
                # Add all records with error marker
                for record in batch:
                    all_results.append({
                        **record,
                        "_enrichment_error": str(e)
                    })

        logger.info(
            f"Organization enrichment complete: {len(all_results)} records")
        return all_results

    def _make_request(
        self,
        endpoint: str,
        payload: Dict[str, Any],
        timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Make POST request with exponential backoff retry logic.

        Args:
            endpoint: API endpoint path
            payload: Request payload
            timeout: Request timeout in seconds

        Returns:
            Response JSON

        Raises:
            ApolloAPIError: If request fails after all retries
        """
        url = f"{self.base_url}{endpoint}"
        timeout = timeout or config.APOLLO_TIMEOUT
        attempt = 0

        while attempt < config.APOLLO_MAX_RETRIES:
            try:
                logger.debug(
                    f"API request attempt {attempt + 1} to {endpoint}")

                response = self.session.post(
                    url,
                    json=payload,
                    timeout=timeout
                )

                # Success
                if response.status_code == 200:
                    return response.json()

                # Rate limit - retry with backoff
                elif response.status_code == 429:
                    backoff = self._calculate_backoff(attempt)
                    logger.warning(
                        f"Rate limited (429). Retrying in {backoff}s "
                        f"(attempt {attempt + 1}/{config.APOLLO_MAX_RETRIES})"
                    )
                    time.sleep(backoff)
                    attempt += 1
                    continue

                # Server error - retry
                elif 500 <= response.status_code < 600:
                    backoff = self._calculate_backoff(attempt)
                    logger.warning(
                        f"Server error ({response.status_code}). "
                        f"Retrying in {backoff}s "
                        f"(attempt {attempt + 1}/{config.APOLLO_MAX_RETRIES})"
                    )
                    time.sleep(backoff)
                    attempt += 1
                    continue

                # Client error - don't retry
                else:
                    error_msg = f"API error {response.status_code}: {response.text}"
                    logger.error(error_msg)
                    raise ApolloAPIError(error_msg)

            except requests.Timeout:
                backoff = self._calculate_backoff(attempt)
                logger.warning(
                    f"Request timeout. Retrying in {backoff}s "
                    f"(attempt {attempt + 1}/{config.APOLLO_MAX_RETRIES})"
                )
                time.sleep(backoff)
                attempt += 1
                timeout = int(timeout * 1.5)  # Increase timeout on retry

            except requests.RequestException as e:
                logger.error(f"Request failed: {e}")
                raise ApolloAPIError(f"Request failed: {e}")

        raise ApolloAPIError(
            f"Request failed after {config.APOLLO_MAX_RETRIES} attempts"
        )

    def _calculate_backoff(self, attempt: int) -> float:
        """
        Calculate exponential backoff time.

        Args:
            attempt: Current attempt number (0-indexed)

        Returns:
            Backoff time in seconds
        """
        backoff = config.APOLLO_INITIAL_BACKOFF * (2 ** attempt)
        return min(backoff, config.APOLLO_MAX_BACKOFF)

    def _prepare_people_payload(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Prepare payload for people bulk match API.

        Args:
            records: List of records

        Returns:
            API payload
        """
        details = []

        for record in records:
            detail = {}

            # Required fields
            if record.get("First Name"):
                detail["first_name"] = record["First Name"]
            if record.get("Last Name"):
                detail["last_name"] = record["Last Name"]

            # Organization context
            if record.get("Website URLs"):
                detail["organization_domain"] = record["Website URLs"]
            elif record.get("Company Name (Based on Website Domain)"):
                detail["organization_name"] = record["Company Name (Based on Website Domain)"]

            # Email if available
            if record.get("Email ID (unique)"):
                detail["email"] = record["Email ID (unique)"]

            details.append(detail)

        return {"details": details}

    def _prepare_org_payload(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Prepare payload for organizations bulk enrich API.
        API expects a top-level "domains" array (domain strings only).

        Args:
            records: List of records with Website URLs (domain)

        Returns:
            API payload {"domains": ["example.com", ...]}
        """
        domains = []
        for record in records:
            if record.get("Website URLs") and str(record["Website URLs"]).strip():
                domain = extract_domain(str(record["Website URLs"]).strip())
                if domain:
                    domains.append(domain)
        return {"domains": domains}

    def _parse_people_response(
        self,
        response: Dict[str, Any],
        original_records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Parse people bulk match API response and map to records.

        Args:
            response: API response
            original_records: Original input records

        Returns:
            List of enriched records
        """
        matches = response.get("matches", [])
        enriched_records = []

        # Map matches back to original records by index
        for i, record in enumerate(original_records):
            enriched = record.copy()

            if i < len(matches) and matches[i]:
                match = matches[i]
                person_data = map_apollo_person_response(match)
                enriched.update(person_data)

                # Also include organization data if present
                if match.get("organization"):
                    org_data = map_apollo_company_response(
                        match["organization"])
                    enriched.update(org_data)

            enriched_records.append(enriched)

        return enriched_records

    def _parse_org_response(
        self,
        response: Dict[str, Any],
        original_records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Parse organizations bulk enrich API response.

        Args:
            response: API response
            original_records: Original input records

        Returns:
            List of enriched records
        """
        matches = response.get("matches", [])
        enriched_records = []

        for i, record in enumerate(original_records):
            enriched = record.copy()

            if i < len(matches) and matches[i]:
                org_data = map_apollo_company_response(matches[i])
                enriched.update(org_data)

            enriched_records.append(enriched)

        return enriched_records


def map_apollo_person_response(person: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Apollo person API response to database columns.

    Args:
        person: Person data from API

    Returns:
        Dictionary with base columns + Apollo Person columns
    """
    mapped = {}

    # Base columns
    if person.get("first_name"):
        mapped["First Name"] = safe_str(person["first_name"])
    if person.get("last_name"):
        mapped["Last Name"] = safe_str(person["last_name"])
    if person.get("title"):
        mapped["Job Title"] = safe_str(person["title"])
    if person.get("email"):
        mapped["Email ID (unique)"] = safe_str(person["email"])
    if person.get("linkedin_url"):
        mapped["Person LinkedIn Profile"] = safe_str(person["linkedin_url"])
    if person.get("country"):
        mapped["Country"] = safe_str(person["country"])
    if person.get("state"):
        mapped["State"] = safe_str(person["state"])
    if person.get("phone_numbers") and person["phone_numbers"]:
        mapped["Contact Number (Person)"] = safe_str(
            person["phone_numbers"][0])

    # Apollo Person columns
    if person.get("email_status"):
        mapped["Apollo Person: Email Status"] = safe_str(
            person["email_status"])
    if person.get("headline"):
        mapped["Apollo Person: Headline"] = safe_str(person["headline"])
    if person.get("seniority"):
        mapped["Apollo Person: Seniority"] = safe_str(person["seniority"])
    if person.get("departments"):
        mapped["Apollo Person: Departments"] = flatten_list_to_string(
            person["departments"])
    if person.get("subdepartments"):
        mapped["Apollo Person: Subdepartments"] = flatten_list_to_string(
            person["subdepartments"])
    if person.get("functions"):
        mapped["Apollo Person: Functions"] = flatten_list_to_string(
            person["functions"])
    if person.get("photo_url"):
        mapped["Apollo Person: Photo URL"] = safe_str(person["photo_url"])
    if person.get("twitter_url"):
        mapped["Apollo Person: Twitter URL"] = safe_str(person["twitter_url"])
    if person.get("github_url"):
        mapped["Apollo Person: Github URL"] = safe_str(person["github_url"])
    if person.get("facebook_url"):
        mapped["Apollo Person: Facebook URL"] = safe_str(
            person["facebook_url"])
    if person.get("is_likely_to_engage") is not None:
        mapped["Apollo Person: Is Likely To Engage"] = "Yes" if person["is_likely_to_engage"] else "No"

    # Current employment info
    if person.get("employment_history") and person["employment_history"]:
        current = person["employment_history"][0]
        if current.get("organization_name"):
            mapped["Apollo Person: Current Org"] = safe_str(
                current["organization_name"])
        if current.get("start_date"):
            mapped["Apollo Person: Current Role Start Date"] = safe_str(
                current["start_date"])

    return mapped


def map_apollo_company_response(org: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Apollo organization API response to database columns.

    Args:
        org: Organization data from API

    Returns:
        Dictionary with base columns + Apollo Company columns
    """
    # Log available fields for debugging
    logger.debug(f"Apollo org fields available: {list(org.keys())}")

    mapped = {}

    # Base columns
    if org.get("name"):
        mapped["Company Name (Based on Website Domain)"] = safe_str(
            org["name"])
    if org.get("industry"):
        mapped["Industry"] = safe_str(org["industry"])
    elif org.get("industries") and org["industries"]:
        mapped["Industry"] = safe_str(org["industries"][0])

    if org.get("website_url"):
        mapped["Website URLs"] = safe_str(org["website_url"])
    if org.get("linkedin_url"):
        mapped["LinkedIn Company Page"] = safe_str(org["linkedin_url"])
    if org.get("estimated_num_employees"):
        mapped["# Employees"] = safe_str(org["estimated_num_employees"])
    if org.get("phone"):
        mapped["Contact Number (Company)"] = safe_str(org["phone"])
    elif org.get("account", {}).get("phone"):
        mapped["Contact Number (Company)"] = safe_str(org["account"]["phone"])

    # Address
    if org.get("raw_address"):
        mapped["Company Address / Headquarters"] = safe_str(org["raw_address"])
    elif org.get("city") or org.get("state") or org.get("country"):
        address_parts = [
            safe_str(org.get("city", "")),
            safe_str(org.get("state", "")),
            safe_str(org.get("country", ""))
        ]
        mapped["Company Address / Headquarters"] = ", ".join(
            p for p in address_parts if p)

    # Listed company
    if org.get("publicly_traded_symbol"):
        mapped["Listed Company"] = "Yes"
    else:
        mapped["Listed Company"] = "No"

    # Revenue field (if Apollo provides it)
    if org.get("estimated_annual_revenue"):
        mapped["Revenue"] = safe_str(org["estimated_annual_revenue"])
    elif org.get("annual_revenue"):
        mapped["Revenue"] = safe_str(org["annual_revenue"])

    # Size field (if Apollo provides it)
    if org.get("size"):
        mapped["Size"] = safe_str(org["size"])
    elif org.get("organization_size"):
        mapped["Size"] = safe_str(org["organization_size"])

    # Apollo Company columns
    if org.get("primary_domain"):
        mapped["Apollo Company: Primary Domain"] = safe_str(
            org["primary_domain"])
    if org.get("founded_year"):
        mapped["Apollo Company: Founded Year"] = safe_str(org["founded_year"])
    if org.get("alexa_ranking"):
        mapped["Apollo Company: Alexa Ranking"] = safe_str(
            org["alexa_ranking"])
    if org.get("seo_description"):
        mapped["Apollo Company: SEO Description"] = safe_str(
            org["seo_description"])
    if org.get("short_description"):
        mapped["Apollo Company: Short Description"] = safe_str(
            org["short_description"])
    if org.get("keywords"):
        mapped["Apollo Company: Keywords"] = flatten_list_to_string(
            org["keywords"])
    if org.get("publicly_traded_symbol"):
        mapped["Apollo Company: Public Ticker"] = safe_str(
            org["publicly_traded_symbol"])
    if org.get("publicly_traded_exchange"):
        mapped["Apollo Company: Public Exchange"] = safe_str(
            org["publicly_traded_exchange"])
    if org.get("logo_url"):
        mapped["Apollo Company: Logo URL"] = safe_str(org["logo_url"])
    if org.get("twitter_url"):
        mapped["Apollo Company: Twitter URL"] = safe_str(org["twitter_url"])
    if org.get("facebook_url"):
        mapped["Apollo Company: Facebook URL"] = safe_str(org["facebook_url"])

    # Department headcount
    if org.get("departmental_head_count"):
        dept_counts = [
            f"{dept}: {count}"
            for dept, count in org["departmental_head_count"].items()
            if count
        ]
        if dept_counts:
            mapped["Apollo Company: Department Headcount"] = flatten_list_to_string(
                dept_counts)

    # Revenue range (Apollo often provides ranges)
    if org.get("revenue_range"):
        mapped["Apollo Company: Revenue Range"] = safe_str(
            org["revenue_range"])

    # Employee count range
    if org.get("employee_count_range"):
        mapped["Apollo Company: Employee Range"] = safe_str(
            org["employee_count_range"])

    # Total funding
    if org.get("total_funding"):
        mapped["Apollo Company: Total Funding"] = safe_str(
            org["total_funding"])

    # Latest funding round
    if org.get("latest_funding_round_date"):
        mapped["Apollo Company: Latest Funding Round Date"] = safe_str(
            org["latest_funding_round_date"])

    # Technology stack
    if org.get("technologies"):
        mapped["Apollo Company: Technologies"] = flatten_list_to_string(
            org["technologies"])

    return mapped
