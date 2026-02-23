"""
Configuration management for Apollo enrichment pipeline.
Loads settings from environment variables with sensible defaults.
"""

import os
from dataclasses import dataclass
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv not installed (e.g. minimal container); rely on env vars from platform
    pass


@dataclass
class Config:
    """
    Application configuration loaded from environment variables.
    """

    # Database Configuration
    DB_PATH: str = os.getenv("DB_PATH", "apollo_truth.db")
    # When set (e.g. Supabase URI), use PostgreSQL instead of SQLite. Session mode recommended.
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")

    # Apollo API Configuration
    APOLLO_API_KEY: str = os.getenv("APOLLO_API_KEY", "")
    APOLLO_BASE_URL: str = os.getenv(
        "APOLLO_BASE_URL",
        "https://api.apollo.io/api/v1"
    )
    APOLLO_BATCH_SIZE: int = int(os.getenv("APOLLO_BATCH_SIZE", "25"))
    APOLLO_TIMEOUT: int = int(os.getenv("APOLLO_TIMEOUT", "30"))
    APOLLO_MAX_RETRIES: int = int(os.getenv("APOLLO_MAX_RETRIES", "5"))
    APOLLO_INITIAL_BACKOFF: float = float(
        os.getenv("APOLLO_INITIAL_BACKOFF", "1.0"))
    APOLLO_MAX_BACKOFF: float = float(os.getenv("APOLLO_MAX_BACKOFF", "60.0"))

    # Processing Configuration
    MAX_FILE_SIZE_MB: int = int(os.getenv("MAX_FILE_SIZE_MB", "50"))
    MAX_ROWS: int = int(os.getenv("MAX_ROWS", "0"))  # 0 = no limit
    EXCEL_CHUNK_SIZE: int = int(os.getenv("EXCEL_CHUNK_SIZE", "1000"))
    ENABLE_PEOPLE_ENRICHMENT: bool = os.getenv(
        "ENABLE_PEOPLE_ENRICHMENT", "true").lower() == "true"
    ENABLE_COMPANY_ENRICHMENT: bool = os.getenv(
        "ENABLE_COMPANY_ENRICHMENT", "true").lower() == "true"

    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: Optional[str] = os.getenv("LOG_FILE", "apollo_pipeline.log")
    LOG_TO_CONSOLE: bool = os.getenv(
        "LOG_TO_CONSOLE", "true").lower() == "true"

    # UI Configuration
    STREAMLIT_PAGE_TITLE: str = os.getenv(
        "STREAMLIT_PAGE_TITLE", "Apollo Enrichment Pipeline")
    PAGE_SIZE_DEFAULT: int = int(os.getenv("PAGE_SIZE_DEFAULT", "50"))
    ENABLE_EXPORT: bool = os.getenv("ENABLE_EXPORT", "true").lower() == "true"

    # Apify (AI extractor for website scraping; token only required when using Scrape website)
    APIFY_API_TOKEN: str = os.getenv("APIFY_API_TOKEN", "")
    APIFY_AI_EXTRACTOR_ACTOR_ID: str = os.getenv(
        "APIFY_AI_EXTRACTOR_ACTOR_ID",
        "apify/ai-web-agent",
    )
    APIFY_RUN_TIMEOUT_SECS: int = int(
        os.getenv("APIFY_RUN_TIMEOUT_SECS", "300"))
    APIFY_EXTRACTION_PROMPT: str = os.getenv(
        "APIFY_EXTRACTION_PROMPT",
        "Important: Extract data first. Do not take screenshots before extracting. "
        "Extract all person names and their organization names from this page. "
        "Return a JSON array of objects with keys: firstName, lastName, organization (optional). "
        "If only a full name is available use firstName for the full name and leave lastName empty. "
        "One object per person. Only take a screenshot afterward if you still need to.",
    )
    # Wait for dynamic content before extracting (e.g. JS-rendered boards/lists)
    APIFY_WAIT_FOR_DYNAMIC_CONTENT: bool = os.getenv(
        "APIFY_WAIT_FOR_DYNAMIC_CONTENT", "true"
    ).lower() == "true"
    APIFY_PAGE_WAIT_SECS: int = int(
        os.getenv("APIFY_PAGE_WAIT_SECS", "5")
    )
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    # HTML-first extraction (try fetch + LLM before Apify browser; uses fewer resources)
    ENABLE_HTML_FIRST_EXTRACTION: bool = os.getenv(
        "ENABLE_HTML_FIRST_EXTRACTION", "true"
    ).lower() == "true"
    HTML_FETCH_TIMEOUT_SECS: int = int(
        os.getenv("HTML_FETCH_TIMEOUT_SECS", "30")
    )
    MAX_HTML_CHARS: int = int(os.getenv("MAX_HTML_CHARS", "120000"))
    OPENAI_EXTRACTION_MODEL: str = os.getenv(
        "OPENAI_EXTRACTION_MODEL", "gpt-4o-mini"
    )

    def validate(self) -> None:
        """
        Validate configuration and raise errors for invalid settings.

        Raises:
            ValueError: If configuration is invalid
        """
        if not self.APOLLO_API_KEY:
            raise ValueError(
                "APOLLO_API_KEY environment variable is required. "
                "Please set it before running the application."
            )

        if self.APOLLO_BATCH_SIZE <= 0 or self.APOLLO_BATCH_SIZE > 100:
            raise ValueError(
                f"APOLLO_BATCH_SIZE must be between 1 and 100, got {self.APOLLO_BATCH_SIZE}"
            )

        if self.APOLLO_TIMEOUT <= 0:
            raise ValueError(
                f"APOLLO_TIMEOUT must be positive, got {self.APOLLO_TIMEOUT}"
            )

        if self.MAX_FILE_SIZE_MB <= 0:
            raise ValueError(
                f"MAX_FILE_SIZE_MB must be positive, got {self.MAX_FILE_SIZE_MB}"
            )

        if self.MAX_ROWS < 0:
            raise ValueError(
                f"MAX_ROWS must be non-negative, got {self.MAX_ROWS}"
            )


# Global configuration instance
config = Config()


def load_config(validate: bool = True) -> Config:
    """
    Load and optionally validate configuration.

    Args:
        validate: Whether to validate configuration

    Returns:
        Configuration instance

    Raises:
        ValueError: If validation enabled and configuration invalid
    """
    if validate:
        config.validate()

    return config


# Base column names for Truth table (exact names from requirements)
BASE_COLUMNS = [
    "S.N.",
    "Company Name (Based on Website Domain)",
    "Industry",
    "Revenue",
    "Size",
    "Company Address / Headquarters",
    "Contact Number (Company)",
    "Listed Company",
    "Website URLs",
    "LinkedIn Company Page",
    "# Employees",
    "First Name",
    "Last Name",
    "Job Title",
    "Email ID (unique)",
    "Person LinkedIn Profile",
    "Contact Number (Person)",
    "Country",
    "State",
    "Lead Source",
    "Client Type",
    "UPDATE AS ON",
    "Email Send (Yes/No)"
]

# Column mappings for flexible Excel parsing (case-insensitive)
COLUMN_MAPPINGS = {
    "Email ID (unique)": [
        "email", "email address", "e-mail", "email id",
        "email id (unique)", "mail", "email_address"
    ],
    "First Name": [
        "first name", "firstname", "fname", "given name",
        "first_name", "given_name"
    ],
    "Last Name": [
        "last name", "lastname", "lname", "surname",
        "family name", "last_name", "family_name"
    ],
    "Company Name (Based on Website Domain)": [
        "company", "company name", "organization", "organisation",
        "org name", "org", "account", "company_name",
        "organization_name", "organisation_name"
    ],
    "Website URLs": [
        "website", "domain", "url", "website url", "website_url",
        "website urls", "web", "site", "company website"
    ],
    "Job Title": [
        "title", "job title", "position", "role", "job_title",
        "job", "designation"
    ],
    "Contact Number (Person)": [
        "phone", "phone number", "contact", "contact number",
        "mobile", "telephone", "tel", "contact_number",
        "phone_number", "person phone"
    ],
    "Contact Number (Company)": [
        "company phone", "company contact", "office phone",
        "company_phone", "office_phone"
    ],
    "Country": [
        "country", "nation", "country_name"
    ],
    "State": [
        "state", "province", "region", "state_name"
    ],
    "LinkedIn Company Page": [
        "company linkedin", "linkedin company", "company_linkedin",
        "linkedin_company", "org linkedin"
    ],
    "Person LinkedIn Profile": [
        "linkedin", "linkedin url", "linkedin profile",
        "person linkedin", "linkedin_url", "linkedin_profile"
    ],
    "Industry": [
        "industry", "sector", "vertical", "industry_name"
    ]
}
