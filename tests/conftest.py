"""
Pytest configuration and fixtures for Apollo enrichment pipeline tests.
"""

import os
import tempfile
import pytest
import sqlite3
from typing import Generator
import pandas as pd

from db import DatabaseManager
from apollo import ApolloClient
from config import config


@pytest.fixture
def temp_db_path() -> Generator[str, None, None]:
    """
    Create a temporary database file path.

    Yields:
        Path to temporary database file
    """
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    yield path
    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def db_manager(temp_db_path: str) -> Generator[DatabaseManager, None, None]:
    """
    Create a DatabaseManager instance with temporary database.

    Args:
        temp_db_path: Temporary database path from fixture

    Yields:
        DatabaseManager instance
    """
    db = DatabaseManager(temp_db_path)
    yield db
    db.close()


@pytest.fixture
def sample_records() -> list:
    """
    Sample records for testing.

    Returns:
        List of sample record dictionaries
    """
    return [
        {
            "Email ID (unique)": "john.doe@example.com",
            "First Name": "John",
            "Last Name": "Doe",
            "Company Name (Based on Website Domain)": "Example Inc",
            "Website URLs": "example.com",
            "Job Title": "Software Engineer",
            "Lead Source": "Excel Upload",
            "Email Send (Yes/No)": "No"
        },
        {
            "Email ID (unique)": "jane.smith@acme.org",
            "First Name": "Jane",
            "Last Name": "Smith",
            "Company Name (Based on Website Domain)": "ACME Corp",
            "Website URLs": "acme.org",
            "Job Title": "Product Manager",
            "Lead Source": "Excel Upload",
            "Email Send (Yes/No)": "No"
        }
    ]


@pytest.fixture
def sample_excel_path() -> Generator[str, None, None]:
    """
    Create a sample Excel file for testing.

    Yields:
        Path to temporary Excel file
    """
    data = {
        "Email": ["test1@example.com", "test2@example.com"],
        "First Name": ["Alice", "Bob"],
        "Last Name": ["Johnson", "Williams"],
        "Company": ["Tech Corp", "Data Inc"],
        "Website": ["techcorp.com", "datainc.com"]
    }
    df = pd.DataFrame(data)

    fd, path = tempfile.mkstemp(suffix='.xlsx')
    os.close(fd)

    df.to_excel(path, index=False, engine='openpyxl')

    yield path

    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def excel_no_email_path() -> Generator[str, None, None]:
    """Excel file with no email column (e.g. only First Name, Company)."""
    df = pd.DataFrame({
        "First Name": ["Alice", "Bob"],
        "Last Name": ["A", "B"],
        "Company": ["Acme", "Beta"],
    })
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    df.to_excel(path, index=False, engine="openpyxl")
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def excel_empty_path() -> Generator[str, None, None]:
    """Excel file with headers only, no data rows."""
    df = pd.DataFrame({
        "Email": [],
        "First Name": [],
        "Last Name": [],
        "Company": [],
    })
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    df.to_excel(path, index=False, engine="openpyxl")
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def excel_all_invalid_email_path() -> Generator[str, None, None]:
    """Excel file with email column but all invalid/empty emails."""
    df = pd.DataFrame({
        "Email": ["", "invalid", "no-at-sign"],
        "First Name": ["A", "B", "C"],
    })
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    df.to_excel(path, index=False, engine="openpyxl")
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def excel_multiple_sheets_path() -> Generator[str, None, None]:
    """Excel file with two sheets (first has email column)."""
    df1 = pd.DataFrame({
        "Email": ["a@b.com"],
        "First Name": ["Alice"],
    })
    df2 = pd.DataFrame({"Other": [1, 2]})
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df1.to_excel(writer, index=False, sheet_name="Sheet1")
        df2.to_excel(writer, index=False, sheet_name="Sheet2")
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def excel_duplicate_columns_path() -> Generator[str, None, None]:
    """Excel file with duplicate column names (e.g. two Email columns)."""
    # pandas forces unique column names; use openpyxl to write duplicate headers
    try:
        import openpyxl
    except ImportError:
        pytest.skip("openpyxl required for duplicate-column Excel")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["Email", "Email", "First Name"])  # duplicate "Email"
    ws.append(["x@y.com", "ignored@z.com", "John"])
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    wb.save(path)
    wb.close()
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def mock_apollo_people_response() -> dict:
    """
    Mock Apollo people bulk match API response.

    Returns:
        Mock response dictionary
    """
    return {
        "matches": [
            {
                "first_name": "John",
                "last_name": "Doe",
                "title": "Software Engineer",
                "email": "john.doe@example.com",
                "linkedin_url": "https://linkedin.com/in/johndoe",
                "country": "United States",
                "state": "California",
                "email_status": "verified",
                "headline": "Senior Software Engineer at Example Inc",
                "seniority": "senior",
                "departments": ["engineering", "product"],
                "subdepartments": ["backend"],
                "functions": ["engineering"],
                "is_likely_to_engage": True,
                "organization": {
                    "id": "12345",
                    "name": "Example Inc",
                    "website_url": "https://example.com",
                    "primary_domain": "example.com",
                    "industry": "Technology",
                    "estimated_num_employees": 500,
                    "founded_year": 2010
                }
            }
        ]
    }


@pytest.fixture
def mock_apollo_org_response() -> dict:
    """
    Mock Apollo organizations bulk enrich API response.

    Returns:
        Mock response dictionary
    """
    return {
        "matches": [
            {
                "id": "12345",
                "name": "Example Inc",
                "website_url": "https://example.com",
                "primary_domain": "example.com",
                "industry": "Technology",
                "industries": ["Technology", "Software"],
                "estimated_num_employees": 500,
                "founded_year": 2010,
                "publicly_traded_symbol": "EXMP",
                "publicly_traded_exchange": "NASDAQ",
                "linkedin_url": "https://linkedin.com/company/example",
                "phone": "+1-555-0100",
                "raw_address": "123 Main St, San Francisco, CA 94102",
                "city": "San Francisco",
                "state": "California",
                "country": "United States",
                "logo_url": "https://example.com/logo.png",
                "keywords": ["software", "cloud", "saas"],
                "short_description": "Leading software company",
                "departmental_head_count": {
                    "engineering": 200,
                    "sales": 100,
                    "marketing": 50
                }
            }
        ]
    }


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """
    Mock environment variables for testing.

    Args:
        monkeypatch: Pytest monkeypatch fixture
    """
    monkeypatch.setenv("APOLLO_API_KEY", "test_api_key_12345")
    monkeypatch.setenv("LOG_LEVEL", "ERROR")  # Reduce log noise in tests
