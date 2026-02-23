"""
Tests for utility functions.
"""

import pytest
from utils import (
    mask_pii, extract_domain, get_utc_timestamp,
    validate_email, normalize_company_name, clean_phone_number,
    chunk_list, safe_dict_get, flatten_list_to_string,
    safe_int, safe_str
)


class TestMaskPII:
    """Tests for PII masking function."""

    def test_mask_email(self):
        """Test email masking."""
        text = "Contact user@example.com for details"
        masked = mask_pii(text)
        assert "user@example.com" not in masked
        assert "u***@e***.com" in masked

    def test_mask_multiple_emails(self):
        """Test multiple email masking."""
        text = "Email john@test.com or jane@demo.org"
        masked = mask_pii(text)
        assert "john@test.com" not in masked
        assert "jane@demo.org" not in masked

    def test_mask_phone_number(self):
        """Test phone number masking."""
        text = "Call +1-555-123-4567"
        masked = mask_pii(text)
        assert "555" not in masked
        assert "67" in masked  # Last 2 digits preserved

    def test_no_pii(self):
        """Test text without PII."""
        text = "This is a normal message"
        masked = mask_pii(text)
        assert masked == text


class TestExtractDomain:
    """Tests for domain extraction."""

    def test_extract_from_url(self):
        """Test extracting domain from URL."""
        assert extract_domain("https://www.example.com/path") == "example.com"
        assert extract_domain("http://subdomain.test.com") == "subdomain.test.com"

    def test_extract_from_domain(self):
        """Test extracting from plain domain."""
        assert extract_domain("example.com") == "example.com"
        assert extract_domain("www.test.org") == "test.org"

    def test_invalid_domain(self):
        """Test invalid domain handling."""
        assert extract_domain("") is None
        assert extract_domain(None) is None


class TestValidateEmail:
    """Tests for email validation."""

    def test_valid_emails(self):
        """Test valid email addresses."""
        assert validate_email("user@example.com") is True
        assert validate_email("test.user+tag@domain.co.uk") is True

    def test_invalid_emails(self):
        """Test invalid email addresses."""
        assert validate_email("not-an-email") is False
        assert validate_email("@example.com") is False
        assert validate_email("user@") is False
        assert validate_email("") is False
        assert validate_email(None) is False


class TestNormalizeCompanyName:
    """Tests for company name normalization."""

    def test_normalize_whitespace(self):
        """Test whitespace normalization."""
        assert normalize_company_name("  Example   Inc  ") == "Example Inc"

    def test_normalize_case(self):
        """Test case normalization."""
        assert normalize_company_name("acme corporation") == "Acme Corporation"

    def test_empty_string(self):
        """Test empty string handling."""
        assert normalize_company_name("") == ""
        assert normalize_company_name(None) == ""


class TestCleanPhoneNumber:
    """Tests for phone number cleaning."""

    def test_clean_formatted_phone(self):
        """Test cleaning formatted phone numbers."""
        assert clean_phone_number("+1-555-123-4567") == "+15551234567"
        assert clean_phone_number("(555) 123-4567") == "5551234567"

    def test_clean_simple_phone(self):
        """Test cleaning simple phone numbers."""
        assert clean_phone_number("5551234567") == "5551234567"

    def test_empty_phone(self):
        """Test empty phone handling."""
        assert clean_phone_number("") == ""
        assert clean_phone_number(None) == ""


class TestChunkList:
    """Tests for list chunking."""

    def test_chunk_exact_division(self):
        """Test chunking with exact division."""
        items = list(range(10))
        chunks = list(chunk_list(items, 5))
        assert len(chunks) == 2
        assert chunks[0] == [0, 1, 2, 3, 4]
        assert chunks[1] == [5, 6, 7, 8, 9]

    def test_chunk_remainder(self):
        """Test chunking with remainder."""
        items = list(range(10))
        chunks = list(chunk_list(items, 3))
        assert len(chunks) == 4
        assert chunks[-1] == [9]

    def test_chunk_empty_list(self):
        """Test chunking empty list."""
        chunks = list(chunk_list([], 5))
        assert len(chunks) == 0


class TestSafeDictGet:
    """Tests for safe dictionary navigation."""

    def test_get_nested_value(self):
        """Test getting nested dictionary value."""
        d = {"person": {"name": {"first": "John"}}}
        assert safe_dict_get(d, "person.name.first") == "John"

    def test_get_missing_key(self):
        """Test getting missing key with default."""
        d = {"person": {"name": "John"}}
        assert safe_dict_get(d, "person.age", "Unknown") == "Unknown"

    def test_get_with_none(self):
        """Test getting from None."""
        assert safe_dict_get({}, "person.name", "Default") == "Default"


class TestFlattenListToString:
    """Tests for list flattening."""

    def test_flatten_strings(self):
        """Test flattening string list."""
        items = ["apple", "banana", "cherry"]
        assert flatten_list_to_string(items) == "apple, banana, cherry"

    def test_flatten_with_custom_separator(self):
        """Test flattening with custom separator."""
        items = ["a", "b", "c"]
        assert flatten_list_to_string(items, " | ") == "a | b | c"

    def test_flatten_empty_list(self):
        """Test flattening empty list."""
        assert flatten_list_to_string([]) == ""

    def test_flatten_with_none_values(self):
        """Test flattening with None values."""
        items = ["a", None, "b", "", "c"]
        result = flatten_list_to_string(items)
        assert "None" not in result


class TestSafeInt:
    """Tests for safe integer conversion."""

    def test_convert_valid_int(self):
        """Test converting valid integers."""
        assert safe_int("123") == 123
        assert safe_int(456) == 456

    def test_convert_invalid_int(self):
        """Test converting invalid values."""
        assert safe_int("not a number", 0) == 0
        assert safe_int(None, -1) == -1


class TestSafeStr:
    """Tests for safe string conversion."""

    def test_convert_valid_str(self):
        """Test converting valid values."""
        assert safe_str("hello") == "hello"
        assert safe_str(123) == "123"

    def test_convert_none(self):
        """Test converting None."""
        assert safe_str(None) == ""
        assert safe_str(None, "default") == "default"

    def test_strip_whitespace(self):
        """Test whitespace stripping."""
        assert safe_str("  hello  ") == "hello"


class TestGetUtcTimestamp:
    """Tests for UTC timestamp generation."""

    def test_timestamp_format(self):
        """Test timestamp format."""
        timestamp = get_utc_timestamp()
        assert "T" in timestamp
        assert timestamp.endswith("Z")
        assert len(timestamp) == 20  # ISO 8601 format
