"""
Tests for Excel ingestion pipeline.
"""

import pytest
import pandas as pd
from ingest import ExcelIngestor
from config import config


class TestExcelParsing:
    """Tests for Excel file parsing."""

    def test_parse_excel(self, db_manager, sample_excel_path):
        """Test parsing Excel file."""
        apollo = None  # Not needed for parsing
        ingestor = ExcelIngestor(db_manager, apollo)

        df = ingestor.parse_excel(sample_excel_path)

        assert len(df) > 0
        assert "Email ID (unique)" in df.columns  # Should be mapped
        assert "First Name" in df.columns

    def test_parse_invalid_file_fails(self, db_manager):
        """Test that parsing invalid file raises error."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        with pytest.raises(Exception):
            ingestor.parse_excel("/nonexistent/file.xlsx")


class TestColumnDetection:
    """Tests for flexible column detection."""

    def test_detect_email_variations(self, db_manager):
        """Test detecting various email column names."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        # Create DataFrame with different column name
        df = pd.DataFrame({
            "e-mail": ["test@example.com"],
            "fname": ["John"],
            "lname": ["Doe"]
        })

        df = ingestor._detect_and_map_columns(df)

        assert "Email ID (unique)" in df.columns

    def test_detect_company_variations(self, db_manager):
        """Test detecting various company column names."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        df = pd.DataFrame({
            "organization": ["ACME Corp"],
            "website": ["acme.com"]
        })

        df = ingestor._detect_and_map_columns(df)

        assert "Company Name (Based on Website Domain)" in df.columns
        assert "Website URLs" in df.columns


class TestDataNormalization:
    """Tests for data normalization."""

    def test_normalize_dataframe(self, db_manager):
        """Test DataFrame normalization."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        df = pd.DataFrame({
            "Email ID (unique)": ["  test@example.com  ", "invalid"],
            "Company Name (Based on Website Domain)": ["  acme corp  ", "Other"],
            "Website URLs": ["https://www.example.com/path", ""],
            "First Name": ["John", "Jane"],
            "Last Name": ["Doe", "Smith"],
        })

        df = ingestor.normalize_dataframe(df)

        # Should clean whitespace
        assert df.iloc[0]["Email ID (unique)"] == "test@example.com"

        # Should normalize company name
        assert df.iloc[0]["Company Name (Based on Website Domain)"] == "Acme Corp"

        # Should extract domain
        assert df.iloc[0]["Website URLs"] == "example.com"

        # Should drop invalid emails
        assert len(df) == 1  # Invalid email row dropped

    def test_normalize_sets_defaults(self, db_manager):
        """Test that normalization sets default values."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        df = pd.DataFrame({
            "Email ID (unique)": ["test@example.com"],
            "First Name": ["John"]
        })

        df = ingestor.normalize_dataframe(df)

        assert df.iloc[0]["Lead Source"] == "Excel Upload"
        assert df.iloc[0]["Email Send (Yes/No)"] == "No"

    def test_normalize_lead_source_override(self, db_manager):
        """Test that lead_source parameter overrides default."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        df = pd.DataFrame({
            "Email ID (unique)": ["test@example.com"],
            "First Name": ["John"],
        })
        df = ingestor.normalize_dataframe(df, lead_source="Website Scrape")

        assert df.iloc[0]["Lead Source"] == "Website Scrape"


class TestDeduplication:
    """Tests for email deduplication."""

    def test_deduplicate_by_email(self, db_manager):
        """Test deduplication by email."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        df = pd.DataFrame({
            "Email ID (unique)": [
                "test@example.com",
                "test@example.com",
                "unique@example.com"
            ],
            "First Name": ["John", "Jane", "Bob"]
        })

        df = ingestor.deduplicate_by_email(df)

        assert len(df) == 2  # One duplicate removed
        assert df.iloc[0]["First Name"] == "John"  # First occurrence kept

    def test_deduplicate_by_name_and_company(self, db_manager):
        """Test deduplication by First Name, Last Name, Company."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        df = pd.DataFrame({
            "First Name": ["John", "John", "Jane"],
            "Last Name": ["Doe", "Doe", "Smith"],
            "Company Name (Based on Website Domain)": ["Acme", "Acme", "Acme"],
        })
        df = ingestor.deduplicate_by_name_and_company(df)

        assert len(df) == 2  # One duplicate (John Doe, Acme) removed
        assert df.iloc[0]["First Name"] == "John"
        assert df.iloc[1]["First Name"] == "Jane"


class TestSaveToDatabase:
    """Tests for saving records to database."""

    def test_save_to_database(self, db_manager):
        """Test saving DataFrame to database."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        df = pd.DataFrame({
            "Email ID (unique)": ["test1@example.com", "test2@example.com"],
            "First Name": ["Alice", "Bob"],
            "Last Name": ["Smith", "Jones"],
            "Lead Source": ["Excel Upload", "Excel Upload"],
            "Email Send (Yes/No)": ["No", "No"]
        })

        stats = ingestor.save_to_database(df)

        assert stats["inserted"] == 2
        assert stats["updated"] == 0
        assert stats["failed"] == 0

        # Verify records in database
        records, _ = db_manager.search_records(limit=10)
        assert len(records) == 2

    def test_save_skips_records_without_email(self, db_manager):
        """Test that records without email are skipped."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        df = pd.DataFrame({
            "Email ID (unique)": ["test@example.com", ""],
            "First Name": ["Alice", "Bob"]
        })

        stats = ingestor.save_to_database(df)

        # Should only save one record
        assert stats["inserted"] == 1


class TestProgressCallback:
    """Tests for progress callback functionality."""

    def test_progress_callback_called(self, db_manager):
        """Test that progress callback is invoked."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        progress_calls = []

        def callback(stage, current, total):
            progress_calls.append((stage, current, total))

        # Test with simple data
        df = pd.DataFrame({
            "Email ID (unique)": ["test@example.com"]
        })

        # Just test the update method
        ingestor._update_progress(callback, "Test Stage", 50, 100)

        assert len(progress_calls) == 1
        assert progress_calls[0] == ("Test Stage", 50, 100)

    def test_progress_callback_with_none(self, db_manager):
        """Test that None callback is handled gracefully."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        # Should not raise error
        ingestor._update_progress(None, "Test", 1, 1)


class TestMergeEnrichedData:
    """Tests for merging enriched data."""

    def test_merge_enriched_data(self, db_manager):
        """Test merging enriched data back into DataFrame."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        # Original DataFrame
        df = pd.DataFrame({
            "Email ID (unique)": ["test@example.com"],
            "First Name": ["John"]
        })

        # Enriched records
        enriched = [{
            "Email ID (unique)": "test@example.com",
            "First Name": "John",
            "Apollo Person: Seniority": "senior",
            "Apollo Company: Founded Year": "2010"
        }]

        df = ingestor._merge_enriched_data(df, enriched)

        # Should have new columns
        assert "Apollo Person: Seniority" in df.columns
        assert df.iloc[0]["Apollo Person: Seniority"] == "senior"

    def test_merge_preserves_original_data(self, db_manager):
        """Test that merge preserves original non-empty values."""
        apollo = None
        ingestor = ExcelIngestor(db_manager, apollo)

        df = pd.DataFrame({
            "Email ID (unique)": ["test@example.com"],
            "First Name": ["Original Name"]
        })

        # Enriched with empty first name
        enriched = [{
            "Email ID (unique)": "test@example.com",
            "First Name": "",
            "Job Title": "Engineer"
        }]

        df = ingestor._merge_enriched_data(df, enriched)

        # Should preserve original name
        assert df.iloc[0]["First Name"] == "Original Name"
        # Should add new field
        assert df.iloc[0]["Job Title"] == "Engineer"


class TestNoEmailColumn:
    """Tests for missing email column (fail fast, no wasted API calls)."""

    def test_process_file_returns_error_when_no_email_column(
        self, db_manager, excel_no_email_path
    ):
        """process_file returns results with error when Excel has no email column."""
        ingestor = ExcelIngestor(db_manager, None)
        results = ingestor.process_file(
            excel_no_email_path,
            enrich_people=False,
            enrich_companies=False,
        )
        assert results.get("processing_failed") is True
        assert len(results["errors"]) >= 1
        assert "email" in results["errors"][0]["message"].lower()
        assert "column" in results["errors"][0]["message"].lower()


class TestEmptyFile:
    """Tests for empty or all-invalid file (empty_reason, no crash)."""

    def test_process_file_empty_file_sets_empty_reason(
        self, db_manager, excel_empty_path
    ):
        """process_file returns empty_reason when file has no data rows."""
        ingestor = ExcelIngestor(db_manager, None)
        results = ingestor.process_file(
            excel_empty_path,
            enrich_people=False,
            enrich_companies=False,
        )
        assert results["empty_reason"] == "no_valid_rows"
        assert results["total_processed"] == 0
        assert results["new_inserts"] == 0

    def test_process_file_all_invalid_email_sets_empty_reason(
        self, db_manager, excel_all_invalid_email_path
    ):
        """process_file returns empty_reason when all rows have invalid/empty email."""
        ingestor = ExcelIngestor(db_manager, None)
        results = ingestor.process_file(
            excel_all_invalid_email_path,
            enrich_people=False,
            enrich_companies=False,
        )
        assert results["empty_reason"] == "no_valid_rows"
        assert results["total_processed"] == 0


class TestParseWarnings:
    """Tests for parse warnings (multiple sheets, duplicate columns)."""

    def test_parse_excel_warns_multiple_sheets(
        self, db_manager, excel_multiple_sheets_path
    ):
        """parse_excel sets _last_parse_warnings when workbook has multiple sheets."""
        ingestor = ExcelIngestor(db_manager, None)
        ingestor.parse_excel(excel_multiple_sheets_path)
        assert any(
            "multiple sheet" in w.lower() for w in ingestor._last_parse_warnings
        )

    def test_parse_excel_warns_duplicate_columns(
        self, db_manager, excel_duplicate_columns_path
    ):
        """parse_excel sets _last_parse_warnings when columns have duplicate names."""
        ingestor = ExcelIngestor(db_manager, None)
        ingestor.parse_excel(excel_duplicate_columns_path)
        assert any(
            "duplicate column" in w.lower() for w in ingestor._last_parse_warnings
        )

    def test_process_file_passes_warnings_to_results(
        self, db_manager, excel_multiple_sheets_path
    ):
        """process_file includes parse warnings in results['warnings']."""
        ingestor = ExcelIngestor(db_manager, None)
        results = ingestor.process_file(
            excel_multiple_sheets_path,
            enrich_people=False,
            enrich_companies=False,
        )
        assert "warnings" in results
        assert any(
            "multiple sheet" in w.lower() for w in results["warnings"]
        )


class TestNormalizeEmptyEmail:
    """Tests for dropping empty/invalid email rows in normalization."""

    def test_normalize_drops_empty_email_rows(self, db_manager):
        """Rows with empty or whitespace-only email are dropped."""
        ingestor = ExcelIngestor(db_manager, None)
        df = pd.DataFrame({
            "Email ID (unique)": ["valid@example.com", "", "  ", None],
            "First Name": ["A", "B", "C", "D"],
        })
        df = ingestor.normalize_dataframe(df)
        assert len(df) == 1
        assert df.iloc[0]["Email ID (unique)"] == "valid@example.com"

    def test_normalize_drops_invalid_email_rows(self, db_manager):
        """Rows with invalid email format are dropped."""
        ingestor = ExcelIngestor(db_manager, None)
        df = pd.DataFrame({
            "Email ID (unique)": ["good@example.com", "bad", "also@bad"],
            "First Name": ["A", "B", "C"],
        })
        df = ingestor.normalize_dataframe(df)
        assert len(df) == 1
        assert df.iloc[0]["Email ID (unique)"] == "good@example.com"


class TestMaxRows:
    """Tests for MAX_ROWS enforcement."""

    def test_parse_excel_raises_when_max_rows_exceeded(
        self, db_manager, sample_excel_path, monkeypatch
    ):
        """parse_excel raises ValueError when row count exceeds MAX_ROWS."""
        monkeypatch.setattr(config, "MAX_ROWS", 1)
        ingestor = ExcelIngestor(db_manager, None)
        with pytest.raises(ValueError, match="max: 1"):
            ingestor.parse_excel(sample_excel_path)
