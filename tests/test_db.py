"""
Tests for database operations.
"""

import pytest
from db import DatabaseManager


class TestDatabaseInitialization:
    """Tests for database initialization."""

    def test_initialize_schema(self, db_manager):
        """Test that schema is created correctly."""
        columns = db_manager.get_column_list()
        assert "S.N." in columns
        assert "Email ID (unique)" in columns
        assert "UPDATE AS ON" in columns
        assert "First Name" in columns
        assert len(columns) >= 23  # Base columns

    def test_column_cache_loaded(self, db_manager):
        """Test that column cache is populated."""
        assert len(db_manager.column_cache) > 0
        assert "Email ID (unique)" in db_manager.column_cache


class TestUpsertOperations:
    """Tests for upsert operations."""

    def test_insert_new_record(self, db_manager, sample_records):
        """Test inserting a new record."""
        record = sample_records[0]
        sn, action = db_manager.upsert_record(record)

        assert sn > 0
        assert action == "insert"

        # Verify record exists
        retrieved = db_manager.get_existing_record(record["Email ID (unique)"])
        assert retrieved is not None
        assert retrieved["First Name"] == record["First Name"]

    def test_update_existing_record(self, db_manager, sample_records):
        """Test merge: filling empty Job Title from new run preserves S.N. and counts as update."""
        record = sample_records[0].copy()
        record["Job Title"] = ""  # Empty so we can fill from next run

        sn1, action1 = db_manager.upsert_record(record)
        assert action1 == "insert"

        # Same email, now with Job Title set (new info for previously empty field)
        record["Job Title"] = "Senior Software Engineer"
        sn2, action2 = db_manager.upsert_record(record)

        assert sn1 == sn2
        assert action2 == "update"

        retrieved = db_manager.get_existing_record(record["Email ID (unique)"])
        assert retrieved["Job Title"] == "Senior Software Engineer"

    def test_upsert_without_email_fails(self, db_manager):
        """Test that upsert without email raises error."""
        record = {"First Name": "John", "Last Name": "Doe"}

        with pytest.raises(ValueError, match="Email ID.*required"):
            db_manager.upsert_record(record)

    def test_upsert_sets_timestamp(self, db_manager, sample_records):
        """Test that UPDATE AS ON is set on insert."""
        record = sample_records[0]
        db_manager.upsert_record(record)

        retrieved = db_manager.get_existing_record(record["Email ID (unique)"])
        assert retrieved["UPDATE AS ON"] != ""
        assert "T" in retrieved["UPDATE AS ON"]  # ISO format

    def test_upsert_sets_default_email_send(self, db_manager, sample_records):
        """Test that Email Send defaults to No on insert."""
        record = sample_records[0].copy()
        record.pop("Email Send (Yes/No)", None)

        db_manager.upsert_record(record)

        retrieved = db_manager.get_existing_record(record["Email ID (unique)"])
        assert retrieved["Email Send (Yes/No)"] == "No"

    def test_merge_does_not_overwrite_email_send(self, db_manager, sample_records):
        """Test that Email Send (Yes/No) is never overwritten on update."""
        record = sample_records[0].copy()
        record["Email Send (Yes/No)"] = "Yes"
        record["Job Title"] = ""  # Empty so next run can fill and trigger an update
        db_manager.upsert_record(record)

        # Same email, incoming says No; we must keep existing Yes. Fill Job Title to trigger update.
        record["Email Send (Yes/No)"] = "No"
        record["Job Title"] = "Senior Engineer"
        db_manager.upsert_record(record)

        retrieved = db_manager.get_existing_record(record["Email ID (unique)"])
        assert retrieved["Email Send (Yes/No)"] == "Yes"
        assert retrieved["Job Title"] == "Senior Engineer"

    def test_skip_when_no_new_info(self, db_manager, sample_records):
        """Test that re-upserting same record with no new info returns skip and does not count as update."""
        record = sample_records[0]
        db_manager.upsert_record(record)

        # Same record again: no empty fields to fill
        _, action = db_manager.upsert_record(record)
        assert action == "skip"

        # Batch: insert all, then same records again -> 0 inserted, 0 updated
        db_manager.upsert_batch(sample_records)
        _, stats = db_manager.upsert_batch(sample_records)
        assert stats["inserted"] == 0
        assert stats["updated"] == 0


class TestBatchUpsert:
    """Tests for batch upsert operations."""

    def test_upsert_batch(self, db_manager, sample_records):
        """Test batch upsert."""
        sns, stats = db_manager.upsert_batch(sample_records)

        assert len(sns) == len(sample_records)
        assert stats["inserted"] == len(sample_records)
        assert stats["updated"] == 0
        assert stats["failed"] == 0

    def test_upsert_batch_with_duplicates(self, db_manager, sample_records):
        """Test batch upsert with duplicate emails: no new info so all skip, 0 updated."""
        # Insert first batch
        db_manager.upsert_batch(sample_records)

        # Same records again: no empty fields to fill, so all skip
        sns, stats = db_manager.upsert_batch(sample_records)

        assert stats["inserted"] == 0
        assert stats["updated"] == 0


class TestApolloColumns:
    """Tests for dynamic Apollo column management."""

    def test_ensure_apollo_columns(self, db_manager):
        """Test adding Apollo columns dynamically."""
        new_columns = [
            "Apollo Person: Email Status",
            "Apollo Company: Founded Year"
        ]

        db_manager.ensure_apollo_columns(new_columns)

        # Verify columns added
        columns = db_manager.get_column_list()
        assert "Apollo Person: Email Status" in columns
        assert "Apollo Company: Founded Year" in columns

    def test_ensure_apollo_columns_cached(self, db_manager):
        """Test that column cache prevents duplicate additions."""
        new_columns = ["Apollo Person: Seniority"]

        # Add once
        db_manager.ensure_apollo_columns(new_columns)
        initial_count = len(db_manager.get_column_list())

        # Add again (should be cached)
        db_manager.ensure_apollo_columns(new_columns)
        final_count = len(db_manager.get_column_list())

        assert initial_count == final_count

    def test_upsert_with_apollo_columns(self, db_manager, sample_records):
        """Test upserting records with Apollo columns."""
        record = sample_records[0].copy()
        record["Apollo Person: Email Status"] = "verified"
        record["Apollo Company: Founded Year"] = "2010"

        sn, action = db_manager.upsert_record(record)
        assert sn > 0
        assert action == "insert"

        # Verify Apollo columns stored
        retrieved = db_manager.get_existing_record(record["Email ID (unique)"])
        assert retrieved["Apollo Person: Email Status"] == "verified"
        assert retrieved["Apollo Company: Founded Year"] == "2010"


class TestSearchRecords:
    """Tests for search and filter functionality."""

    def test_search_without_filters(self, db_manager, sample_records):
        """Test searching without filters."""
        db_manager.upsert_batch(sample_records)

        records, total = db_manager.search_records(limit=10, offset=0)

        assert len(records) == len(sample_records)
        assert total == len(sample_records)

    def test_search_with_email_filter(self, db_manager, sample_records):
        """Test searching with email filter."""
        db_manager.upsert_batch(sample_records)

        filters = {"Email ID (unique)": "john"}
        records, total = db_manager.search_records(filters=filters, limit=10)

        assert len(records) == 1
        assert "john" in records[0]["Email ID (unique)"].lower()

    def test_search_with_company_filter(self, db_manager, sample_records):
        """Test searching with company filter."""
        db_manager.upsert_batch(sample_records)

        filters = {"Company Name (Based on Website Domain)": "Example"}
        records, total = db_manager.search_records(filters=filters, limit=10)

        assert len(records) == 1
        assert "Example" in records[0]["Company Name (Based on Website Domain)"]

    def test_search_pagination(self, db_manager, sample_records):
        """Test search pagination."""
        db_manager.upsert_batch(sample_records)

        # First page
        records, total = db_manager.search_records(limit=1, offset=0)
        assert len(records) == 1
        assert total == len(sample_records)

        # Second page
        records, total = db_manager.search_records(limit=1, offset=1)
        assert len(records) == 1


class TestExportToDataFrame:
    """Tests for DataFrame export."""

    def test_export_all_records(self, db_manager, sample_records):
        """Test exporting all records."""
        db_manager.upsert_batch(sample_records)

        df = db_manager.export_to_dataframe()

        assert len(df) == len(sample_records)
        assert "Email ID (unique)" in df.columns

    def test_export_with_filters(self, db_manager, sample_records):
        """Test exporting with filters."""
        db_manager.upsert_batch(sample_records)

        filters = {"Company Name (Based on Website Domain)": "ACME"}
        df = db_manager.export_to_dataframe(filters=filters)

        assert len(df) == 1
        assert "ACME" in df.iloc[0]["Company Name (Based on Website Domain)"]


class TestStatistics:
    """Tests for database statistics."""

    def test_get_statistics(self, db_manager, sample_records):
        """Test getting database statistics."""
        db_manager.upsert_batch(sample_records)

        stats = db_manager.get_statistics()

        assert stats["total_records"] == len(sample_records)
        assert stats["total_columns"] >= 23
        assert "by_lead_source" in stats
        assert stats["by_lead_source"].get("Excel Upload") == len(sample_records)
