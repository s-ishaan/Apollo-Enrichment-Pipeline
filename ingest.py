"""
Excel ingestion and enrichment pipeline orchestration.
Handles parsing, normalization, enrichment, and database insertion.
"""

import os
import re
import logging
from typing import Dict, List, Any, Optional, Callable
import pandas as pd

from config import config, BASE_COLUMNS, COLUMN_MAPPINGS
from utils import (
    setup_logging, extract_domain, validate_email,
    normalize_company_name, get_utc_timestamp, mask_pii
)
from db import DatabaseManager
from apollo import ApolloClient

# Scraper imports only when process_scrape is used (optional Apify dependency)
try:
    from scraper import run_ai_extractor, scraped_items_to_truth_rows, ScraperError
except ImportError:
    run_ai_extractor = None
    scraped_items_to_truth_rows = None
    ScraperError = Exception  # noqa: A001

logger = setup_logging(config.LOG_LEVEL, config.LOG_FILE, config.LOG_TO_CONSOLE)


class ExcelIngestor:
    """
    Orchestrates Excel parsing, enrichment, and database insertion.
    """

    def __init__(
        self,
        db_manager: DatabaseManager,
        apollo_client: Optional[ApolloClient] = None
    ):
        """
        Initialize ingestor with dependencies.

        Args:
            db_manager: Database manager instance
            apollo_client: Apollo API client instance (optional, for enrichment)
        """
        self.db = db_manager
        self.apollo = apollo_client
        self._last_parse_warnings: List[str] = []
        logger.info("Excel ingestor initialized")

    def process_file(
        self,
        file_path: str,
        enrich_people: bool = True,
        enrich_companies: bool = True,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> Dict[str, Any]:
        """
        Main entry point for processing uploaded Excel file.

        Args:
            file_path: Path to Excel file
            enrich_people: Whether to enrich people data
            enrich_companies: Whether to enrich company data
            progress_callback: Optional callback(stage, current, total)

        Returns:
            Summary statistics dictionary
        """
        logger.info(f"Starting file processing: {file_path}")

        # Initialize results
        results = {
            "total_processed": 0,
            "new_inserts": 0,
            "updates": 0,
            "failed": 0,
            "errors": [],
            "people_enriched": 0,
            "orgs_enriched": 0,
            "warnings": [],
            "empty_reason": None,
            "org_enrichment_skipped_no_domain": 0,
        }

        try:
            # Stage 1: Parse Excel
            self._update_progress(progress_callback, "Parsing Excel file", 0, 100)
            df = self.parse_excel(file_path)
            results["warnings"] = list(self._last_parse_warnings)
            logger.info(f"Parsed {len(df)} rows from Excel")

            # Stage 2: Normalize and validate
            self._update_progress(progress_callback, "Normalizing data", 10, 100)
            df = self.normalize_dataframe(df)
            logger.info(f"Normalized to {len(df)} valid rows")

            # No rows to process (empty file or all rows dropped)
            if len(df) == 0:
                logger.warning("No valid records to process")
                results["empty_reason"] = "no_valid_rows"
                return results

            # Require email column so we don't call Apollo for rows we can't save
            if "Email ID (unique)" not in df.columns:
                raise ValueError(
                    "The Excel file must contain an email column "
                    "(e.g. 'Email', 'Email Address', 'E-mail'). "
                    "No rows can be saved without email."
                )

            # Stage 3: Deduplicate
            self._update_progress(progress_callback, "Deduplicating records", 20, 100)
            original_count = len(df)
            df = self.deduplicate_by_email(df)
            logger.info(f"Deduplicated: {original_count} -> {len(df)} records")

            if len(df) == 0:
                logger.warning("No valid records to process after deduplication")
                results["empty_reason"] = "no_valid_rows"
                return results

            results["total_processed"] = len(df)

            # Stage 4: Enrichment
            if enrich_people or enrich_companies:
                self._update_progress(progress_callback, "Enriching data", 30, 100)
                df, enrich_stats = self.enrich_records(
                    df, enrich_people, enrich_companies, progress_callback
                )
                results["people_enriched"] = enrich_stats.get("people", 0)
                results["orgs_enriched"] = enrich_stats.get("orgs", 0)
                results["org_enrichment_skipped_no_domain"] = enrich_stats.get(
                    "orgs_skipped_no_domain", 0
                )

            # Stage 5: Save to database
            self._update_progress(progress_callback, "Saving to database", 80, 100)
            db_stats = self.save_to_database(df)
            results["new_inserts"] = db_stats["inserted"]
            results["updates"] = db_stats["updated"]
            results["failed"] = db_stats["failed"]  # only records that raised during upsert
            results["inserted_emails"] = db_stats.get("inserted_emails", [])
            results["updated_emails"] = db_stats.get("updated_emails", [])
            results["failed_records"] = db_stats.get("failed_records", [])
            results["skipped_no_email"] = db_stats.get("skipped_no_email", 0)

            # Collect errors
            for idx, row in df.iterrows():
                if "_enrichment_error" in row and row["_enrichment_error"]:
                    results["errors"].append({
                        "email": mask_pii(row.get("Email ID (unique)", "Unknown")),
                        "message": str(row["_enrichment_error"])
                    })

            self._update_progress(progress_callback, "Complete", 100, 100)
            logger.info(f"Processing complete: {results}")

        except Exception as e:
            logger.error(f"File processing failed: {e}", exc_info=True)
            results["errors"].append({
                "email": "N/A",
                "message": f"Processing failed: {str(e)}"
            })
            results["processing_failed"] = True
            if results["total_processed"] > 0:
                results["failed"] = results["total_processed"]

        return results

    def parse_excel(self, file_path: str) -> pd.DataFrame:
        """
        Parse Excel file with flexible column detection.

        Args:
            file_path: Path to Excel file

        Returns:
            DataFrame with standardized column names

        Raises:
            ValueError: If file invalid or columns not detected
        """
        # Check file size
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        if file_size_mb > config.MAX_FILE_SIZE_MB:
            raise ValueError(
                f"File too large: {file_size_mb:.1f}MB "
                f"(max: {config.MAX_FILE_SIZE_MB}MB)"
            )

        logger.info(f"Reading Excel file ({file_size_mb:.1f}MB)")

        self._last_parse_warnings = []
        with pd.ExcelFile(file_path, engine="openpyxl") as xl:
            sheet_names = xl.sheet_names
            if len(sheet_names) > 1:
                self._last_parse_warnings.append(
                    "Multiple sheets detected; only the first sheet was read."
                )
            df = pd.read_excel(xl, sheet_name=0)

        if config.MAX_ROWS > 0 and len(df) > config.MAX_ROWS:
            raise ValueError(
                f"File has {len(df)} rows (max: {config.MAX_ROWS}). "
                "Set MAX_ROWS in .env to allow more, or reduce the file size."
            )

        logger.info(f"Read {len(df)} rows, {len(df.columns)} columns")

        # Detect and map columns
        df = self._detect_and_map_columns(df)

        # Warn if pandas created duplicate-style column names (e.g. email.1)
        duplicate_suffix = re.compile(r"^.+\.\d+$", re.IGNORECASE)
        dup_cols = [c for c in df.columns if duplicate_suffix.match(str(c).strip())]
        if dup_cols:
            self._last_parse_warnings.append(
                "Duplicate column names detected (e.g. 'Email.1'); "
                "only the first matching column is used for each field."
            )

        return df

    def _detect_and_map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect columns flexibly and map to standard names.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with standardized column names
        """
        # Create lowercase version of column names for matching
        column_map = {}

        for col in df.columns:
            col_lower = str(col).strip().lower()

            # Try to match against known mappings
            for standard_col, aliases in COLUMN_MAPPINGS.items():
                if col_lower in aliases or col_lower == standard_col.lower():
                    column_map[col] = standard_col
                    logger.debug(f"Mapped '{col}' -> '{standard_col}'")
                    break

            # If no match, keep original name
            if col not in column_map:
                column_map[col] = col

        # Rename columns
        df = df.rename(columns=column_map)

        logger.info(f"Column detection complete: {len(column_map)} columns mapped")

        return df

    def normalize_dataframe(
        self,
        df: pd.DataFrame,
        lead_source: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Clean and normalize data.

        Args:
            df: Input DataFrame
            lead_source: If provided, set Lead Source to this value; else "Excel Upload".

        Returns:
            Normalized DataFrame
        """
        # Drop pandas index columns (e.g. "Unnamed: 0") so re-uploaded exports don't fail
        unnamed_pattern = re.compile(r"^Unnamed:\s*\d+$", re.IGNORECASE)
        drop_cols = [c for c in df.columns if unnamed_pattern.match(str(c).strip())]
        if drop_cols:
            df = df.drop(columns=drop_cols)
            logger.debug(f"Dropped index columns: {drop_cols}")

        # Drop completely empty rows
        df = df.dropna(how='all')

        # Extract domain from website URL if present
        if "Website URLs" in df.columns:
            df["Website URLs"] = df["Website URLs"].apply(
                lambda x: extract_domain(str(x)) if pd.notna(x) else None
            )

        # Normalize company names
        if "Company Name (Based on Website Domain)" in df.columns:
            df["Company Name (Based on Website Domain)"] = df[
                "Company Name (Based on Website Domain)"
            ].apply(
                lambda x: normalize_company_name(str(x)) if pd.notna(x) else None
            )

        # Clean whitespace in text columns
        text_columns = ["First Name", "Last Name", "Job Title", "Email ID (unique)"]
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: str(x).strip() if pd.notna(x) else None
                )

        # Validate emails only when column exists; drop invalid or empty
        if "Email ID (unique)" in df.columns:
            original_count = len(df)

            def keep_row_email(val):
                if pd.isna(val):
                    return False
                s = str(val).strip()
                if not s:
                    return False
                return validate_email(s)

            df = df[df["Email ID (unique)"].apply(keep_row_email)]
            dropped = original_count - len(df)
            if dropped > 0:
                logger.warning(f"Dropped {dropped} rows with missing or invalid emails")

        # Add default values
        df["Lead Source"] = lead_source if lead_source is not None else "Excel Upload"
        df["Email Send (Yes/No)"] = df.get("Email Send (Yes/No)", "No")

        # Fill NaN with empty string for consistency
        df = df.fillna("")

        logger.info("Data normalization complete")

        return df

    def deduplicate_by_name_and_company(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate (First Name, Last Name, Company) rows, keeping first occurrence.
        Used for scrape-origin data before Apollo enrichment.

        Args:
            df: Input DataFrame

        Returns:
            Deduplicated DataFrame
        """
        key_cols = [
            "First Name",
            "Last Name",
            "Company Name (Based on Website Domain)",
        ]
        missing = [c for c in key_cols if c not in df.columns]
        if missing:
            logger.warning(
                f"Columns {missing} not found, skipping name+company deduplication"
            )
            return df

        original_count = len(df)
        df = df.drop_duplicates(subset=key_cols, keep="first")
        duplicates = original_count - len(df)
        if duplicates > 0:
            logger.info(f"Removed {duplicates} duplicate name+company rows")

        return df

    def deduplicate_by_email(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate emails, keeping first occurrence.

        Args:
            df: Input DataFrame

        Returns:
            Deduplicated DataFrame
        """
        if "Email ID (unique)" not in df.columns:
            logger.warning("Email column not found, skipping deduplication")
            return df

        original_count = len(df)
        df = df.drop_duplicates(subset=["Email ID (unique)"], keep='first')
        duplicates = original_count - len(df)

        if duplicates > 0:
            logger.info(f"Removed {duplicates} duplicate emails")

        return df

    def enrich_records(
        self,
        df: pd.DataFrame,
        enrich_people: bool,
        enrich_companies: bool,
        progress_callback: Optional[Callable] = None
    ) -> tuple[pd.DataFrame, Dict[str, int]]:
        """
        Call Apollo APIs and merge results back into DataFrame.
        Only sends rows with at least one identifier for people enrichment.
        Tracks how many rows had no website for org enrichment.
        """
        stats: Dict[str, int] = {"people": 0, "orgs": 0, "orgs_skipped_no_domain": 0}

        # Skip enrichment if no Apollo client
        if not self.apollo:
            logger.info("No Apollo client provided, skipping enrichment")
            return df, stats

        records = df.to_dict("records")

        # Count rows with no domain (for org enrichment skip message)
        for r in records:
            has_domain = r.get("Website URLs") and str(r.get("Website URLs", "")).strip()
            if not has_domain:
                stats["orgs_skipped_no_domain"] += 1

        # Enrich people: only send rows with at least one identifier
        if enrich_people:
            logger.info("Starting people enrichment")
            self._update_progress(progress_callback, "Enriching people data", 40, 100)

            def has_people_identifier(rec: Dict[str, Any]) -> bool:
                email = rec.get("Email ID (unique)") and str(rec.get("Email ID (unique)", "")).strip()
                first = rec.get("First Name") and str(rec.get("First Name", "")).strip()
                last = rec.get("Last Name") and str(rec.get("Last Name", "")).strip()
                company = rec.get("Company Name (Based on Website Domain)") and str(
                    rec.get("Company Name (Based on Website Domain)", "")
                ).strip()
                website = rec.get("Website URLs") and str(rec.get("Website URLs", "")).strip()
                return bool(email or (first and last) or company or website)

            mask = df.apply(lambda row: has_people_identifier(row.to_dict()), axis=1)
            subset_idx = df.index[mask].tolist()
            records_subset = df.loc[mask].to_dict("records")

            if records_subset:
                enriched_subset = self.apollo.enrich_people_bulk(records_subset)
                stats["people"] = len(
                    [r for r in enriched_subset if "_enrichment_error" not in r]
                )
            else:
                enriched_subset = []

            # Build full-length list: enriched where we called API, else original + skip marker
            enriched_people_full: List[Dict[str, Any]] = []
            j = 0
            for i, row in df.iterrows():
                if i in subset_idx:
                    enriched_people_full.append(enriched_subset[j])
                    j += 1
                else:
                    d = row.to_dict()
                    d["_enrichment_error"] = "Skipped (no identifier for matching)"
                    enriched_people_full.append(d)

            df = self._merge_enriched_data(df, enriched_people_full)
            logger.info(f"People enrichment complete: {stats['people']} enriched")

        # Enrich companies
        if enrich_companies:
            logger.info("Starting company enrichment")
            self._update_progress(progress_callback, "Enriching company data", 60, 100)

            enriched_orgs = self.apollo.enrich_organizations_bulk(records)
            stats["orgs"] = len([r for r in enriched_orgs if "_enrichment_error" not in r])

            df = self._merge_enriched_data(df, enriched_orgs)
            logger.info(f"Company enrichment complete: {stats['orgs']} enriched")

        return df, stats

    def _merge_enriched_data(
        self,
        df: pd.DataFrame,
        enriched_records: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Merge enriched data back into DataFrame.

        Args:
            df: Original DataFrame
            enriched_records: List of enriched record dicts

        Returns:
            Merged DataFrame
        """
        # Convert enriched records to DataFrame
        enriched_df = pd.DataFrame(enriched_records)

        # Update original DataFrame with enriched data
        # For each column in enriched_df, update df if value is not empty
        for col in enriched_df.columns:
            if col not in df.columns:
                df[col] = ""

            # Update non-empty values
            mask = enriched_df[col].notna() & (enriched_df[col] != "")
            df.loc[mask, col] = enriched_df.loc[mask, col]

        return df

    def save_to_database(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Upsert all records to database.

        Args:
            df: DataFrame with records to save

        Returns:
            Statistics dictionary (inserted, updated, failed, skipped_no_email,
            inserted_emails, updated_emails, failed_records)
        """
        logger.info(f"Saving {len(df)} records to database")

        # Convert DataFrame to list of dicts
        records = df.to_dict('records')

        # Clean up records - ensure all base columns present
        cleaned_records = []
        skipped_no_email = 0
        for record in records:
            cleaned = {}

            # Add all columns from record
            for key, value in record.items():
                # Skip internal fields
                if key.startswith("_"):
                    continue

                # Convert to string, handle NaN/None
                if pd.isna(value) or value == "":
                    cleaned[key] = ""
                else:
                    cleaned[key] = str(value)

            # Ensure Email ID is present
            if not cleaned.get("Email ID (unique)"):
                logger.warning("Skipping record without email")
                skipped_no_email += 1
                continue

            cleaned_records.append(cleaned)

        # Batch upsert (failed = only records that raised during upsert)
        _, stats = self.db.upsert_batch(cleaned_records)
        stats["skipped_no_email"] = skipped_no_email

        logger.info(
            f"Database save complete: {stats['inserted']} inserted, "
            f"{stats['updated']} updated, {stats['failed']} failed, "
            f"{skipped_no_email} skipped (no email)"
        )

        return stats

    def _update_progress(
        self,
        callback: Optional[Callable],
        stage: str,
        current: int,
        total: int
    ) -> None:
        """
        Update progress via callback if provided.

        Args:
            callback: Progress callback function
            stage: Current stage description
            current: Current progress value
            total: Total progress value
        """
        if callback:
            try:
                callback(stage, current, total)
            except Exception as e:
                logger.warning(f"Progress callback failed: {e}")


def process_scrape(
    url: str,
    enrich_people: bool = True,
    enrich_companies: bool = True,
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
) -> Dict[str, Any]:
    """
    Run AI extractor on URL, map to Truth rows, normalize, dedupe by name+company,
    enrich with Apollo, and save only rows with email.

    Args:
        url: Page URL to scrape.
        enrich_people: Whether to enrich people via Apollo.
        enrich_companies: Whether to enrich companies via Apollo.
        progress_callback: Optional callback(stage, current, total).

    Returns:
        Results dict: total_processed, new_inserts, updates, failed, errors,
        people_enriched, orgs_enriched, skipped_no_email.
    """
    results = {
        "total_processed": 0,
        "new_inserts": 0,
        "updates": 0,
        "failed": 0,
        "errors": [],
        "people_enriched": 0,
        "orgs_enriched": 0,
        "skipped_no_email": 0,
    }

    if run_ai_extractor is None or scraped_items_to_truth_rows is None:
        results["errors"].append({
            "email": "N/A",
            "message": "Scraper module not available. Install apify-client and ensure scraper.py exists.",
        })
        return results

    try:
        _update_progress(progress_callback, "Running AI extractor", 0, 100)
        items = run_ai_extractor(url)
    except ScraperError as e:
        logger.exception("Scrape failed")
        results["errors"].append({"email": "N/A", "message": str(e)})
        return results

    if not items:
        logger.warning("AI extractor returned no items")
        return results

    _update_progress(progress_callback, "Mapping to Truth columns", 5, 100)
    df = scraped_items_to_truth_rows(items, source_url=url)

    if df.empty:
        logger.warning("No valid rows after mapping")
        return results

    db = DatabaseManager()
    apollo = ApolloClient()
    ingestor = ExcelIngestor(db, apollo)

    _update_progress(progress_callback, "Normalizing data", 15, 100)
    df = ingestor.normalize_dataframe(df, lead_source="Website Scrape")

    _update_progress(progress_callback, "Deduplicating by name and company", 25, 100)
    df = ingestor.deduplicate_by_name_and_company(df)

    if df.empty:
        db.close()
        return results

    results["total_processed"] = len(df)

    if enrich_people or enrich_companies:
        _update_progress(progress_callback, "Enriching with Apollo", 30, 100)
        df, enrich_stats = ingestor.enrich_records(
            df, enrich_people, enrich_companies, progress_callback
        )
        results["people_enriched"] = enrich_stats.get("people", 0)
        results["orgs_enriched"] = enrich_stats.get("orgs", 0)

    _update_progress(progress_callback, "Saving to database", 80, 100)

    # Count rows that will be skipped (no email after enrichment) and capture names/orgs for UI
    if "Email ID (unique)" in df.columns:
        no_email = df["Email ID (unique)"].isna() | (df["Email ID (unique)"].astype(str).str.strip() == "")
        results["skipped_no_email"] = int(no_email.sum())
        skipped_df = df.loc[no_email]
        results["skipped_no_email_records"] = [
            {
                "First Name": _str_val(row.get("First Name")),
                "Last Name": _str_val(row.get("Last Name")),
                "Company Name (Based on Website Domain)": _str_val(
                    row.get("Company Name (Based on Website Domain)")
                ),
            }
            for _, row in skipped_df.iterrows()
        ]
        # Records that will be saved (have email) — for UI to show names/orgs of updated & new
        has_email = ~no_email
        saved_df = df.loc[has_email]
        results["saved_records"] = [
            {
                "First Name": _str_val(row.get("First Name")),
                "Last Name": _str_val(row.get("Last Name")),
                "Company Name (Based on Website Domain)": _str_val(
                    row.get("Company Name (Based on Website Domain)")
                ),
            }
            for _, row in saved_df.iterrows()
        ]
    else:
        results["skipped_no_email"] = len(df)
        results["skipped_no_email_records"] = [
            {
                "First Name": _str_val(row.get("First Name")),
                "Last Name": _str_val(row.get("Last Name")),
                "Company Name (Based on Website Domain)": _str_val(
                    row.get("Company Name (Based on Website Domain)")
                ),
            }
            for _, row in df.iterrows()
        ]
        results["saved_records"] = []

    db_stats = ingestor.save_to_database(df)
    results["new_inserts"] = db_stats["inserted"]
    results["updates"] = db_stats["updated"]
    results["failed"] = db_stats["failed"]

    for idx, row in df.iterrows():
        if row.get("_enrichment_error"):
            results["errors"].append({
                "email": mask_pii(row.get("Email ID (unique)", "Unknown")),
                "message": str(row["_enrichment_error"]),
            })

    _update_progress(progress_callback, "Complete", 100, 100)
    db.close()
    logger.info(f"Scrape process complete: {results}")
    return results


def _str_val(val: Any) -> str:
    """Return non-empty string for display; empty string for NaN/None/blank."""
    if pd.isna(val) or val is None or str(val).strip() == "":
        return ""
    return str(val).strip()


def _update_progress(
    callback: Optional[Callable],
    stage: str,
    current: int,
    total: int,
) -> None:
    """Invoke progress callback if provided."""
    if callback:
        try:
            callback(stage, current, total)
        except Exception as e:
            logger.warning(f"Progress callback failed: {e}")
