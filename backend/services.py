"""
UI-free pipeline services for the FastAPI backend.
Uses existing db, ingest, and apollo modules.
"""

import io
import os
import tempfile
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd

from config import load_config
from db import DatabaseManager
from apollo import ApolloClient
from ingest import ExcelIngestor, process_scrape

import logging

logger = logging.getLogger(__name__)


def run_enrichment_pipeline_core(
    file_bytes: bytes,
    enrich_people: bool,
    enrich_companies: bool,
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
    db: Optional[DatabaseManager] = None,
) -> Optional[Dict[str, Any]]:
    """
    Execute enrichment pipeline (no Streamlit).
    Returns results dict on success, None on failure.
    If db is provided (e.g. from FastAPI), it is not closed; otherwise a new one is created and closed.
    """
    tmp_file_path: Optional[str] = None
    own_db = db is None
    try:
        load_config(validate=True)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file.write(file_bytes)
            tmp_file_path = tmp_file.name

        if db is None:
            db = DatabaseManager()
        apollo = ApolloClient()
        ingestor = ExcelIngestor(db, apollo)

        results = ingestor.process_file(
            tmp_file_path,
            enrich_people=enrich_people,
            enrich_companies=enrich_companies,
            progress_callback=progress_callback,
        )
        if own_db:
            db.close()
        return results
    except Exception as e:
        logger.exception("Enrichment pipeline failed")
        raise
    finally:
        if tmp_file_path and os.path.isfile(tmp_file_path):
            try:
                os.unlink(tmp_file_path)
            except OSError as cleanup_err:
                logger.warning("Failed to remove temp file %s: %s", tmp_file_path, cleanup_err)


def upload_base_data_core(
    file_bytes: bytes,
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
    db: Optional[DatabaseManager] = None,
) -> Dict[str, Any]:
    """
    Upload base Excel data to database without enrichment.
    Returns {"stats": {...}, "total": N}.
    If db is provided (e.g. from FastAPI), it is not closed; otherwise a new one is created and closed.
    """
    tmp_file_path: Optional[str] = None
    own_db = db is None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file.write(file_bytes)
            tmp_file_path = tmp_file.name

        def _progress(stage: str, current: int, total: int) -> None:
            if progress_callback:
                progress_callback(stage, current, total)

        _progress("Parsing Excel", 10, 100)
        if db is None:
            db = DatabaseManager()
        ingestor = ExcelIngestor(db, None)

        df = ingestor.parse_excel(tmp_file_path)
        _progress("Normalizing data", 30, 100)
        df = ingestor.normalize_dataframe(df)
        _progress("Deduplicating records", 50, 100)
        df = ingestor.deduplicate_by_email(df)
        _progress("Saving to database", 70, 100)
        stats = ingestor.save_to_database(df)
        if own_db:
            db.close()

        return {"stats": stats, "total": len(df)}
    finally:
        if tmp_file_path and os.path.isfile(tmp_file_path):
            try:
                os.unlink(tmp_file_path)
            except OSError as cleanup_err:
                logger.warning("Failed to remove temp file %s: %s", tmp_file_path, cleanup_err)


def run_scrape_core(
    url: str,
    enrich_people: bool,
    enrich_companies: bool,
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
) -> Dict[str, Any]:
    """
    Run scrape and enrich via process_scrape (already UI-free).
    Returns results dict.
    """
    return process_scrape(
        url.strip(),
        enrich_people=enrich_people,
        enrich_companies=enrich_companies,
        progress_callback=progress_callback,
    )


def export_to_excel(
    filters: Dict[str, Any],
    db: Optional[DatabaseManager] = None,
) -> Tuple[bytes, str]:
    """
    Export filtered data to Excel in memory.
    Returns (file_bytes, filename).
    If db is provided (e.g. from FastAPI), it is not closed; otherwise a new one is created and closed.
    """
    own_db = db is None
    if db is None:
        db = DatabaseManager()
    try:
        df = db.export_to_dataframe(filters)
        if len(df) == 0:
            if own_db:
                db.close()
            raise ValueError("No data to export")

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Truth Data")
        buffer.seek(0)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"apollo_export_{timestamp}.xlsx"
        return buffer.getvalue(), filename
    finally:
        if own_db:
            db.close()
