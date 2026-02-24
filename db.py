"""
Database layer for Apollo enrichment pipeline.
Manages SQLite or PostgreSQL (Supabase) operations including schema, upserts, queries, and exports.
When DATABASE_URL is set, uses PostgreSQL; otherwise uses SQLite.
"""

import math
import sqlite3
import logging
from contextlib import contextmanager
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd

from config import BASE_COLUMNS, config
from utils import get_utc_timestamp, setup_logging

logger = setup_logging(config.LOG_LEVEL, config.LOG_FILE,
                       config.LOG_TO_CONSOLE)

# Column that must never be overwritten on update (always keep existing value).
EMAIL_SEND_COLUMN = "Email Send (Yes/No)"


def _quote_identifier(name: str) -> str:
    """Quote a SQL identifier and escape any double quotes inside it (SQLite and Postgres)."""
    return '"' + str(name).replace('"', '""') + '"'


def _is_empty(val: Any) -> bool:
    """Return True if value is considered empty (only None and ""). NaN treated as empty."""
    if val is None:
        return True
    if isinstance(val, str) and val == "":
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    return False


def _has_value(val: Any) -> bool:
    """Return True if value is non-empty."""
    return not _is_empty(val)


# ---------------------------------------------------------------------------
# SQLite backend
# ---------------------------------------------------------------------------


class _SQLiteBackend:
    """SQLite implementation of the Truth table operations."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.column_cache: set = set()
        self.initialize_schema()
        self._load_column_cache()
        logger.info(f"Database initialized at {db_path}")

    @contextmanager
    def get_cursor(self):
        cursor = self.conn.cursor()
        try:
            yield cursor
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Database transaction failed: {e}")
            raise
        finally:
            cursor.close()

    def initialize_schema(self) -> None:
        column_definitions = []
        for col in BASE_COLUMNS:
            if col == "S.N.":
                column_definitions.append(
                    f'"{col}" INTEGER PRIMARY KEY AUTOINCREMENT')
            elif col == "Email ID (unique)":
                column_definitions.append(f'"{col}" TEXT UNIQUE NOT NULL')
            elif col == "UPDATE AS ON":
                column_definitions.append(f'"{col}" TEXT NOT NULL')
            else:
                column_definitions.append(f'"{col}" TEXT')
        create_table_sql = f"CREATE TABLE IF NOT EXISTS Truth ({', '.join(column_definitions)})"
        with self.get_cursor() as cursor:
            cursor.execute(create_table_sql)
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_email ON Truth("Email ID (unique)")')
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_company ON Truth("Company Name (Based on Website Domain)")')
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_updated ON Truth("UPDATE AS ON")')
        logger.info("Database schema initialized with base columns")

    def _load_column_cache(self) -> None:
        with self.get_cursor() as cursor:
            cursor.execute("PRAGMA table_info(Truth)")
            self.column_cache = {row[1] for row in cursor.fetchall()}
        logger.debug(f"Loaded {len(self.column_cache)} columns into cache")

    def ensure_apollo_columns(self, column_names: List[str]) -> None:
        new_columns = [
            col for col in column_names if col not in self.column_cache]
        if new_columns:
            logger.info(f"Adding {len(new_columns)} new Apollo columns")
            with self.get_cursor() as cursor:
                for col in new_columns:
                    try:
                        cursor.execute(
                            f'ALTER TABLE Truth ADD COLUMN {_quote_identifier(col)} TEXT')
                        self.column_cache.add(col)
                        logger.debug(f"Added column: {col}")
                    except sqlite3.OperationalError as e:
                        if "duplicate column" not in str(e).lower():
                            raise

    def get_existing_record(self, email: str) -> Optional[Dict[str, Any]]:
        with self.get_cursor() as cursor:
            cursor.execute(
                f'SELECT * FROM Truth WHERE {_quote_identifier("Email ID (unique)")} = ?',
                (email,),
            )
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None

    def _insert_record(self, record: Dict[str, Any]) -> int:
        record_copy = record.copy()
        record_copy.pop("S.N.", None)
        apollo_columns = [
            col for col in record_copy.keys()
            if col.startswith(("Apollo Person:", "Apollo Company:"))
        ]
        if apollo_columns:
            self.ensure_apollo_columns(apollo_columns)
        columns = list(record_copy.keys())
        placeholders = ", ".join(["?" for _ in columns])
        column_names = ", ".join(_quote_identifier(col) for col in columns)
        sql = f"INSERT INTO Truth ({column_names}) VALUES ({placeholders})"
        with self.get_cursor() as cursor:
            cursor.execute(sql, list(record_copy.values()))
            sn = cursor.lastrowid
        logger.debug(f"Inserted new record with S.N. {sn}")
        return sn

    def _update_record(self, record: Dict[str, Any]) -> int:
        sn = record["S.N."]
        apollo_columns = [
            col for col in record.keys()
            if col.startswith(("Apollo Person:", "Apollo Company:"))
        ]
        if apollo_columns:
            self.ensure_apollo_columns(apollo_columns)
        update_cols = [col for col in record.keys() if col != "S.N."]
        set_clause = ", ".join(
            f"{_quote_identifier(col)} = ?" for col in update_cols)
        values = [record[col] for col in update_cols]
        sql = f'UPDATE Truth SET {set_clause} WHERE {_quote_identifier("S.N.")} = ?'
        values.append(sn)
        with self.get_cursor() as cursor:
            cursor.execute(sql, values)
        logger.debug(f"Updated record with S.N. {sn}")
        return sn

    def search_records(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        where_clauses = []
        params = []
        if filters:
            for col, value in filters.items():
                if value:
                    where_clauses.append(f"{_quote_identifier(col)} LIKE ?")
                    params.append(f"%{value}%")
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        count_sql = f"SELECT COUNT(*) FROM Truth {where_sql}"
        with self.get_cursor() as cursor:
            cursor.execute(count_sql, params)
            total_count = cursor.fetchone()[0]
        query_sql = f"SELECT * FROM Truth {where_sql} ORDER BY {_quote_identifier('S.N.')} ASC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        with self.get_cursor() as cursor:
            cursor.execute(query_sql, params)
            rows = cursor.fetchall()
            records = [dict(row) for row in rows]
        logger.debug(
            f"Search returned {len(records)} records (total: {total_count}, offset: {offset})")
        return records, total_count

    def get_column_list(self) -> List[str]:
        with self.get_cursor() as cursor:
            cursor.execute("PRAGMA table_info(Truth)")
            return [row[1] for row in cursor.fetchall()]

    def get_statistics(self) -> Dict[str, Any]:
        with self.get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM Truth")
            total_records = cursor.fetchone()[0]
            cursor.execute(
                'SELECT "Lead Source", COUNT(*) FROM Truth GROUP BY "Lead Source"')
            by_source = dict(cursor.fetchall())
            cursor.execute(
                'SELECT COUNT(*) FROM Truth WHERE "UPDATE AS ON" >= datetime("now", "-7 days")')
            recent_updates = cursor.fetchone()[0]
        return {
            "total_records": total_records,
            "by_lead_source": by_source,
            "recent_updates_7_days": recent_updates,
            "total_columns": len(self.get_column_list()),
        }

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


# ---------------------------------------------------------------------------
# Postgres backend (supports psycopg2 or psycopg3)
# ---------------------------------------------------------------------------

_psycopg2_err: Optional[Exception] = None
_psycopg_err: Optional[Exception] = None

try:
    import psycopg2
    from psycopg2 import extras as pg_extras
    from psycopg2 import errors as pg_errors
except ImportError as e:
    psycopg2 = None  # type: ignore
    pg_extras = None  # type: ignore
    pg_errors = None  # type: ignore
    _psycopg2_err = e

try:
    import psycopg
    from psycopg.rows import dict_row as pg3_dict_row
except ImportError as e:
    psycopg = None  # type: ignore
    pg3_dict_row = None  # type: ignore
    _psycopg_err = e


class _PostgresBackend:
    """PostgreSQL (Supabase) implementation of the Truth table operations."""

    def __init__(self, database_url: str) -> None:
        self._pg3 = False
        if psycopg2 is not None:
            try:
                self.conn = psycopg2.connect(database_url)
            except Exception:
                logger.exception(
                    "Failed to connect to PostgreSQL. Check DATABASE_URL and network.")
                raise
        elif psycopg is not None and pg3_dict_row is not None:
            try:
                self.conn = psycopg.connect(database_url)
                self._pg3 = True
            except Exception:
                logger.exception(
                    "Failed to connect to PostgreSQL. Check DATABASE_URL and network.")
                raise
        else:
            parts = [
                "A PostgreSQL driver is required when DATABASE_URL is set. "
                "Install: pip install psycopg2-binary",
            ]
            if _psycopg2_err is not None:
                parts.append(f" (psycopg2 import failed: {_psycopg2_err})")
            if _psycopg_err is not None:
                parts.append(f" (psycopg import failed: {_psycopg_err})")
            parts.append(
                ". On Streamlit Cloud: put requirements.txt in the same directory as your app entrypoint (e.g. app.py) and include: psycopg2-binary>=2.9.0"
            )
            raise ImportError("".join(parts))
        self.column_cache: set = set()
        self.initialize_schema()
        self._load_column_cache()
        logger.info("Database initialized (PostgreSQL)")

    @contextmanager
    def get_cursor(self):
        if self._pg3:
            cursor = self.conn.cursor(row_factory=pg3_dict_row)
        else:
            cursor = self.conn.cursor(cursor_factory=pg_extras.RealDictCursor)
        try:
            yield cursor
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Database transaction failed: {e}")
            raise
        finally:
            cursor.close()

    def initialize_schema(self) -> None:
        column_definitions = []
        for col in BASE_COLUMNS:
            if col == "S.N.":
                column_definitions.append(f'"{col}" SERIAL PRIMARY KEY')
            elif col == "Email ID (unique)":
                column_definitions.append(f'"{col}" TEXT UNIQUE NOT NULL')
            elif col == "UPDATE AS ON":
                column_definitions.append(f'"{col}" TEXT NOT NULL')
            else:
                column_definitions.append(f'"{col}" TEXT')
        create_table_sql = f'CREATE TABLE IF NOT EXISTS "Truth" ({", ".join(column_definitions)})'
        with self.get_cursor() as cursor:
            cursor.execute(create_table_sql)
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_email ON "Truth"("Email ID (unique)")')
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_company ON "Truth"("Company Name (Based on Website Domain)")')
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_updated ON "Truth"("UPDATE AS ON")')
        logger.info("Database schema initialized with base columns")

    def _load_column_cache(self) -> None:
        with self.get_cursor() as cursor:
            cursor.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'Truth' ORDER BY ordinal_position"
            )
            self.column_cache = {row["column_name"]
                                 for row in cursor.fetchall()}
        logger.debug(f"Loaded {len(self.column_cache)} columns into cache")

    def ensure_apollo_columns(self, column_names: List[str]) -> None:
        new_columns = [
            col for col in column_names if col not in self.column_cache]
        if new_columns:
            logger.info(f"Adding {len(new_columns)} new Apollo columns")
            with self.get_cursor() as cursor:
                for col in new_columns:
                    try:
                        cursor.execute(
                            f'ALTER TABLE "Truth" ADD COLUMN {_quote_identifier(col)} TEXT')
                        self.column_cache.add(col)
                        logger.debug(f"Added column: {col}")
                    except Exception as e:
                        if getattr(e, "pgcode", None) != "42701" and "already exists" not in str(e).lower():
                            raise

    def get_existing_record(self, email: str) -> Optional[Dict[str, Any]]:
        with self.get_cursor() as cursor:
            cursor.execute(
                f'SELECT * FROM "Truth" WHERE {_quote_identifier("Email ID (unique)")} = %s',
                (email,),
            )
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None

    def _insert_record(self, record: Dict[str, Any]) -> int:
        record_copy = record.copy()
        record_copy.pop("S.N.", None)
        apollo_columns = [
            col for col in record_copy.keys()
            if col.startswith(("Apollo Person:", "Apollo Company:"))
        ]
        if apollo_columns:
            self.ensure_apollo_columns(apollo_columns)
        columns = list(record_copy.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        column_names = ", ".join(_quote_identifier(col) for col in columns)
        sql = f'INSERT INTO "Truth" ({column_names}) VALUES ({placeholders}) RETURNING "S.N."'
        with self.get_cursor() as cursor:
            cursor.execute(sql, list(record_copy.values()))
            row = cursor.fetchone()
            sn = row["S.N."]
        logger.debug(f"Inserted new record with S.N. {sn}")
        return sn

    def _update_record(self, record: Dict[str, Any]) -> int:
        sn = record["S.N."]
        apollo_columns = [
            col for col in record.keys()
            if col.startswith(("Apollo Person:", "Apollo Company:"))
        ]
        if apollo_columns:
            self.ensure_apollo_columns(apollo_columns)
        update_cols = [col for col in record.keys() if col != "S.N."]
        set_clause = ", ".join(
            f"{_quote_identifier(col)} = %s" for col in update_cols)
        values = [record[col] for col in update_cols]
        sql = f'UPDATE "Truth" SET {set_clause} WHERE {_quote_identifier("S.N.")} = %s'
        values.append(sn)
        with self.get_cursor() as cursor:
            cursor.execute(sql, values)
        logger.debug(f"Updated record with S.N. {sn}")
        return sn

    def search_records(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        where_clauses = []
        params: List[Any] = []
        if filters:
            for col, value in filters.items():
                if value:
                    where_clauses.append(f"{_quote_identifier(col)} LIKE %s")
                    params.append(f"%{value}%")
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        count_sql = f'SELECT COUNT(*) FROM "Truth" {where_sql}'
        with self.get_cursor() as cursor:
            cursor.execute(count_sql, params)
            total_count = cursor.fetchone()["count"]
        query_sql = f'SELECT * FROM "Truth" {where_sql} ORDER BY {_quote_identifier("S.N.")} ASC LIMIT %s OFFSET %s'
        params.extend([limit, offset])
        with self.get_cursor() as cursor:
            cursor.execute(query_sql, params)
            rows = cursor.fetchall()
            records = [dict(row) for row in rows]
        logger.debug(
            f"Search returned {len(records)} records (total: {total_count}, offset: {offset})")
        return records, total_count

    def get_column_list(self) -> List[str]:
        with self.get_cursor() as cursor:
            cursor.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'Truth' ORDER BY ordinal_position"
            )
            return [row["column_name"] for row in cursor.fetchall()]

    def get_statistics(self) -> Dict[str, Any]:
        with self.get_cursor() as cursor:
            cursor.execute('SELECT COUNT(*) AS cnt FROM "Truth"')
            total_records = cursor.fetchone()["cnt"]
            cursor.execute(
                'SELECT "Lead Source", COUNT(*) FROM "Truth" GROUP BY "Lead Source"')
            by_source = {row["Lead Source"]: row["count"]
                         for row in cursor.fetchall()}
            cursor.execute(
                "SELECT COUNT(*) AS cnt FROM \"Truth\" "
                "WHERE \"UPDATE AS ON\" >= to_char((CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '7 days'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')"
            )
            recent_updates = cursor.fetchone()["cnt"]
        return {
            "total_records": total_records,
            "by_lead_source": by_source,
            "recent_updates_7_days": recent_updates,
            "total_columns": len(self.get_column_list()),
        }

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


# ---------------------------------------------------------------------------
# DatabaseManager facade
# ---------------------------------------------------------------------------


class DatabaseManager:
    """
    Manages all database operations for the Truth table.
    Uses SQLite when DATABASE_URL is unset, PostgreSQL (e.g. Supabase) when set.
    Supports dynamic column addition and idempotent upserts.
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        """
        Initialize database connection and schema.

        Args:
            db_path: Path to SQLite database file (used only when DATABASE_URL is not set).
        """
        if config.DATABASE_URL and config.DATABASE_URL.strip():
            self._backend: Any = _PostgresBackend(config.DATABASE_URL.strip())
            self.db_path = ""
        else:
            path = db_path or config.DB_PATH
            self._backend = _SQLiteBackend(path)
            self.db_path = path
        self.conn = self._backend.conn
        self.column_cache = self._backend.column_cache

    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor with automatic commit/rollback."""
        with self._backend.get_cursor() as cursor:
            yield cursor

    def initialize_schema(self) -> None:
        self._backend.initialize_schema()

    def _load_column_cache(self) -> None:
        self._backend._load_column_cache()
        self.column_cache = self._backend.column_cache

    def ensure_apollo_columns(self, column_names: List[str]) -> None:
        self._backend.ensure_apollo_columns(column_names)
        self.column_cache = self._backend.column_cache

    def get_existing_record(self, email: str) -> Optional[Dict[str, Any]]:
        return self._backend.get_existing_record(email)

    def upsert_record(self, record: Dict[str, Any]) -> Tuple[int, str]:
        email = record.get("Email ID (unique)")
        if not email:
            raise ValueError("Email ID (unique) is required for upsert")
        existing = self.get_existing_record(email)
        if not existing:
            record = dict(record)
            record["UPDATE AS ON"] = get_utc_timestamp()
            if EMAIL_SEND_COLUMN not in record:
                record[EMAIL_SEND_COLUMN] = "No"
            sn = self._backend._insert_record(record)
            return (sn, "insert")
        merged = dict(existing)
        fields_filled = 0
        timestamp_key = "UPDATE AS ON"
        for key, incoming_val in record.items():
            if key == "S.N." or key == EMAIL_SEND_COLUMN:
                continue
            if key not in merged:
                merged[key] = incoming_val
                if _has_value(incoming_val):
                    fields_filled += 1
            else:
                if _is_empty(merged[key]) and _has_value(incoming_val):
                    merged[key] = incoming_val
                    if key != timestamp_key:
                        fields_filled += 1
        if fields_filled == 0:
            return (existing["S.N."], "skip")
        merged["UPDATE AS ON"] = get_utc_timestamp()
        merged["S.N."] = existing["S.N."]
        self._backend._update_record(merged)
        return (existing["S.N."], "update")

    def _insert_record(self, record: Dict[str, Any]) -> int:
        return self._backend._insert_record(record)

    def _update_record(self, record: Dict[str, Any]) -> int:
        return self._backend._update_record(record)

    def upsert_batch(self, records: List[Dict[str, Any]]) -> Tuple[List[int], Dict[str, Any]]:
        stats: Dict[str, Any] = {
            "inserted": 0,
            "updated": 0,
            "failed": 0,
            "inserted_emails": [],
            "updated_emails": [],
            "failed_records": [],
        }
        sns = []
        logger.info(f"Starting batch upsert of {len(records)} records")
        for i, record in enumerate(records):
            email = record.get("Email ID (unique)") or ""
            try:
                sn, action = self.upsert_record(record)
                sns.append(sn)
                if action == "insert":
                    stats["inserted"] += 1
                    stats["inserted_emails"].append(email)
                elif action == "update":
                    stats["updated"] += 1
                    stats["updated_emails"].append(email)
                if (i + 1) % 100 == 0:
                    logger.info(f"Processed {i + 1}/{len(records)} records")
            except Exception as e:
                logger.error(f"Failed to upsert record {i}: {e}")
                stats["failed"] += 1
                stats["failed_records"].append(
                    {"email": email, "error": str(e)})
                sns.append(-1)
        logger.info(
            f"Batch upsert complete: {stats['inserted']} inserted, "
            f"{stats['updated']} updated, {stats['failed']} failed"
        )
        return sns, stats

    def search_records(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        return self._backend.search_records(filters=filters, limit=limit, offset=offset)

    def export_to_dataframe(
        self,
        filters: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        records, _ = self.search_records(filters, limit=1000000, offset=0)
        df = pd.DataFrame(records)
        logger.info(f"Exported {len(df)} records to DataFrame")
        return df

    def get_column_list(self) -> List[str]:
        return self._backend.get_column_list()

    def get_statistics(self) -> Dict[str, Any]:
        return self._backend.get_statistics()

    def close(self) -> None:
        self._backend.close()
        self.conn = None  # type: ignore

    def __enter__(self) -> "DatabaseManager":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
