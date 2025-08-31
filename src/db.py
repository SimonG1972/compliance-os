# src/db.py
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterable, Set

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -------------------------------------------------------------------
# Engine / connection helpers
# -------------------------------------------------------------------

_DB_URL = os.environ.get("COMPLIANCE_DB_URL") or f"sqlite:///{os.path.join(os.getcwd(), 'compliance.db')}"
_ENGINE: Engine | None = None


def get_engine() -> Engine:
    """Return a cached SQLAlchemy Engine (SQLite by default)."""
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(_DB_URL, future=True, echo=False)
    return _ENGINE


@contextmanager
def begin():
    """Yield a connection inside a transaction (SQLAlchemy 2.x style)."""
    eng = get_engine()
    with eng.begin() as conn:
        yield conn


# -------------------------------------------------------------------
# Schema bootstrap / migrations (idempotent, additive-only)
# -------------------------------------------------------------------

_REQUIRED_DOC_COLS: dict[str, str] = {
    # identity
    "url": "TEXT PRIMARY KEY",
    "url_original": "TEXT",
    # metadata
    "doc_type": "TEXT",
    "jurisdiction": "TEXT",
    "source": "TEXT",
    "title": "TEXT",
    # content
    "body": "TEXT",
    "clean_text": "TEXT",
    # fetch info
    "status_code": "INTEGER",
    "render_mode": "TEXT",  # 'static' or 'js'
    "fetched_at": "TEXT",
    # change tracking
    "content_hash": "TEXT",
    "revisions": "INTEGER DEFAULT 0",
    # retries / errors
    "last_error": "TEXT",
    "retry_count": "INTEGER DEFAULT 0",
}

# For compat, don't try to add PRIMARY KEY via ALTER TABLE.
_REQUIRED_META_COLS: dict[str, str] = {"k": "TEXT", "v": "TEXT"}


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        text("SELECT name FROM sqlite_master WHERE type='table' AND name=:name"),
        {"name": table},
    ).fetchone()
    return bool(row)


def _table_columns(conn, table: str) -> Set[str]:
    """
    Return existing column names for `table`.

    NOTE: SQLite PRAGMA does **not** accept bound parameters reliably,
    so we must inline the table name here. We still validate it to avoid injection.
    """
    if not table.replace("_", "").isalnum():
        raise ValueError(f"Invalid table name: {table!r}")
    rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    return {r[1] for r in rows}  # r[1] is the 'name' column


def _ensure_columns(conn, table: str, required: dict[str, str]) -> None:
    existing = _table_columns(conn, table)
    to_add: Iterable[tuple[str, str]] = (
        (col, coltype) for col, coltype in required.items() if col not in existing
    )
    for col, coltype in to_add:
        # Never attempt to add a PRIMARY KEY via ALTER TABLE; SQLite disallows it.
        safe_coltype = coltype.replace("PRIMARY KEY", "").strip()
        if safe_coltype:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {safe_coltype}"))
        else:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col}"))


def init_base_tables(engine: Engine | None = None) -> None:
    """Create/upgrade core tables without destructive changes."""
    eng = engine or get_engine()
    with eng.begin() as conn:
        # Create documents if missing (create with full schema; PRIMARY KEY only on create)
        if not _table_exists(conn, "documents"):
            conn.execute(
                text(
                    """
                    CREATE TABLE documents (
                        url TEXT PRIMARY KEY,
                        url_original TEXT,
                        doc_type TEXT,
                        jurisdiction TEXT,
                        source TEXT,
                        title TEXT,
                        body TEXT,
                        clean_text TEXT,
                        status_code INTEGER,
                        render_mode TEXT,
                        fetched_at TEXT,
                        content_hash TEXT,
                        revisions INTEGER DEFAULT 0,
                        last_error TEXT,
                        retry_count INTEGER DEFAULT 0
                    )
                    """
                )
            )
        else:
            # Ensure any columns missing on older DBs are added.
            _ensure_columns(conn, "documents", _REQUIRED_DOC_COLS)


def init_meta_tables(engine: Engine | None = None) -> None:
    """
    Ensure we have a key/value store.

    Compatibility rules:
    - If 'meta' does not exist: create with k TEXT PRIMARY KEY, v TEXT.
    - If 'meta' exists with legacy columns 'key'/'value': leave it alone
      and create a compatibility VIEW 'meta_kv(k, v)' pointing to legacy columns.
    - If 'meta' exists with some other shape: ensure 'k' and 'v' columns exist
      (plain TEXT, no PK added via ALTER).
    """
    eng = engine or get_engine()
    with eng.begin() as conn:
        if not _table_exists(conn, "meta"):
            conn.execute(
                text(
                    """
                    CREATE TABLE meta (
                        k TEXT PRIMARY KEY,
                        v TEXT
                    )
                    """
                )
            )
            return

        cols = _table_columns(conn, "meta")

        # Legacy shape detected: key/value
        if {"key", "value"}.issubset(cols):
            # Create a compatibility view if not present
            conn.execute(
                text(
                    """
                    CREATE VIEW IF NOT EXISTS meta_kv AS
                    SELECT "key" AS k, "value" AS v FROM meta
                    """
                )
            )
            # Do not attempt to ALTER legacy table
            return

        # Modern or partial shape: ensure k/v exist (no PKs added during ALTER)
        if "k" not in cols:
            conn.execute(text("ALTER TABLE meta ADD COLUMN k TEXT"))
        if "v" not in cols:
            conn.execute(text("ALTER TABLE meta ADD COLUMN v TEXT"))


def init_fts(engine: Engine | None = None) -> None:
    """
    Create contentless FTS5 tables if not present.
    documents_fts indexes raw body; documents_clean_fts indexes clean_text.
    """
    eng = engine or get_engine()
    with eng.begin() as conn:
        if not _table_exists(conn, "documents_fts"):
            conn.execute(
                text(
                    """
                    CREATE VIRTUAL TABLE documents_fts USING fts5(
                        url UNINDEXED,
                        title,
                        body,
                        doc_type,
                        jurisdiction,
                        source,
                        content=''
                    )
                    """
                )
            )
        if not _table_exists(conn, "documents_clean_fts"):
            conn.execute(
                text(
                    """
                    CREATE VIRTUAL TABLE documents_clean_fts USING fts5(
                        url UNINDEXED,
                        title,
                        clean_text,
                        content=''
                    )
                    """
                )
            )


# -------------------------------------------------------------------
# FTS maintenance
# -------------------------------------------------------------------

def rebuild_fts_from_documents() -> None:
    """
    Rebuild both FTS indexes from current 'documents'.

    NOTE: For contentless FTS5 tables (content=''), you cannot use DELETE.
    You must use the special 'delete-all' command.
    """
    eng = get_engine()
    with eng.begin() as conn:
        # Ensure FTS tables exist (no-ops if present)
        conn.execute(
            text(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
                    url UNINDEXED, title, body, doc_type, jurisdiction, source, content=''
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS documents_clean_fts USING fts5(
                    url UNINDEXED, title, clean_text, content=''
                )
                """
            )
        )

        # Clear using FTS5 control command
        conn.execute(text("INSERT INTO documents_fts(documents_fts) VALUES ('delete-all')"))
        conn.execute(text("INSERT INTO documents_clean_fts(documents_clean_fts) VALUES ('delete-all')"))

        # Pull fresh rows from documents
        rows = conn.execute(
            text(
                """
                SELECT
                    url,
                    COALESCE(NULLIF(title,''), url) AS title,
                    COALESCE(body, '') AS body,
                    COALESCE(clean_text, '') AS clean_text,
                    COALESCE(doc_type, '') AS doc_type,
                    COALESCE(jurisdiction, '') AS jurisdiction,
                    COALESCE(source, '') AS source
                FROM documents
                """
            )
        ).fetchall()

        if rows:
            # Refill body FTS
            conn.execute(
                text(
                    """
                    INSERT INTO documents_fts (url, title, body, doc_type, jurisdiction, source)
                    VALUES (:url, :title, :body, :doc_type, :jurisdiction, :source)
                    """
                ),
                [
                    {
                        "url": r[0],
                        "title": r[1],
                        "body": r[2],
                        "doc_type": r[4],
                        "jurisdiction": r[5],
                        "source": r[6],
                    }
                    for r in rows
                ],
            )

            # Refill clean_text FTS
            conn.execute(
                text(
                    """
                    INSERT INTO documents_clean_fts (url, title, clean_text)
                    VALUES (:url, :title, :clean_text)
                    """
                ),
                [
                    {
                        "url": r[0],
                        "title": r[1],
                        "clean_text": r[3],
                    }
                    for r in rows
                ],
            )


# -------------------------------------------------------------------
# Optional small helper indexes (safe)
# -------------------------------------------------------------------

def ensure_perf_indexes() -> None:
    """Non-unique helper indexes that speed up selection at scale."""
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_documents_fetched_at ON documents(fetched_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_documents_retry_count ON documents(retry_count)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_documents_last_error ON documents(last_error)"))
