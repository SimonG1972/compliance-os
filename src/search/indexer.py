# src/search/indexer.py
from __future__ import annotations

from sqlalchemy import text

from ..db import begin
from ..log import info, warn
from ..db import init_base_tables, init_meta_tables, rebuild_fts_from_documents, init_fts


def init_fts() -> None:
    """
    Ensure base schema exists, then (re)build a standalone FTS from 'documents'.
    """
    with begin() as conn:
        init_base_tables(conn)
        init_meta_tables(conn)
        # If 'documents' is empty, still ensure FTS exists
        result = conn.execute(text("SELECT COUNT(1) FROM sqlite_master WHERE type='table' AND name='documents'"))
        _ = result.scalar()
        # Rebuild FTS from current base table
        rebuild_fts_from_documents(conn)
        info("FTS rebuilt (standalone)")
