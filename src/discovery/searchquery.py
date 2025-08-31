# src/search/query.py
from __future__ import annotations

from typing import List, Dict

from ..db import get_engine
from ..log import warn


def search(q: str, limit: int = 25) -> List[Dict]:
    """
    Full-text search using the FTS virtual table created by init_fts().
    Assumes documents_fts is an FTS5 table with content='documents' so rowid matches.
    Returns a list of dicts with url, doc_type, title.
    """
    engine = get_engine()
    rows: List[Dict] = []

    sql_try = [
        # Most common layout: FTS content table bound to documents; join on rowid
        """
        SELECT d.url, d.doc_type, COALESCE(d.title, '') AS title
        FROM documents d
        JOIN documents_fts f ON d.rowid = f.rowid
        WHERE f MATCH :q
        LIMIT :limit
        """,
        # Fallback if a different alias is required (some setups expose table name as documents_fts)
        """
        SELECT d.url, d.doc_type, COALESCE(d.title, '') AS title
        FROM documents d, documents_fts
        WHERE documents_fts MATCH :q
          AND d.rowid = documents_fts.rowid
        LIMIT :limit
        """,
        # Last resort: try to query the FTS table alone (if columns include url, doc_type, title)
        """
        SELECT COALESCE(url,'') AS url,
               COALESCE(doc_type,'') AS doc_type,
               COALESCE(title,'') AS title
        FROM documents_fts
        WHERE documents_fts MATCH :q
        LIMIT :limit
        """,
    ]

    with engine.begin() as conn:
        for stmt in sql_try:
            try:
                res = conn.exec_driver_sql(stmt, {"q": q, "limit": int(limit)})
                rows = [{"url": r[0], "doc_type": r[1], "title": r[2]} for r in res]
                if rows:
                    break
            except Exception as e:
                # Try next strategy
                last_err = e
                rows = []
                continue

    if not rows:
        warn("No results (or FTS layout did not match expected schema).")
    return rows
