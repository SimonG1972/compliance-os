# src/search/query.py
from __future__ import annotations

from typing import List, Tuple, Optional
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from ..db import get_engine

# Returned row: (doc_type, url, title_fallback, jurisdiction, snippet)
Row = Tuple[str, str, str, str, str]


def _canon_expr(url_col: str = "url") -> str:
    return (
        "lower("
        "replace("
        "replace("
        "replace(coalesce({u},''),'http://www.','https://'),"
        "'http://','https://'"
        "),"
        "'https://www.','https://'"
        ")"
        ")".format(u=url_col)
    )


def _platform_norm_expr(col: str = "platform_or_regulator") -> str:
    return (
        "trim("
        "replace("
        "replace(lower(coalesce({c},'')),'www.',''),"
        "'.com',''"
        ")"
        ")".format(c=col)
    )


def _jurisdiction_norm_expr(col: str = "jurisdiction") -> str:
    return "trim(lower(coalesce({c},'')))".format(c=col)


def search(
    q: str,
    limit: int = 25,
    platform: Optional[str] = None,
    jurisdiction: Optional[str] = None,
) -> List[Row]:
    engine = get_engine()
    params = {"q": q, "limit": int(limit)}

    filters = ""
    if platform:
        platform_norm = platform.strip().lower()
        if platform_norm.startswith("www."):
            platform_norm = platform_norm[4:]
        if platform_norm.endswith(".com"):
            platform_norm = platform_norm[:-4]
        params["platform_norm"] = platform_norm
        filters += f" AND {_platform_norm_expr()} = :platform_norm "

    if jurisdiction:
        params["juris_norm"] = jurisdiction.strip().lower()
        filters += f" AND {_jurisdiction_norm_expr()} = :juris_norm "

    canon = _canon_expr("url")

    # Snippet fallback chain: body -> title -> url -> body preview
    snip_expr = (
        "CASE "
        "  WHEN snippet(documents_fts, 5, '[', ']', '…', 24) <> '' "
        "       THEN snippet(documents_fts, 5, '[', ']', '…', 24) "
        "  WHEN snippet(documents_fts, 4, '[', ']', '…', 16) <> '' "
        "       THEN snippet(documents_fts, 4, '[', ']', '…', 16) "
        "  WHEN snippet(documents_fts, 0, '[', ']', '…', 24) <> '' "
        "       THEN snippet(documents_fts, 0, '[', ']', '…', 24) "
        "  ELSE substr(coalesce(body,''), 1, 160) "
        "END"
    )

    sql_bm25 = f"""
        WITH base AS (
          SELECT
            doc_type,
            url,
            title,
            jurisdiction,
            {canon} AS canon,
            bm25(documents_fts) AS rank,
            {snip_expr} AS snip
          FROM documents_fts
          WHERE documents_fts MATCH :q
          {filters}
        ),
        ranked AS (
          SELECT
            doc_type, url, title, jurisdiction, canon, rank, snip,
            ROW_NUMBER() OVER (PARTITION BY canon ORDER BY rank, url) AS rn
          FROM base
        )
        SELECT
          coalesce(nullif(doc_type,''), '')                             AS doc_type,
          coalesce(nullif(url,''), '')                                  AS url,
          coalesce(nullif(title,''), coalesce(url,''))                  AS title_fallback,
          coalesce(nullif(jurisdiction,''), 'global')                   AS jurisdiction,
          coalesce(snip, '')                                            AS snippet
        FROM ranked
        WHERE rn = 1
        ORDER BY rank, url
        LIMIT :limit
    """

    sql_simple = f"""
        WITH base AS (
          SELECT
            doc_type,
            url,
            title,
            jurisdiction,
            {canon} AS canon,
            {snip_expr} AS snip
          FROM documents_fts
          WHERE documents_fts MATCH :q
          {filters}
        )
        SELECT
          coalesce(nullif(doc_type,''), '')                             AS doc_type,
          coalesce(nullif(url,''), '')                                  AS url,
          coalesce(nullif(title,''), coalesce(url,''))                  AS title_fallback,
          coalesce(nullif(jurisdiction,''), 'global')                   AS jurisdiction,
          coalesce(snip, '')                                            AS snippet
        FROM base
        GROUP BY canon
        ORDER BY url
        LIMIT :limit
    """

    with engine.begin() as conn:
        try:
            rows = conn.execute(text(sql_bm25), params).fetchall()
        except OperationalError:
            rows = conn.execute(text(sql_simple), params).fetchall()

    return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]
