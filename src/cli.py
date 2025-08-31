# src/cli.py
from __future__ import annotations

import time
from typing import Optional

import typer

# Optional: enable verbose SQL/heartbeat if COMPLIANCE_VERBOSE/COMPLIANCE_HEARTBEAT are set
try:
    import src.observability  # noqa: F401
except Exception:
    pass

from src.db import (
    get_engine,
    init_base_tables,
    init_meta_tables,
    init_fts,
    ensure_perf_indexes,
    rebuild_fts_from_documents,  # zero-arg in your repo
)

# Hydrator may not exist in every build; guard it.
try:
    from src.tools.hydrate import hydrate as hydrate_runner
except Exception:
    hydrate_runner = None  # type: ignore


app = typer.Typer(help="Compliance-OS CLI")


# -----------------------
# Helpers
# -----------------------
def _column_exists(conn, table: str, column: str) -> bool:
    rows = conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()
    # sqlite Row has .name attr for column name
    return any(getattr(r, "name", None) == column for r in rows)


def _safe_top_hosts(conn, limit: int = 15):
    """
    Prefer 'platform' column if present, otherwise derive host from URL.
    """
    if _column_exists(conn, "documents", "platform"):
        try:
            return conn.exec_driver_sql(
                """
                SELECT COALESCE(NULLIF(platform,''), 'unknown') AS host, COUNT(1) AS c
                FROM documents
                GROUP BY host
                ORDER BY c DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        except Exception:
            pass

    # Fallback: derive host with string ops (no external libs)
    return conn.exec_driver_sql(
        """
        WITH hosts AS (
          SELECT
            CASE
              WHEN INSTR(url,'//') > 0 THEN SUBSTR(url, INSTR(url,'//')+2)
              ELSE url
            END AS after_scheme
          FROM documents
          WHERE COALESCE(url,'') <> ''
        )
        SELECT
          CASE
            WHEN INSTR(after_scheme,'/') > 0
              THEN SUBSTR(after_scheme, 1, INSTR(after_scheme,'/')-1)
            ELSE after_scheme
          END AS host,
          COUNT(1) AS c
        FROM hosts
        GROUP BY host
        ORDER BY c DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


# -----------------------
# Core Commands
# -----------------------
@app.command()
def reindex() -> None:
    """
    Create/upgrade tables and rebuild both FTS indexes from documents.
    NOTE: Do NOT pass a Connection into init_* helpers (they manage their own transactions).
    """
    # Non-destructive initializations (each helper opens/closes its own transaction)
    init_base_tables()
    init_meta_tables()
    init_fts()
    ensure_perf_indexes()

    # Rebuild both FTS tables from documents
    rebuild_fts_from_documents()  # zero-arg in your repo
    typer.echo("FTS rebuilt")


@app.command()
def status() -> None:
    """
    Show key counts and top hosts/doc types.
    """
    eng = get_engine()
    with eng.begin() as conn:
        discovered = conn.exec_driver_sql("SELECT COUNT(1) FROM documents").scalar() or 0
        hydrated = conn.exec_driver_sql(
            "SELECT COUNT(1) FROM documents WHERE COALESCE(body,'') <> ''"
        ).scalar() or 0
        fts_rows = conn.exec_driver_sql("SELECT COUNT(1) FROM documents_fts").scalar() or 0

        typer.echo("=== Compliance-OS Status ===")
        typer.echo(f"discovered (documents): {discovered}")
        typer.echo(f"fts rows:               {fts_rows}")
        typer.echo(f"hydrated (body != ''):  {hydrated}")
        typer.echo(f"empty bodies:           {discovered - hydrated}")

        # Top hosts
        top_hosts = _safe_top_hosts(conn, limit=15)
        typer.echo("\nTop hosts:")
        for r in top_hosts:
            host = getattr(r, "host", None) or getattr(r, "platform", None) or "unknown"
            typer.echo(f" - {host}: {r.c}")

        # Doc types
        typer.echo("\nDoc types:")
        doc_types = conn.exec_driver_sql(
            """
            SELECT COALESCE(NULLIF(doc_type,''), '') AS t, COUNT(1) AS c
            FROM documents
            GROUP BY t
            ORDER BY c DESC
            """
        ).fetchall()
        for r in doc_types:
            label = getattr(r, "t", "") or ""
            typer.echo(f" - {label}: {r.c}")


@app.command()
def search(query: str, clean: bool = typer.Option(False, "--clean"), limit: int = 20) -> None:
    """
    Full-text search (FTS). By default searches raw HTML body index.
    Use --clean to query the cleaned-text index.
    """
    eng = get_engine()
    table = "documents_clean_fts" if clean else "documents_fts"

    sql = f"""
        SELECT d.url,
               COALESCE(NULLIF(d.title,''), d.url) AS title,
               snippet({table}, 2, '[', ']', ' … ', 10) AS snip
        FROM {table} AS f
        JOIN documents AS d ON d.url = f.url
        WHERE {table} MATCH ?
        LIMIT ?
    """

    with eng.begin() as conn:
        rows = conn.exec_driver_sql(sql, (query, limit)).fetchall()

    if not rows:
        typer.echo("No results.")
        return

    for r in rows:
        snip = getattr(r, "snip", "") or ""
        if len(snip) > 400:
            snip = snip[:400] + "…"
        typer.echo(f"- {r.url}\n  {snip}\n")


# -----------------------
# Hydration wrapper (optional)
# -----------------------
@app.command("hydrate")
def hydrate_cmd(
    contains: Optional[str] = typer.Option(None, "--contains", help="Only URLs containing this substring"),
    limit: int = typer.Option(200, "--limit", help="Max URLs to attempt"),
    pause: float = typer.Option(0.2, "--pause", help="Seconds between requests"),
    changed_only: bool = typer.Option(
        False,
        "--changed-only",
        help="Skip URLs whose body already exists unless the source changed",
    ),
) -> None:
    """
    Fetch pages and store their HTML into documents.body (respecting robots/4xx).
    """
    if hydrate_runner is None:
        typer.echo("Hydrate tool not available in this build.")
        raise typer.Exit(code=1)

    hydrated_count = hydrate_runner(
        contains=contains,
        limit=limit,
        pause=pause,
        changed_only=changed_only,
    )
    typer.echo(f"Hydrated {hydrated_count} document(s).")


# -----------------------
# Monitoring / Debug UX
# -----------------------
@app.command()
def peek(limit: int = 15) -> None:
    """
    Show the most recently hydrated docs and the most recent errors (one-shot).
    """
    eng = get_engine()
    with eng.begin() as conn:
        hydrated = conn.exec_driver_sql(
            """
            SELECT url,
                   COALESCE(length(body),0) AS body_len,
                   COALESCE(datetime(fetched_at), '') AS fetched_at
            FROM documents
            WHERE COALESCE(body,'') <> ''
            ORDER BY COALESCE(fetched_at, 0) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        errors = conn.exec_driver_sql(
            """
            SELECT url,
                   COALESCE(last_error, '') AS last_error,
                   COALESCE(retry_count, 0) AS retry_count
            FROM documents
            WHERE COALESCE(last_error,'') <> ''
            ORDER BY retry_count DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    typer.echo("=== Peek: recent hydrated ===")
    if not hydrated:
        typer.echo("(none yet)")
    else:
        for r in hydrated:
            typer.echo(f"- {r.url}  (len={r.body_len}, fetched_at={r.fetched_at})")

    typer.echo("\n=== Peek: recent errors ===")
    if not errors:
        typer.echo("(none)")
    else:
        for r in errors:
            msg = (getattr(r, "last_error", "") or "").replace("\n", " ")
            if len(msg) > 160:
                msg = msg[:160] + "…"
            typer.echo(f"- {r.url}  [retries={r.retry_count}]  {msg}")


@app.command()
def watch(interval: float = 5.0, show: int = 10) -> None:
    """
    Live dashboard: totals + recent hydrated + recent errors. Ctrl+C to stop.
    """
    try:
        while True:
            _render_dashboard(show)
            time.sleep(interval)
    except KeyboardInterrupt:
        typer.echo("\nStopped watch.")


def _render_dashboard(show: int) -> None:
    eng = get_engine()
    with eng.begin() as conn:
        discovered = conn.exec_driver_sql("SELECT COUNT(1) FROM documents").scalar() or 0
        hydrated = conn.exec_driver_sql(
            "SELECT COUNT(1) FROM documents WHERE COALESCE(body,'') <> ''"
        ).scalar() or 0
        fts_rows = conn.exec_driver_sql("SELECT COUNT(1) FROM documents_fts").scalar() or 0

        top_hosts = _safe_top_hosts(conn, limit=10)

        hydrated_rows = conn.exec_driver_sql(
            """
            SELECT url,
                   COALESCE(length(body),0) AS body_len,
                   COALESCE(datetime(fetched_at), '') AS fetched_at
            FROM documents
            WHERE COALESCE(body,'') <> ''
            ORDER BY COALESCE(fetched_at, 0) DESC
            LIMIT ?
            """,
            (show,),
        ).fetchall()

        error_rows = conn.exec_driver_sql(
            """
            SELECT url,
                   COALESCE(last_error, '') AS last_error,
                   COALESCE(retry_count, 0) AS retry_count
            FROM documents
            WHERE COALESCE(last_error,'') <> ''
            ORDER BY retry_count DESC
            LIMIT ?
            """,
            (show,),
        ).fetchall()

    # Clear-ish screen without external deps
    typer.echo("\033[2J\033[H", nl=False)
    typer.echo("=== Compliance-OS Watch ===")
    typer.echo(f"discovered: {discovered:,} | hydrated: {hydrated:,} | fts_rows: {fts_rows:,}")
    typer.echo("\nTop hosts:")
    if top_hosts:
        for r in top_hosts:
            host = getattr(r, "host", None) or getattr(r, "platform", None) or "unknown"
            typer.echo(f" - {host}: {r.c}")
    else:
        typer.echo(" - (none)")

    typer.echo("\nRecent hydrated:")
    if hydrated_rows:
        for r in hydrated_rows:
            typer.echo(f" - {r.url}  (len={r.body_len}, fetched_at={r.fetched_at})")
    else:
        typer.echo(" - (none)")

    typer.echo("\nRecent errors:")
    if error_rows:
        for r in error_rows:
            msg = (getattr(r, "last_error", "") or "").replace("\n", " ")
            if len(msg) > 120:
                msg = msg[:120] + "…"
            typer.echo(f" - {r.url} [retries={r.retry_count}] {msg}")
    else:
        typer.echo(" - (none)")


@app.command("debug-fts")
def debug_fts(query: str = "privacy") -> None:
    """
    Sanity check on both FTS tables for a query string.
    Shows up to 5 sample hits per index.
    """
    eng = get_engine()

    def _run(sql: str, params: tuple[str, int]) -> list:
        with eng.begin() as conn:
            return conn.exec_driver_sql(sql, params).fetchall()

    typer.echo(f"== documents_fts MATCH {query!r} ==")
    rows = _run(
        """
        SELECT d.url,
               COALESCE(NULLIF(d.title,''), d.url) AS title
        FROM documents_fts AS f
        JOIN documents AS d ON d.url = f.url
        WHERE documents_fts MATCH ?
        LIMIT ?
        """,
        (query, 5),
    )
    if not rows:
        typer.echo("(no matches)")
    else:
        for r in rows:
            typer.echo(f"- {r.url} | {r.title}")

    typer.echo(f"\n== documents_clean_fts MATCH {query!r} ==")
    rows2 = _run(
        """
        SELECT d.url,
               COALESCE(NULLIF(d.title,''), d.url) AS title,
               snippet(documents_clean_fts, 2, '[', ']', ' … ', 10) AS snip
        FROM documents_clean_fts AS f
        JOIN documents AS d ON d.url = f.url
        WHERE documents_clean_fts MATCH ?
        LIMIT ?
        """,
        (query, 5),
    )
    if not rows2:
        typer.echo("(no matches)")
    else:
        for r in rows2:
            snip = getattr(r, "snip", "") or ""
            if len(snip) > 140:
                snip = snip[:140] + "…"
            typer.echo(f"- {r.url} | {snip}")


# -----------------------
# Entrypoint
# -----------------------
if __name__ == "__main__":
    app()
