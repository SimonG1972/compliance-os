from __future__ import annotations

import os
from typing import Union

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from .log import info

# --- DB bootstrap ---
ROOT = os.path.dirname(os.path.dirname(__file__))  # repo root
DB_PATH = os.path.join(ROOT, "compliance.db")

# future=True makes 2.x style API explicit
_engine: Engine = create_engine(f"sqlite:///{DB_PATH}", future=True)

# session factory bound to our engine; we can also create one bound to any Engine passed in
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False, future=True)

info(f"DB ready at {DB_PATH}")


def get_engine() -> Engine:
    """Expose the process-wide engine (used by discovery shim)."""
    return _engine


def get_session() -> Session:
    """Open a new Session on the default engine."""
    return SessionLocal()


# --- schema bootstrap (minimal; no-op if table already exists) ---
def _ensure_schema() -> None:
    with _engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS documents (
                  url TEXT PRIMARY KEY,
                  source_name TEXT,
                  platform_or_regulator TEXT,
                  doc_type TEXT,
                  jurisdiction TEXT,
                  volatility TEXT
                )
                """
            )
        )


_ensure_schema()


def _upsert_with_session(session: Session, doc: dict) -> None:
    """
    Perform the UPSERT using a real Session (works under SQLAlchemy 2.x).
    """
    session.execute(
        text(
            """
            INSERT INTO documents(
              url, source_name, platform_or_regulator, doc_type, jurisdiction, volatility
            ) VALUES (
              :url, :source_name, :platform_or_regulator, :doc_type, :jurisdiction, :volatility
            )
            ON CONFLICT(url) DO UPDATE SET
              source_name = excluded.source_name,
              platform_or_regulator = excluded.platform_or_regulator,
              doc_type = excluded.doc_type,
              jurisdiction = excluded.jurisdiction,
              volatility = excluded.volatility
            """
        ),
        doc,
    )
    session.commit()


def upsert_document(session_or_engine: Union[Session, Engine], doc: dict) -> None:
    """
    Backwards/forwards compatible entrypoint:

    - If given a Session: use it directly
    - If given an Engine: open a Session and run the upsert
    """
    if isinstance(session_or_engine, Session):
        _upsert_with_session(session_or_engine, doc)
        return

    if isinstance(session_or_engine, Engine):
        Local = sessionmaker(bind=session_or_engine, autoflush=False, autocommit=False, future=True)
        with Local() as session:
            _upsert_with_session(session, doc)
        return

    raise TypeError("upsert_document expects a SQLAlchemy Session or Engine as the first argument")
