#!/usr/bin/env python
import os, sqlite3

DB = os.path.join(os.getcwd(), "compliance.db")

def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()

    # Drop FTS table and its shadow tables if present
    drops = [
        "document_chunks_fts",
        "document_chunks_fts_config",
        "document_chunks_fts_content",
        "document_chunks_fts_data",
        "document_chunks_fts_docsize",
        "document_chunks_fts_idx",
    ]
    for t in drops:
        try:
            cur.execute(f"DROP TABLE IF EXISTS {t}")
        except Exception as e:
            print(f"[warn] drop {t}: {e}")

    # Recreate a fresh FTS
    cur.execute("""
        CREATE VIRTUAL TABLE document_chunks_fts
        USING fts5 (
            url,
            chunk_text,
            tokenize = 'porter'
        )
    """)

    con.commit()
    con.close()
    print("[done] document_chunks_fts reset.")

if __name__ == "__main__":
    main()
