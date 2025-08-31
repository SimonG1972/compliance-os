#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Create lightweight views and a materialized 'host_stats' table for per-host QA.
No changes to existing scripts required.
"""
import os, sqlite3, time
DB = os.path.join(os.getcwd(), "compliance.db")

def run():
    con = sqlite3.connect(DB); c = con.cursor()

    # Host extraction helper (works for https://host/whatever)
    c.execute("""
    CREATE VIEW IF NOT EXISTS v_documents_basic AS
    SELECT
      id,
      url,
      lower(substr(url, instr(url, '://')+3, instr(substr(url, instr(url,'://')+3), '/')-1)) AS host,
      status_code,
      render_mode,
      length(COALESCE(body,'')) AS body_len,
      length(COALESCE(clean_text,'')) AS clean_len,
      last_error
    FROM documents
    """)

    # Discovery queue join (status = hydrated/pending)
    c.execute("""
    CREATE VIEW IF NOT EXISTS v_docs_with_queue AS
    SELECT d.*, q.status AS q_status
    FROM v_documents_basic d
    LEFT JOIN discovery_queue q ON q.url = d.url
    """)

    # Materialized host_stats (refresh each run)
    c.execute("DROP TABLE IF EXISTS host_stats")
    c.execute("""
    CREATE TABLE host_stats AS
    SELECT
      host,
      COUNT(*)                                 AS total_docs,
      SUM(CASE WHEN status_code IN (200,304) THEN 1 ELSE 0 END) AS ok_docs,
      SUM(CASE WHEN status_code NOT IN (200,304) OR status_code IS NULL THEN 1 ELSE 0 END) AS bad_docs,
      SUM(CASE WHEN render_mode='render' THEN 1 ELSE 0 END) AS render_docs,
      SUM(CASE WHEN body_len=0 THEN 1 ELSE 0 END) AS zero_body_docs,
      SUM(CASE WHEN clean_len=0 THEN 1 ELSE 0 END) AS zero_clean_docs,
      SUM(CASE WHEN COALESCE(last_error,'')!='' THEN 1 ELSE 0 END) AS errors
    FROM v_documents_basic
    GROUP BY host
    """)

    # Param anomalies (e.g., google/youtube localization params; trackers)
    c.execute("""
    CREATE VIEW IF NOT EXISTS v_url_params AS
    SELECT
      host, url,
      instr(url, '?hl=')>0           AS has_hl,
      instr(url, 'override_hl=')>0   AS has_override_hl,
      (url LIKE '%utm_%' OR url LIKE '%fbclid=%' OR url LIKE '%gclid=%') AS has_tracker
    FROM v_documents_basic
    """)

    con.commit(); con.close()
    print("[ensure_quality_views] views & host_stats ready.")

if __name__ == "__main__":
    run()
