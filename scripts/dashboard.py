# scripts/dashboard.py
from flask import Flask, render_template_string
import sqlite3, os, datetime
from urllib.parse import urlparse
from collections import Counter

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(ROOT, "compliance.db")

TEMPLATE = """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Compliance-OS Dashboard</title>
    <meta http-equiv="refresh" content="5">
    <style>
      body { font-family: system-ui, Arial, sans-serif; margin: 24px; }
      h1 { margin: 0 0 12px 0; }
      .grid { display: grid; grid-template-columns: repeat(3, minmax(260px, 1fr)); gap: 16px; }
      .card { border: 1px solid #ddd; border-radius: 10px; padding: 14px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
      table { width: 100%; border-collapse: collapse; }
      th, td { text-align: left; padding: 6px 4px; border-bottom: 1px solid #eee; font-size: 14px; }
      small { color: #666; }
      .mono { font-family: ui-monospace, Menlo, Consolas, monospace; }
      .muted { color: #777; }
    </style>
  </head>
  <body>
    <h1>Compliance-OS Dashboard</h1>
    <small class="mono">DB: {{ db_path }} &middot; Updated: {{ now }}</small>

    <div class="grid" style="margin-top:16px;">
      <div class="card">
        <h3>Counts</h3>
        <table>
          <tr><td>Discovered (documents)</td><td class="mono">{{ discovered }}</td></tr>
          <tr><td>Hydrated (body != '')</td><td class="mono">{{ hydrated }}</td></tr>
          <tr><td>Empty bodies</td><td class="mono">{{ empty_bodies }}</td></tr>
          <tr><td>FTS rows</td><td class="mono">{{ fts_rows }}</td></tr>
        </table>
      </div>

      <div class="card">
        <h3>Top Hosts</h3>
        <table>
          <tr><th>Host</th><th>Count</th></tr>
          {% for host, c in top_hosts %}
            <tr><td class="mono">{{ host }}</td><td class="mono">{{ c }}</td></tr>
          {% endfor %}
          {% if not top_hosts %}
            <tr><td class="muted" colspan="2">No hosts yet.</td></tr>
          {% endif %}
        </table>
      </div>

      <div class="card">
        <h3>Doc Types</h3>
        <table>
          <tr><th>Type</th><th>Count</th></tr>
          {% for t, c in doc_types %}
            <tr><td class="mono">{{ t }}</td><td class="mono">{{ c }}</td></tr>
          {% endfor %}
          {% if not doc_types %}
            <tr><td class="muted" colspan="2">No doc types yet.</td></tr>
          {% endif %}
        </table>
      </div>
    </div>
  </body>
</html>
"""

def safe_count(cur, sql):
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0

def safe_rows(cur, sql, params=()):
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    except Exception:
        return []

def compute_top_hosts(cur, limit=15):
    # pull all URLs and aggregate hostnames in Python (no need for a 'platform' column)
    urls = safe_rows(cur, "SELECT url FROM documents WHERE COALESCE(url,'') <> ''")
    ctr = Counter()
    for (u,) in urls:
        try:
            host = urlparse(u).netloc or "unknown"
            ctr[host] += 1
        except Exception:
            ctr["unknown"] += 1
    return ctr.most_common(limit)

app = Flask(__name__)

@app.route("/")
def home():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    discovered = safe_count(cur, "SELECT COUNT(1) FROM documents")
    hydrated   = safe_count(cur, "SELECT COUNT(1) FROM documents WHERE COALESCE(body,'') <> ''")
    empty_bodies = discovered - hydrated
    fts_rows   = safe_count(cur, "SELECT COUNT(1) FROM documents_fts")

    top_hosts = compute_top_hosts(cur, limit=15)

    doc_types = safe_rows(cur, """
        SELECT COALESCE(NULLIF(doc_type,''), '') AS t, COUNT(1) AS c
        FROM documents
        GROUP BY t
        ORDER BY c DESC
    """)

    con.close()
    return render_template_string(
        TEMPLATE,
        db_path=DB_PATH,
        now=datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
        discovered=discovered,
        hydrated=hydrated,
        empty_bodies=empty_bodies,
        fts_rows=fts_rows,
        top_hosts=top_hosts,
        doc_types=doc_types,
    )

if __name__ == "__main__":
    print(f"Using DB: {DB_PATH}")
    # host=127.0.0.1, port=5000 — visit http://127.0.0.1:5000/
    app.run(host="127.0.0.1", port=5000, debug=False)
