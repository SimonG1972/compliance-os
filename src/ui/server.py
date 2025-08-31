import os
import sqlite3
from contextlib import closing
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

# Render (and many PaaS) provide PORT env var
PORT = int(os.environ.get("PORT", "10000"))
DB_PATH = os.environ.get("DB_PATH", "compliance.db")  # safe default

app = FastAPI(title="compliance-os UI", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def table_exists(con, name: str) -> bool:
    try:
        with closing(con.cursor()) as cur:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
            return cur.fetchone() is not None
    except Exception:
        return False

@app.get("/")
def root():
    return {"status": "ok", "service": "compliance-os", "port": PORT}

@app.get("/healthz")
def health():
    return PlainTextResponse("OK")

@app.get("/env")
def env():
    return {"DB_PATH": DB_PATH}

@app.get("/counts")
def counts():
    # Safe counts that don't crash if DB or tables are missing
    if not os.path.exists(DB_PATH):
        return JSONResponse({"db_exists": False, "documents": 0, "chunks": 0, "queue": 0})
    try:
        con = sqlite3.connect(DB_PATH)
        with closing(con.cursor()) as cur:
            docs = 0
            chunks = 0
            q = 0
            if table_exists(con, "documents"):
                docs = cur.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
            if table_exists(con, "chunks"):
                chunks = cur.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
            if table_exists(con, "discovery_queue"):
                q = cur.execute("SELECT COUNT(*) FROM discovery_queue WHERE status='pending'").fetchone()[0]
        con.close()
        return JSONResponse({"db_exists": True, "documents": docs, "chunks": chunks, "queue": q})
    except Exception as e:
        return JSONResponse({"db_exists": True, "error": str(e)})

if __name__ == "__main__":
    # Local/dev fallback; in Render we’ll run via gunicorn/uvicorn worker
    import uvicorn
    uvicorn.run("src.ui.server:app", host="0.0.0.0", port=PORT, reload=False)
