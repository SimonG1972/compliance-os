# src/ui/server.py
import os
import sys
import json
import time
import glob
import shlex
import sqlite3
import subprocess
import re
from typing import Dict, Any, List, Tuple

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

# ---------- Paths & constants ----------

HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
SCRIPTS_DIR = os.path.join(ROOT, "scripts")
DB_PATH = os.path.join(ROOT, "compliance.db")
PIPELINE_JSON = os.path.join(ROOT, "pipeline.json")
SEEDS_PATH = os.path.join(ROOT, "config", "seeds.json")  # NEW

SECTION_META = {
    "Discovery & Hydration": {"key": "discover", "color": "#1864ab"},
    "Chunking & Indexing":   {"key": "chunk",    "color": "#2b8a3e"},
    "Search & Synthesis":    {"key": "search",   "color": "#5c7cfa"},
    "Tagging & Metadata":    {"key": "tag",      "color": "#e8590c"},
    "Reports & Exports":     {"key": "report",   "color": "#0b7285"},
    "Maintenance":           {"key": "maint",    "color": "#c92a2a"},
}

SCRIPT_DESCRIPTIONS = {
    "discover.py": "Crawl a platform/domain for new URLs from seeds/keywords.",
    "hydrate_smart.py": "Fetch & refresh HTML/text for discovered URLs (JS fallbacks).",
    "check_changes.py": "List recent content changes for tracked URLs.",
    "detect_changes.py": "Diff revisions and flag material changes.",
    "auto_tag_chunks.py": "Auto-apply tags to chunks via regex rules.",
    "search_by_tag.py": "Search chunks by one or more tags.",
    "chunk_docs.py": "Split documents into chunks (paragraph/sentence).",
    "chunk_documents.py": "Chunk raw docs with min/max boundaries.",
    "rebuild_chunks_fts.py": "Rebuild FTS5 index for chunks.",
    "repair_chunks_fts.py": "Repair FTS index when counts drift.",
    "build_vectors.py": "Create embeddings for chunks (if ST installed).",
    "hybrid_search.py": "Hybrid (FTS + embeddings) search with re-rank.",
    "search_chunks.py": "Full-text search over chunks.",
    "search_chunks_counts.py": "Counts per term/category.",
    "search_chunks_export.py": "Export search results to CSV/JSON.",
    "answer_synth.py": "Synthesize sourced answers from top passages.",
    "report.py": "Generate compliance summaries and metrics.",
    "hybrid_export.py": "Export hybrid search results.",
    "db_check.py": "PRAGMA integrity + row counts.",
    "db_repair.py": "Copy/backup, vacuum, reindex, validate.",
    "migrate_revisions.py": "Backfill/migrate document_revisions.",
    "migrate_simhash.py": "Build/migrate chunk simhashes.",
    "init_chunks.py": "Initialize chunk tables.",
    "init_tags.py": "Initialize tag tables.",
    "init_upgrade_pack.py": "Apply schema/data upgrade pack.",
    "init_revisions.py": "Initialize revisions table.",
    "simulate_changes.py": "Inject fake changes for testing.",
    "dashboard.py": "Flask dashboard (requires Flask).",
    "chunks_status.py": "Quick counts of docs/chunks/FTS/URLs.",
    "build_pipeline.py": "Build pipeline.json from scripts.",
    "alert_changes.py": "Email alert for changes in last X hours.",
    "text_clean.py": "Run the HTML/text cleaner pipeline.",
}

SECTION_RULES: List[Tuple[str, str]] = [
    (r"discover|hydrate", "Discovery & Hydration"),
    (r"chunk",            "Chunking & Indexing"),
    (r"search|answer|hybrid", "Search & Synthesis"),
    (r"tag",              "Tagging & Metadata"),
    (r"report|export",    "Reports & Exports"),
    (r"db_|migrate|repair|init|simulate|dashboard", "Maintenance"),
]

# ---------- FastAPI ----------

app = FastAPI(title="Compliance-OS Command Center")
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Helpers ----------

def read_pipeline_file() -> List[Dict[str, Any]]:
    if not os.path.exists(PIPELINE_JSON):
        return []
    try:
        with open(PIPELINE_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    if isinstance(data, dict) and isinstance(data.get("scripts"), list):
        return data["scripts"]
    if isinstance(data, list):
        return data
    return []

def scan_scripts_dir() -> List[Dict[str, Any]]:
    out = []
    for p in sorted(glob.glob(os.path.join(SCRIPTS_DIR, "*.py"))):
        name = os.path.basename(p)
        if name == "__init__.py":
            continue
        out.append({"name": name, "path": p})
    return out

def guess_section(name: str) -> str:
    low = name.lower()
    for pat, sec in SECTION_RULES:
        if re.search(pat, low):
            return sec
    return "Maintenance"

def merge_scripts() -> List[Dict[str, Any]]:
    base = {s["name"]: s for s in read_pipeline_file() if "name" in s and "path" in s}
    for s in scan_scripts_dir():
        base.setdefault(s["name"], s)
    merged = []
    for name, s in sorted(base.items()):
        s = dict(s)
        s["display_name"] = s.get("display_name", name)
        s["description"] = s.get("description") or SCRIPT_DESCRIPTIONS.get(name, "")
        s["section"] = s.get("section") or guess_section(name)
        merged.append(s)
    return merged

def group_by_section(scripts: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for s in scripts:
        groups.setdefault(s["section"], []).append(s)
    for k in groups:
        groups[k].sort(key=lambda x: x["name"])
    return groups

def quick_db_summary() -> Dict[str, Any]:
    exists = os.path.exists(DB_PATH)
    size = os.path.getsize(DB_PATH) if exists else None
    mtime = os.path.getmtime(DB_PATH) if exists else None
    return {
        "path": DB_PATH,
        "exists": exists,
        "size": f"{(size or 0)/1024/1024/1024:.1f} GB" if size else "…",
        "modified": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mtime)) if mtime else "…",
    }

def deep_db_stats() -> Dict[str, Any]:
    out = quick_db_summary()
    if not out["exists"]:
        return {**out, "integrity": "missing", "counts": {}, "top_tags": [], "verticals": []}
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    try:
        integrity = cur.execute("PRAGMA integrity_check;").fetchone()[0]
    except Exception as e:
        integrity = f"ERR({e.__class__.__name__})"
    counts = {}
    for t in ["documents","document_chunks","document_chunks_fts","chunk_tags","document_revisions"]:
        try:
            counts[t] = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception:
            counts[t] = None
    try:
        counts["urls_chunked"] = cur.execute("SELECT COUNT(DISTINCT url) FROM document_chunks").fetchone()[0]
    except Exception:
        counts["urls_chunked"] = None
    try:
        top_tags = cur.execute("""
            SELECT tag, COUNT(*) c FROM chunk_tags
            GROUP BY tag
            ORDER BY c DESC
            LIMIT 8
        """).fetchall()
    except Exception:
        top_tags = []
    try:
        verticals = cur.execute("""
            SELECT REPLACE(tag,'vertical:','') AS v, COUNT(*) c
            FROM chunk_tags
            WHERE tag LIKE 'vertical:%'
            GROUP BY v
            ORDER BY c DESC
            LIMIT 8
        """).fetchall()
    except Exception:
        verticals = []
    con.close()
    return {
        **out,
        "integrity": integrity,
        "counts": counts,
        "top_tags": [{"tag": t, "count": c} for t,c in top_tags],
        "verticals": [{"vertical": v, "count": c} for v,c in verticals],
    }

def run_help(path: str, timeout: int = 12) -> Tuple[str, int]:
    if not os.path.exists(path):
        return ("", 127)
    cmd = [sys.executable, path, "--help"]
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=ROOT,
            timeout=timeout,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        return (proc.stdout or "", proc.returncode)
    except subprocess.TimeoutExpired:
        return ("(help timed out)", 124)
    except Exception as e:
        return (f"(help failed: {e})", 1)

POS_LINE = re.compile(r"^\s{0,6}([\w\-\.\:\/]+)\s{2,}(.*)$")

def parse_help_fields(help_text: str) -> Dict[str, List[Dict[str, Any]]]:
    positionals: List[Dict[str, Any]] = []
    options: List[Dict[str, Any]] = []
    lines = help_text.splitlines()
    block = None
    for raw in lines:
        s = raw.rstrip("\n")
        st = s.strip().lower()
        if st.startswith("positional arguments") or st.startswith("arguments") or st.startswith("positional:"):
            block = "pos"; continue
        if st.startswith("options") or st.startswith("optional arguments"):
            block = "opt"; continue
        if block == "pos":
            m = POS_LINE.match(s)
            if m:
                name, desc = m.groups()
                if name in ("options:", "optional", "-h", "--help"):
                    continue
                positionals.append({
                    "name": name.strip(),
                    "help": (desc or "").strip(),
                    "placeholder": (desc or "").split()[0] if desc else "",
                    "input_type": "text",
                })
        elif block == "opt":
            ss = s.strip()
            if not ss.startswith("-"):
                continue
            parts = re.split(r"\s{2,}", ss, maxsplit=1)
            flag_part = parts[0]
            desc = parts[1] if len(parts) > 1 else ""
            flags = [f.strip() for f in flag_part.split(",")]
            long = max(flags, key=len)
            toks = long.split()
            flag = toks[0]
            metavar = toks[1] if len(toks) > 1 and re.match(r"^[A-Z\[\]\w\-\._]+$", toks[1]) else None
            name = flag.lstrip("-").replace("-", "_")
            is_flag = (metavar is None)
            input_type = "text"
            hint = (metavar or "") + " " + (desc or "")
            if is_flag:
                input_type = "checkbox"
            else:
                if re.search(r"(INT|NUM|COUNT|LIMIT|HOURS|DAYS|MAX|N)\b", hint.upper()):
                    input_type = "number"
            options.append({
                "flag": flag, "name": name, "is_flag": is_flag, "help": desc.strip(),
                "placeholder": (metavar or ""), "input_type": input_type,
            })
    return {"positionals": positionals, "options": options}

def build_command(path: str, args: Dict[str, Any], positionals: List[str]) -> List[str]:
    cmd = [sys.executable, path]
    for p in positionals:
        if p is None or str(p).strip()=="":
            continue
        cmd.append(str(p))
    for k, v in args.items():
        if v in [None, ""]:
            continue
        flag = "--" + k.replace("_", "-")
        if isinstance(v, bool):
            if v:
                cmd.append(flag)
        else:
            if isinstance(v, (list, tuple)):
                for item in v:
                    cmd.extend([flag, str(item)])
            else:
                cmd.extend([flag, str(v)])
    return cmd

# ---------- Seeds support (no script changes needed) ----------

DEFAULT_SEEDS = {
    "social media": [
        "https://www.facebook.com", "https://www.instagram.com",
        "https://www.tiktok.com", "https://www.snap.com",
        "https://www.youtube.com", "https://x.com"
    ],
    "finance": [
        "https://www.chase.com", "https://www.bankofamerica.com",
        "https://www.citi.com", "https://www.wellsfargo.com"
    ],
    "healthcare": [
        "https://www.hhs.gov", "https://www.cdc.gov",
        "https://www.nih.gov"
    ],
    "ads": [
        "https://ads.google.com", "https://business.facebook.com",
        "https://advertising.twitter.com"
    ],
    "kids": [
        "https://www.nick.com", "https://www.disneyplus.com",
        "https://www.roblox.com"
    ]
}

def load_seeds() -> Dict[str, List[str]]:
    if os.path.exists(SEEDS_PATH):
        try:
            with open(SEEDS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    # normalize lists
                    norm = {}
                    for k, v in data.items():
                        if isinstance(v, list):
                            norm[k.lower()] = v
                    return norm
        except Exception:
            pass
    return {k.lower(): v for k, v in DEFAULT_SEEDS.items()}

# ---------- API ----------

@app.get("/api/meta")
def api_meta():
    scripts = merge_scripts()
    groups = group_by_section(scripts)
    sections = []
    for title in SECTION_META:
        meta = SECTION_META[title]
        items = [{
            "name": it["name"],
            "display_name": it.get("display_name", it["name"]),
            "path": it["path"],
            "description": it.get("description", ""),
        } for it in groups.get(title, [])]
        sections.append({"title": title, "key": meta["key"], "color": meta["color"], "items": items})
    return {"db": quick_db_summary(), "sections": sections}

@app.get("/api/db/deep_stats")
def api_db_deep():
    return deep_db_stats()

@app.get("/api/help")
def api_help(path: str):
    text, rc = run_help(path)
    fields = parse_help_fields(text)
    return {"returncode": rc, "help_text": text, **fields}

@app.get("/api/seeds")  # NEW
def api_seeds(q: str | None = None):
    seeds = load_seeds()
    if not q:
        return {"seeds": seeds}
    ql = q.lower().strip()
    matches = {}
    for k, roots in seeds.items():
        if ql in k or k in ql:
            matches[k] = roots
    return {"seeds": matches}

@app.post("/api/run")
def api_run(payload: Dict[str, Any]):
    path = payload.get("path")
    if not path or not os.path.exists(path):
        raise HTTPException(400, "Invalid script path")
    args = payload.get("args") or {}
    positionals = payload.get("positionals") or []
    cmd = build_command(path, args, positionals)
    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
        )
        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "command": " ".join(shlex.quote(c) for c in cmd),
            "duration_sec": round(time.time() - t0, 3),
            "output": proc.stdout or "",
        }
    except Exception as e:
        return {
            "ok": False, "returncode": 1,
            "command": " ".join(shlex.quote(c) for c in cmd),
            "duration_sec": round(time.time() - t0, 3),
            "output": f"Execution failed: {e}",
        }

# ---------- UI ----------

INDEX_HTML = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>Compliance-OS Command Center</title>
<style>
  :root{
    --bg:#f8f9fa; --panel:#ffffff; --ink:#1c1c1c; --sub:#5f6b7a; --muted:#95a1af; --line:#e9ecef;
    --accent:#1864ab; --radius:14px;
  }
  *{box-sizing:border-box} html,body{height:100%}
  body{margin:0; background:var(--bg); color:var(--ink); font:15px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial}
  .header{position:sticky; top:0; z-index:10; background:linear-gradient(180deg,#ffffff,#f7f8fb); border-bottom:1px solid var(--line)}
  .wrap{max-width:1200px; margin:0 auto; padding:18px 20px}
  .title{font-size:22px; font-weight:800; letter-spacing:.2px}
  .chips{display:flex; gap:10px; flex-wrap:wrap; margin-top:8px; align-items:center}
  .chip{display:inline-flex; align-items:center; gap:8px; padding:6px 10px; border:1px solid var(--line); border-radius:999px; background:#fff; color:var(--sub); font-size:12px}
  .chip b{color:var(--ink)}
  .btn{appearance:none; border:1px solid var(--line); background:#fff; color:var(--ink); padding:8px 12px; border-radius:10px; cursor:pointer; font-weight:600; transition:all .15s ease; box-shadow:0 1px 0 rgba(0,0,0,.02)}
  .btn:hover{transform:translateY(-1px); box-shadow:0 3px 12px rgba(0,0,0,.06)} .btn.primary{background:var(--accent); color:#fff; border-color:var(--accent)}
  .panel{background:var(--panel); border:1px solid var(--line); border-radius:var(--radius); padding:14px; box-shadow:0 1px 2px rgba(0,0,0,.03)}
  .tabs{display:flex; gap:8px; flex-wrap:wrap; margin:18px 0 8px}
  .tab{padding:8px 12px; border-radius:999px; border:1px solid var(--line); background:#fff; cursor:pointer; font-weight:600; color:var(--sub)}
  .tab.active{background:#0b7285; border-color:#0b7285; color:#fff}
  .sections{display:none} .sections.active{display:block}
  .grid{display:grid; grid-template-columns:repeat(2, minmax(320px,1fr)); gap:14px} @media (max-width:900px){.grid{grid-template-columns:1fr}}
  .card{background:var(--panel); border:1px solid var(--line); border-radius:var(--radius); padding:14px; display:flex; gap:12px; box-shadow:0 1px 2px rgba(0,0,0,.03)}
  .swatch{width:8px; border-radius:999px}
  .card h3{margin:0 0 6px; font-size:15px}
  .card .path{color:var(--muted); font-size:12px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap}
  .desc{color:var(--sub); font-size:13px; margin:6px 0 10px}
  .row{display:flex; gap:8px; align-items:center; flex-wrap:wrap}
  .pill{font-size:11px; color:#fff; padding:4px 8px; border-radius:999px}
  .form{border:1px dashed var(--line); border-radius:10px; padding:10px; margin-top:8px; background:#fcfcfd}
  .field{display:flex; flex-direction:column; gap:6px; margin:8px 0}
  .field label{font-weight:700; font-size:12px}
  .field input, .field textarea, .field select{padding:8px 10px; border:1px solid var(--line); border-radius:8px; background:#fff; font-size:14px; color:var(--ink)}
  .helptext{white-space:pre-wrap; background:#f8f9fb; border:1px solid var(--line); padding:8px; border-radius:8px; max-height:180px; overflow:auto; font-size:12px}
  .console{background:#0a0d12; color:#d3e2ff; border-radius:12px; padding:14px; font-family:ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; border:1px solid #0f141d}
  .console pre{white-space:pre-wrap; margin:0}
</style>
</head>
<body>
  <div class="header">
    <div class="wrap">
      <div class="title">Compliance-OS Command Center</div>
      <div id="chips" class="chips">
        <div class="chip">DB: <b>…</b></div>
        <div class="chip">Integrity: <b>checking…</b></div>
        <div class="chip">Size: <b>…</b></div>
        <button id="btnDeep" class="btn">Load deep DB stats</button>
        <span style="color:#6b7280;font-size:12px">Counts run on demand so the page is instant.</span>
      </div>
    </div>
  </div>

  <div class="wrap" style="margin-top:10px">
    <div class="panel">
      <div id="tabs" class="tabs"></div>
      <div id="sections"></div>
    </div>

    <div style="height:18px"></div>

    <div class="panel">
      <div style="font-weight:800;margin-bottom:8px">Live Output</div>
      <div id="console" class="console"><pre class="muted">Ready. Select a command above, configure inputs, and press Run.</pre></div>
    </div>
  </div>

<script>
const state = { meta:null, activeKey:null };

function esc(s){ return (s||"").replace(/[&<>"]/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }
function setConsole(t, muted=false){ document.querySelector("#console").innerHTML = "<pre"+(muted?' class=\"muted\"':'')+">"+esc(t)+"</pre>"; }
async function getJSON(u){ const r=await fetch(u); if(!r.ok) throw new Error("HTTP "+r.status); return await r.json(); }
async function postJSON(u, d){ const r=await fetch(u,{method:"POST",headers:{'Content-Type':'application/json'},body:JSON.stringify(d)}); if(!r.ok) throw new Error("HTTP "+r.status); return await r.json(); }

function renderChipsDeep(deep){
  const chips = document.querySelector("#chips");
  const tagStr = (deep.top_tags||[]).map(t => `${esc(t.tag)} (${t.count})`).slice(0,5).join(", ");
  const vertStr = (deep.verticals||[]).map(v => `${esc(v.vertical)} (${v.count})`).slice(0,5).join(", ");
  chips.innerHTML = `
    <div class="chip">DB: <b>${esc(deep.path||'...')}</b></div>
    <div class="chip">Integrity: <b>${esc(deep.integrity||'...')}</b></div>
    <div class="chip">Size: <b>${esc(state.meta?.db?.size||'...')}</b></div>
    <div class="chip">Docs: <b>${esc(String(deep.counts?.documents??'…'))}</b></div>
    <div class="chip">Chunks: <b>${esc(String(deep.counts?.document_chunks??'…'))}</b></div>
    <div class="chip">FTS: <b>${esc(String(deep.counts?.document_chunks_fts??'…'))}</b></div>
    <div class="chip">URLs: <b>${esc(String(deep.counts?.urls_chunked??'…'))}</b></div>
    <div class="chip">Top tags: <b>${esc(tagStr || '—')}</b></div>
    <div class="chip">Verticals: <b>${esc(vertStr || '—')}</b></div>
    <button id="btnDeep" class="btn">Reload</button>
    <span style="color:#6b7280;font-size:12px">Counts reflect current DB.</span>
  `;
  document.querySelector("#btnDeep").onclick = async ()=>{
    try{ const d = await getJSON("/api/db/deep_stats"); renderChipsDeep(d); setConsole("Deep stats reloaded.\n\n"+JSON.stringify(d,null,2), true); }
    catch(e){ setConsole("Failed to load deep stats: "+e, false); }
  };
}

function renderTabs(meta){
  const tabs = document.querySelector("#tabs");
  tabs.innerHTML = meta.sections.map((sec,i)=>{
    return `<button class="tab ${i===0?'active':''}" data-key="${esc(sec.key)}">${esc(sec.title)}</button>`;
  }).join("");
  state.activeKey = meta.sections[0]?.key || null;
  tabs.querySelectorAll(".tab").forEach(btn=>{
    btn.onclick = ()=>{
      state.activeKey = btn.getAttribute("data-key");
      tabs.querySelectorAll(".tab").forEach(b=>b.classList.remove("active"));
      btn.classList.add("active");
      renderSections(state.meta);
    };
  });
}

function buildFieldHtml(fid, f, positional=false){
  const baseAttr = `data-argname="${esc(f.name)}"${positional?' data-positional="1"':''}`;
  if(f.input_type==="checkbox"){
    return `<div class="field"><label><input type="checkbox" ${baseAttr}/> ${esc(f.flag)} ${f.help?`— ${esc(f.help)}`:''}</label></div>`;
  }else{
    return `<div class="field">
      <label>${positional?esc(f.name):esc(f.flag)} ${f.placeholder?`<span style="color:#6b7280">(${esc(f.placeholder)})</span>`:''}</label>
      <input type="${f.input_type||'text'}" placeholder="${esc(f.placeholder||'')}" ${baseAttr}/>
      ${f.help?`<div style="color:#6b7280;font-size:12px">${esc(f.help)}</div>`:''}
    </div>`;
  }
}

function ensureDiscoverKeywordUI(formEl, path){
  // add once
  if(formEl.querySelector("[data-keyword-ui]")) return;
  const html = `
    <div class="field" data-keyword-ui>
      <label>Keyword / Vertical (optional)</label>
      <input type="text" placeholder="e.g. finance, social media, healthcare" data-disc-keywords="1"/>
      <div style="color:#6b7280;font-size:12px">Toggle keyword mode to expand to seed roots from <code>config/seeds.json</code>.</div>
    </div>
    <div class="field" data-keyword-ui>
      <label><input type="checkbox" data-disc-keymode="1"/> Use keyword mode (expand to seeds & run discover.py per root)</label>
    </div>
  `;
  formEl.insertAdjacentHTML("afterbegin", html);
}

function renderSections(meta){
  const container = document.querySelector("#sections");
  const active = meta.sections.find(s=>s.key===state.activeKey) || meta.sections[0];
  const color = active.color;
  container.className = "sections active";
  container.innerHTML = `
    <div class="grid">
      ${active.items.map(item=>`
        <div class="card">
          <div class="swatch" style="background:${color}"></div>
          <div style="flex:1">
            <h3>${esc(item.name)}</h3>
            <div class="path">${esc(item.path)}</div>
            <div class="desc">${esc(item.description||'')}</div>
            <div class="row">
              <span class="pill" style="background:${color}">${esc(active.title)}</span>
              <button class="btn" data-help="${esc(item.path)}">Load inputs</button>
              <button class="btn primary" data-run="${esc(item.path)}">Run</button>
            </div>
            <div class="form" id="form_${btoa(item.path).replace(/=/g,'')}" data-path="${esc(item.path)}" style="display:none"></div>
            <div class="helptext" id="help_${btoa(item.path).replace(/=/g,'')}" style="display:none;margin-top:8px"></div>
          </div>
        </div>
      `).join("")}
    </div>`;

  // Load inputs
  container.querySelectorAll("button[data-help]").forEach(btn=>{
    btn.onclick = async ()=>{
      const path = btn.getAttribute("data-help");
      const fid = "form_"+btoa(path).replace(/=/g,'');
      const hid = "help_"+btoa(path).replace(/=/g,'');
      const form = document.getElementById(fid);
      const help = document.getElementById(hid);
      form.style.display = form.style.display==="none" ? "block" : "none";
      help.style.display = form.style.display;
      if(form.dataset.loaded==="1"){ return; }
      form.innerHTML = `<div style="color:#6b7280">Loading --help…</div>`;
      help.textContent = "";
      try{
        const res = await getJSON(`/api/help?path=${encodeURIComponent(path)}`);
        const pos = res.positionals||[];
        const opt = res.options||[];
        help.textContent = res.help_text||"";
        if(pos.length===0 && opt.length===0){
          form.innerHTML = `<div style="color:#6b7280">No structured options detected. You can still run with custom args below.</div>`;
        }else{
          form.innerHTML = pos.map(f=>buildFieldHtml(fid,f,true)).join("") +
                           opt.map(f=>buildFieldHtml(fid,f,false)).join("");
        }
        // Keyword mode only for discover.py
        if(path.toLowerCase().endsWith("discover.py")){
          ensureDiscoverKeywordUI(form, path);
        }
        form.innerHTML += `<div class="field"><label>Extra args (optional)</label><input data-freeform="1" type="text" placeholder='e.g. --max 200 --since "-2 days"'></div>`;
        form.dataset.loaded = "1";
      }catch(e){
        form.innerHTML = `<div style="color:#c92a2a">Failed to load help: ${esc(String(e))}</div>`;
      }
    };
  });

  // Run
  container.querySelectorAll("button[data-run]").forEach(btn=>{
    btn.onclick = async ()=>{
      const path = btn.getAttribute("data-run");
      const fid = "form_"+btoa(path).replace(/=/g,'');
      const form = document.getElementById(fid);
      const args = {};
      const positionals = [];
      const collectFromForm = ()=>{
        if(!form || form.style.display==="none") return;
        const fields = form.querySelectorAll("[data-argname]");
        fields.forEach(el=>{
          const name = el.getAttribute("data-argname");
          const isPos = el.hasAttribute("data-positional");
          if(el.type==="checkbox"){
            if(el.checked){
              if(isPos){ positionals.push(true); } else { args[name] = true; }
            }
          }else{
            if(el.value && el.value.trim()!==""){
              if(isPos){ positionals.push(el.value.trim()); } else { args[name] = el.value.trim(); }
            }
          }
        });
        const extra = form.querySelector("[data-freeform='1']");
        if(extra && extra.value){
          const toks = extra.value.match(/(?:[^\s"]+|"[^"]*")+/g) || [];
          for(let i=0;i<toks.length;i++){
            const t = toks[i];
            if(t.startsWith("--")){
              const nm = t.replace(/^--/,"").replace(/-/g,"_");
              const nx = toks[i+1];
              if(!nx || nx.startsWith("--")){ args[nm]=true; }
              else{ args[nm]=nx.replace(/^"(.*)"$/,"$1"); i++; }
            }else{
              positionals.push(t.replace(/^"(.*)"$/,"$1"));
            }
          }
        }
      };
      collectFromForm();

      // Keyword mode hook for discover.py
      const isDiscover = path.toLowerCase().endsWith("discover.py");
      const kwMode = form && form.querySelector("[data-disc-keymode='1']")?.checked;
      const kwText = form && form.querySelector("[data-disc-keywords='1']")?.value?.trim();

      if(isDiscover && kwMode && kwText){
        // Expand to seeds and run batch
        let seedsRes = {};
        try{ seedsRes = await getJSON(`/api/seeds?q=${encodeURIComponent(kwText)}`); }
        catch(e){ setConsole("Failed to load seeds: "+String(e)); return; }
        const seeds = seedsRes.seeds || {};
        // Accept comma-separated list and fuzzy match
        const wanted = kwText.split(",").map(s=>s.trim().toLowerCase()).filter(Boolean);
        let roots = new Set();
        for(const [k, arr] of Object.entries(seeds)){
          for(const w of wanted){
            if(k.includes(w) || w.includes(k)){ (arr||[]).forEach(u=>roots.add(u)); }
          }
        }
        // If any token looks like a URL, include it directly
        for(const w of wanted){
          if(/^https?:\/\//i.test(w) || w.includes(".")){ roots.add(w); }
        }
        const rootList = Array.from(roots);
        if(rootList.length===0){
          setConsole(`No seeds matched "${kwText}". Add entries in config/seeds.json or type a URL.`, false);
          return;
        }
        // Remove any positional 'root' we captured earlier; replace with each seed
        const posFiltered = positionals.filter(p=>!(typeof p==="string" && (p.startsWith("http://")||p.startsWith("https://")||p.includes("."))));
        setConsole(`Keyword mode: ${kwText}\n\nRunning discover.py for ${rootList.length} seed(s):\n`+rootList.map(r=>" - "+r).join("\n")+"\n\nPlease wait...");
        btn.disabled = true;
        let outputs = [];
        for(const root of rootList){
          try{
            const res = await postJSON("/api/run", {path, args, positionals: [root, ...posFiltered]});
            outputs.push(`${res.ok?'SUCCESS':'ERROR'} | exit ${res.returncode} | ${res.duration_sec}s\n\nCommand:\n${res.command}\n\nOutput:\n${res.output}`);
          }catch(e){
            outputs.push("Run failed for root "+root+": "+String(e));
          }
          setConsole(outputs.join("\n\n--------------------------\n\n"));
        }
        btn.disabled = false;
        return;
      }

      // Normal single-run path
      setConsole(`Running ${path}\n\nPositionals:\n${JSON.stringify(positionals,null,2)}\n\nArgs:\n${JSON.stringify(args,null,2)}\n\nPlease wait...`);
      btn.disabled = true;
      try{
        const res = await postJSON("/api/run", {path, args, positionals});
        const badge = res.ok ? "SUCCESS" : "ERROR";
        setConsole(`${badge} | exit ${res.returncode} | ${res.duration_sec}s\n\nCommand:\n${res.command}\n\nOutput:\n${res.output}`);
      }catch(e){ setConsole("Run failed: "+String(e)); }
      finally{ btn.disabled = false; }
    };
  });
}

async function boot(){
  try{
    const meta = await getJSON("/api/meta");
    state.meta = meta;
    renderTabs(meta);
    renderSections(meta);
    try{
      const deep = await getJSON("/api/db/deep_stats");
      const chips = document.querySelector("#chips");
      renderChipsDeep(deep);
      setConsole("Deep stats loaded.\n\n"+JSON.stringify(deep,null,2), true);
    }catch(e){
      const chips = document.querySelector("#chips");
      chips.querySelector("#btnDeep").onclick = async ()=>{
        try{ const deep = await getJSON("/api/db/deep_stats"); renderChipsDeep(deep); }
        catch(err){ setConsole("Failed to load deep stats: "+err); }
      };
    }
  }catch(e){
    setConsole("Failed to initialize UI: "+String(e));
  }
}
boot();
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def index(_: Request):
    return HTMLResponse(INDEX_HTML)

@app.get("/health")
def health():
    return {"ok": True}
