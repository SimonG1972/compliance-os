#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate draft policy YAML suggestions per host based on observed anomalies.
NEW: --auto-promote-low-risk copies a *filtered* YAML (only safe normalizations)
     into config/policies/<host>.yml
"""
import os, sqlite3, json, argparse, shutil

ROOT = os.getcwd()
DB = os.path.join(ROOT, "compliance.db")
OUT_DIR = os.path.join(ROOT, "config", "policies", "_suggestions")
ACTIVE_DIR = os.path.join(ROOT, "config", "policies")
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(ACTIVE_DIR, exist_ok=True)

SAFE_PARAM_DROPS = {"hl", "override_hl", "utm_*", "fbclid", "gclid"}
SAFE_PATH_DENIES = {r"^/legal/open-source", r"^/legal/page/.*/open-source/"}

def fetch_stats():
    con = sqlite3.connect(DB); c = con.cursor()
    hosts = c.execute("""
      SELECT host, total_docs, ok_docs, bad_docs, render_docs, zero_body_docs, zero_clean_docs, errors
      FROM host_stats
    """).fetchall()
    params = dict(((h,),) + (c.execute("""
      SELECT SUM(has_hl), SUM(has_override_hl), SUM(has_tracker)
      FROM v_url_params WHERE host=?
    """,(h,)).fetchone() or (0,0,0,)) for h, *_ in hosts)
    con.close()
    return hosts, params

def suggest_for_host(host, tot, okd, badd, rnd, zb, zc, err, pvals):
    hlc, ohlc, trk = pvals or (0,0,0)
    s = {
        "host": host,
        "normalization": {},
        "hydration": {},
        "discovery": {},
        "tagging": {"add": []},
        "_notes": []
    }
    if (hlc or 0) >= 3 or (ohlc or 0) >= 3:
        s["normalization"].setdefault("query_param_drops", []).extend(["hl","override_hl"])
        s["_notes"].append("Drop locale duplication via ?hl/override_hl.")
    if (trk or 0) >= 5:
        s["normalization"].setdefault("query_param_drops", []).extend(["utm_*","fbclid","gclid"])
        s["_notes"].append("Drop tracker params (utm_*, fbclid, gclid).")
    if tot > 0 and (rnd/tot) > 0.20:
        s["hydration"]["render_on"] = [401,403,406,429]
        s["hydration"]["render_timeout"] = 25
        s["_notes"].append("High render reliance; keep render fallback.")
    if tot > 0 and (zc/tot) > 0.10:
        s["hydration"]["min_html_len"] = 1200
        s["_notes"].append("Many empty clean_text; likely auth/cookie walls.")
    if tot > 0 and (err/tot) > 0.05:
        s["hydration"]["pause"] = 0.4
        s["_notes"].append("Elevated errors; back off a bit.")

    if "tiktok.com" in host:
        s["normalization"].setdefault("path_deny_regexes", []).extend([
            r"^/legal/open-source", r"^/legal/page/.*/open-source/"
        ])
        s["_notes"].append("Exclude giant open-source pages.")
    if "vimeo.com" in host:
        s["normalization"].setdefault("path_deny_regexes", []).append(r"^/blog/")
    if "youtube.com" in host or "x.com" in host or "twitter.com" in host:
        s["discovery"].setdefault("cross_allow_hosts", [])
        if "youtube.com" in host: s["discovery"]["cross_allow_hosts"].append("policies.google.com")
        if "x.com" in host or "twitter.com" in host:
            s["discovery"]["cross_allow_hosts"] += ["help.twitter.com","legal.twitter.com","privacy.x.com","business.x.com","developer.x.com"]

    # dedupe
    for k in ("query_param_drops","path_deny_regexes","cross_allow_hosts","add"):
        for section in ("normalization","discovery","tagging"):
            if isinstance(s.get(section,{}).get(k), list):
                s[section][k] = sorted(set(s[section][k]))
    return s

def write_yaml(obj, path):
    def dump_yaml(d, indent=0):
        lines=[]; pad="  "*indent
        for k,v in d.items():
            if isinstance(v, dict):
                lines.append(f"{pad}{k}:")
                lines.extend(dump_yaml(v, indent+1))
            elif isinstance(v, list):
                if not v: lines.append(f"{pad}{k}: []"); continue
                lines.append(f"{pad}{k}:")
                for it in v:
                    lines.append(f"{pad}  - {it}")
            else:
                vv = json.dumps(v) if isinstance(v,(int,float)) else str(v)
                lines.append(f"{pad}{k}: {vv}")
        return "\n".join(lines)
    with open(path, "w", encoding="utf-8") as f:
        f.write(dump_yaml(obj))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--auto-promote-low-risk", action="store_true")
    args = ap.parse_args()

    hosts, param_map = fetch_stats()
    wrote = 0; promoted = 0
    for host, tot, okd, badd, rnd, zb, zc, err in hosts:
        s = suggest_for_host(host, tot, okd, badd, rnd, zb, zc, err, param_map.get((host,), (0,0,0)))
        # skip empty
        if not any([s["normalization"], s["hydration"], s["discovery"], s["tagging"].get("add")]):
            continue
        outp = os.path.join(OUT_DIR, f"{host}.yml")
        write_yaml(s, outp); wrote += 1

        if args.auto-promote-low-risk:
            # filter to only safe normalizations
            safe_norm = {}
            qpd = [p for p in s.get("normalization",{}).get("query_param_drops",[]) if p in SAFE_PARAM_DROPS]
            pdeny = [p for p in s.get("normalization",{}).get("path_deny_regexes",[]) if p in SAFE_PATH_DENIES]
            if qpd or pdeny:
                safe_norm["query_param_drops"] = sorted(set(qpd)) if qpd else []
                if pdeny: safe_norm["path_deny_regexes"] = sorted(set(pdeny))
                active = {
                    "host": host,
                    "normalization": safe_norm
                }
                act_path = os.path.join(ACTIVE_DIR, f"{host}.yml")
                write_yaml(active, act_path)
                promoted += 1

    print(f"[autosuggest] wrote {wrote} suggestion file(s) to {OUT_DIR}")
    if args.auto-promote-low-risk:
        print(f"[autosuggest] auto-promoted {promoted} low-risk policy file(s) to {ACTIVE_DIR}")

if __name__ == "__main__":
    main()
