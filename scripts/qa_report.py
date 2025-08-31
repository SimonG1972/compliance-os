#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Prints a per-host QA summary + flags anomalies that typically demand policy tweaks.
Safe to run anytime.
"""
import os, sqlite3
DB = os.path.join(os.getcwd(), "compliance.db")

# Thresholds (tune as you like)
RENDER_RATE_WARN   = 0.20  # >20% docs rendered → consider enabling render_on in policy
ZERO_CLEAN_WARN    = 0.10  # >10% clean_text empty → likely cookie/login walls
ZERO_BODY_WARN     = 0.10
ERROR_RATE_WARN    = 0.05
HL_DUP_WARN        = 3     # >=3 URLs with ?hl=/override_hl → suggest param drop
TRACKER_WARN       = 5     # >=5 URLs with utm_*/fbclid/gclid → normalization

def pct(n, d): 
    return 0 if d==0 else n/d

def main():
    con = sqlite3.connect(DB); c = con.cursor()

    print("\n=== Host stats (top 20 by total_docs) ===")
    rows = c.execute("""
      SELECT host, total_docs, ok_docs, bad_docs, render_docs, zero_body_docs, zero_clean_docs, errors
      FROM host_stats ORDER BY total_docs DESC LIMIT 20
    """).fetchall()
    for r in rows:
        host, tot, okd, badd, rnd, zb, zc, err = r
        print(f"{host:25} tot={tot:4} ok={okd:4} render={rnd:3} "
              f"zero_body={zb:3} zero_clean={zc:3} errors={err:3}")

    print("\n=== Anomaly flags (actionable) ===")
    # 1) High render reliance / zero-clean / zero-body / errors
    anomalies = []
    for host, tot, okd, badd, rnd, zb, zc, err in rows:
        msgs = []
        if pct(rnd, tot) > RENDER_RATE_WARN: msgs.append("high-render-rate")
        if pct(zc, tot)  > ZERO_CLEAN_WARN:  msgs.append("zero-clean-many")
        if pct(zb, tot)  > ZERO_BODY_WARN:   msgs.append("zero-body-many")
        if pct(err, tot) > ERROR_RATE_WARN:  msgs.append("fetch-errors-high")
        if msgs:
            anomalies.append((host, msgs))

    # 2) Param anomalies
    params = c.execute("""
      SELECT host,
             SUM(has_hl) AS hl_cnt,
             SUM(has_override_hl) AS ohl_cnt,
             SUM(has_tracker) AS tracker_cnt
      FROM v_url_params GROUP BY host
      ORDER BY (hl_cnt+ohl_cnt+tracker_cnt) DESC
    """).fetchall()
    for host, hlc, ohlc, trk in params:
        msgs=[]
        if (hlc or 0) >= HL_DUP_WARN or (ohlc or 0) >= HL_DUP_WARN: msgs.append("hl-param-dup")
        if (trk or 0) >= TRACKER_WARN: msgs.append("tracker-params")
        if msgs:
            anomalies.append((host, msgs))

    if not anomalies:
        print("No major anomalies detected.")
    else:
        # Deduplicate + print
        seen=set()
        for host, msgs in anomalies:
            key=(host, tuple(sorted(msgs)))
            if key in seen: continue
            seen.add(key)
            print(f"- {host:25} -> {', '.join(sorted(set(msgs)))}")

    con.close()

if __name__ == "__main__":
    main()
