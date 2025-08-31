#!/usr/bin/env python
import os, sqlite3, argparse, smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta

DB = os.path.join(os.getcwd(), "compliance.db")

def send_email(to_addr, subject, body):
    host = os.environ.get("SMTP_HOST")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    pwd  = os.environ.get("SMTP_PASS")
    frm  = os.environ.get("SMTP_FROM", user or "alerts@localhost")
    if not host or not user or not pwd:
        print("SMTP not configured; printing alert:\n", body[:2000])
        return
    msg = EmailMessage()
    msg["From"], msg["To"], msg["Subject"] = frm, to_addr, subject
    msg.set_content(body)
    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, pwd)
        s.send_message(msg)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--tags", default="children_data,cookies,advertising,account_deletion,retention")
    ap.add_argument("--hosts", default="snap.com,tiktok.com,youtube.com,instagram.com,facebook.com,reddit.com,discord.com,shopify.com,plaid.com,ebay.com")
    ap.add_argument("--to", required=False, help="email address")
    args = ap.parse_args()

    since_iso = (datetime.utcnow() - timedelta(hours=args.hours)).isoformat(timespec="seconds") + "Z"
    tags = [t.strip() for t in args.tags.split(",") if t.strip()]
    hosts = [h.strip() for h in args.hosts.split(",") if h.strip()]

    con = sqlite3.connect(DB)
    cur = con.cursor()
    rows = cur.execute("""
        SELECT e.url, e.changed_at, e.diff_summary
        FROM change_events e
        WHERE e.changed_at >= ?
        ORDER BY e.changed_at DESC
        LIMIT 200
    """, (since_iso,)).fetchall()

    hits = []
    for url, ts, diff in rows:
        host = url.split("/")[2] if "://" in url else url
        # host filter
        if not any(host.endswith(h) for h in hosts):
            continue
        # tag filter via chunk_tags
        has_tag = False
        for (t,) in cur.execute("SELECT DISTINCT tag FROM chunk_tags WHERE url=? LIMIT 50", (url,)):
            if t in tags:
                has_tag = True; break
        if has_tag:
            hits.append((url, ts, diff))

    con.close()

    if not hits:
        print("No alertable changes.")
        return
    body = []
    for url, ts, diff in hits[:50]:
        body.append(f"{ts}  {url}\n{diff}\n{'-'*60}\n")
    content = "".join(body)
    subject = f"[Compliance-OS] {len(hits)} change(s) in last {args.hours}h"
    if args.to:
        send_email(args.to, subject, content)
    else:
        print(subject, "\n", content[:5000])

if __name__ == "__main__":
    main()
