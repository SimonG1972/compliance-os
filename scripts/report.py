#!/usr/bin/env python
from __future__ import annotations
import sys
from pathlib import Path
import argparse
from datetime import datetime, timezone

# Ensure 'import src' works when running from ./scripts
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.report.pdf_build import (
    build_report_from_md,
    build_report_from_query,
)

def main():
    parser = argparse.ArgumentParser(
        description="Build a PDF report from a synthesized Markdown (or directly from a query)."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # From an existing Markdown file
    p_md = sub.add_parser("from-md", help="Render a PDF from an existing synthesized Markdown file")
    p_md.add_argument("md_path", help="Path to synthesized .md file")
    p_md.add_argument("--title", default="Policy Intelligence Report")
    p_md.add_argument("--subtitle", default="Automated Analysis")
    p_md.add_argument("--query", default="")
    p_md.add_argument("--tags", default="", help="Comma-separated tags")
    p_md.add_argument("--out", default="", help="Output PDF path (default: reports/<title>_<date>.pdf)")
    # Client-ready extras
    p_md.add_argument("--client-ready", action="store_true", help="Add Executive Summary and Findings/Risks/Recommendations pages")
    p_md.add_argument("--summary-bullets", type=int, default=6, help="Max bullets in Executive Summary (auto mode)")
    p_md.add_argument("--logo", default=None, help="Path to a logo image for the cover")
    p_md.add_argument("--disclaimer", default="Internal draft — do not distribute")

    # Optional: call your synth CLI then render
    p_q = sub.add_parser("from-query", help="Run synth CLI, then render a PDF")
    p_q.add_argument("query", help="Search question / prompt")
    p_q.add_argument("--k", type=int, default=40)
    p_q.add_argument("--near", default=None)
    p_q.add_argument("--title", default="Policy Intelligence Report")
    p_q.add_argument("--subtitle", default="Automated Analysis")
    p_q.add_argument("--tags", default="", help="Comma-separated tags")
    p_q.add_argument("--out", default="", help="Output PDF path (default: reports/<title>_<date>.pdf)")
    # Client-ready extras
    p_q.add_argument("--client-ready", action="store_true")
    p_q.add_argument("--summary-bullets", type=int, default=6)
    p_q.add_argument("--logo", default=None)
    p_q.add_argument("--disclaimer", default="Internal draft — do not distribute")

    args = parser.parse_args()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_default = Path("reports") / f"{args.title.replace(' ', '_')}_{today}.pdf"

    meta = dict(
        title=getattr(args, "title", "Policy Intelligence Report"),
        subtitle=getattr(args, "subtitle", "Automated Analysis"),
        query=getattr(args, "query", ""),
        tags=[t.strip() for t in getattr(args, "tags", "").split(",") if t.strip()],
        logo=getattr(args, "logo", None),
        disclaimer=getattr(args, "disclaimer", "Internal draft — do not distribute"),
        client_ready=getattr(args, "client_ready", False),
        summary_bullets=getattr(args, "summary_bullets", 6),
    )

    if args.cmd == "from-md":
        out_pdf = Path(args.out) if args.out else out_default
        out_pdf.parent.mkdir(parents=True, exist_ok=True)
        build_report_from_md(
            md_path=Path(args.md_path),
            meta=meta,
            out_pdf=out_pdf,
        )
        print(f"Wrote {out_pdf}")

    elif args.cmd == "from-query":
        out_pdf = Path(args.out) if args.out else out_default
        out_pdf.parent.mkdir(parents=True, exist_ok=True)
        build_report_from_query(
            query=args.query,
            k=args.k,
            near=args.near,
            meta=meta,
            out_pdf=out_pdf,
        )
        print(f"Wrote {out_pdf}")

if __name__ == "__main__":
    main()
