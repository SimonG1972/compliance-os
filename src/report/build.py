# Remove old file and any bytecode caches
Remove-Item .\src\report\build.py -Force -ErrorAction SilentlyContinue
Remove-Item .\src\report\__pycache__ -Recurse -Force -ErrorAction SilentlyContinue

# Write fresh file
$code = @"
from __future__ import annotations
from pathlib import Path
from datetime import datetime
import re, html, subprocess, sys, tempfile

# --- Pure-Python PDF engine (ReportLab) ---
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak,
    ListFlowable, ListItem, Preformatted
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

def _md_inline_to_rl(text: str) -> str:
    t = html.escape(text)
    t = re.sub(r'\[(.+?)\]\((https?://[^\s)]+)\)', r'<a href="\2">\1</a>', t)   # links
    t = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', t)                                 # bold
    t = re.sub(r'(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)', r'<i>\1</i>', t)           # italics
    t = re.sub(r'`([^`]+?)`', r'<font face=\"Courier\">\1</font>', t)             # inline code
    return t

def _md_to_flowables(md_text: str, styles) -> list:
    flows, in_code, code_lines, list_items = [], False, [], []

    def flush_list():
        nonlocal list_items
        if list_items:
            flows.append(ListFlowable(list_items, bulletType='bullet', leftIndent=12))
            list_items = []

    for line in md_text.splitlines():
        if line.strip().startswith('```'):
            if in_code:
                flows.append(Preformatted('\\n'.join(code_lines), styles['Code']))
                code_lines, in_code = [], False
            else:
                flush_list(); in_code = True
            continue
        if in_code:
            code_lines.append(line); continue

        m = re.match(r'^(#{1,6})\\s+(.*)$', line)
        if m:
            flush_list()
            level, text = len(m.group(1)), _md_inline_to_rl(m.group(2).strip())
            flows.append(Paragraph(text, styles['H1' if level==1 else 'H2' if level==2 else 'H3']))
            flows.append(Spacer(1, 6))
            continue

        if line.strip().startswith('>'):
            flush_list()
            flows.append(Paragraph(_md_inline_to_rl(line.strip()[1:].lstrip()), styles['Quote']))
            continue

        if re.match(r'^\\s*[-*]\\s+', line):
            item = re.sub(r'^\\s*[-*]\\s+', '', line)
            list_items.append(ListItem(Paragraph(_md_inline_to_rl(item), styles['Body'])))
            continue

        if not line.strip():
            flush_list(); flows.append(Spacer(1, 6)); continue

        flush_list()
        flows.append(Paragraph(_md_inline_to_rl(line), styles['Body']))

    flush_list()
    return flows

def _rl_styles():
    ss = getSampleStyleSheet()
    ss.add(ParagraphStyle(name='Body', parent=ss['BodyText'], fontSize=10.5, leading=13.5))
    ss.add(ParagraphStyle(name='H1', parent=ss['Heading1'], fontSize=18, leading=22, spaceAfter=6))
    ss.add(ParagraphStyle(name='H2', parent=ss['Heading2'], fontSize=14, leading=18, spaceAfter=4))
    ss.add(ParagraphStyle(name='H3', parent=ss['Heading3'], fontSize=12, leading=16, spaceAfter=2))
    ss.add(ParagraphStyle(name='TitleBig', parent=ss['Title'], fontSize=24, leading=28, spaceAfter=6))
    ss.add(ParagraphStyle(name='Subtitle', parent=ss['BodyText'], fontSize=13, textColor=colors.darkgray, spaceAfter=10))
    ss.add(ParagraphStyle(name='Meta', parent=ss['BodyText'], fontSize=9.5, textColor=colors.gray, leading=12))
    ss.add(ParagraphStyle(name='Quote', parent=ss['BodyText'], leftIndent=10, textColor=colors.HexColor('#333333'), backColor=colors.HexColor('#fbfbfb')))
    ss.add(ParagraphStyle(name='Code', parent=ss['Code'], fontName='Courier', fontSize=9.5, leading=12, backColor=colors.whitesmoke))
    return ss

def _cover_flow(meta: dict, styles) -> list:
    flows = []
    title = meta.get('title') or 'Policy Intelligence Report'
    subtitle = meta.get('subtitle') or 'Automated Analysis'
    query = meta.get('query') or ''
    generated_at = meta.get('generated_at') or datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
    tags = meta.get('tags') or []
    flows.append(Spacer(1, 30))
    flows.append(Paragraph(html.escape(title), styles['TitleBig']))
    flows.append(Paragraph(html.escape(subtitle), styles['Subtitle']))
    meta_lines = []
    if query: meta_lines.append(f"<b>Query:</b> {html.escape(query)}")
    meta_lines.append(f"<b>Generated:</b> {html.escape(generated_at)}")
    if tags: meta_lines.append('<b>Tags:</b> ' + ', '.join([html.escape(t) for t in tags]))
    flows.append(Paragraph('<br/>'.join(meta_lines), styles['Meta']))
    flows.append(Spacer(1, 20))
    flows.append(PageBreak())
    return flows

def _rl_build_pdf_from_markdown(md_text: str, meta: dict, out_pdf: Path) -> None:
    styles = _rl_styles()
    doc = SimpleDocTemplate(str(out_pdf), pagesize=letter,
                            leftMargin=18*mm, rightMargin=18*mm,
                            topMargin=22*mm, bottomMargin=22*mm)
    story = []
    story.extend(_cover_flow(meta, styles))
    story.extend(_md_to_flowables(md_text, styles))
    def _numbered(canvas, doc):
        canvas.setFont('Helvetica', 9)
        canvas.setFillColorRGB(0.4,0.4,0.4)
        canvas.drawCentredString(letter[0]/2.0, 12*mm, str(canvas.getPageNumber()))
    doc.build(story, onFirstPage=_numbered, onLaterPages=_numbered)

# --------------- Public API ----------------
def build_report_from_md(md_path: Path, meta: dict, out_pdf: Path) -> None:
    md_text = Path(md_path).read_text(encoding='utf-8')
    _rl_build_pdf_from_markdown(md_text, meta, out_pdf)
    print('Report engine: ReportLab (pure Python)')

def build_report_from_query(query: str, k: int, near: str | None, meta: dict, out_pdf: Path) -> None:
    with tempfile.TemporaryDirectory() as tmpd:
        md_path = Path(tmpd) / 'answer.md'
        cmd = [sys.executable, 'scripts/answer_synth.py', query, '--k', str(k), '--out', str(md_path)]
        if near:
            cmd.extend(['--near', near])
        subprocess.run(cmd, check=True)
        build_report_from_md(md_path=md_path, meta=meta, out_pdf=out_pdf)
"@
$code | Set-Content -Encoding UTF8 .\src\report\build.py

# Confirm no 'weasyprint' remains:
Select-String -Path .\src\report\build.py -Pattern "weasyprint" -SimpleMatch
