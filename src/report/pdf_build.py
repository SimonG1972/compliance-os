from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import re, html, subprocess, sys, tempfile, unicodedata as _ud

# --- ReportLab (pure Python) ---
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak,
    ListFlowable, ListItem, Preformatted, Image as RLImage, Table, TableStyle
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

# ---------------- Patterns ----------------
INLINE_FN_STRICT = re.compile(r'\[\^(\d+)\]')               # [^1]
INLINE_FN_LOOSE  = re.compile(r'\[\^(\d+)\b')               # [^1 (missing ])
FOOTNOTE_DEF_RE  = re.compile(r'^\s*\[\^(\d+)\]:\s*(.+)$')  # [^1]: text  (for detection)
BLOCK_FN_RE      = re.compile(r'^\[\^(\d+)\]:\s*(.+)$', re.MULTILINE)
HEADING_RE       = re.compile(r'^(#{1,6})\s+(.*)$', re.MULTILINE)

# ---------------- Sanitizers ----------------
def _strip_controls(t: str) -> str:
    t = t.replace('\r', '\n')
    return ''.join(ch for ch in t if ch in '\n\t' or not _ud.category(ch).startswith('C'))

def _fix_cp1252_garble(t: str) -> str:
    """Fix common UTF-8→cp1252 garbling (quotes/dashes) + stray artifacts."""
    replacements = {
        'â€™': '’', 'â€˜': '‘', 'â€œ': '“', 'â€': '”',
        'â€“': '–', 'â€”': '—', 'Â ': '', 'Â': '',
        'ï»¿': '',   # BOM shown as text
        '¦': ' ',    # stray graphics bar
    }
    for k, v in replacements.items():
        t = t.replace(k, v)
    # Heuristics observed in some outputs:
    t = re.sub(r'([A-Za-z])ãs\b', r'\1’s', t)  # Childrenãs → Children’s
    t = re.sub(r'([A-Za-z])âs\b', r'\1’s', t)
    # Lone/word-joined 'á' used where a dash/sep should be
    t = re.sub(r'(\w)á\s', r'\1 — ', t)    # wordá␠ → word — ␠
    t = re.sub(r'\sá(\w)', r' — \1', t)    # ␠áword →  — word
    t = t.replace('á', '')
    return t

def _dequote_jsonish(t: str) -> str:
    if '\\"' in t or '\\n' in t or '\\t' in t:
        t = t.replace('\\n', '\n').replace('\\r', '\n').replace('\\t', ' ')
        t = t.replace('\\"', '"')
        t = re.sub(r'\\\\(?!`)', r'\\', t)
    return t

def _strip_simple_blocks_with_key(text: str, key: str) -> str:
    pat = re.compile(r'\{[^{}]*\}')
    prev = None
    while prev != text:
        prev = text
        text = pat.sub(lambda m: '' if key in m.group(0) else m.group(0), text)
    return text

def _strip_quoted_object_blobs(text: str) -> str:
    return re.sub(r'"\{[^{}]*\}"', '', text)

def _strip_rsc_arrays(text: str) -> str:
    return re.sub(r'\[\s*"\$"[^]\n]*\]', '', text)

def _strip_trailing_junk(line: str) -> str:
    line = re.sub(r'[\s,;:]*[\]\)\}]+["“”\'’]*[,;:]*\s*$', '', line)
    line = re.sub(r'["“”\'’]+[,;:]*\s*$', '', line)
    line = re.sub(r'^\s*[\[\(\{]+["“”\'’]*\s*', '', line)
    return line.strip()

def _dedupe_consecutive_lines(text: str) -> str:
    out, prev = [], None
    for ln in text.splitlines():
        if prev is not None and ln.strip() and ln.strip() == prev.strip():
            continue
        out.append(ln); prev = ln
    return '\n'.join(out)

def _sanitize_md(text: str) -> str:
    t = _strip_controls(text)
    t = _dequote_jsonish(t)
    t = _fix_cp1252_garble(t)

    # Drop UI blobs
    for key in ('className', 'children', 'props'):
        t = _strip_simple_blocks_with_key(t, key)
    t = _strip_quoted_object_blobs(t)
    t = _strip_rsc_arrays(t)
    t = re.sub(r'"children"\s*:\s*\[[^\]]*\]', '', t)
    t = re.sub(r'^\s*[\{\}]\s*$', '', t, flags=re.MULTILINE)
    t = re.sub(r'["\\]{3,}', '', t)

    # IMPORTANT: clean lines, but DO NOT touch footnote definition lines
    cleaned_lines = []
    for ln in t.splitlines():
        if FOOTNOTE_DEF_RE.match(ln):
            cleaned_lines.append(ln.strip())
        else:
            cleaned_lines.append(_strip_trailing_junk(ln))
    t = '\n'.join(cleaned_lines)

    # Whitespace & duplicates
    t = re.sub(r'[ \t]{2,}', ' ', t)
    t = re.sub(r'\n{3,}', '\n\n', t)
    t = _dedupe_consecutive_lines(t)
    return t

def _extract_footnotes_and_strip_sources_heading(text: str):
    notes: dict[str, str] = {}
    def _grab(m: re.Match):
        notes[m.group(1)] = m.group(2).strip()
        return ''
    body = BLOCK_FN_RE.sub(_grab, text)
    body = re.sub(r'\n?\s*^#{1,6}\s+Sources\s*$\s*\n?', '\n', body, flags=re.MULTILINE)
    return body, notes

# ---------------- Helpers: headings / sections ----------------
def _iter_headings(md_text: str):
    """Yield (level:int, title:str, start_idx:int) for each heading."""
    for m in HEADING_RE.finditer(md_text):
        hashes, title = m.group(1), m.group(2).strip()
        yield len(hashes), title, m.start()

def _extract_section(md_text: str, patterns: list[str]) -> str | None:
    """
    Return the markdown content under the first heading whose title matches
    any of the case-insensitive regex patterns. Stops at the next heading
    of the same or higher level.
    """
    matches = []
    for lvl, title, pos in _iter_headings(md_text):
        for pat in patterns:
            if re.search(pat, title, re.IGNORECASE):
                matches.append((pos, lvl, title))
                break
    if not matches:
        return None
    # pick the first occurrence
    start_pos, level, _ = sorted(matches, key=lambda x: x[0])[0]
    # find end
    after = md_text[start_pos+1:]
    end = len(md_text)
    for lvl, _, p in _iter_headings(md_text[start_pos+1:]):
        if lvl <= level:
            end = start_pos + 1 + p
            break
    # slice after the heading line
    line_end = md_text.find('\n', start_pos)
    if line_end == -1:
        return None
    return md_text[line_end+1:end].strip()

def _bullets_from_markdown(md: str, max_items: int | None = None) -> list[str]:
    items = []
    for ln in md.splitlines():
        m = re.match(r'^\s*[-*]\s+(.*)$', ln)
        if m:
            val = _strip_trailing_junk(m.group(1)).strip()
            if val:
                items.append(val)
                if max_items and len(items) >= max_items:
                    break
    return items

def _first_sentences(md: str, max_chars: int = 800) -> list[str]:
    text = []
    for ln in md.splitlines():
        if re.match(r'^\s*[-*]\s+', ln) or ln.strip().startswith('>') or HEADING_RE.match(ln):
            continue
        piece = _strip_trailing_junk(ln.strip())
        if piece:
            text.append(piece)
        if len(' '.join(text)) > max_chars:
            break
    blob = ' '.join(text)
    # naive sent split
    parts = re.split(r'(?<=[\.\!\?])\s+', blob)
    return [p.strip() for p in parts if p.strip()]

# ---------------- Markdown → Flowables ----------------
def _md_inline_to_rl(text: str) -> str:
    t = html.escape(text)
    t = re.sub(r'\[(.+?)\]\((https?://[^\s)]+)\)', r'<a href="\2">\1</a>', t)
    t = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', t)
    t = re.sub(r'(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)', r'<i>\1</i>', t)
    t = re.sub(r'`([^`]+?)`', r'<font face="Courier">\1</font>', t)
    # footnotes: strict then loose
    t = INLINE_FN_STRICT.sub(r'<super>[\1]</super>', t)
    t = INLINE_FN_LOOSE.sub(r'<super>[\1]</super>', t)
    return t

def _md_to_flowables(md_text: str, styles) -> list:
    flows, in_code, code_lines, list_items = [], False, [], []

    def flush_list():
        nonlocal list_items
        if list_items:
            flows.append(ListFlowable(list_items, bulletType='bullet', leftIndent=12, spaceBefore=4, spaceAfter=6))
            list_items = []

    for raw in md_text.splitlines():
        line = raw

        # fenced code
        if line.strip().startswith('```'):
            if in_code:
                flows.append(Preformatted('\n'.join(code_lines), styles['CodeBlock']))
                code_lines, in_code = [], False
            else:
                flush_list(); in_code = True
            continue
        if in_code:
            code_lines.append(line); continue

        # headings
        m = re.match(r'^(#{1,6})\s+(.*)$', line)
        if m:
            flush_list()
            level, text = len(m.group(1)), _md_inline_to_rl(_strip_trailing_junk(m.group(2).strip()))
            flows.append(Paragraph(text, styles['H1' if level==1 else 'H2' if level==2 else 'H3']))
            flows.append(Spacer(1, 6))
            continue

        # blockquote
        if line.strip().startswith('>'):
            flush_list()
            flows.append(Paragraph(_md_inline_to_rl(_strip_trailing_junk(line.strip()[1:].lstrip())), styles['Quote']))
            continue

        # bullets
        if re.match(r'^\s*[-*]\s+', line):
            item = re.sub(r'^\s*[-*]\s+', '', line)
            item = _strip_rsc_arrays(item)
            item = _strip_quoted_object_blobs(item)
            item = _strip_simple_blocks_with_key(item, 'className')
            item = _strip_simple_blocks_with_key(item, 'children')
            item = _strip_trailing_junk(item)
            list_items.append(ListItem(Paragraph(_md_inline_to_rl(item), styles['Body'])))
            continue

        # blank
        if not line.strip():
            flush_list(); flows.append(Spacer(1, 6)); continue

        # paragraph
        flush_list()
        flows.append(Paragraph(_md_inline_to_rl(_strip_trailing_junk(line)), styles['Body']))

    flush_list()
    return flows

def _rl_styles():
    ss = getSampleStyleSheet()
    ss.add(ParagraphStyle(name='Body', parent=ss['BodyText'], fontSize=11.0, leading=15.0, spaceAfter=2))
    ss.add(ParagraphStyle(name='H1', parent=ss['Heading1'], fontSize=18, leading=22, spaceAfter=8))
    ss.add(ParagraphStyle(name='H2', parent=ss['Heading2'], fontSize=14, leading=18, spaceAfter=6))
    ss.add(ParagraphStyle(name='H3', parent=ss['Heading3'], fontSize=12, leading=16, spaceAfter=4))
    ss.add(ParagraphStyle(name='TitleBigCenter', parent=ss['Title'], fontSize=26, leading=30, spaceAfter=10, alignment=TA_CENTER))
    ss.add(ParagraphStyle(name='SubtitleCenter', parent=ss['BodyText'], fontSize=13.5, textColor=colors.darkgray, spaceAfter=12, alignment=TA_CENTER))
    ss.add(ParagraphStyle(name='MetaCenter', parent=ss['BodyText'], fontSize=9.5, textColor=colors.gray, leading=12, alignment=TA_CENTER))
    ss.add(ParagraphStyle(name='Quote', parent=ss['BodyText'], leftIndent=10, textColor=colors.HexColor('#333333'), backColor=colors.HexColor('#fbfbfb')))
    base_code = ss['Code'] if 'Code' in ss else ss['BodyText']
    ss.add(ParagraphStyle(name='CodeBlock', parent=base_code, fontName='Courier', fontSize=9.5, leading=12, backColor=colors.whitesmoke))
    return ss

# ---------------- Cover ----------------
def _cover_flow(meta: dict, styles) -> list:
    flows = []
    title = meta.get('title') or 'Policy Intelligence Report'
    subtitle = meta.get('subtitle') or 'Automated Analysis'
    query = meta.get('query') or ''
    generated_at = meta.get('generated_at') or datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    tags = meta.get('tags') or []
    logo_path = meta.get('logo') or meta.get('logo_path')

    flows.append(Spacer(1, 28))
    if logo_path and Path(logo_path).exists():
        try:
            img = RLImage(str(logo_path))
            max_w = 80 * mm
            iw = getattr(img, 'imageWidth', max_w) or max_w
            ih = getattr(img, 'imageHeight', max_w * 0.35) or (max_w * 0.35)
            scale = min(1.0, float(max_w) / float(iw))
            img.drawWidth = float(iw) * scale
            img.drawHeight = float(ih) * scale
            img.hAlign = 'CENTER'
            flows.append(img); flows.append(Spacer(1, 10))
        except Exception:
            pass

    flows.append(Paragraph(html.escape(title), styles['TitleBigCenter']))
    flows.append(Paragraph(html.escape(subtitle), styles['SubtitleCenter']))

    meta_lines = []
    if query: meta_lines.append(f"<b>Query:</b> {html.escape(query)}")
    meta_lines.append(f"<b>Generated:</b> {html.escape(generated_at)}")
    if tags: meta_lines.append('<b>Tags:</b> ' + ', '.join([html.escape(t) for t in tags]))
    flows.append(Paragraph('<br/>'.join(meta_lines), styles['MetaCenter']))
    flows.append(Spacer(1, 24)); flows.append(PageBreak())
    return flows

# ---------------- Summary & FRR pages ----------------
def _exec_summary_flow(md_text: str, styles, max_bullets: int) -> list:
    # Prefer explicit "Executive Summary" section; else auto from top bullets/first sentences
    explicit = _extract_section(md_text, [r'^\s*executive\s+summary\s*$', r'^\s*summary\s*$'])
    bullets = _bullets_from_markdown(explicit or md_text, max_items=max_bullets) if explicit else _bullets_from_markdown(md_text, max_items=max_bullets)
    flows = [Paragraph('Executive Summary', styles['H1'])]
    if bullets:
        flows.append(ListFlowable([ListItem(Paragraph(_md_inline_to_rl(b), styles['Body'])) for b in bullets],
                                  bulletType='bullet', leftIndent=12, spaceBefore=2, spaceAfter=8))
    else:
        # fall back: first sentences
        for s in _first_sentences(md_text, max_chars=900)[:5]:
            flows.append(Paragraph(_md_inline_to_rl(s), styles['Body']))
            flows.append(Spacer(1, 3))
    flows.append(PageBreak())
    return flows

def _section_or_auto(md_text: str, title: str, patterns: list[str], fallback_take_from_all: bool, max_items: int = 8):
    sec = _extract_section(md_text, patterns)
    if sec:
        items = _bullets_from_markdown(sec, max_items=max_items)
        if items:
            return title, items
    if fallback_take_from_all:
        items = _bullets_from_markdown(md_text, max_items=max_items)
        if items:
            return title, items
    return title, []

def _frr_flow(md_text: str, styles) -> list:
    flows = [Paragraph('Findings, Risks & Recommendations', styles['H1'])]
    blocks = []

    title, items = _section_or_auto(md_text, 'Findings', [r'^\s*findings?\s*$'], True, 8)
    blocks.append((title, items))

    title, items = _section_or_auto(md_text, 'Risks', [r'^\s*risks?\s*$', r'^\s*issues?\s*$', r'^\s*concerns?\s*$'], False, 8)
    blocks.append((title, items))

    title, items = _section_or_auto(md_text, 'Recommendations', [r'^\s*recommendations?\s*$', r'^\s*next\s*steps?\s*$', r'^\s*actions?\s*$'], False, 8)
    blocks.append((title, items))

    for idx, (heading, items) in enumerate(blocks):
        flows.append(Paragraph(heading, styles['H2']))
        if items:
            flows.append(ListFlowable([ListItem(Paragraph(_md_inline_to_rl(b), styles['Body'])) for b in items],
                                      bulletType='bullet', leftIndent=12, spaceBefore=2, spaceAfter=8))
        else:
            flows.append(Paragraph('<i>No explicit items detected.</i>', styles['Body']))
            flows.append(Spacer(1, 6))

    flows.append(PageBreak())
    return flows

# ---------------- Sources (two-column) ----------------
def _sources_flow(notes: dict[str, str], styles, available_width: float) -> list:
    if not notes: return []
    items = [Paragraph(f'[{k}] ' + _md_inline_to_rl(v), styles['Body'])
             for k, v in sorted(notes.items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else kv[0])]
    mid = (len(items) + 1) // 2
    col1, col2 = items[:mid], items[mid:]
    rows = []
    for i in range(max(len(col1), len(col2))):
        rows.append([col1[i] if i < len(col1) else Spacer(1,0),
                     col2[i] if i < len(col2) else Spacer(1,0)])
    col_w = available_width / 2.0 - 4
    table = Table(rows, colWidths=[col_w, col_w])
    table.setStyle(TableStyle([
        ('VALIGN',(0,0),(-1,-1),'TOP'),
        ('LEFTPADDING',(0,0),(-1,-1),2),
        ('RIGHTPADDING',(0,0),(-1,-1),4),
        ('TOPPADDING',(0,0),(-1,-1),1),
        ('BOTTOMPADDING',(0,0),(-1,-1),1),
    ]))
    return [Paragraph('Sources', styles['H2']), table]

# ---------------- Header / Footer ----------------
def _make_header_footer(meta: dict):
    title = (meta.get('title') or 'Policy Intelligence Report')[:120]
    disclaimer = meta.get('disclaimer', 'Internal draft — do not distribute')
    def _draw(canvas, doc):
        w, h = doc.pagesize
        canvas.saveState()
        canvas.setFont('Helvetica', 9.5); canvas.setFillColorRGB(0.2,0.2,0.2)
        canvas.drawString(doc.leftMargin, h - 12*mm, title)
        canvas.setFont('Helvetica', 9); canvas.setFillColorRGB(0.4,0.4,0.4)
        canvas.drawCentredString(w/2.0, 12*mm, str(canvas.getPageNumber()))
        if disclaimer:
            canvas.setFont('Helvetica-Oblique', 8); canvas.setFillColorRGB(0.45,0.45,0.45)
            canvas.drawString(doc.leftMargin, 8*mm, disclaimer)
        canvas.restoreState()
    return _draw

# ---------------- Build core ----------------
def _rl_build_pdf_from_markdown(md_text: str, meta: dict, out_pdf: Path) -> None:
    styles = _rl_styles()
    md_text = _sanitize_md(md_text)
    body_text, notes = _extract_footnotes_and_strip_sources_heading(md_text)

    doc = SimpleDocTemplate(str(out_pdf), pagesize=letter,
                            leftMargin=18*mm, rightMargin=18*mm,
                            topMargin=24*mm, bottomMargin=22*mm)
    story = []
    story.extend(_cover_flow(meta, styles))

    # Client-ready extras (Executive Summary + FRR)
    if meta.get('client_ready'):
        story.extend(_exec_summary_flow(body_text, styles, max_bullets=int(meta.get('summary_bullets', 6))))
        story.extend(_frr_flow(body_text, styles))

    # body
    story.extend(_md_to_flowables(body_text, styles))
    story.append(Spacer(1, 12))
    story.extend(_sources_flow(notes, styles, available_width=doc.width))

    header_footer = _make_header_footer(meta)
    doc.build(story, onFirstPage=header_footer, onLaterPages=header_footer)

# --------------- Public API ----------------
def build_report_from_md(md_path: Path, meta: dict, out_pdf: Path) -> None:
    md_text = Path(md_path).read_text(encoding='utf-8')
    _rl_build_pdf_from_markdown(md_text, meta, out_pdf)
    print('Report engine: ReportLab (pure Python)')

def build_report_from_query(query: str, k: int, near: str | None, meta: dict, out_pdf: Path) -> None:
    with tempfile.TemporaryDirectory() as tmpd:
        md_path = Path(tmpd) / 'answer.md'
        cmd = [sys.executable, 'scripts/answer_synth.py', query, '--k', str(k), '--out', str(md_path)]
        if near: cmd.extend(['--near', near])
        subprocess.run(cmd, check=True)
        build_report_from_md(md_path=md_path, meta=meta, out_pdf=out_pdf)
