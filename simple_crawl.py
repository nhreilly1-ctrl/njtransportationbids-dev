"""
simple_crawl.py
---------------
Targets the cleanest, highest-quality NJ transportation bid sources:

  1. NJDOT Construction Services       — HTML list, trusted, daily
  2. NJDOT Professional Services       — HTML table, trusted, daily
  3. DRJTBC Current Procurements       — HTML blocks, trusted, daily
  4. NJTA Current Solicitations        — HTML table, construction + PS
  5. NJ Transit Active Solicitations   — HTML table, IFB=construction RFP=PS
  6. SJTA Doing Business               — HTML table, BidX job numbers

All sources are static HTML, publicly accessible, no auth required.
Output writes to data/opportunities.json and merges with existing records.

Usage:
    python simple_crawl.py              # full crawl
    python simple_crawl.py --dry-run   # print results, don't save
    python simple_crawl.py --source njdot-construction  # one source only
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("simple_crawl")

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE     = Path(__file__).parent
DATA_DIR = BASE / "data"
OPP_F    = DATA_DIR / "opportunities.json"
DATA_DIR.mkdir(exist_ok=True)

# ── HTTP ──────────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "NJTransportationBids-crawler/1.0 "
        "(+https://www.njtransportationbids.com)"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

def fetch(url: str, timeout: int = 25) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        log.warning(f"  Fetch failed {url}: {e}")
        return None


def fetch_js(url: str, click_text: str | None = None, wait_ms: int = 2500) -> str | None:
    """
    Fetch a JavaScript-rendered page using a headless Chromium browser (Playwright).
    If click_text is provided, clicks the first button/link containing that text
    (e.g. "Expand All") and waits wait_ms milliseconds before capturing the HTML.
    Falls back gracefully if Playwright is not installed.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("  Playwright not installed — falling back to requests fetch")
        return fetch(url)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                )
            )
            # Use "domcontentloaded" — njtransit.com never reaches networkidle
            # due to background polling. Wait a fixed interval for JS to render.
            page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            page.wait_for_timeout(wait_ms)   # let JS render the table
            if click_text:
                try:
                    page.get_by_text(click_text, exact=False).first.click()
                    page.wait_for_timeout(wait_ms)
                except Exception:
                    # Button not found or already expanded — continue anyway
                    pass
            html = page.content()
            browser.close()
            log.info(f"  JS fetch OK ({len(html):,} chars): {url}")
            return html
    except Exception as e:
        log.warning(f"  JS fetch failed {url}: {e}")
        return None

# ── Helpers ───────────────────────────────────────────────────────────────────
DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")
LEVEL_CODE_RE = re.compile(r"^[A-Z]-\d+\s+Level\s+[A-Z]$", re.IGNORECASE)

# NJDOT construction bids always start with an action verb
NJDOT_ACTION_WORDS = {
    "maintenance", "installation", "construction", "repair", "repairs",
    "resurfacing", "rehabilitation", "replacement", "reconstruction",
    "restoration", "drainage", "widening", "realignment", "improvement",
    "improvements", "bridge", "pavement", "milling", "overlay",
    "guiderail", "signal", "lighting", "fencing", "dredging", "removal",
    "cleaning", "painting", "inspection", "retrofit", "upgrade",
    "emergency", "safety", "access", "route", "interstate", "highway",
    "roadway", "intersection", "culvert", "retaining", "structural",
}

def _clean(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s*[-\u2013\u2014]\s*", " \u2014 ", text)
    return text

def _make_id(source_id: str, title: str, url: str = "") -> str:
    raw = f"{source_id}:{title.lower().strip()}:{url}"
    return "opp-" + hashlib.md5(raw.encode()).hexdigest()[:12]

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _is_garbage(title: str) -> bool:
    t = title.lower().strip()
    if len(t) < 8:
        return True
    if len(t.split()) < 2:
        return True
    # Only flag if the ENTIRE title is a noise word — not a substring match
    # (e.g. "PE No: N/A" in a real bid title shouldn't trigger this)
    GARBAGE_EXACT = {
        "click here", "view details", "see attachment", "download",
        "n/a", "none", "tbd", "pending", "unknown", "untitled",
        "[no title]", "no title",
    }
    if t in GARBAGE_EXACT:
        return True
    return False

def _record(
    source_id: str,
    source_name: str,
    title: str,
    url: str,
    due_date_raw: str = "",
    county: str = "Statewide",
    record_type: str = "construction",
    access_type: str = "Public access",
    contract_number: str = "",
    description: str = "",
    doc_url: str = "",
) -> dict:
    return {
        "id":            _make_id(source_id, title, url),
        "title":         title,
        "source_id":     source_id,
        "source_name":   source_name,
        "county":        county,
        "due_date_raw":  due_date_raw,
        "due_date":      due_date_raw,   # compat alias
        "url":           url,
        "access_type":   access_type,
        "record_type":   record_type,
        "contract_number": contract_number,
        "description":   description,
        "doc_url":       doc_url,
        "status":        "open",
        "crawled_at":    _now(),
    }

# ── Source definitions ────────────────────────────────────────────────────────
SOURCES = {
    "njdot-construction": {
        "id":          "state-njdot-construction",
        "name":        "NJDOT Construction Services",
        "url":         "https://www.nj.gov/transportation/business/procurement/ConstrServ/curradvproj.shtm",
        "record_type": "construction",
        "parser":      "njdot_construction",
    },
    "njdot-profserv": {
        "id":          "state-njdot-profserv",
        "name":        "NJDOT Professional Services",
        "url":         "https://www.nj.gov/transportation/business/procurement/ProfServ/CurrentSolic.shtm",
        "record_type": "professional_services",
        "parser":      "njdot_profserv",
    },
    "drjtbc-profserv": {
        "id":          "state-drjtbc-profserv",
        "name":        "DRJTBC Current Procurements",
        "url":         "https://www.drjtbc.org/professional-services/current/",
        "record_type": "professional_services",
        "parser":      "drjtbc_profserv",
    },
    "njta": {
        "id":          "state-njta",
        "name":        "NJ Turnpike Authority",
        "url":         "https://www.njta.gov/business-hub/current-solicitations/",
        "record_type": "construction",   # per-record type resolved by URL pattern
        "parser":      "njta",
    },
    "njtransit": {
        "id":          "state-njtransit",
        "name":        "NJ Transit",
        "url":         "https://www.njtransit.com/procurement/calendar",
        "record_type": "construction",   # per-record type resolved by IFB/RFP in doc href
        "parser":      "njtransit",
        "county":      "Statewide",
        "use_js":      True,             # page is JS-rendered; requires Playwright
        "js_click":    "Expand All",     # click this button before scraping
    },
    "sjta": {
        "id":          "state-sjta",
        "name":        "South Jersey Transportation Authority",
        "url":         "https://www.bidexpress.com/businesses/29894/home?agency=true",
        "record_type": "construction",   # per-record type inferred from title
        "parser":      "sjta",
        "county":      "Atlantic",
        "use_js":      True,             # BidExpress is JS-rendered
    },
}

# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_njdot_construction(html: str, src: dict) -> list[dict]:
    """
    NJDOT Construction: two-column table (Letting Date | Project).
    Parse each row directly — captures all linked projects regardless of
    pre-bid notice text in the same cell.
    """
    soup  = BeautifulSoup(html, "html.parser")
    base  = src["url"]
    items: list[dict] = []
    seen:  set[str]   = set()

    content = soup.find(id="content") or soup

    # Find the "CURRENTLY ADVERTISED PROJECTS" table
    # It follows a heading with that text
    target_table = None
    for tag in content.find_all(["h1","h2","h3","h4","p","b","strong","caption"]):
        if "currently advertised" in tag.get_text(strip=True).lower():
            target_table = tag.find_next("table")
            break
    if not target_table:
        target_table = content.find("table")

    if not target_table:
        log.warning("  NJDOT Construction: no table found — falling back to link scan")
        # Fallback: grab all links in content area with nearby date
        for anchor in content.find_all("a", href=True):
            title = _clean(anchor.get_text(" ", strip=True)).rstrip(".")
            href  = urljoin(base, anchor["href"])
            if not title or _is_garbage(title) or len(title.split()) < 4:
                continue
            parent_text = _clean((anchor.find_parent() or anchor).get_text(" ", strip=True))
            date_m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b", parent_text)
            due = date_m.group(1) if date_m else ""
            uid = _make_id(src["id"], title, href)
            if uid not in seen:
                seen.add(uid)
                items.append(_record(src["id"], src["name"], title, href, due, record_type=src["record_type"]))
        log.info(f"  NJDOT Construction (fallback): {len(items)} records")
        return items[:60]

    for row in target_table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        date_text = _clean(cells[0].get_text(" ", strip=True))
        if not re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", date_text):
            continue
        due_date = date_text

        # Log every anchor found in this row
        project_cell = cells[1]
        row_anchors  = project_cell.find_all("a", href=True)
        log.info(f"  Row {due_date}: {len(row_anchors)} anchors found")
        for a in row_anchors:
            log.info(f"    → [{_clean(a.get_text(' ',strip=True))[:80]}]")

        for anchor in row_anchors:
            title = _clean(anchor.get_text(" ", strip=True)).rstrip(".")
            href  = urljoin(base, anchor["href"])

            if not title or _is_garbage(title) or len(title.split()) < 4:
                continue

            title_lower = title.lower()

            # Skip pre-bid meeting announcements
            if any(p in title_lower for p in [
                "pre-bid meeting", "pre-bid is being held", "is holding a voluntary",
                "voluntary pre-bid", "voluntary pre bid", "pre-bid conference",
            ]):
                continue

            # NJDOT construction bids always start with an action word
            first_word = title_lower.split()[0].rstrip(",.")
            if first_word not in NJDOT_ACTION_WORDS:
                log.info(f"    SKIP (not action word '{first_word}'): {title[:60]}")
                continue

            log.info(f"    ACCEPT: {title[:80]}")

            uid = _make_id(src["id"], title, href)
            if uid in seen:
                continue
            seen.add(uid)

            items.append(_record(
                source_id    = src["id"],
                source_name  = src["name"],
                title        = title,
                url          = href,
                due_date_raw = due_date,
                record_type  = src["record_type"],
            ))

    log.info(f"  NJDOT Construction: {len(items)} records")
    return items[:60]


def parse_njdot_profserv(html: str, src: dict) -> list[dict]:
    """
    NJDOT Professional Services: simple table, TP number links go to BidX.
    Status "Advertised" = open; "Pending Selection" = closed → skip.
    We grab the solicitation text from each row and keep the BidX link.
    access_type note tells users the link forwards to BidX.
    """
    soup  = BeautifulSoup(html, "html.parser")
    base  = src["url"]
    items: list[dict] = []
    seen:  set[str]   = set()

    content = soup.find(id="content") or soup
    tables  = content.find_all("table")

    # TP numbers on the page render as "TP – 817" (en dash + spaces), not "TP-817"
    # Match: TP-817 / TP817 / TP – 817 / TP — 817 (any dash variant, optional spaces)
    TP_RE = re.compile(r"\bTP\s*[-–—]?\s*(\d+)\b", re.IGNORECASE)

    for table in tables:
        rows = table.find_all("tr")
        # Log table header for diagnostics
        if rows:
            header_cells = rows[0].find_all(["th", "td"])
            log.debug(f"  Table header: {[_clean(c.get_text(' ',strip=True))[:30] for c in header_cells]}")

        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            row_text = _clean(row.get_text(" ", strip=True))

            # Skip closed solicitations
            row_lower = row_text.lower()
            if "pending selection" in row_lower or "awarded" in row_lower or "cancelled" in row_lower:
                log.info(f"  NJDOT PS SKIP (closed): {row_text[:60]}")
                continue

            # Find the TP anchor — the link to BidX
            tp_anchor = None
            tp_num    = ""
            href      = ""

            for cell in cells:
                for a in cell.find_all("a", href=True):
                    atxt = _clean(a.get_text(" ", strip=True))
                    m = TP_RE.search(atxt)
                    if m or TP_RE.search(a["href"]):
                        tp_anchor = a
                        tp_num    = f"TP-{m.group(1)}" if m else atxt
                        href      = urljoin(base, a["href"])
                        break
                if tp_anchor:
                    break

            # Also accept rows where first cell text matches TP pattern (no link)
            if not tp_anchor:
                first_text = _clean(cells[0].get_text(" ", strip=True))
                m = TP_RE.search(first_text)
                if m:
                    tp_num = f"TP-{m.group(1)}"
                    a = cells[0].find("a", href=True)
                    href = urljoin(base, a["href"]) if a else src["url"]

            if not tp_num:
                log.info(f"  NJDOT PS SKIP (no TP): {row_text[:60]}")
                continue

            # Collect description from non-TP, non-date, non-status cells
            desc_parts: list[str] = []
            due_date = ""

            for cell in cells:
                ctext = _clean(cell.get_text(" ", strip=True))
                if not ctext:
                    continue
                if TP_RE.search(ctext) and len(ctext) < 20:
                    continue   # just the TP number cell
                if re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", ctext):
                    due_date = ctext
                    continue
                if ctext.lower() in {"advertised", "pending selection", "open",
                                     "closed", "cancelled", "awarded"}:
                    continue
                if len(ctext) >= 10:
                    desc_parts.append(ctext)

            desc  = max(desc_parts, key=len) if desc_parts else ""

            # Clean up NJDOT PS titles:
            # 1. Strip trailing "Advertised MM/DD/YYYY" — already in due_date field
            # 2. Strip leading "(N) —" sub-project number — noise in the title
            desc_clean = re.sub(r'\s+Advertised\s+\d{1,2}/\d{1,2}/\d{4}$', '', desc).strip()
            desc_clean = re.sub(r'^\(\d+\)\s*[—\-]\s*', '', desc_clean).strip()

            title = f"{tp_num} — {desc_clean}" if desc_clean else tp_num

            if _is_garbage(title) or len(title.split()) < 3:
                continue

            uid = _make_id(src["id"], title, href)
            if uid in seen:
                continue
            seen.add(uid)

            log.info(f"  NJDOT PS ACCEPT: {title[:80]} | Due: {due_date}")

            # Description: the project name (cleaned) gives users context on the detail page.
            items.append(_record(
                source_id    = src["id"],
                source_name  = src["name"],
                title        = title,
                url          = href,
                due_date_raw = due_date,
                record_type  = src["record_type"],
                access_type  = "BidX (NJDOT forwards to BidX for documents)",
                contract_number = tp_num,
                description  = desc_clean,
            ))

    log.info(f"  NJDOT ProfServ: {len(items)} records")
    return items[:60]


def parse_drjtbc_profserv(html: str, src: dict) -> list[dict]:
    """
    DRJTBC Professional Services: each solicitation is its own block/table.
    Structure per ad:
      - Starts with "Contract No. XXXX"
      - Title line
      - Paragraph description
      - PDF download link(s)
      - Sidebar dates: Posted | Pre-Proposal Meeting | Deadline for Inquiries | Solicitation Date
    "Solicitation Date" is the closing date — skip if it's before today.
    """
    soup  = BeautifulSoup(html, "html.parser")
    base  = src["url"]
    items: list[dict] = []
    seen:  set[str]   = set()
    today = date.today()

    content = (
        soup.find("div", class_=re.compile(r"entry-content|page-content|wpb_wrapper|main-content", re.I))
        or soup.find("main")
        or soup.find("article")
        or soup
    )

    # Handles "Contract No. DB-823A" and "Contract No. DB – 823A" (en-dash with spaces)
    CONTRACT_RE = re.compile(
        r"Contract\s+No\.?\s*([\w]+(?:\s*[-–—]\s*[\w]+)*)",
        re.IGNORECASE
    )

    # Solicitation Date regex — labeled field in the sidebar.
    # The page uses en-dash dates like "4 – 7 – 2026", so we normalize first.
    # Label may appear as "Solicitation Date", "Solicitation:", "Solicitation Deadline", etc.
    SOLICIT_RE = re.compile(
        r"Solicitation\s*(?:Date|Deadline|Due)?\s*[:\-–]?\s*"
        r"(\w+\.?\s+\d{1,2},?\s*\d{4}|\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})",
        re.IGNORECASE
    )

    DATE_PARSE_FMTS = [
        "%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y",
        "%B. %d, %Y", "%m/%d/%Y", "%m/%d/%y",
    ]

    def normalize_dates(text: str) -> str:
        """Convert en-dash/em-dash date formats like '4 – 7 – 2026' → '4/7/2026'."""
        return re.sub(
            r"\b(\d{1,2})\s*[–—]\s*(\d{1,2})\s*[–—]\s*(\d{4})\b",
            r"\1/\2/\3",
            text
        )

    def parse_date_str(s: str) -> date | None:
        s = s.strip().rstrip(".")
        for fmt in DATE_PARSE_FMTS:
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                pass
        return None

    # ── Strategy: find every "Contract No." element, then identify its block ──

    # Collect candidate container elements — tables, divs, sections, articles
    # that contain "Contract No." text
    BLOCK_TAGS = ["table", "div", "section", "article", "li"]

    processed_blocks: set[int] = set()  # track by id() to avoid duplicates

    for contract_node in content.find_all(string=CONTRACT_RE):
        # Walk up to find a "meaningful" block that contains date sidebar info
        block = contract_node.find_parent()
        block_text = ""

        for _ in range(12):
            if not block or block.name in ["html", "body"]:
                break
            bt = _clean(block.get_text(" ", strip=True))
            # A good block contains the solicitation date label
            if SOLICIT_RE.search(bt):
                block_text = bt
                break
            # Also stop if we've grown large enough (whole-page fallback)
            if len(bt) > 2000:
                break
            block = block.find_parent()

        if not block_text:
            # Use whatever parent we stopped at
            block_text = _clean((block or contract_node.find_parent()).get_text(" ", strip=True))

        # Normalize en-dash dates ("4 – 7 – 2026" → "4/7/2026") before regex search
        block_text_norm = normalize_dates(block_text)

        # Deduplicate blocks
        block_id = id(block)
        if block_id in processed_blocks:
            continue
        processed_blocks.add(block_id)

        # ── Solicitation Date (= closing date) ──────────────────────────────
        sol_m = SOLICIT_RE.search(block_text_norm)
        if not sol_m:
            log.info(f"  DRJTBC SKIP (no Solicitation Date label): {block_text_norm[:80]}")
            continue

        sol_date_raw = sol_m.group(1).strip()
        sol_date     = parse_date_str(sol_date_raw)

        if sol_date and sol_date < today:
            log.info(f"  DRJTBC SKIP (closed {sol_date_raw}): {block_text_norm[:60]}")
            continue

        # ── Contract number ──────────────────────────────────────────────────
        cm = CONTRACT_RE.search(block_text_norm)
        contract_no = _clean(cm.group(0)) if cm else ""

        # ── Title ────────────────────────────────────────────────────────────
        # Look for a heading tag within the block
        title = ""
        if block:
            for tag in block.find_all(["h2", "h3", "h4", "h5", "strong", "b"]):
                t = _clean(tag.get_text(" ", strip=True))
                # Skip if it's just the contract number or a sidebar label
                if len(t) < 10:
                    continue
                if CONTRACT_RE.match(t):
                    continue
                if re.match(r"^(Posted|Pre-Proposal|Deadline|Solicitation|Date)", t, re.I):
                    continue
                title = t
                break

        # Fallback: text immediately after "Contract No. XXXX" line
        if not title and cm:
            after = block_text_norm[cm.end():].lstrip(" \t:—-")
            for chunk in re.split(r"\s{3,}|\n", after):
                chunk = _clean(chunk)
                if len(chunk) >= 12 and not re.match(r"^\d{1,2}/\d", chunk):
                    title = chunk[:250]
                    break

        if not title:
            title = contract_no or "DRJTBC Solicitation"

        # Combine contract number + title
        full_title = (
            f"{contract_no} — {title}"
            if contract_no and contract_no.lower() not in title.lower()
            else title
        )

        if _is_garbage(full_title):
            continue

        # ── PDF download link ─────────────────────────────────────────────────
        pdf_url = ""
        if block:
            for a in block.find_all("a", href=True):
                a_href = urljoin(base, a["href"])
                if a_href.lower().endswith(".pdf"):
                    pdf_url = a_href
                    break

        # ── Extract paragraphs from the block for title and description ──────
        GENERIC_HEADINGS = {
            "professional services", "construction services", "current procurements",
            "procurement", "solicitation", "services",
        }

        # Collect all substantial text chunks from the block (excluding dates/labels)
        paragraphs: list[str] = []
        if block:
            for p_tag in block.find_all(["p", "div", "td", "li"]):
                p_text = _clean(p_tag.get_text(" ", strip=True))
                if len(p_text) < 20:
                    continue
                if CONTRACT_RE.match(p_text):
                    continue
                if re.match(r"^(Posted|Pre-Proposal|Deadline|Solicitation|Contract No|Download|View|Click)", p_text, re.I):
                    continue
                if p_text.lower().strip() == title.lower().strip():
                    continue
                # Skip if it's just a date
                if re.match(r"^\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}$", p_text):
                    continue
                if p_text not in paragraphs:
                    paragraphs.append(p_text[:500])

        # If heading was generic, first paragraph becomes the real title
        if title.lower().strip() in GENERIC_HEADINGS and paragraphs:
            real_title = paragraphs[0][:200]
            description = paragraphs[1][:500] if len(paragraphs) > 1 else ""
        else:
            real_title = title
            description = paragraphs[0][:500] if paragraphs else ""

        # Rebuild full title with contract number
        full_title = (
            f"{contract_no} — {real_title}"
            if contract_no and contract_no.lower() not in real_title.lower()
            else real_title
        )

        if _is_garbage(full_title):
            continue

        # Main URL: PDF if available, otherwise source page
        main_url = pdf_url if pdf_url else src["url"]

        # ── Dedup & record ────────────────────────────────────────────────────
        uid = _make_id(src["id"], full_title, main_url)
        if uid in seen:
            continue
        seen.add(uid)

        log.info(f"  DRJTBC ACCEPT: {full_title[:80]} | Solicitation: {sol_date_raw}")

        items.append(_record(
            source_id       = src["id"],
            source_name     = src["name"],
            title           = full_title,
            url             = main_url,
            due_date_raw    = sol_date_raw,
            county          = "Warren/Hunterdon/Mercer",
            record_type     = src["record_type"],
            contract_number = contract_no,
            description     = description,
            doc_url         = pdf_url,
        ))

    log.info(f"  DRJTBC ProfServ: {len(items)} records")
    return items[:10]



def parse_njta(html: str, src: dict) -> list[dict]:
    """
    NJTA Current Solicitations: standard HTML table.
    - Closing date column
    - Status column: "Open" / "Closed"
    - Project name is a hyperlink:
        "contract-no-"                  in href → construction
        "order-for-professional-services" in href → professional_services
    - Detail page is unpopulated but has a single BidX link; we store the NJTA
      detail URL and flag access_type so the detail page tells users to go to BidX.
    """
    soup  = BeautifulSoup(html, "html.parser")
    base  = src["url"]
    items: list[dict] = []
    seen:  set[str]   = set()

    PS_HREF_WORDS  = {"order-for-professional-services", "professional-services", "rfq", "rfp"}
    CON_HREF_WORDS = {"contract-no", "contract-number"}
    PS_TITLE_WORDS = {"engineer", "design", "inspection", "consulting", "planning",
                      "study", "management", "survey", "environmental", "architect"}

    content = (soup.find("main") or soup.find(id=re.compile(r"content|main", re.I))
               or soup)

    for table in content.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            # Skip obvious header rows
            row_lower = " ".join(c.get_text(strip=True) for c in cells).lower()
            if ("project" in row_lower or "solicitation" in row_lower) and "status" in row_lower and len(row_lower) < 120:
                continue

            # Skip closed rows
            status_text = cells[-1].get_text(strip=True).lower()
            if any(w in status_text for w in ("closed", "awarded", "cancelled")):
                log.info(f"  NJTA SKIP (closed): {row_lower[:60]}")
                continue

            # Find project link
            project_link = None
            title = ""
            for cell in cells:
                for a in cell.find_all("a", href=True):
                    href_l = a["href"].lower()
                    if any(w in href_l for w in CON_HREF_WORDS | PS_HREF_WORDS):
                        project_link = a
                        title = _clean(a.get_text(" ", strip=True))
                        break
                if project_link:
                    break

            # Fallback: first substantial link in row
            if not project_link:
                for cell in cells:
                    a = cell.find("a", href=True)
                    if a:
                        t = _clean(a.get_text(" ", strip=True))
                        if len(t) > 10 and not _is_garbage(t):
                            project_link = a
                            title = t
                            break

            if not title or _is_garbage(title):
                continue

            href      = urljoin(base, project_link["href"]) if project_link else src["url"]
            href_l    = href.lower()

            # Determine record type
            if any(w in href_l for w in PS_HREF_WORDS):
                record_type = "professional_services"
            elif any(w in href_l for w in CON_HREF_WORDS):
                record_type = "construction"
            else:
                record_type = "professional_services" if any(w in title.lower() for w in PS_TITLE_WORDS) else "construction"

            # Closing date
            due_date = ""
            for cell in cells:
                m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", cell.get_text())
                if m:
                    due_date = m.group(1)
                    break

            uid = _make_id(src["id"], title, href)
            if uid in seen:
                continue
            seen.add(uid)

            log.info(f"  NJTA ACCEPT: {title[:80]} | {record_type} | Due: {due_date}")
            items.append(_record(
                source_id    = src["id"],
                source_name  = src["name"],
                title        = title,
                url          = href,
                due_date_raw = due_date,
                county       = "Statewide",
                record_type  = record_type,
                access_type  = "BidX (NJTA)",
                description  = title,
            ))

    log.info(f"  NJTA: {len(items)} records")
    return items[:60]


def _parse_bidexpress(html: str, src: dict, infer_type_fn=None) -> list[dict]:
    """
    Generic BidExpress agency listing page parser.

    Target URL pattern:
        https://www.bidexpress.com/businesses/{id}/home?agency=true

    BidExpress table columns (standard agency view):
        Ad Date  |  Job No.  |  Description  |  Letting Date  |  Status

    - Only "Active" rows are kept.
    - Job No. cell contains an <a href="/businesses/{id}/projects/{pid}"> link.
    - Status cell contains plain text: Active / Awarded / Cancelled / etc.

    infer_type_fn(title, job_no) -> 'construction' | 'professional_services'
    If None, falls back to src['record_type'].
    """
    BIDX_BASE = "https://www.bidexpress.com"
    soup  = BeautifulSoup(html, "html.parser")
    items: list[dict] = []
    seen:  set[str]   = set()

    tables = soup.find_all("table")
    if not tables:
        log.warning(f"  {src['name']}: no <table> found — page may require JavaScript rendering")
        return []

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # ── Column detection from header row ─────────────────────────────────
        header_cells = rows[0].find_all(["th", "td"])
        headers = [c.get_text(strip=True).lower() for c in header_cells]
        log.info(f"  {src['name']} table headers: {headers}")

        def _col(keywords):
            return next(
                (i for i, h in enumerate(headers) if any(kw in h for kw in keywords)),
                None,
            )

        job_col    = _col(["job no", "job #", "job num", "contract no", "number", "bid no", "solicitation"])
        desc_col   = _col(["desc", "title", "project name", "project desc"])
        date_col   = _col(["letting", "closing", "due date", "event date", "bid opening", "opening", "deadline"])
        status_col = _col(["status", "phase", "state"])

        # ── Data rows ─────────────────────────────────────────────────────────
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            if all(c.name == "th" for c in cells):
                continue   # sub-header row

            row_text  = " ".join(c.get_text(" ", strip=True) for c in cells)
            row_lower = row_text.lower().strip()
            if not row_lower:
                continue

            # ── Status filter ─────────────────────────────────────────────────
            status_text = ""
            if status_col is not None and status_col < len(cells):
                status_text = cells[status_col].get_text(strip=True).lower()
            else:
                for cell in cells:
                    ct = cell.get_text(strip=True).lower()
                    if ct in ("active", "awarded", "cancelled", "closed",
                              "complete", "completed", "award pending"):
                        status_text = ct
                        break

            SKIP_STATUS = {"awarded", "cancelled", "closed", "complete",
                           "completed", "award pending"}
            if status_text in SKIP_STATUS:
                log.info(f"  {src['name']} SKIP ({status_text}): {row_lower[:60]}")
                continue

            # ── Job number + detail URL ───────────────────────────────────────
            job_no     = ""
            detail_url = src["url"]

            if job_col is not None and job_col < len(cells):
                jcell = cells[job_col]
                a = jcell.find("a", href=True)
                if a:
                    href = a["href"]
                    detail_url = (href if href.startswith("http")
                                  else urljoin(BIDX_BASE, href))
                job_no = _clean(jcell.get_text(" ", strip=True))

            if not job_no:
                # Fallback: any link to a BidExpress detail page
                for cell in cells:
                    a = cell.find("a", href=True)
                    href = a["href"] if a else ""
                    if a and any(p in href for p in ("/projects/", "/bids/", "/solicitations/")):
                        detail_url = (href if href.startswith("http")
                                      else urljoin(BIDX_BASE, href))
                        job_no = _clean(cell.get_text(" ", strip=True))
                        break

            if not job_no:
                continue

            # ── Title / description ───────────────────────────────────────────
            title = ""
            if desc_col is not None and desc_col < len(cells):
                title = _clean(cells[desc_col].get_text(" ", strip=True))[:200]

            if not title:
                for i, cell in enumerate(cells):
                    if i == job_col or i == status_col:
                        continue
                    t = _clean(cell.get_text(" ", strip=True))
                    if (len(t) > 15
                            and not re.match(r"^\d{1,2}/\d", t)
                            and t.lower() not in ("active", "awarded", "cancelled")):
                        title = t[:200]
                        break

            if not title:
                title = f"Contract {job_no}"

            if _is_garbage(title):
                continue

            # ── Due date ─────────────────────────────────────────────────────
            due_date = ""
            if date_col is not None and date_col < len(cells):
                m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b",
                              cells[date_col].get_text())
                if m:
                    due_date = m.group(1)

            if not due_date:
                for cell in cells:
                    m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", cell.get_text())
                    if m:
                        due_date = m.group(1)
                        break

            # ── Skip past records ─────────────────────────────────────────────
            if due_date:
                try:
                    parts = due_date.split("/")
                    due = date(int(parts[2]), int(parts[0]), int(parts[1]))
                    if due < date.today():
                        continue
                except Exception:
                    pass

            # ── Record type ───────────────────────────────────────────────────
            if infer_type_fn:
                record_type = infer_type_fn(title, job_no)
            else:
                record_type = src.get("record_type", "construction")

            full_title = (f"{job_no} — {title}"
                          if job_no and job_no not in title else title)

            uid = _make_id(src["id"], full_title, detail_url)
            if uid in seen:
                continue
            seen.add(uid)

            log.info(
                f"  {src['name']} ACCEPT: {full_title[:80]} "
                f"| {record_type} | Due: {due_date}"
            )
            items.append(_record(
                source_id       = src["id"],
                source_name     = src["name"],
                title           = full_title,
                url             = detail_url,
                due_date_raw    = due_date,
                county          = src.get("county", "Statewide"),
                record_type     = record_type,
                access_type     = "BidExpress",
                contract_number = job_no,
                description     = title,
            ))

    log.info(f"  {src['name']}: {len(items)} records")
    return items[:60]


def parse_njtransit(html: str, src: dict) -> list[dict]:
    """
    NJ Transit Procurement Calendar: https://www.njtransit.com/procurement/calendar

    Page structure (one row per solicitation, expand-all reveals detail):
      - Column: Event Date  → closing date
      - Column: Description cell — contains 3 paragraphs:
            Para 1:  "Description" (header label — skip)
            Para 2:  Project name  (use this as title)
            Para 3+: BidExpress boilerplate + other links (ignore)
        GOLD LINK in the cell: direct href to the actual bid document.
            "IFB" anywhere in that href  → construction
            "RFP" anywhere in that href  → professional_services
            Ignore all other links (BidExpress, other internal NJ Transit links).

    The expand-all toggle is CSS/JS only; full HTML is present in the page source.
    """
    soup  = BeautifulSoup(html, "html.parser")
    base  = src["url"]
    items: list[dict] = []
    seen:  set[str]   = set()

    # BidExpress / boilerplate link patterns to IGNORE
    IGNORE_HREF = re.compile(
        r"bidexpress\.com|njtransit\.com/procurement$|/procurement/calendar$",
        re.I,
    )
    # The gold doc link: must contain IFB or RFP (case-insensitive)
    DOC_HREF = re.compile(r"\bIFB\b|\bRFP\b|\bRFQ\b", re.I)

    def _find_doc_link(container):
        """Return (url, record_type) for the gold bid-doc link, or ('', 'construction')."""
        for a in container.find_all("a", href=True):
            href = a["href"]
            if IGNORE_HREF.search(href):
                continue
            if DOC_HREF.search(href):
                full = href if href.startswith("http") else urljoin(base, href)
                rtype = "professional_services" if re.search(r"\bRFP\b|\bRFQ\b", href, re.I) else "construction"
                return full, rtype
        return "", "construction"

    # ── Try table rows first ──────────────────────────────────────────────────
    # Some layouts use <tr> rows; others use <div> blocks.
    rows_found = 0
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            if all(c.name == "th" for c in cells):
                continue  # header row

            # ── Closing date ──────────────────────────────────────────────────
            due_date = ""
            for cell in cells:
                m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", cell.get_text())
                if m:
                    due_date = m.group(1)
                    break

            # ── Skip past records (archive entries) ───────────────────────────
            if due_date:
                try:
                    parts = due_date.split("/")
                    due = date(int(parts[2]), int(parts[0]), int(parts[1]))
                    if due < date.today():
                        continue
                except Exception:
                    pass

            # ── Find the description cell (the one with >1 paragraph or a doc link) ──
            desc_cell = None
            for cell in cells:
                if cell.find("a", href=DOC_HREF):
                    desc_cell = cell
                    break
                if len(cell.find_all(["p", "div"])) >= 2:
                    desc_cell = cell

            if desc_cell is None:
                continue

            # ── Gold doc link ─────────────────────────────────────────────────
            doc_url, record_type = _find_doc_link(desc_cell)

            # ── Project title: 2nd non-empty paragraph, skip "Description" ───
            title = ""
            paras = desc_cell.find_all(["p", "div", "li", "span"])
            for p in paras:
                t = _clean(p.get_text(" ", strip=True))
                if not t or len(t) < 6:
                    continue
                if t.lower().strip() in ("description", "project description", "title"):
                    continue
                if IGNORE_HREF.search(t):
                    continue
                if re.match(r"^\d{1,2}/\d", t):
                    continue
                title = t[:200]
                break

            # Fallback: longest cell text segment
            if not title:
                for seg in desc_cell.stripped_strings:
                    t = _clean(seg)
                    if (len(t) > 15
                            and t.lower() not in ("description",)
                            and not re.match(r"^\d{1,2}/\d", t)):
                        title = t[:200]
                        break

            if not title or _is_garbage(title):
                continue

            rows_found += 1
            uid = _make_id(src["id"], title, doc_url or base)
            if uid in seen:
                continue
            seen.add(uid)

            log.info(
                f"  NJ Transit ACCEPT: {title[:80]} "
                f"| {record_type} | Due: {due_date}"
            )
            items.append(_record(
                source_id    = src["id"],
                source_name  = src["name"],
                title        = title,
                url          = doc_url or base,
                due_date_raw = due_date,
                county       = "Statewide",
                record_type  = record_type,
                access_type  = "Public" if doc_url else "BidExpress (NJ Transit)",
                description  = title,
                doc_url      = doc_url,
            ))

    # ── Fallback: div-based layout (if no table rows found) ──────────────────
    if rows_found == 0:
        log.info("  NJ Transit: no table rows matched — trying div layout")
        for block in soup.find_all(
            "div",
            class_=re.compile(r"row|item|procure|solicitation|calendar", re.I)
        ):
            doc_url, record_type = _find_doc_link(block)
            if not doc_url:
                continue

            due_date = ""
            m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", block.get_text())
            if m:
                due_date = m.group(1)

            title = ""
            for p in block.find_all(["p", "h3", "h4", "strong", "div"]):
                t = _clean(p.get_text(" ", strip=True))
                if (len(t) > 15
                        and t.lower() not in ("description",)
                        and not re.match(r"^\d{1,2}/\d", t)
                        and not IGNORE_HREF.search(t)):
                    title = t[:200]
                    break

            if not title or _is_garbage(title):
                continue

            uid = _make_id(src["id"], title, doc_url)
            if uid in seen:
                continue
            seen.add(uid)

            log.info(
                f"  NJ Transit ACCEPT (div): {title[:80]} "
                f"| {record_type} | Due: {due_date}"
            )
            items.append(_record(
                source_id    = src["id"],
                source_name  = src["name"],
                title        = title,
                url          = doc_url,
                due_date_raw = due_date,
                county       = "Statewide",
                record_type  = record_type,
                access_type  = "Public",
                description  = title,
                doc_url      = doc_url,
            ))

    log.info(f"  NJ Transit: {len(items)} records")
    return items[:60]


def parse_sjta(html: str, src: dict) -> list[dict]:
    """
    South Jersey Transportation Authority on BidExpress (businesses/29894).
    SJTA is a highway + airport authority; type inferred from title keywords.
    """
    PS_WORDS = {
        "engineer", "engineering", "design", "inspection", "consulting",
        "planning", "study", "management", "survey", "environmental",
        "architect", "professional services",
    }

    def infer_type(title: str, job_no: str) -> str:
        tl = title.lower()
        return ("professional_services"
                if any(w in tl for w in PS_WORDS)
                else "construction")

    return _parse_bidexpress(html, src, infer_type_fn=infer_type)


# ── Parser dispatch ───────────────────────────────────────────────────────────
PARSERS = {
    "njdot_construction": parse_njdot_construction,
    "njdot_profserv":     parse_njdot_profserv,
    "drjtbc_profserv":    parse_drjtbc_profserv,
    "njta":               parse_njta,
    "njtransit":          parse_njtransit,
    "sjta":               parse_sjta,
}

# ── Data management ───────────────────────────────────────────────────────────

def load_existing() -> list[dict]:
    if not OPP_F.exists():
        return []
    try:
        with open(OPP_F, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save(records: list[dict]) -> None:
    with open(OPP_F, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=str)

def merge(existing: list[dict], fresh: list[dict]) -> list[dict]:
    """
    Merge fresh crawl results into existing records.
    - New records are added.
    - Existing records get their crawled_at and due_date_raw updated.
    - Records from non-crawled sources are preserved as-is.
    - Expired records (due date passed) are marked expired but kept.
    """
    by_id = {r["id"]: r for r in existing}
    fresh_source_ids = {r["source_id"] for r in fresh}

    # Mark stale records from crawled sources as potentially expired
    today = date.today()
    fresh_ids = {r["id"] for r in fresh}

    for rec in existing:
        if rec.get("source_id") not in fresh_source_ids:
            continue   # not touched by this crawl
        if rec["id"] in fresh_ids:
            continue   # still active — will be updated below
        # Was in a crawled source but not in fresh results → expired
        due_raw = rec.get("due_date_raw") or rec.get("due_date", "")
        if due_raw and DATE_RE.match(due_raw):
            try:
                parts = due_raw.split("/")
                due = date(int(parts[2]), int(parts[0]), int(parts[1]))
                if due < today:
                    rec["status"] = "expired"
            except Exception:
                pass

    # Merge fresh records in
    for rec in fresh:
        if rec["id"] in by_id:
            old = by_id[rec["id"]]
            # Preserve any manual overrides
            for field in ("status_override", "noise_flagged", "record_type_override"):
                if old.get(field):
                    rec[field] = old[field]
            rec["crawled_at"] = _now()
        by_id[rec["id"]] = rec

    # Sort: open first by due date, then no-date, then expired
    def sort_key(r: dict) -> tuple:
        status = r.get("status", "")
        due    = r.get("due_date_raw") or r.get("due_date", "")
        if status == "expired":
            return (2, due)
        if due and DATE_RE.match(due):
            parts = due.split("/")
            return (0, f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}")
        return (1, "9999")

    return sorted(by_id.values(), key=sort_key)


# ── Main ──────────────────────────────────────────────────────────────────────

def crawl_source(key: str, src: dict) -> list[dict]:
    log.info(f"Crawling: {src['name']}")
    if src.get("use_js"):
        html = fetch_js(src["url"], click_text=src.get("js_click"))
    else:
        html = fetch(src["url"])
    if not html:
        log.error(f"  Skipped — fetch failed")
        return []
    parser_fn = PARSERS.get(src["parser"])
    if not parser_fn:
        log.error(f"  No parser for: {src['parser']}")
        return []
    return parser_fn(html, src)


def main() -> None:
    ap = argparse.ArgumentParser(description="NJ Transportation Bids — simple crawler")
    ap.add_argument("--dry-run", action="store_true",  help="Print results, don't save")
    ap.add_argument("--source",  type=str, default="",  help="Run only one source key")
    args = ap.parse_args()

    log.info("=" * 55)
    log.info("NJ Transportation Bids — Simple Crawler")
    log.info(f"Date: {date.today()}")
    log.info("=" * 55)

    targets = (
        {args.source: SOURCES[args.source]}
        if args.source and args.source in SOURCES
        else SOURCES
    )

    if args.source and args.source not in SOURCES:
        log.error(f"Unknown source: {args.source}. Options: {list(SOURCES)}")
        sys.exit(1)

    all_fresh: list[dict] = []

    for key, src in targets.items():
        records = crawl_source(key, src)
        all_fresh.extend(records)
        time.sleep(1.5)

    log.info(f"\nTotal fresh records: {len(all_fresh)}")

    if args.dry_run:
        log.info("Dry run — not saving. Sample output:")
        for r in all_fresh[:8]:
            print(json.dumps({
                k: r.get(k)
                for k in ["title", "source_name", "record_type", "due_date_raw", "status"]
            }, indent=2))
        return

    existing = load_existing()
    log.info(f"Existing records: {len(existing)}")

    merged = merge(existing, all_fresh)
    save(merged)

    open_ct    = sum(1 for r in merged if r.get("status") == "open")
    expired_ct = sum(1 for r in merged if r.get("status") == "expired")
    log.info(f"Saved {len(merged)} total  |  Open: {open_ct}  |  Expired: {expired_ct}")
    log.info("Done.")


if __name__ == "__main__":
    main()