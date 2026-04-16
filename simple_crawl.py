"""
simple_crawl.py
---------------
Targets the 4 cleanest, highest-quality NJ transportation bid sources:

  1. NJDOT Construction Services       — HTML list, trusted, daily
  2. NJDOT Professional Services       — HTML table, trusted, daily
  3. DRJTBC Notice to Contractors      — HTML list, trusted, daily
  4. DRJTBC Current Procurements       — HTML list, trusted, daily

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
            title = f"{tp_num} — {desc}" if desc else tp_num

            if _is_garbage(title) or len(title.split()) < 3:
                continue

            uid = _make_id(src["id"], title, href)
            if uid in seen:
                continue
            seen.add(uid)

            log.info(f"  NJDOT PS ACCEPT: {title[:80]} | Due: {due_date}")

            # NJDOT PS table has no separate description column — the desc is
            # just the title text, so we don't store it to avoid redundancy.
            # next_step guides users to search BidX by TP number.
            items.append(_record(
                source_id    = src["id"],
                source_name  = src["name"],
                title        = title,
                url          = href,
                due_date_raw = due_date,
                record_type  = src["record_type"],
                access_type  = "BidX (NJDOT forwards to BidX for documents)",
                contract_number = tp_num,
                description  = "",  # no separate desc on NJDOT PS table
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

        # ── Description: first substantial paragraph in the block ─────────────
        # Also used as fallback title if the heading was too generic
        GENERIC_HEADINGS = {
            "professional services", "construction services", "current procurements",
            "procurement", "solicitation", "services",
        }
        description = ""
        if block:
            for p_tag in block.find_all(["p", "div", "td"]):
                p_text = _clean(p_tag.get_text(" ", strip=True))
                if len(p_text) < 20:
                    continue
                if CONTRACT_RE.match(p_text):
                    continue
                if re.match(r"^(Posted|Pre-Proposal|Deadline|Solicitation|Contract No)", p_text, re.I):
                    continue
                # Skip if it's just the heading we already extracted
                if p_text.lower().strip() == title.lower().strip():
                    continue
                description = p_text[:500]
                break

        # If heading was generic ("Professional Services"), use description as title instead
        if title.lower().strip() in GENERIC_HEADINGS and description:
            real_title = description[:200]
            description = ""   # don't show it twice
        else:
            real_title = title

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



# ── Parser dispatch ───────────────────────────────────────────────────────────
PARSERS = {
    "njdot_construction": parse_njdot_construction,
    "njdot_profserv":     parse_njdot_profserv,
    "drjtbc_profserv":    parse_drjtbc_profserv,
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
        time.sleep(1.5)   # polite pause between requests

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
