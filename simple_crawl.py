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
    NJDOT Professional Services: HTML table.
    Columns vary but always contain TP number (linked), discipline, description, due date.
    We find any row where first cell contains a TP-XXXXXX style number.
    """
    soup  = BeautifulSoup(html, "html.parser")
    base  = src["url"]
    items: list[dict] = []
    seen:  set[str]   = set()

    content = soup.find(id="content") or soup

    # Find all tables in content — profserv sometimes splits into multiple tables
    tables = content.find_all("table")
    if not tables:
        log.warning("  NJDOT ProfServ: no table found — scanning all links for TP numbers")
        # Fallback: scan all links in content for TP numbers
        TP_RE2 = re.compile(r"\bTP-\d+\b", re.IGNORECASE)
        for anchor in content.find_all("a", href=True):
            href  = urljoin(base, anchor["href"])
            atext = _clean(anchor.get_text(" ", strip=True))
            if TP_RE2.search(atext):
                uid = _make_id(src["id"], atext, href)
                if uid not in seen:
                    seen.add(uid)
                    items.append(_record(src["id"], src["name"], atext, href,
                                         record_type=src["record_type"]))
        log.info(f"  NJDOT ProfServ (fallback): {len(items)} records")
        return items[:60]

    # Debug: log what the first row of the first table looks like
    first_table = tables[0]
    first_rows  = first_table.find_all("tr")[:3]
    for i, row in enumerate(first_rows):
        cells = row.find_all(["td","th"])
        log.debug(f"  Table row {i}: {[_clean(c.get_text(' ',strip=True))[:40] for c in cells]}")

    TP_RE = re.compile(r"\bTP-\d+\b", re.IGNORECASE)

    for table in tables:
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            # First cell must contain a TP number
            first_text = _clean(cells[0].get_text(" ", strip=True))
            tp_match   = TP_RE.search(first_text)
            if not tp_match:
                continue
            tp_num = tp_match.group(0).upper()

            anchor = cells[0].find("a", href=True)
            if not anchor:
                continue
            href = urljoin(base, anchor["href"])

            # Scan remaining cells for description and due date
            desc     = ""
            due_date = ""
            discipline = ""

            for i, cell in enumerate(cells[1:], start=1):
                cell_text = _clean(cell.get_text(" ", strip=True))
                if not cell_text:
                    continue
                # Due date cell: matches date pattern
                if DATE_RE.match(cell_text) or re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", cell_text):
                    due_date = cell_text
                    continue
                # Discipline codes: short alphanumeric codes like "B-1 H-1"
                if re.match(r"^([A-Z]-\d+\s*)+$", cell_text):
                    discipline = cell_text
                    continue
                # Longest remaining cell is usually the description
                if len(cell_text) > len(desc) and len(cell_text) > 10:
                    desc = cell_text

            if not desc:
                desc = first_text  # fallback

            title = f"{tp_num} — {desc}"
            if discipline:
                title = f"[{discipline.strip()}] {title}"

            if _is_garbage(title):
                continue

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

    log.info(f"  NJDOT ProfServ: {len(items)} records")
    return items[:60]


def parse_drjtbc_profserv(html: str, src: dict) -> list[dict]:
    """
    DRJTBC Professional Services: WordPress page with RFP blocks.
    Each active RFP has a solicitation deadline date somewhere in its block.
    Strategy:
      1. Find all RFPFINAL.PDF links
      2. Walk UP the DOM (up to 8 levels) to find a section that contains
         a solicitation deadline date AND a heading/title
      3. Only keep records that have a deadline — filters out expired/archived RFPs
    """
    soup  = BeautifulSoup(html, "html.parser")
    base  = src["url"]
    items: list[dict] = []
    seen:  set[str]   = set()

    content = (
        soup.find("div", class_=re.compile(r"entry-content|page-content|wpb_wrapper|main", re.I))
        or soup.find("main")
        or soup.find("article")
        or soup
    )

    CONTRACT_RE = re.compile(
        r"(?:Contract|RFP|RFQ|IFB|Project)\s+No\.?\s*([\w\-]+)", re.IGNORECASE
    )
    DATE_ANY = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b")
    DEADLINE_RE = re.compile(
        r"(?:solicitation|submission|proposal|due|deadline|closing)\s+(?:date|deadline)?[:\s]*"
        r"(\d{1,2}/\d{1,2}/\d{4})",
        re.IGNORECASE
    )

    for anchor in content.find_all("a", href=True):
        href = urljoin(base, anchor["href"])

        # Match any PDF that has both "RFP" and "FINAL" in the filename
        fname_upper = href.split("/")[-1].upper()
        if not (fname_upper.endswith(".PDF") and "RFP" in fname_upper and "FINAL" in fname_upper):
            continue

        # Walk up the DOM to find a section large enough to contain
        # the full bid block (contract no + name + deadline)
        section = anchor.parent
        section_text = ""
        for _ in range(8):
            p = section.find_parent()
            if not p or p.name in ["html", "body"]:
                break
            section = p
            section_text = _clean(section.get_text(" ", strip=True))
            # Stop when we have a block with enough context
            if len(section_text) > 80 and DATE_ANY.search(section_text):
                break

        if not section_text:
            section_text = _clean(anchor.parent.get_text(" ", strip=True))

        # Only keep blocks with a solicitation deadline date
        deadline_match = DEADLINE_RE.search(section_text)
        date_match     = DATE_ANY.search(section_text)
        if not deadline_match and not date_match:
            log.info(f"  DRJTBC SKIP (no deadline): {href.split('/')[-1]}")
            continue

        due_date = (deadline_match.group(1) if deadline_match
                    else date_match.group(1) if date_match else "")

        # Find the nearest heading above the PDF link — that's the RFP name
        heading = anchor.find_previous(["h1", "h2", "h3", "h4", "h5", "strong", "b"])
        if heading:
            title = _clean(heading.get_text(" ", strip=True))
        else:
            title = ""

        # Fallback: extract first substantial non-boilerplate line from section
        if not title or len(title.split()) < 3:
            for chunk in section_text.split("  "):
                chunk = _clean(chunk)
                if len(chunk) < 10:
                    continue
                if re.match(r"^(document|download|rfp|request for proposal|click|pdf|view)", chunk, re.I):
                    continue
                if len(chunk.split()) >= 3:
                    title = chunk
                    break

        # Final fallback: clean up the PDF filename
        if not title or len(title.split()) < 2:
            fname = href.split("/")[-1]
            fname = re.sub(r"RFPFINAL\.PDF$|_RFP\.PDF$|-RFP\.PDF$|RFP_FINAL\.PDF$", "",
                           fname, flags=re.I)
            title = _clean(fname.replace("-", " ").replace("_", " "))

        if not title or _is_garbage(title):
            continue

        # Prepend contract number if found and not already in title
        contract_no = ""
        cm = CONTRACT_RE.search(section_text)
        if cm:
            contract_no = _clean(cm.group(0))
            if contract_no.lower() not in title.lower():
                title = f"{contract_no} — {title}"

        log.info(f"  DRJTBC ACCEPT: {title[:80]} | Due: {due_date}")

        uid = _make_id(src["id"], title, href)
        if uid in seen:
            continue
        seen.add(uid)

        items.append(_record(
            source_id       = src["id"],
            source_name     = src["name"],
            title           = title,
            url             = href,
            due_date_raw    = due_date,
            county          = "Warren/Hunterdon/Mercer",
            record_type     = src["record_type"],
            contract_number = contract_no,
        ))

    log.info(f"  DRJTBC ProfServ: {len(items)} records")
    return items[:10]


def parse_drjtbc(html: str, src: dict) -> list[dict]:
    """
    DRJTBC: WordPress-based site. Bid items appear as linked text in the
    main content area, often as <li> or <p> elements with PDF links.
    Strategy: find all anchors in the page body, filter aggressively
    to keep only bid-relevant links (PDFs, bid detail pages).
    """
    soup  = BeautifulSoup(html, "html.parser")
    base  = src["url"]
    items: list[dict] = []
    seen:  set[str]   = set()

    # Try multiple content selectors
    content = (
        soup.find("div", class_=re.compile(r"entry-content|page-content|wpb_wrapper|main-content", re.I))
        or soup.find("main")
        or soup.find("article")
        or soup.find("div", id=re.compile(r"content|main", re.I))
        or soup.body
        or soup
    )

    # Nav/footer words to skip
    NAV_SKIP = {
        "home", "about", "contact", "news", "events", "careers",
        "accessibility", "privacy", "sitemap", "faq", "search",
        "construction services", "professional services", "notice to contractors",
        "current procurements", "doing business", "back to top",
        "bid express", "login", "register", "skip to content",
    }

    # DRJTBC bid links are either PDFs or specific bid detail sub-pages
    BID_KEYWORDS = [
        "contract", "rfp", "rfq", "ifb", "solicitation", "bid",
        "notice to contractors", "procurement", "project", "repair",
        "bridge", "resurfacing", "drainage", "construction", "inspection",
        "engineering", "design", "services for", "services -", "no.",
    ]

    SKIP_URL_PATTERNS = [
        "/construction-services/$", "/professional-services/$",
        "/doing-business/", "/about/", "/news/", "/contact/",
        "/careers/", "/board/", "/forms/", "/maps/",
    ]

    for anchor in content.find_all("a", href=True):
        raw_href = anchor["href"]
        href     = urljoin(base, raw_href)
        title    = _clean(anchor.get_text(" ", strip=True))

        if not title or len(title) < 10:
            continue
        if title.lower().strip() in NAV_SKIP:
            continue
        if any(skip in raw_href for skip in ["#", "javascript:", "mailto:", "tel:"]):
            continue
        # Only keep links to drjtbc.org
        if "drjtbc.org" not in href:
            continue
        # Skip category/nav pages
        if href.rstrip("/") == src["url"].rstrip("/"):
            continue
        if any(re.search(p, href, re.I) for p in SKIP_URL_PATTERNS):
            continue
        # DRJTBC: only accept PDF links — their actual bid docs are always PDFs
        # HTML page links on this site are mostly nav/info pages, not bids
        if not href.lower().endswith(".pdf"):
            continue
        # Require at least 4 words for a meaningful bid title
        if len(title.split()) < 4:
            continue

        # Look for a date in the surrounding text
        due_date = ""
        for parent in [anchor.find_parent(t) for t in ["li", "p", "tr", "div"] if anchor.find_parent(t)]:
            if parent:
                nearby = _clean(parent.get_text(" ", strip=True))
                # Look for dates in various formats
                date_m = re.search(
                    r"\b(\d{1,2}/\d{1,2}/\d{4}|\w+ \d{1,2},\s*\d{4})\b",
                    nearby
                )
                if date_m:
                    due_date = date_m.group(1)
                    break

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
            county       = "Warren/Hunterdon/Mercer",
            record_type  = src["record_type"],
        ))

    log.info(f"  {src['name']}: {len(items)} records")
    return items[:40]


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
