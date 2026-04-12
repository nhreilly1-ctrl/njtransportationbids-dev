import csv
import io
import os
import re
import secrets
import time

import psycopg2
import requests
from fastapi import FastAPI, Depends, HTTPException, Query, Form, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials

app = FastAPI(title="NJ Bid Registry")
security = HTTPBasic()

NJDOT_CONSTRUCTION_URL = "https://www.nj.gov/transportation/business/procurement/ConstrServ/curradvproj.shtm"
NJDOT_PROFSERV_URL = "https://www.nj.gov/transportation/business/procurement/ProfServ/CurrentSolic.shtm"
NJTA_URL = "https://www.njta.gov/business-hub/current-solicitations/"
MONMOUTH_URL = "https://pol.co.monmouth.nj.us/"
NJTRANSIT_URL = "https://www.njtransit.com/procurement/calendar"
DRJTBC_CONSTRUCTION_URL = "https://www.drjtbc.org/construction-services/notice-to-contractors/"
DRJTBC_PROFSERV_URL = "https://www.drjtbc.org/professional-services/current/"


def get_conn():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(database_url)


def check_auth(credentials: HTTPBasicCredentials = Depends(security)):
    expected_username = os.environ.get("ADMIN_USERNAME", "")
    expected_password = os.environ.get("ADMIN_PASSWORD", "")

    username_ok = secrets.compare_digest(credentials.username, expected_username)
    password_ok = secrets.compare_digest(credentials.password, expected_password)

    if not (username_ok and password_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


def build_redirect_url(base: str, status_filter: str | None = None, q: str | None = None, sort_by: str | None = None):
    params = []
    if status_filter:
        params.append(f"status={requests.utils.quote(status_filter)}")
    if q:
        params.append(f"q={requests.utils.quote(q)}")
    if sort_by:
        params.append(f"sort_by={requests.utils.quote(sort_by)}")
    if not params:
        return base
    return f"{base}?{'&'.join(params)}"


def csv_response(filename: str, headers: list[str], rows: list[list[str]]):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(rows)
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS registry_sources (
                    id SERIAL PRIMARY KEY,
                    source_id TEXT UNIQUE NOT NULL,
                    source_name TEXT NOT NULL,
                    entity_type TEXT,
                    county TEXT,
                    source_url TEXT,
                    priority_tier TEXT,
                    website_ready TEXT,
                    crawl_enabled BOOLEAN DEFAULT FALSE,
                    crawl_method TEXT,
                    last_crawl_at TIMESTAMP NULL,
                    last_crawl_status TEXT,
                    last_leads_found INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS opportunities (
                    id SERIAL PRIMARY KEY,
                    opportunity_id TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    agency TEXT NOT NULL,
                    county TEXT,
                    source_id TEXT,
                    due_date TEXT,
                    status TEXT,
                    opportunity_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS opportunity_leads (
                    id SERIAL PRIMARY KEY,
                    lead_id TEXT UNIQUE NOT NULL,
                    source_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    agency TEXT,
                    county TEXT,
                    posted_date TEXT,
                    due_date TEXT,
                    status TEXT,
                    source_url TEXT,
                    raw_text TEXT,
                    duplicate_key TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS crawl_runs (
                    id SERIAL PRIMARY KEY,
                    crawl_run_id TEXT UNIQUE NOT NULL,
                    source_id TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    leads_found INTEGER DEFAULT 0,
                    notes TEXT,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("ALTER TABLE registry_sources ADD COLUMN IF NOT EXISTS crawl_enabled BOOLEAN DEFAULT FALSE;")
            cur.execute("ALTER TABLE registry_sources ADD COLUMN IF NOT EXISTS crawl_method TEXT;")
            cur.execute("ALTER TABLE registry_sources ADD COLUMN IF NOT EXISTS last_crawl_at TIMESTAMP NULL;")
            cur.execute("ALTER TABLE registry_sources ADD COLUMN IF NOT EXISTS last_crawl_status TEXT;")
            cur.execute("ALTER TABLE registry_sources ADD COLUMN IF NOT EXISTS last_leads_found INTEGER DEFAULT 0;")

            cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS duplicate_key TEXT;")
            cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

            cur.execute("ALTER TABLE crawl_runs ADD COLUMN IF NOT EXISTS leads_found INTEGER DEFAULT 0;")
            cur.execute("ALTER TABLE crawl_runs ADD COLUMN IF NOT EXISTS notes TEXT;")
            cur.execute("ALTER TABLE crawl_runs ADD COLUMN IF NOT EXISTS started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

            cur.execute("""
                INSERT INTO registry_sources (
                    source_id, source_name, entity_type, county, source_url,
                    priority_tier, website_ready, crawl_enabled, crawl_method
                )
                VALUES
                ('state-njdot-construction','NJDOT Construction Services','State Agency','Statewide',%s,'Tier 1','Yes',TRUE,'manual_html'),
                ('state-njdot-profserv','NJDOT Professional Services','State Agency','Statewide',%s,'Tier 1','Yes',TRUE,'manual_html'),
                ('state-njta','NJ Turnpike Authority Current Solicitations','Transportation Authority','Statewide',%s,'Tier 1','Yes',TRUE,'manual_html'),
                ('state-njtransit','NJ TRANSIT Procurement Calendar','Transit Agency','Statewide',%s,'Tier 1','Yes',TRUE,'manual_html'),
                ('state-sjta','South Jersey Transportation Authority Legal Notices','Transportation Authority','Atlantic','https://www.sjta.com/legal-notices','Tier 1','Yes',FALSE,'manual_html'),
                ('state-drjtbc-construction','DRJTBC Notice To Contractors','Bi-State Authority','Warren/Hunterdon/Mercer',%s,'Tier 1','Yes',TRUE,'manual_html'),
                ('state-drjtbc-profserv','DRJTBC Current Procurements','Bi-State Authority','Warren/Hunterdon/Mercer',%s,'Tier 1','Yes',TRUE,'manual_html'),
                ('state-panynj-construction','Port Authority Construction Opportunities','Bi-State Authority','Hudson/Essex/Union','https://www.panynj.gov/port-authority/en/business-opportunities/solicitations-advertisements/Construction.html','Tier 1','Yes',FALSE,'manual_html'),
                ('state-panynj-profserv','Port Authority Professional Services','Bi-State Authority','Hudson/Essex/Union','https://www.panynj.gov/port-authority/en/business-opportunities/solicitations-advertisements/professional-services.html','Tier 1','Yes',FALSE,'manual_html'),
                ('county-monmouth','Monmouth County Purchasing','County','Monmouth',%s,'Tier 1','Yes',TRUE,'manual_html'),
                ('county-atlantic','Atlantic County Open Bids','County','Atlantic','https://www.atlanticcountynj.gov/government/county-departments/department-of-administrative-services/division-of-budget-and-purchasing/open-bids','Tier 1','Yes',FALSE,'manual_html'),
                ('county-bergen','Bergen County Bids','County','Bergen','https://bergenbids.com/','Tier 1','Yes',FALSE,'manual_html'),
                ('county-burlington','Burlington County Bid Solicitations','County','Burlington','https://www.co.burlington.nj.us/490/Bid-Solicitations','Tier 1','Yes',FALSE,'manual_html'),
                ('county-camden','Camden County Procurements','County','Camden','https://procurements.camdencounty.com/','Tier 1','Yes',FALSE,'manual_html'),
                ('county-cape-may','Cape May County Bids and RFPs','County','Cape May','https://capemaycountynj.gov/1072/Bids-and-RFPs','Tier 2','Yes',FALSE,'manual_html'),
                ('county-cumberland','Cumberland County Bids','County','Cumberland','https://www.cumberlandcountynj.gov/bids','Tier 2','Yes',FALSE,'manual_html'),
                ('county-essex','Essex County Procurement','County','Essex','https://www.essexcountynjprocure.org/bids/search?rfp_filter_status=current','Tier 1','Yes',FALSE,'manual_html'),
                ('county-gloucester','Gloucester County Bids','County','Gloucester','https://www.gloucestercountynj.gov/Bids.aspx','Tier 2','Yes',FALSE,'manual_html'),
                ('county-hudson','Hudson County Purchasing','County','Hudson','https://www.hcnj.us/finance/purchasing/','Tier 1','Yes',FALSE,'manual_html'),
                ('county-hunterdon','Hunterdon County Bids','County','Hunterdon','https://www.co.hunterdon.nj.us/Bids.aspx','Tier 2','Yes',FALSE,'manual_html'),
                ('county-mercer','Mercer County Bidding Opportunities','County','Mercer','https://www.mercercounty.org/departments/purchasing/bidding-opportunities','Tier 1','Yes',FALSE,'manual_html'),
                ('county-middlesex','Middlesex County Improvement Authority Opportunities','County','Middlesex','https://www.middlesexcountynj.gov/government/departments/department-of-economic-development/middlesex-county-improvement-authority/current-bidding-opportunities','Tier 1','Yes',FALSE,'manual_html'),
                ('county-morris','Morris County Bids and Quotes','County','Morris','https://www.morriscountynj.gov/Departments/Purchasing/Bids-and-Quotes','Tier 1','Yes',FALSE,'manual_html'),
                ('county-ocean','Ocean County Purchasing','County','Ocean','https://www.co.ocean.nj.us/oc/purchasing/frmhomepdept.aspx','Tier 1','Yes',FALSE,'manual_html'),
                ('county-union','Union County Invitations to Bid','County','Union','https://ucnj.org/vendor-opportunities/invitations-to-bid/current/','Tier 1','Yes',FALSE,'manual_html')
                ON CONFLICT (source_id) DO UPDATE SET
                    source_name = EXCLUDED.source_name,
                    entity_type = EXCLUDED.entity_type,
                    county = EXCLUDED.county,
                    source_url = EXCLUDED.source_url,
                    priority_tier = EXCLUDED.priority_tier,
                    website_ready = EXCLUDED.website_ready,
                    crawl_enabled = EXCLUDED.crawl_enabled,
                    crawl_method = EXCLUDED.crawl_method
            """, (
                NJDOT_CONSTRUCTION_URL,
                NJDOT_PROFSERV_URL,
                NJTA_URL,
                NJTRANSIT_URL,
                DRJTBC_CONSTRUCTION_URL,
                DRJTBC_PROFSERV_URL,
                MONMOUTH_URL
            ))
        conn.commit()


def fetch_sources():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source_id, source_name, entity_type, county, source_url, priority_tier, website_ready,
                       crawl_enabled, crawl_method, last_crawl_at, last_crawl_status, last_leads_found
                FROM registry_sources
                ORDER BY source_name
                LIMIT 250
            """)
            rows = cur.fetchall()

    return [
        {
            "source_id": row[0],
            "source_name": row[1],
            "entity_type": row[2],
            "county": row[3],
            "source_url": row[4],
            "priority_tier": row[5],
            "website_ready": row[6],
            "crawl_enabled": row[7],
            "crawl_method": row[8],
            "last_crawl_at": str(row[9]) if row[9] else None,
            "last_crawl_status": row[10],
            "last_leads_found": row[11],
        }
        for row in rows
    ]


def fetch_source_map():
    return {row["source_id"]: row for row in fetch_sources()}


def fetch_enabled_crawl_sources():
    return [s for s in fetch_sources() if s["crawl_enabled"]]


def fetch_opportunities(county_filter=None, agency_filter=None, source_filter=None, q=None):
    sql = """
        SELECT opportunity_id, title, agency, county, source_id, due_date, status, opportunity_url, created_at
        FROM opportunities
        WHERE 1=1
    """
    params = []

    if county_filter:
        sql += " AND county = %s"
        params.append(county_filter)
    if agency_filter:
        sql += " AND agency = %s"
        params.append(agency_filter)
    if source_filter:
        sql += " AND source_id = %s"
        params.append(source_filter)
    if q:
        like_val = f"%{q.lower()}%"
        sql += " AND (LOWER(title) LIKE %s OR LOWER(agency) LIKE %s OR LOWER(COALESCE(county, '')) LIKE %s)"
        params.extend([like_val, like_val, like_val])

    sql += " ORDER BY created_at DESC, due_date NULLS LAST, title LIMIT 500"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

    source_map = fetch_source_map()
    return [
        {
            "opportunity_id": row[0],
            "title": row[1],
            "agency": row[2],
            "county": row[3],
            "source_id": row[4],
            "source_name": source_map.get(row[4], {}).get("source_name", row[4]),
            "due_date": row[5],
            "status": row[6],
            "opportunity_url": row[7],
            "created_at": str(row[8]),
        }
        for row in rows
    ]


def fetch_opportunity_by_id(opportunity_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT opportunity_id, title, agency, county, source_id, due_date, status, opportunity_url, created_at
                FROM opportunities
                WHERE opportunity_id = %s
            """, (opportunity_id,))
            row = cur.fetchone()

    if not row:
        return None

    source_map = fetch_source_map()
    return {
        "opportunity_id": row[0],
        "title": row[1],
        "agency": row[2],
        "county": row[3],
        "source_id": row[4],
        "source_name": source_map.get(row[4], {}).get("source_name", row[4]),
        "due_date": row[5],
        "status": row[6],
        "opportunity_url": row[7],
        "created_at": str(row[8]),
    }


def fetch_leads(status_filter=None, q=None, sort_by=None):
    sql = """
        SELECT lead_id, source_id, title, agency, county, posted_date, due_date, status, source_url, duplicate_key, created_at
        FROM opportunity_leads
        WHERE 1=1
    """
    params = []

    if status_filter and status_filter in {"New", "Promoted", "Rejected"}:
        sql += " AND status = %s"
        params.append(status_filter)

    if q:
        like_val = f"%{q.lower()}%"
        sql += " AND (LOWER(title) LIKE %s OR LOWER(COALESCE(agency,'')) LIKE %s OR LOWER(COALESCE(county,'')) LIKE %s OR LOWER(source_id) LIKE %s)"
        params.extend([like_val, like_val, like_val, like_val])

    if sort_by == "due_date":
        sql += """
            ORDER BY
                CASE WHEN due_date IS NULL OR due_date = '' THEN 1 ELSE 0 END,
                due_date,
                created_at DESC
            LIMIT 500
        """
    else:
        sql += """
            ORDER BY
                CASE
                    WHEN status = 'New' THEN 1
                    WHEN status = 'Promoted' THEN 2
                    WHEN status = 'Rejected' THEN 3
                    ELSE 4
                END,
                created_at DESC,
                title
            LIMIT 500
        """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

    source_map = fetch_source_map()
    return [
        {
            "lead_id": row[0],
            "source_id": row[1],
            "source_name": source_map.get(row[1], {}).get("source_name", row[1]),
            "title": row[2],
            "agency": row[3],
            "county": row[4],
            "posted_date": row[5],
            "due_date": row[6],
            "status": row[7],
            "source_url": row[8],
            "duplicate_key": row[9],
            "created_at": str(row[10]),
        }
        for row in rows
    ]


def fetch_crawl_runs(limit=150):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT crawl_run_id, source_id, source_name, status, leads_found, notes, started_at
                FROM crawl_runs
                ORDER BY started_at DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()

    return [
        {
            "crawl_run_id": row[0],
            "source_id": row[1],
            "source_name": row[2],
            "status": row[3],
            "leads_found": row[4],
            "notes": row[5],
            "started_at": str(row[6]),
        }
        for row in rows
    ]


def fetch_admin_summary():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM registry_sources")
            source_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM opportunities")
            opportunity_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM opportunity_leads")
            lead_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM opportunity_leads WHERE status = 'New'")
            new_lead_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM opportunity_leads WHERE status = 'Promoted'")
            promoted_lead_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM opportunity_leads WHERE status = 'Rejected'")
            rejected_lead_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM crawl_runs")
            crawl_run_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM crawl_runs WHERE status = 'Success'")
            successful_crawl_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM crawl_runs WHERE status = 'Failed'")
            failed_crawl_count = cur.fetchone()[0]

    return {
        "source_count": source_count,
        "opportunity_count": opportunity_count,
        "lead_count": lead_count,
        "new_lead_count": new_lead_count,
        "promoted_lead_count": promoted_lead_count,
        "rejected_lead_count": rejected_lead_count,
        "crawl_run_count": crawl_run_count,
        "successful_crawl_count": successful_crawl_count,
        "failed_crawl_count": failed_crawl_count,
    }


def strip_html(text):
    text = re.sub(r"<script.*?</script>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;|&#160;", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_title(title):
    title = re.sub(r"\s+", " ", title).strip()
    title = re.sub(r"\s*-\s*", " - ", title)
    return title[:220]


def make_duplicate_key(source_id, title, due_date):
    norm_title = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    norm_date = (due_date or "nodate").lower().replace(" ", "-").replace(",", "")
    return f"{source_id}|{norm_title}|{norm_date}"[:500]


def upsert_leads(source_key, source_id, agency, county, source_url, titles):
    inserted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for idx, raw_title in enumerate(titles, start=1):
                title = normalize_title(raw_title)
                lead_id = f"lead-{source_key}-{idx}"
                due_date_match = re.search(
                    r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}",
                    title,
                    flags=re.I,
                )
                due_date = due_date_match.group(0) if due_date_match else None
                duplicate_key = make_duplicate_key(source_id, title, due_date)

                cur.execute("""
                    INSERT INTO opportunity_leads (
                        lead_id, source_id, title, agency, county, posted_date, due_date, status,
                        source_url, raw_text, duplicate_key
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (lead_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        agency = EXCLUDED.agency,
                        county = EXCLUDED.county,
                        due_date = EXCLUDED.due_date,
                        status = EXCLUDED.status,
                        source_url = EXCLUDED.source_url,
                        raw_text = EXCLUDED.raw_text,
                        duplicate_key = EXCLUDED.duplicate_key
                """, (
                    lead_id, source_id, title, agency, county, None, due_date, "New",
                    source_url, title, duplicate_key
                ))
                inserted += 1
        conn.commit()

    return inserted


def log_crawl_run(source_id, source_name, status_text, leads_found, notes):
    crawl_run_id = f"crawl-{source_id}-{int(time.time())}"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO crawl_runs (
                    crawl_run_id, source_id, source_name, status, leads_found, notes
                )
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (crawl_run_id, source_id, source_name, status_text, leads_found, notes))
        conn.commit()


def update_source_crawl_status(source_id, status_text, leads_found):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE registry_sources
                SET last_crawl_at = CURRENT_TIMESTAMP,
                    last_crawl_status = %s,
                    last_leads_found = %s
                WHERE source_id = %s
            """, (status_text, leads_found, source_id))
        conn.commit()


def parse_construction_titles(cleaned):
    titles = []
    for match in re.finditer(r"(Contract [A-Z0-9\-.]+.*?)(?=Contract [A-Z0-9\-.]+|$)", cleaned, flags=re.I):
        chunk = match.group(1).strip()
        if len(chunk) > 30:
            titles.append(chunk)

    if not titles:
        for sentence in re.split(r"(?<=[.!?])\s+", cleaned):
            if "contract" in sentence.lower() or "proposal" in sentence.lower():
                if len(sentence.strip()) > 30:
                    titles.append(sentence.strip())

    deduped = []
    seen = set()
    for title in titles:
        short = normalize_title(title)
        if short and short not in seen:
            seen.add(short)
            deduped.append(short)
    return deduped[:15]


def parse_profserv_titles(cleaned):
    titles = []
    for match in re.finditer(r"(TP[-\s]?\d+.*?)(?=TP[-\s]?\d+|$)", cleaned, flags=re.I):
        chunk = match.group(1).strip()
        if len(chunk) > 30:
            titles.append(chunk)

    if not titles:
        for sentence in re.split(r"(?<=[.!?])\s+", cleaned):
            s = sentence.lower()
            if "tp-" in s or "technical proposal" in s or "professional services" in s:
                if len(sentence.strip()) > 30:
                    titles.append(sentence.strip())

    deduped = []
    seen = set()
    for title in titles:
        short = normalize_title(title)
        if short and short not in seen:
            seen.add(short)
            deduped.append(short)
    return deduped[:15]


def parse_njta_titles(cleaned):
    titles = []
    for match in re.finditer(r"((Order for Professional Services|Contract No\.|Request for Expression of Interest|Solicitation)\s+.*?)(?=(Order for Professional Services|Contract No\.|Request for Expression of Interest|Solicitation)\s+|$)", cleaned, flags=re.I):
        chunk = match.group(1).strip()
        if len(chunk) > 30:
            titles.append(chunk)

    if not titles:
        for sentence in re.split(r"(?<=[.!?])\s+", cleaned):
            s = sentence.lower()
            if "closing date" in s or "contract no." in s or "professional services" in s:
                if len(sentence.strip()) > 30:
                    titles.append(sentence.strip())

    deduped = []
    seen = set()
    for title in titles:
        short = normalize_title(title)
        if short and short not in seen:
            seen.add(short)
            deduped.append(short)
    return deduped[:15]


def parse_monmouth_titles(cleaned):
    titles = []
    for match in re.finditer(r"((Request ID|RFB|RFQ|RFP).*?)(?=(Request ID|RFB|RFQ|RFP).*?|$)", cleaned, flags=re.I):
        chunk = match.group(1).strip()
        if len(chunk) > 30:
            titles.append(chunk)

    if not titles:
        for sentence in re.split(r"(?<=[.!?])\s+", cleaned):
            s = sentence.lower()
            if "intersection improvements" in s or "county route" in s or "rfp" in s or "rfq" in s or "rfb" in s:
                if len(sentence.strip()) > 30:
                    titles.append(sentence.strip())

    deduped = []
    seen = set()
    for title in titles:
        short = normalize_title(title)
        if short and short not in seen:
            seen.add(short)
            deduped.append(short)
    return deduped[:15]


def parse_njtransit_titles(cleaned):
    titles = []
    for match in re.finditer(r"((IFB|RFP|RFQ|P\d{4,}|T\d{4,}).*?)(?=(IFB|RFP|RFQ|P\d{4,}|T\d{4,}).*?|$)", cleaned, flags=re.I):
        chunk = match.group(1).strip()
        if len(chunk) > 30:
            titles.append(chunk)

    if not titles:
        for sentence in re.split(r"(?<=[.!?])\s+", cleaned):
            s = sentence.lower()
            if "invitation for bid" in s or "request for proposals" in s or "procurement" in s:
                if len(sentence.strip()) > 30:
                    titles.append(sentence.strip())

    deduped = []
    seen = set()
    for title in titles:
        short = normalize_title(title)
        if short and short not in seen:
            seen.add(short)
            deduped.append(short)
    return deduped[:15]


def parse_drjtbc_construction_titles(cleaned):
    titles = []
    for match in re.finditer(r"((Contract|Notice To Contractors|Project)\s+.*?)(?=(Contract|Notice To Contractors|Project)\s+|$)", cleaned, flags=re.I):
        chunk = match.group(1).strip()
        if len(chunk) > 30:
            titles.append(chunk)

    if not titles:
        for sentence in re.split(r"(?<=[.!?])\s+", cleaned):
            s = sentence.lower()
            if "notice to contractors" in s or "bridge" in s or "project" in s or "contract" in s:
                if len(sentence.strip()) > 30:
                    titles.append(sentence.strip())

    deduped = []
    seen = set()
    for title in titles:
        short = normalize_title(title)
        if short and short not in seen:
            seen.add(short)
            deduped.append(short)
    return deduped[:15]


def parse_drjtbc_profserv_titles(cleaned):
    titles = []
    for match in re.finditer(r"((Professional Services|Request for Proposal|RFP|RFQ).*?)(?=(Professional Services|Request for Proposal|RFP|RFQ).*?|$)", cleaned, flags=re.I):
        chunk = match.group(1).strip()
        if len(chunk) > 30:
            titles.append(chunk)

    if not titles:
        for sentence in re.split(r"(?<=[.!?])\s+", cleaned):
            s = sentence.lower()
            if "professional services" in s or "request for proposal" in s or "rfp" in s or "rfq" in s:
                if len(sentence.strip()) > 30:
                    titles.append(sentence.strip())

    deduped = []
    seen = set()
    for title in titles:
        short = normalize_title(title)
        if short and short not in seen:
            seen.add(short)
            deduped.append(short)
    return deduped[:15]


def crawl_generic(url, parser, source_key, source_id, agency, county, source_name):
    headers = {"User-Agent": "Mozilla/5.0 NJTransportationBids/1.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        cleaned = strip_html(resp.text)
        deduped = parser(cleaned) if cleaned else []
        inserted = upsert_leads(
            source_key=source_key,
            source_id=source_id,
            agency=agency,
            county=county,
            source_url=url,
            titles=deduped,
        )
        log_crawl_run(source_id, source_name, "Success", inserted, "Manual crawl completed")
        update_source_crawl_status(source_id, "Success", inserted)
        return {"inserted": inserted, "titles": deduped}
    except Exception as e:
        log_crawl_run(source_id, source_name, "Failed", 0, str(e))
        update_source_crawl_status(source_id, "Failed", 0)
        raise


def manual_crawl_njdot_construction():
    return crawl_generic(
        NJDOT_CONSTRUCTION_URL, parse_construction_titles, "njdot-construction",
        "state-njdot-construction", "NJDOT Construction Services", "Statewide",
        "NJDOT Construction Services"
    )


def manual_crawl_njdot_profserv():
    return crawl_generic(
        NJDOT_PROFSERV_URL, parse_profserv_titles, "njdot-profserv",
        "state-njdot-profserv", "NJDOT Professional Services", "Statewide",
        "NJDOT Professional Services"
    )


def manual_crawl_njta():
    return crawl_generic(
        NJTA_URL, parse_njta_titles, "njta",
        "state-njta", "NJ Turnpike Authority Current Solicitations", "Statewide",
        "NJ Turnpike Authority Current Solicitations"
    )


def manual_crawl_monmouth():
    return crawl_generic(
        MONMOUTH_URL, parse_monmouth_titles, "monmouth",
        "county-monmouth", "Monmouth County Purchasing", "Monmouth",
        "Monmouth County Purchasing"
    )


def manual_crawl_njtransit():
    return crawl_generic(
        NJTRANSIT_URL, parse_njtransit_titles, "njtransit",
        "state-njtransit", "NJ TRANSIT Procurement Calendar", "Statewide",
        "NJ TRANSIT Procurement Calendar"
    )


def manual_crawl_drjtbc_construction():
    return crawl_generic(
        DRJTBC_CONSTRUCTION_URL, parse_drjtbc_construction_titles, "drjtbc-construction",
        "state-drjtbc-construction", "DRJTBC Notice To Contractors", "Warren/Hunterdon/Mercer",
        "DRJTBC Notice To Contractors"
    )


def manual_crawl_drjtbc_profserv():
    return crawl_generic(
        DRJTBC_PROFSERV_URL, parse_drjtbc_profserv_titles, "drjtbc-profserv",
        "state-drjtbc-profserv", "DRJTBC Current Procurements", "Warren/Hunterdon/Mercer",
        "DRJTBC Current Procurements"
    )


def run_enabled_crawlers():
    results = []
    for source in fetch_enabled_crawl_sources():
        sid = source["source_id"]
        try:
            if sid == "state-njdot-construction":
                result = manual_crawl_njdot_construction()
            elif sid == "state-njdot-profserv":
                result = manual_crawl_njdot_profserv()
            elif sid == "state-njta":
                result = manual_crawl_njta()
            elif sid == "county-monmouth":
                result = manual_crawl_monmouth()
            elif sid == "state-njtransit":
                result = manual_crawl_njtransit()
            elif sid == "state-drjtbc-construction":
                result = manual_crawl_drjtbc_construction()
            elif sid == "state-drjtbc-profserv":
                result = manual_crawl_drjtbc_profserv()
            else:
                continue

            results.append({
                "source_id": sid,
                "source_name": source["source_name"],
                "status": "Success",
                "leads_found": result["inserted"],
            })
        except Exception as e:
            results.append({
                "source_id": sid,
                "source_name": source["source_name"],
                "status": "Failed",
                "leads_found": 0,
                "error": str(e),
            })
    return results


def promote_lead(lead_id):
    bulk_update_leads([lead_id], "promote")


def bulk_update_leads(lead_ids, action):
    if not lead_ids:
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            for lead_id in lead_ids:
                if action == "promote":
                    cur.execute("""
                        SELECT lead_id, source_id, title, agency, county, due_date, source_url
                        FROM opportunity_leads
                        WHERE lead_id = %s
                    """, (lead_id,))
                    row = cur.fetchone()
                    if not row:
                        continue

                    cur.execute("""
                        SELECT COUNT(*)
                        FROM opportunities
                        WHERE source_id = %s
                          AND title = %s
                          AND COALESCE(due_date, '') = COALESCE(%s, '')
                    """, (row[1], row[2], row[5]))
                    duplicate_count = cur.fetchone()[0]

                    if duplicate_count == 0:
                        opportunity_id = f"opp-{row[0]}"
                        cur.execute("""
                            INSERT INTO opportunities (
                                opportunity_id, title, agency, county, source_id, due_date, status, opportunity_url
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (opportunity_id) DO UPDATE SET
                                title = EXCLUDED.title,
                                agency = EXCLUDED.agency,
                                county = EXCLUDED.county,
                                source_id = EXCLUDED.source_id,
                                due_date = EXCLUDED.due_date,
                                status = EXCLUDED.status,
                                opportunity_url = EXCLUDED.opportunity_url
                        """, (
                            opportunity_id, row[2], row[3] or "Unknown Agency",
                            row[4], row[1], row[5], "Open", row[6]
                        ))
                    cur.execute("UPDATE opportunity_leads SET status = 'Promoted' WHERE lead_id = %s", (lead_id,))

                elif action == "reject":
                    cur.execute("UPDATE opportunity_leads SET status = 'Rejected' WHERE lead_id = %s", (lead_id,))
                elif action == "reset":
                    cur.execute("UPDATE opportunity_leads SET status = 'New' WHERE lead_id = %s", (lead_id,))
        conn.commit()


def reject_lead(lead_id):
    bulk_update_leads([lead_id], "reject")


def reset_lead_to_new(lead_id):
    bulk_update_leads([lead_id], "reset")


@app.on_event("startup")
def startup_event():
    init_db()


@app.get("/", response_class=HTMLResponse)
def home():
    sources = fetch_sources()
    opportunities = fetch_opportunities()
    summary = fetch_admin_summary()

    return f"""
    <html><head><title>NJ Transportation Bids</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; line-height: 1.5; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 1000px; margin: 0 auto; }}
      .hero {{ background: white; border: 1px solid #e5e7eb; border-radius: 16px; padding: 28px; margin-bottom: 24px; }}
      .stats {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 18px 0; }}
      .stat {{ background: #f3f4f6; border-radius: 12px; padding: 16px; min-width: 180px; }}
      .nav {{ display: flex; gap: 12px; flex-wrap: wrap; margin-top: 20px; }}
      .nav a {{ display: inline-block; background: #0b57d0; color: white; padding: 10px 14px; border-radius: 10px; text-decoration: none; }}
      .nav a.secondary {{ background: #374151; }}
      .section {{ background: white; border: 1px solid #e5e7eb; border-radius: 16px; padding: 24px; }}
    </style></head>
    <body><div class="wrap">
      <div class="hero">
        <h1>NJ Transportation Bids</h1>
        <div class="stats">
          <div class="stat"><strong>{len(sources)}</strong><br>live source records</div>
          <div class="stat"><strong>{len(opportunities)}</strong><br>published opportunities</div>
          <div class="stat"><strong>{summary['lead_count']}</strong><br>total leads</div>
          <div class="stat"><strong>{summary['crawl_run_count']}</strong><br>crawl runs</div>
        </div>
        <div class="nav">
          <a href="/sources">View Sources</a>
          <a href="/opportunities">View Opportunities</a>
          <a href="/admin" class="secondary">Admin</a>
          <a href="/health" class="secondary">Health</a>
          <a href="/ready" class="secondary">Readiness</a>
          <a href="/export/opportunities.csv" class="secondary">Export Opportunities CSV</a>
        </div>
      </div>

      <div class="section">
        <h2>Latest big bundle</h2>
        <ul>
          <li>Filter persistence after admin actions</li>
          <li>CSV export for leads and opportunities</li>
          <li>DRJTBC construction crawler added</li>
          <li>DRJTBC professional services crawler added</li>
        </ul>
      </div>
    </div></body></html>
    """


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/ready")
def ready():
    return {"ok": True}


@app.get("/api/sources")
def api_sources():
    return JSONResponse(content=fetch_sources())


@app.get("/api/opportunities")
def api_opportunities(county: str | None = None, agency: str | None = None, source_id: str | None = None, q: str | None = None):
    return JSONResponse(content=fetch_opportunities(county, agency, source_id, q))


@app.get("/api/admin/summary")
def api_admin_summary(username: str = Depends(check_auth)):
    return JSONResponse(content=fetch_admin_summary())


@app.get("/api/admin/leads")
def api_admin_leads(
    username: str = Depends(check_auth),
    status_filter: str | None = Query(default=None, alias="status"),
    q: str | None = None,
    sort_by: str | None = None,
):
    return JSONResponse(content=fetch_leads(status_filter, q, sort_by))


@app.get("/api/admin/crawl-runs")
def api_admin_crawl_runs(username: str = Depends(check_auth)):
    return JSONResponse(content=fetch_crawl_runs())


@app.get("/export/opportunities.csv")
def export_opportunities_csv(
    county: str | None = None,
    agency: str | None = None,
    source_id: str | None = None,
    q: str | None = None,
):
    opportunities = fetch_opportunities(county, agency, source_id, q)
    rows = [
        [
            o["opportunity_id"], o["title"], o["agency"], o["county"], o["source_id"],
            o["source_name"], o["due_date"], o["status"], o["opportunity_url"], o["created_at"]
        ]
        for o in opportunities
    ]
    return csv_response(
        "opportunities.csv",
        ["opportunity_id", "title", "agency", "county", "source_id", "source_name", "due_date", "status", "opportunity_url", "created_at"],
        rows
    )


@app.get("/admin/export/leads.csv")
def export_leads_csv(
    username: str = Depends(check_auth),
    status_filter: str | None = Query(default=None, alias="status"),
    q: str | None = None,
    sort_by: str | None = None,
):
    leads = fetch_leads(status_filter, q, sort_by)
    rows = [
        [
            l["lead_id"], l["title"], l["source_id"], l["source_name"], l["agency"], l["county"],
            l["due_date"], l["status"], l["duplicate_key"], l["source_url"], l["created_at"]
        ]
        for l in leads
    ]
    return csv_response(
        "leads.csv",
        ["lead_id", "title", "source_id", "source_name", "agency", "county", "due_date", "status", "duplicate_key", "source_url", "created_at"],
        rows
    )


@app.post("/admin/crawl/njdot-construction")
def admin_crawl_njdot_construction(username: str = Depends(check_auth)):
    manual_crawl_njdot_construction()
    return RedirectResponse(url="/admin/leads", status_code=303)


@app.post("/admin/crawl/njdot-profserv")
def admin_crawl_njdot_profserv(username: str = Depends(check_auth)):
    manual_crawl_njdot_profserv()
    return RedirectResponse(url="/admin/leads", status_code=303)


@app.post("/admin/crawl/njta")
def admin_crawl_njta(username: str = Depends(check_auth)):
    manual_crawl_njta()
    return RedirectResponse(url="/admin/leads", status_code=303)


@app.post("/admin/crawl/monmouth")
def admin_crawl_monmouth(username: str = Depends(check_auth)):
    manual_crawl_monmouth()
    return RedirectResponse(url="/admin/leads", status_code=303)


@app.post("/admin/crawl/njtransit")
def admin_crawl_njtransit(username: str = Depends(check_auth)):
    manual_crawl_njtransit()
    return RedirectResponse(url="/admin/leads", status_code=303)


@app.post("/admin/crawl/drjtbc-construction")
def admin_crawl_drjtbc_construction(username: str = Depends(check_auth)):
    manual_crawl_drjtbc_construction()
    return RedirectResponse(url="/admin/leads", status_code=303)


@app.post("/admin/crawl/drjtbc-profserv")
def admin_crawl_drjtbc_profserv(username: str = Depends(check_auth)):
    manual_crawl_drjtbc_profserv()
    return RedirectResponse(url="/admin/leads", status_code=303)


@app.post("/admin/crawl/run-enabled")
def admin_run_enabled(username: str = Depends(check_auth)):
    run_enabled_crawlers()
    return RedirectResponse(url="/admin/sources", status_code=303)


@app.post("/admin/leads/{lead_id}/promote")
def admin_promote_lead(
    lead_id: str,
    username: str = Depends(check_auth),
    return_status: str | None = Form(default=None),
    return_q: str | None = Form(default=None),
    return_sort_by: str | None = Form(default=None),
):
    promote_lead(lead_id)
    return RedirectResponse(url=build_redirect_url("/admin/leads", return_status, return_q, return_sort_by), status_code=303)


@app.post("/admin/leads/{lead_id}/reject")
def admin_reject_lead(
    lead_id: str,
    username: str = Depends(check_auth),
    return_status: str | None = Form(default=None),
    return_q: str | None = Form(default=None),
    return_sort_by: str | None = Form(default=None),
):
    reject_lead(lead_id)
    return RedirectResponse(url=build_redirect_url("/admin/leads", return_status, return_q, return_sort_by), status_code=303)


@app.post("/admin/leads/{lead_id}/reset")
def admin_reset_lead(
    lead_id: str,
    username: str = Depends(check_auth),
    return_status: str | None = Form(default=None),
    return_q: str | None = Form(default=None),
    return_sort_by: str | None = Form(default=None),
):
    reset_lead_to_new(lead_id)
    return RedirectResponse(url=build_redirect_url("/admin/leads", return_status, return_q, return_sort_by), status_code=303)


@app.post("/admin/leads/bulk")
def admin_bulk_leads_action(
    username: str = Depends(check_auth),
    action: str = Form(...),
    selected_lead_ids: list[str] = Form(default=[]),
    return_status: str | None = Form(default=None),
    return_q: str | None = Form(default=None),
    return_sort_by: str | None = Form(default=None),
):
    if action in {"promote", "reject", "reset"} and selected_lead_ids:
        bulk_update_leads(selected_lead_ids, action)
    return RedirectResponse(url=build_redirect_url("/admin/leads", return_status, return_q, return_sort_by), status_code=303)


@app.get("/sources", response_class=HTMLResponse)
def sources_page():
    sources = fetch_sources()
    items = ""
    for row in sources:
        items += f"<tr><td><a href='{row['source_url']}' target='_blank'>{row['source_name']}</a></td><td>{row['entity_type'] or ''}</td><td>{row['county'] or ''}</td><td>{row['priority_tier'] or ''}</td><td>{row['website_ready'] or ''}</td></tr>"

    return f"""
    <html><head><title>Sources</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 1100px; margin: 0 auto; }}
      table {{ border-collapse: collapse; width: 100%; background: white; }}
      th, td {{ border: 1px solid #e5e7eb; padding: 10px; text-align: left; }}
      th {{ background: #f3f4f6; }}
      a {{ color: #0b57d0; text-decoration: none; }}
    </style></head>
    <body><div class="wrap">
      <a href="/">← Back to home</a>
      <h1>Registry Sources</h1>
      <p>{len(sources)} sources currently loaded</p>
      <table>
        <thead><tr><th>Source Name</th><th>Entity Type</th><th>County</th><th>Priority</th><th>Website Ready</th></tr></thead>
        <tbody>{items}</tbody>
      </table>
    </div></body></html>
    """


@app.get("/opportunities", response_class=HTMLResponse)
def opportunities_page(county: str | None = None, agency: str | None = None, source_id: str | None = None, q: str | None = None):
    opportunities = fetch_opportunities(county, agency, source_id, q)
    sources = fetch_sources()

    counties = sorted({s["county"] for s in sources if s["county"]})
    agencies = sorted({o["agency"] for o in fetch_opportunities() if o["agency"]})
    source_opts = sorted({(s["source_id"], s["source_name"]) for s in sources})

    items = ""
    for row in opportunities:
        items += f"""
        <tr>
            <td><a href="/opportunities/{row['opportunity_id']}">{row['title']}</a></td>
            <td>{row['agency'] or ''}</td>
            <td>{row['county'] or ''}</td>
            <td>{row['source_name'] or ''}</td>
            <td>{row['due_date'] or ''}</td>
            <td>{row['status'] or ''}</td>
        </tr>
        """

    county_options = "<option value=''>All counties</option>" + "".join(
        f"<option value='{c}' {'selected' if county == c else ''}>{c}</option>" for c in counties
    )
    agency_options = "<option value=''>All agencies</option>" + "".join(
        f"<option value='{a}' {'selected' if agency == a else ''}>{a}</option>" for a in agencies
    )
    source_options = "<option value=''>All sources</option>" + "".join(
        f"<option value='{sid}' {'selected' if source_id == sid else ''}>{sname}</option>" for sid, sname in source_opts
    )

    return f"""
    <html><head><title>Opportunities</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 1300px; margin: 0 auto; }}
      table {{ border-collapse: collapse; width: 100%; background: white; }}
      th, td {{ border: 1px solid #e5e7eb; padding: 10px; text-align: left; }}
      th {{ background: #f3f4f6; }}
      a {{ color: #0b57d0; text-decoration: none; }}
      .filters {{ background: white; border: 1px solid #e5e7eb; padding: 16px; border-radius: 12px; margin-bottom: 20px; }}
      .filters input, .filters select {{ margin-right: 10px; padding: 8px; }}
      .filters button {{ padding: 8px 12px; }}
      .tools a {{ display:inline-block; margin-bottom:16px; }}
    </style></head>
    <body><div class="wrap">
      <a href="/">← Back to home</a>
      <h1>Opportunities</h1>
      <p>{len(opportunities)} published opportunities currently loaded</p>

      <div class="tools">
        <a href="/export/opportunities.csv">Export opportunities CSV</a>
      </div>

      <form method="get" action="/opportunities" class="filters">
        <input type="text" name="q" placeholder="Search title / agency / county" value="{q or ''}">
        <select name="county">{county_options}</select>
        <select name="agency">{agency_options}</select>
        <select name="source_id">{source_options}</select>
        <button type="submit">Filter</button>
      </form>

      <table>
        <thead><tr><th>Title</th><th>Agency</th><th>County</th><th>Source</th><th>Due Date</th><th>Status</th></tr></thead>
        <tbody>{items}</tbody>
      </table>
    </div></body></html>
    """


@app.get("/opportunities/{opportunity_id}", response_class=HTMLResponse)
def opportunity_detail_page(opportunity_id: str):
    opp = fetch_opportunity_by_id(opportunity_id)
    if not opp:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    return f"""
    <html><head><title>{opp['title']}</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 900px; margin: 0 auto; }}
      .card {{ background: white; border: 1px solid #e5e7eb; border-radius: 16px; padding: 28px; }}
      a {{ color: #0b57d0; text-decoration: none; }}
      .row {{ margin-bottom: 12px; }}
      .label {{ font-weight: bold; display: inline-block; min-width: 140px; }}
    </style></head>
    <body><div class="wrap"><div class="card">
      <a href="/opportunities">← Back to opportunities</a>
      <h1>{opp['title']}</h1>
      <div class="row"><span class="label">Agency:</span> {opp['agency'] or ''}</div>
      <div class="row"><span class="label">County:</span> {opp['county'] or ''}</div>
      <div class="row"><span class="label">Source:</span> {opp['source_name'] or ''}</div>
      <div class="row"><span class="label">Source ID:</span> {opp['source_id'] or ''}</div>
      <div class="row"><span class="label">Due Date:</span> {opp['due_date'] or ''}</div>
      <div class="row"><span class="label">Status:</span> {opp['status'] or ''}</div>
      <div class="row"><span class="label">Published:</span> {opp['created_at']}</div>
      <div class="row"><span class="label">Original Link:</span> <a href="{opp['opportunity_url']}" target="_blank">{opp['opportunity_url']}</a></div>
    </div></div></body></html>
    """


@app.get("/admin", response_class=HTMLResponse)
def admin_page(username: str = Depends(check_auth)):
    summary = fetch_admin_summary()
    crawl_runs = fetch_crawl_runs(limit=10)

    crawl_items = "".join(
        f"<li>{row['started_at']} — {row['source_name']} — {row['status']} ({row['leads_found']} leads)</li>"
        for row in crawl_runs
    )

    return f"""
    <html><head><title>Admin</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 1200px; margin: 0 auto; }}
      .card {{ background: white; border: 1px solid #e5e7eb; border-radius: 16px; padding: 28px; }}
      .stats {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 18px 0 24px 0; }}
      .stat {{ background: #f3f4f6; border-radius: 12px; padding: 16px; min-width: 180px; }}
      .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
      .panel {{ background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 12px; padding: 18px; }}
      a {{ color: #0b57d0; text-decoration: none; }}
      .button {{ display: inline-block; background: #0b57d0; color: white; padding: 10px 14px; border: none; border-radius: 10px; cursor: pointer; margin-right:8px; margin-bottom:8px; }}
      form {{ margin: 0; display:inline-block; }}
      ul {{ padding-left: 18px; }}
    </style></head>
    <body><div class="wrap"><div class="card">
      <a href="/">← Back to home</a>
      <h1>Admin Dashboard</h1>
      <p>Signed in as <strong>{username}</strong></p>

      <div class="stats">
        <div class="stat"><strong>{summary['source_count']}</strong><br>source records</div>
        <div class="stat"><strong>{summary['opportunity_count']}</strong><br>published opportunities</div>
        <div class="stat"><strong>{summary['lead_count']}</strong><br>total leads</div>
        <div class="stat"><strong>{summary['new_lead_count']}</strong><br>new leads</div>
        <div class="stat"><strong>{summary['promoted_lead_count']}</strong><br>promoted leads</div>
        <div class="stat"><strong>{summary['rejected_lead_count']}</strong><br>rejected leads</div>
        <div class="stat"><strong>{summary['crawl_run_count']}</strong><br>crawl runs</div>
      </div>

      <div class="grid" style="margin-top:20px;">
        <div class="panel">
          <h3>Manual crawl controls</h3>
          <form action="/admin/crawl/njdot-construction" method="post"><button class="button" type="submit">Run NJDOT Construction Crawl</button></form>
          <form action="/admin/crawl/njdot-profserv" method="post"><button class="button" type="submit">Run NJDOT Professional Services Crawl</button></form>
          <form action="/admin/crawl/njta" method="post"><button class="button" type="submit">Run NJTA Crawl</button></form>
          <form action="/admin/crawl/monmouth" method="post"><button class="button" type="submit">Run Monmouth County Crawl</button></form>
          <form action="/admin/crawl/njtransit" method="post"><button class="button" type="submit">Run NJ TRANSIT Crawl</button></form>
          <form action="/admin/crawl/drjtbc-construction" method="post"><button class="button" type="submit">Run DRJTBC Construction Crawl</button></form>
          <form action="/admin/crawl/drjtbc-profserv" method="post"><button class="button" type="submit">Run DRJTBC Prof Services Crawl</button></form>
          <form action="/admin/crawl/run-enabled" method="post"><button class="button" type="submit">Run All Enabled Crawlers</button></form>
        </div>
        <div class="panel"><h3>Latest crawl runs</h3><ul>{crawl_items}</ul></div>
      </div>

      <h3>Admin links</h3>
      <p><a href="/admin/sources">View source crawl status page</a></p>
      <p><a href="/admin/leads">View admin leads page</a></p>
      <p><a href="/admin/export/leads.csv">Export leads CSV</a></p>
      <p><a href="/api/admin/summary">Admin summary JSON</a></p>
      <p><a href="/api/admin/leads">Admin leads JSON</a></p>
      <p><a href="/api/admin/crawl-runs">Admin crawl runs JSON</a></p>
    </div></div></body></html>
    """


@app.get("/admin/sources", response_class=HTMLResponse)
def admin_sources_page(username: str = Depends(check_auth)):
    sources = fetch_sources()

    items = ""
    for row in sources:
        crawl_enabled_text = "Yes" if row["crawl_enabled"] else "No"
        items += f"""
        <tr>
            <td>{row['source_name']}</td>
            <td>{row['source_id']}</td>
            <td>{row['county'] or ''}</td>
            <td>{crawl_enabled_text}</td>
            <td>{row['crawl_method'] or ''}</td>
            <td>{row['last_crawl_at'] or ''}</td>
            <td>{row['last_crawl_status'] or ''}</td>
            <td>{row['last_leads_found'] or 0}</td>
        </tr>
        """

    return f"""
    <html><head><title>Admin Sources</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 1400px; margin: 0 auto; }}
      table {{ border-collapse: collapse; width: 100%; background: white; }}
      th, td {{ border: 1px solid #e5e7eb; padding: 10px; text-align: left; }}
      th {{ background: #f3f4f6; }}
      a {{ color: #0b57d0; text-decoration: none; }}
    </style></head>
    <body><div class="wrap">
      <a href="/admin">← Back to admin</a>
      <h1>Source Crawl Status</h1>
      <table>
        <thead><tr><th>Source Name</th><th>Source ID</th><th>County</th><th>Crawl Enabled</th><th>Crawl Method</th><th>Last Crawl At</th><th>Last Status</th><th>Last Leads Found</th></tr></thead>
        <tbody>{items}</tbody>
      </table>
    </div></body></html>
    """


@app.get("/admin/leads", response_class=HTMLResponse)
def admin_leads_page(
    username: str = Depends(check_auth),
    status_filter: str | None = Query(default=None, alias="status"),
    q: str | None = None,
    sort_by: str | None = None,
):
    leads = fetch_leads(status_filter, q, sort_by)
    summary = fetch_admin_summary()

    current_status = status_filter or ""
    current_q = q or ""
    current_sort = sort_by or ""

    items = ""
    for row in leads:
        if row["status"] == "New":
            action_html = f"""
                <button type="submit" formaction="/admin/leads/{row['lead_id']}/promote" formmethod="post" style="background:#0b57d0;color:white;border:none;padding:8px 10px;border-radius:8px;cursor:pointer;margin-right:6px;">Promote</button>
                <button type="submit" formaction="/admin/leads/{row['lead_id']}/reject" formmethod="post" style="background:#b91c1c;color:white;border:none;padding:8px 10px;border-radius:8px;cursor:pointer;">Reject</button>
            """
        else:
            action_html = f"""
                <button type="submit" formaction="/admin/leads/{row['lead_id']}/reset" formmethod="post" style="background:#374151;color:white;border:none;padding:8px 10px;border-radius:8px;cursor:pointer;">Reset to New</button>
            """

        items += f"""
        <tr>
            <td><input type="checkbox" name="selected_lead_ids" value="{row['lead_id']}"></td>
            <td>{row['title']}</td>
            <td>{row['source_name']}</td>
            <td>{row['lead_id']}</td>
            <td>{row['agency'] or ''}</td>
            <td>{row['county'] or ''}</td>
            <td>{row['due_date'] or ''}</td>
            <td>{row['status'] or ''}</td>
            <td>{row['duplicate_key'] or ''}</td>
            <td><a href="{row['source_url']}" target="_blank">source</a></td>
            <td>{action_html}</td>
        </tr>
        """

    current_label = status_filter if status_filter else "All"
    export_url = build_redirect_url("/admin/export/leads.csv", status_filter, q, sort_by)

    return f"""
    <html><head><title>Admin Leads</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 1600px; margin: 0 auto; }}
      table {{ border-collapse: collapse; width: 100%; background: white; }}
      th, td {{ border: 1px solid #e5e7eb; padding: 10px; text-align: left; vertical-align: top; }}
      th {{ background: #f3f4f6; }}
      .filters a {{
        display:inline-block; margin-right:8px; margin-bottom:8px; padding:8px 12px;
        border-radius:8px; background:#e5e7eb; color:#111827; text-decoration:none;
      }}
      .searchbar {{ background:white; border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin-bottom:16px; }}
      .searchbar input, .searchbar select {{ margin-right:8px; padding:8px; }}
      .bulkbar {{ background:white; border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin-bottom:16px; }}
      .bulkbar button {{ margin-right:8px; padding:8px 12px; }}
      a {{ color: #0b57d0; text-decoration: none; }}
    </style></head>
    <body><div class="wrap">
      <a href="/admin">← Back to admin</a>
      <h1>Admin Leads</h1>
      <p>{len(leads)} leads currently shown — filter: {current_label}</p>

      <div class="filters">
        <a href="/admin/leads">All ({summary['lead_count']})</a>
        <a href="/admin/leads?status=New">New ({summary['new_lead_count']})</a>
        <a href="/admin/leads?status=Promoted">Promoted ({summary['promoted_lead_count']})</a>
        <a href="/admin/leads?status=Rejected">Rejected ({summary['rejected_lead_count']})</a>
        <a href="{export_url}">Export filtered leads CSV</a>
      </div>

      <form method="get" action="/admin/leads" class="searchbar">
        <input type="text" name="q" placeholder="Search title / source / agency / county" value="{current_q}">
        <select name="status">
          <option value="" {"selected" if not current_status else ""}>All statuses</option>
          <option value="New" {"selected" if current_status == "New" else ""}>New</option>
          <option value="Promoted" {"selected" if current_status == "Promoted" else ""}>Promoted</option>
          <option value="Rejected" {"selected" if current_status == "Rejected" else ""}>Rejected</option>
        </select>
        <select name="sort_by">
          <option value="" {"selected" if not current_sort else ""}>Default sort</option>
          <option value="due_date" {"selected" if current_sort == "due_date" else ""}>Sort by due date</option>
        </select>
        <button type="submit">Apply</button>
      </form>

      <form method="post" action="/admin/leads/bulk">
        <input type="hidden" name="return_status" value="{current_status}">
        <input type="hidden" name="return_q" value="{current_q}">
        <input type="hidden" name="return_sort_by" value="{current_sort}">

        <div class="bulkbar">
          <button type="submit" name="action" value="promote">Bulk Promote</button>
          <button type="submit" name="action" value="reject">Bulk Reject</button>
          <button type="submit" name="action" value="reset">Bulk Reset to New</button>
        </div>

        <table>
          <thead>
            <tr>
              <th>Select</th>
              <th>Title</th>
              <th>Source</th>
              <th>Lead ID</th>
              <th>Agency</th>
              <th>County</th>
              <th>Due Date</th>
              <th>Status</th>
              <th>Duplicate Key</th>
              <th>Link</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>{items}</tbody>
        </table>
      </form>
    </div></body></html>
    """
