import csv
import hashlib
import io
import os
import re
import secrets
import time
from urllib.parse import urljoin

import psycopg2
import requests
from bs4 import BeautifulSoup
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
DOS_PUBLIC_NOTICES_URL = "https://www.nj.gov/state/dos-public-notices.shtml"
DOS_LEGAL_NOTICES_URL = "https://www.nj.gov/state/dos-legal-notices.shtml"
TREASURY_LEGAL_NOTICES_URL = "https://www.nj.gov/treasury/legalnotices.shtml"
NJDOT_PROCUREMENT_NOTICES_URL = "https://nj.gov/transportation/business/procurement/notices.shtm"

ACCESS_TYPE_OPTIONS = [
    "Public access",
    "Free registration required",
    "Login required",
    "Document fee possible",
    "Platform submission required",
    "Limited public details",
    "Unknown",
]

PLATFORM_NAME_OPTIONS = [
    "Agency website",
    "NJDOT website",
    "NJDOS legal notices portal",
    "NJTA procurement portal",
    "NJ TRANSIT procurement portal",
    "DRJTBC procurement portal",
    "County procurement portal",
    "BidNet",
    "QuestCDN",
    "Bonfire",
    "Bid Express",
    "Unknown",
]

PUBLIC_NOTICE_TRANSPORTATION_TERMS = [
    "transportation",
    "transit",
    "bridge",
    "bridges",
    "roadway",
    "road",
    "roads",
    "highway",
    "paving",
    "resurfacing",
    "milling",
    "drainage",
    "traffic signal",
    "guide rail",
    "streetscape",
    "sidewalk",
    "intersection",
    "culvert",
    "airport",
    "rail",
    "bus",
    "marine terminal",
    "port authority",
    "turnpike",
]

PUBLIC_NOTICE_CONSTRUCTION_TERMS = [
    "construction",
    "contractor",
    "public works",
    "improvements",
    "rehabilitation",
    "reconstruction",
    "repair",
    "installation",
    "replacement",
    "maintenance",
    "bid",
    "ifb",
    "invitation for bids",
    "request for bids",
    "rfb",
]

PUBLIC_NOTICE_PROFESSIONAL_TERMS = [
    "professional services",
    "engineering",
    "design services",
    "construction inspection",
    "construction management",
    "cei",
    "surveying",
    "environmental services",
    "architectural services",
    "rfp",
    "rfq",
    "request for proposals",
    "request for qualifications",
]

PUBLIC_NOTICE_NEGATIVE_TERMS = [
    "election",
    "zoning board",
    "planning board",
    "tax sale",
    "foreclosure",
    "hearing",
    "ordinance",
    "rulemaking",
    "medicaid",
    "healthcare",
    "hospital",
    "school board",
    "curriculum",
    "lottery",
    "liquor",
    "marriage",
]

PUBLIC_NOTICE_SOURCES = [
    {
        "source_key": "dos-public-notices",
        "source_id": "state-dos-public-notices",
        "source_name": "NJ Department of State Public Notices",
        "agency": "NJ Department of State Public Notices",
        "county": "Statewide",
        "url": DOS_PUBLIC_NOTICES_URL,
    },
    {
        "source_key": "dos-legal-notices",
        "source_id": "state-dos-legal-notices",
        "source_name": "NJ Department of State Legal Notices",
        "agency": "NJ Department of State Legal Notices",
        "county": "Statewide",
        "url": DOS_LEGAL_NOTICES_URL,
    },
    {
        "source_key": "treasury-legal-notices",
        "source_id": "state-treasury-legal-notices",
        "source_name": "NJ Treasury Legal Notices",
        "agency": "NJ Treasury Legal Notices",
        "county": "Statewide",
        "url": TREASURY_LEGAL_NOTICES_URL,
    },
    {
        "source_key": "njdot-procurement-notices",
        "source_id": "state-njdot-procurement-notices",
        "source_name": "NJDOT Procurement Notices",
        "agency": "NJDOT Procurement Notices",
        "county": "Statewide",
        "url": NJDOT_PROCUREMENT_NOTICES_URL,
    },
]


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


def build_redirect_url(
    base: str,
    status_filter: str | None = None,
    q: str | None = None,
    sort_by: str | None = None,
    source_id: str | None = None,
    public_notice_only: bool = False,
):
    params = []
    if status_filter:
        params.append(f"status={requests.utils.quote(status_filter)}")
    if q:
        params.append(f"q={requests.utils.quote(q)}")
    if sort_by:
        params.append(f"sort_by={requests.utils.quote(sort_by)}")
    if source_id:
        params.append(f"source_id={requests.utils.quote(source_id)}")
    if public_notice_only:
        params.append("public_notice_only=true")
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


def get_public_notice_source_ids() -> list[str]:
    return [source["source_id"] for source in PUBLIC_NOTICE_SOURCES]


def normalize_for_rules(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip().lower()


def classify_access_guidance(source_id: str, title: str | None, raw_text: str | None) -> dict:
    result = get_source_defaults(source_id).copy()

    haystack = normalize_for_rules(f"{title or ''} {raw_text or ''}")

    if "bid express" in haystack or "bidexpress" in haystack:
        result["platform_name"] = "Bid Express"
        result["access_type"] = "Platform submission required"
        result["next_step"] = "Open the Bid Express workflow and follow the solicitation instructions"
        result["docs_path_note"] = "Documents and submission steps may be handled through Bid Express"
        result["addenda_note"] = "Monitor the Bid Express workflow and official source for updates"

    elif "questcdn" in haystack or "ebiddoc" in haystack:
        result["platform_name"] = "QuestCDN"
        result["access_type"] = "Document fee possible"
        result["next_step"] = "Open the QuestCDN project page and download the eBidDoc"
        result["docs_path_note"] = "Bid documents may require downloading the eBidDoc through QuestCDN"
        result["addenda_note"] = "Check QuestCDN and the official source for addenda"

    elif "bonfire" in haystack:
        result["platform_name"] = "Bonfire"
        result["access_type"] = "Free registration required"
        result["next_step"] = "Open the Bonfire opportunity and follow the vendor submission workflow"
        result["docs_path_note"] = "Documents and submission details may be housed in Bonfire"
        result["addenda_note"] = "Monitor Bonfire and the official source for updates"

    elif "bidnet" in haystack or "bid net" in haystack:
        result["platform_name"] = "BidNet"
        result["access_type"] = "Free registration required"
        result["next_step"] = "Open the BidNet posting and review vendor access requirements"
        result["docs_path_note"] = "Documents and opportunity details may require BidNet access"
        result["addenda_note"] = "Check BidNet and the official source for updates"

    if "login required" in haystack or "log in" in haystack or "sign in" in haystack:
        result["access_type"] = "Login required"

    elif "register" in haystack or "vendor registration" in haystack or "create account" in haystack:
        if result["access_type"] not in {"Document fee possible", "Platform submission required"}:
            result["access_type"] = "Free registration required"

    if ("fee" in haystack or "purchase" in haystack or "cost" in haystack) and (
        "document" in haystack or "bid doc" in haystack or "ebiddoc" in haystack
    ):
        result["access_type"] = "Document fee possible"
        if result["platform_name"] == "Unknown":
            result["docs_path_note"] = "Document download may require payment or purchase"
            result["next_step"] = "Open the official posting and confirm document download requirements"

    if "planholder" in haystack or "plan holder" in haystack:
        result["addenda_note"] = "Registered planholders may receive addenda notices; check the official source before bidding"

    elif "addenda" in haystack:
        result["addenda_note"] = "Check the official posting and procurement workflow for addenda before bidding"

    if "download specifications" in haystack or "download documents" in haystack or "download bid documents" in haystack:
        result["docs_path_note"] = "Bid documents appear to be downloadable from the official posting or linked platform"

    if "proposal" in haystack and "professional services" in haystack and result["next_step"] == "":
        result["next_step"] = "Open the official solicitation and review proposal requirements"

    return result


def get_source_defaults(source_id: str) -> dict:
    defaults = {
        "access_type": "Unknown",
        "platform_name": "Unknown",
        "next_step": "Open the official source and follow the solicitation instructions",
        "docs_path_note": "See the official source for bid documents and instructions",
        "addenda_note": "Check the official source for updates or addenda",
    }

    mapping = {
        "state-njdot-construction": {
            "access_type": "Public access",
            "platform_name": "NJDOT website",
            "next_step": "Open the official posting and review bid documents",
            "docs_path_note": "Bid documents are typically linked from the NJDOT solicitation page",
            "addenda_note": "Check the official NJDOT posting for addenda before bidding",
        },
        "state-njdot-profserv": {
            "access_type": "Public access",
            "platform_name": "NJDOT website",
            "next_step": "Open the official solicitation and review proposal requirements",
            "docs_path_note": "Professional services materials are typically linked from the official page",
            "addenda_note": "Monitor the official posting for updates or addenda",
        },
        "state-njta": {
            "access_type": "Public access",
            "platform_name": "NJTA procurement portal",
            "next_step": "Open the official solicitation page and review documents",
            "docs_path_note": "Documents and instructions are posted through the NJTA solicitation workflow",
            "addenda_note": "Check the official NJTA posting for updates",
        },
        "state-dos-public-notices": {
            "access_type": "Public access",
            "platform_name": "NJDOS legal notices portal",
            "next_step": "Open the notice, identify the issuing agency, and follow the linked procurement instructions",
            "docs_path_note": "Bid documents may be linked in the notice itself or on the issuing agency website",
            "addenda_note": "Monitor the legal notice and issuing agency for updates or addenda",
        },
        "state-dos-legal-notices": {
            "access_type": "Public access",
            "platform_name": "NJDOS legal notices portal",
            "next_step": "Open the legal notice and follow the issuing agency's procurement instructions",
            "docs_path_note": "Supporting bid documents may be linked directly in the notice or on the agency website",
            "addenda_note": "Check the issuing agency and legal notice posting for updates",
        },
        "state-treasury-legal-notices": {
            "access_type": "Public access",
            "platform_name": "Agency website",
            "next_step": "Open the treasury notice and follow the linked procurement or PM&C instructions",
            "docs_path_note": "Project documents may live on Treasury, PM&C, or a linked procurement page",
            "addenda_note": "Monitor the treasury notice page and linked procurement source for updates",
        },
        "state-njdot-procurement-notices": {
            "access_type": "Public access",
            "platform_name": "NJDOT website",
            "next_step": "Open the NJDOT procurement notice and review linked solicitation details",
            "docs_path_note": "Notice details and solicitation materials are typically linked from the NJDOT procurement page",
            "addenda_note": "Check the NJDOT notice page and linked solicitation for updates",
        },
        "state-njtransit": {
            "access_type": "Platform submission required",
            "platform_name": "NJ TRANSIT procurement portal",
            "next_step": "Open the procurement calendar and follow the linked solicitation workflow",
            "docs_path_note": "Documents may be accessed through the linked procurement platform",
            "addenda_note": "Monitor the official procurement workflow for updates",
        },
        "state-drjtbc-construction": {
            "access_type": "Public access",
            "platform_name": "DRJTBC procurement portal",
            "next_step": "Open the official DRJTBC posting and review bid documents",
            "docs_path_note": "Construction documents are typically linked from the official DRJTBC notice page",
            "addenda_note": "Check the official posting for addenda",
        },
        "state-drjtbc-profserv": {
            "access_type": "Public access",
            "platform_name": "DRJTBC procurement portal",
            "next_step": "Open the official DRJTBC professional services posting",
            "docs_path_note": "Proposal documents and requirements are typically linked from the official page",
            "addenda_note": "Check the official posting for updates",
        },
        "county-monmouth": {
            "access_type": "Limited public details",
            "platform_name": "County procurement portal",
            "next_step": "Open the official county posting and follow the procurement instructions",
            "docs_path_note": "Document access may depend on the county procurement system",
            "addenda_note": "Check the official source for updates and addenda",
        },
    }

    defaults.update(mapping.get(source_id, {}))
    return defaults


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
                    access_type TEXT,
                    platform_name TEXT,
                    next_step TEXT,
                    docs_path_note TEXT,
                    addenda_note TEXT,
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
                    possible_duplicate BOOLEAN DEFAULT FALSE,
                    quality_score INTEGER DEFAULT 0,
                    admin_notes TEXT,
                    access_type TEXT,
                    platform_name TEXT,
                    next_step TEXT,
                    docs_path_note TEXT,
                    addenda_note TEXT,
                    access_notes TEXT,
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
            cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS possible_duplicate BOOLEAN DEFAULT FALSE;")
            cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS quality_score INTEGER DEFAULT 0;")
            cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS admin_notes TEXT;")
            cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS access_type TEXT;")
            cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS platform_name TEXT;")
            cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS next_step TEXT;")
            cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS docs_path_note TEXT;")
            cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS addenda_note TEXT;")
            cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS access_notes TEXT;")

            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS access_type TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS platform_name TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS next_step TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS docs_path_note TEXT;")
            cur.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS addenda_note TEXT;")

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
                ('state-dos-public-notices','NJ Department of State Public Notices','State Agency','Statewide',%s,'Tier 1','Yes',TRUE,'manual_html'),
                ('state-dos-legal-notices','NJ Department of State Legal Notices','State Agency','Statewide',%s,'Tier 1','Yes',TRUE,'manual_html'),
                ('state-treasury-legal-notices','NJ Treasury Legal Notices','State Agency','Statewide',%s,'Tier 1','Yes',TRUE,'manual_html'),
                ('state-njdot-procurement-notices','NJDOT Procurement Notices','State Agency','Statewide',%s,'Tier 1','Yes',TRUE,'manual_html'),
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
                DOS_PUBLIC_NOTICES_URL,
                DOS_LEGAL_NOTICES_URL,
                TREASURY_LEGAL_NOTICES_URL,
                NJDOT_PROCUREMENT_NOTICES_URL,
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


def fetch_recent_opportunities(limit=8):
    return fetch_opportunities()[:limit]


def fetch_source_detail(source_id: str):
    source_map = fetch_source_map()
    source = source_map.get(source_id)
    if not source:
        return None

    defaults = get_source_defaults(source_id)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM opportunity_leads WHERE source_id = %s", (source_id,))
            lead_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM opportunity_leads WHERE source_id = %s AND status = 'New'", (source_id,))
            new_lead_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM opportunities WHERE source_id = %s", (source_id,))
            opportunity_count = cur.fetchone()[0]

            cur.execute("""
                SELECT lead_id, title, due_date, status, access_type, platform_name, created_at
                FROM opportunity_leads
                WHERE source_id = %s
                ORDER BY created_at DESC
                LIMIT 12
            """, (source_id,))
            recent_leads = cur.fetchall()

            cur.execute("""
                SELECT opportunity_id, title, due_date, status, access_type, platform_name, created_at
                FROM opportunities
                WHERE source_id = %s
                ORDER BY created_at DESC
                LIMIT 12
            """, (source_id,))
            recent_opps = cur.fetchall()

    return {
        "source": source,
        "defaults": defaults,
        "lead_count": lead_count,
        "new_lead_count": new_lead_count,
        "opportunity_count": opportunity_count,
        "recent_leads": [
            {
                "lead_id": row[0],
                "title": row[1],
                "due_date": row[2],
                "status": row[3],
                "access_type": row[4],
                "platform_name": row[5],
                "created_at": str(row[6]),
            }
            for row in recent_leads
        ],
        "recent_opportunities": [
            {
                "opportunity_id": row[0],
                "title": row[1],
                "due_date": row[2],
                "status": row[3],
                "access_type": row[4],
                "platform_name": row[5],
                "created_at": str(row[6]),
            }
            for row in recent_opps
        ],
    }


def fetch_opportunities(county_filter=None, agency_filter=None, source_filter=None, q=None, access_filter=None, platform_filter=None):
    sql = """
        SELECT opportunity_id, title, agency, county, source_id, due_date, status, opportunity_url,
               access_type, platform_name, next_step, docs_path_note, addenda_note, created_at
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
    if access_filter:
        sql += " AND access_type = %s"
        params.append(access_filter)
    if platform_filter:
        sql += " AND platform_name = %s"
        params.append(platform_filter)
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
            "access_type": row[8],
            "platform_name": row[9],
            "next_step": row[10],
            "docs_path_note": row[11],
            "addenda_note": row[12],
            "created_at": str(row[13]),
        }
        for row in rows
    ]


def fetch_opportunity_by_id(opportunity_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT opportunity_id, title, agency, county, source_id, due_date, status, opportunity_url,
                       access_type, platform_name, next_step, docs_path_note, addenda_note, created_at
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
        "access_type": row[8],
        "platform_name": row[9],
        "next_step": row[10],
        "docs_path_note": row[11],
        "addenda_note": row[12],
        "created_at": str(row[13]),
    }


def fetch_leads(status_filter=None, q=None, sort_by=None, duplicates_only=False, source_filter=None, public_notice_only=False):
    sql = """
        SELECT lead_id, source_id, title, agency, county, posted_date, due_date, status,
               source_url, duplicate_key, possible_duplicate, quality_score, admin_notes,
               access_type, platform_name, next_step, docs_path_note, addenda_note, access_notes, created_at
        FROM opportunity_leads
        WHERE 1=1
    """
    params = []

    if status_filter and status_filter in {"New", "Promoted", "Rejected"}:
        sql += " AND status = %s"
        params.append(status_filter)

    if duplicates_only:
        sql += " AND possible_duplicate = TRUE"

    if public_notice_only:
        notice_source_ids = get_public_notice_source_ids()
        placeholders = ", ".join(["%s"] * len(notice_source_ids))
        sql += f" AND source_id IN ({placeholders})"
        params.extend(notice_source_ids)

    if source_filter:
        sql += " AND source_id = %s"
        params.append(source_filter)

    if q:
        like_val = f"%{q.lower()}%"
        sql += " AND (LOWER(title) LIKE %s OR LOWER(COALESCE(agency,'')) LIKE %s OR LOWER(COALESCE(county,'')) LIKE %s OR LOWER(source_id) LIKE %s OR LOWER(COALESCE(admin_notes,'')) LIKE %s)"
        params.extend([like_val, like_val, like_val, like_val, like_val])

    if sort_by == "due_date":
        sql += """
            ORDER BY
                CASE WHEN due_date IS NULL OR due_date = '' THEN 1 ELSE 0 END,
                due_date,
                created_at DESC
            LIMIT 500
        """
    elif sort_by == "quality":
        sql += " ORDER BY quality_score DESC, created_at DESC LIMIT 500"
    else:
        sql += """
            ORDER BY
                CASE
                    WHEN status = 'New' THEN 1
                    WHEN status = 'Promoted' THEN 2
                    WHEN status = 'Rejected' THEN 3
                    ELSE 4
                END,
                possible_duplicate DESC,
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
            "possible_duplicate": row[10],
            "quality_score": row[11],
            "admin_notes": row[12] or "",
            "access_type": row[13],
            "platform_name": row[14],
            "next_step": row[15],
            "docs_path_note": row[16],
            "addenda_note": row[17],
            "access_notes": row[18] or "",
            "created_at": str(row[19]),
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
            cur.execute("SELECT COUNT(*) FROM opportunity_leads WHERE possible_duplicate = TRUE")
            duplicate_lead_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM crawl_runs")
            crawl_run_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM crawl_runs WHERE status = 'Success'")
            successful_crawl_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM crawl_runs WHERE status = 'Failed'")
            failed_crawl_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM opportunity_leads WHERE access_type IS NOT NULL AND access_type <> ''")
            access_populated_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM opportunities WHERE access_type IS NOT NULL AND access_type <> ''")
            opportunity_access_count = cur.fetchone()[0]

    return {
        "source_count": source_count,
        "opportunity_count": opportunity_count,
        "lead_count": lead_count,
        "new_lead_count": new_lead_count,
        "promoted_lead_count": promoted_lead_count,
        "rejected_lead_count": rejected_lead_count,
        "duplicate_lead_count": duplicate_lead_count,
        "crawl_run_count": crawl_run_count,
        "successful_crawl_count": successful_crawl_count,
        "failed_crawl_count": failed_crawl_count,
        "access_populated_count": access_populated_count,
        "opportunity_access_count": opportunity_access_count,
    }


def strip_html(text):
    text = re.sub(r"<script.*?</script>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;|&#160;", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def cleanup_title(title):
    title = re.sub(r"\s+", " ", title).strip()
    title = re.sub(r"\s*-\s*", " - ", title)
    title = re.sub(r"^(ADVERTISEMENT|NOTICE|SOLICITATION)\s*[:\-]\s*", "", title, flags=re.I)
    title = re.sub(r"\bCLICK HERE\b", "", title, flags=re.I)
    title = re.sub(r"\bMORE INFO\b", "", title, flags=re.I)
    title = re.sub(r"\bOPENING DATE\b", "Opening Date", title, flags=re.I)
    title = re.sub(r"\bCLOSING DATE\b", "Closing Date", title, flags=re.I)
    title = re.sub(r"\s{2,}", " ", title).strip(" -")
    return title[:240]


def normalize_title(title):
    return cleanup_title(title)


def extract_due_date(text):
    patterns = [
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}",
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
        r"\b\d{4}-\d{2}-\d{2}\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if match:
            return match.group(0)
    return None


def build_stable_lead_id(source_key: str, title: str, source_url: str | None = None) -> str:
    raw = f"{source_key}|{title}|{source_url or ''}"
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]
    return f"lead-{source_key}-{digest}"


def match_terms(text: str, terms: list[str]) -> list[str]:
    return [term for term in terms if term in text]


def score_public_notice_relevance(title: str, raw_text: str) -> tuple[int, list[str], str]:
    haystack = normalize_for_rules(f"{title} {raw_text}")

    transport_matches = match_terms(haystack, PUBLIC_NOTICE_TRANSPORTATION_TERMS)
    construction_matches = match_terms(haystack, PUBLIC_NOTICE_CONSTRUCTION_TERMS)
    professional_matches = match_terms(haystack, PUBLIC_NOTICE_PROFESSIONAL_TERMS)
    negative_matches = match_terms(haystack, PUBLIC_NOTICE_NEGATIVE_TERMS)

    score = 0
    if transport_matches:
        score += 35
    if construction_matches:
        score += 30
    if professional_matches:
        score += 25
    if "njdot" in haystack or "department of transportation" in haystack:
        score += 20
    if "turnpike" in haystack or "transit" in haystack or "bridge commission" in haystack:
        score += 15
    if "contract no" in haystack or "solicitation" in haystack:
        score += 10
    if "request for proposals" in haystack or "request for qualifications" in haystack:
        score += 10
    if "request for bids" in haystack or "invitation for bids" in haystack:
        score += 10
    if "public notice" in haystack or "legal notice" in haystack:
        score += 5
    if "county" in haystack or "municipal" in haystack or "township" in haystack or "borough" in haystack:
        score += 5
    if "proposal" in haystack and ("engineering" in haystack or "construction management" in haystack):
        score += 10
    if negative_matches:
        score -= 25
    if ("meeting" in haystack or "minutes" in haystack) and not (construction_matches or professional_matches):
        score -= 25
    if "award" in haystack and not ("bid" in haystack or "rfp" in haystack or "rfq" in haystack):
        score -= 10

    matched_labels = []
    if transport_matches:
        matched_labels.append("transportation")
    if construction_matches:
        matched_labels.append("construction")
    if professional_matches:
        matched_labels.append("professional-services")

    if transport_matches and professional_matches:
        category = "Transportation Professional Services"
    elif transport_matches and construction_matches:
        category = "Transportation Construction"
    elif professional_matches:
        category = "Professional Services"
    elif construction_matches:
        category = "Construction Services"
    elif transport_matches:
        category = "Transportation"
    else:
        category = "General Notice"

    return max(min(score, 100), 0), matched_labels, category


def build_public_notice_admin_note(score: int, matched_labels: list[str], category: str, source_name: str) -> str:
    labels = ", ".join(matched_labels) if matched_labels else "general"
    return f"Statewide public notice review | source: {source_name} | category: {category} | relevance score: {score} | matched: {labels}"


def make_duplicate_key(source_id, title, due_date):
    norm_title = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    norm_date = (due_date or "nodate").lower().replace(" ", "-").replace(",", "")
    return f"{source_id}|{norm_title}|{norm_date}"[:500]


def compute_quality_score(title, due_date, agency, county):
    score = 0
    if title and len(title) >= 25:
        score += 30
    if title and len(title) >= 50:
        score += 10
    if due_date:
        score += 25
    if agency:
        score += 15
    if county:
        score += 10
    if title and any(token in title.lower() for token in ["contract", "rfp", "rfq", "ifb", "proposal", "project", "services"]):
        score += 10
    return min(score, 100)


def refresh_duplicate_flags():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE opportunity_leads SET possible_duplicate = FALSE")

            cur.execute("""
                UPDATE opportunity_leads l
                SET possible_duplicate = TRUE
                WHERE duplicate_key IN (
                    SELECT duplicate_key
                    FROM opportunity_leads
                    WHERE duplicate_key IS NOT NULL AND duplicate_key <> ''
                    GROUP BY duplicate_key
                    HAVING COUNT(*) > 1
                )
            """)

            cur.execute("""
                UPDATE opportunity_leads l
                SET possible_duplicate = TRUE
                WHERE EXISTS (
                    SELECT 1
                    FROM opportunities o
                    WHERE o.source_id = l.source_id
                      AND LOWER(o.title) = LOWER(l.title)
                      AND COALESCE(o.due_date, '') = COALESCE(l.due_date, '')
                )
            """)
        conn.commit()


def upsert_leads(source_key, source_id, agency, county, source_url, titles):
    inserted = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for idx, raw_item in enumerate(titles, start=1):
                if isinstance(raw_item, dict):
                    title = normalize_title(raw_item.get("title", ""))
                    item_source_url = raw_item.get("source_url") or source_url
                    raw_text = raw_item.get("raw_text") or title
                    posted_date = raw_item.get("posted_date")
                    due_date = raw_item.get("due_date") or extract_due_date(raw_text)
                    quality_score = raw_item.get("quality_score")
                    admin_notes = raw_item.get("admin_notes", "")
                    access_notes = raw_item.get("access_notes", "")
                    lead_id = raw_item.get("lead_id") or build_stable_lead_id(source_key, title, item_source_url)
                else:
                    title = normalize_title(raw_item)
                    item_source_url = source_url
                    raw_text = title
                    posted_date = None
                    due_date = extract_due_date(title)
                    quality_score = None
                    admin_notes = ""
                    access_notes = ""
                    lead_id = f"lead-{source_key}-{idx}"

                if not title:
                    continue

                duplicate_key = make_duplicate_key(source_id, title, due_date)
                if quality_score is None:
                    quality_score = compute_quality_score(title, due_date, agency, county)

                guidance = classify_access_guidance(source_id, title, raw_text)

                cur.execute("""
                    INSERT INTO opportunity_leads (
                        lead_id, source_id, title, agency, county, posted_date, due_date, status,
                        source_url, raw_text, duplicate_key, quality_score, admin_notes,
                        access_type, platform_name, next_step, docs_path_note, addenda_note, access_notes
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (lead_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        agency = EXCLUDED.agency,
                        county = EXCLUDED.county,
                        posted_date = EXCLUDED.posted_date,
                        due_date = EXCLUDED.due_date,
                        status = EXCLUDED.status,
                        source_url = EXCLUDED.source_url,
                        raw_text = EXCLUDED.raw_text,
                        duplicate_key = EXCLUDED.duplicate_key,
                        quality_score = EXCLUDED.quality_score,
                        admin_notes = EXCLUDED.admin_notes,
                        access_type = EXCLUDED.access_type,
                        platform_name = EXCLUDED.platform_name,
                        next_step = EXCLUDED.next_step,
                        docs_path_note = EXCLUDED.docs_path_note,
                        addenda_note = EXCLUDED.addenda_note,
                        access_notes = EXCLUDED.access_notes
                """, (
                    lead_id, source_id, title, agency, county, posted_date, due_date, "New",
                    item_source_url, raw_text, duplicate_key, quality_score, admin_notes,
                    guidance["access_type"],
                    guidance["platform_name"],
                    guidance["next_step"],
                    guidance["docs_path_note"],
                    guidance["addenda_note"],
                    access_notes,
                ))
                inserted += 1
        conn.commit()

    refresh_duplicate_flags()
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
    return deduped[:20]


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
    return deduped[:20]


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
    return deduped[:20]


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
    return deduped[:20]


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
    return deduped[:20]


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
    return deduped[:20]


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
    return deduped[:20]


def parse_public_notice_entries(html: str, page_url: str, source_key: str, source_name: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    entries = []
    seen = set()

    for link in soup.find_all("a", href=True):
        title = normalize_title(link.get_text(" ", strip=True))
        if len(title) < 12:
            continue

        href = link.get("href", "").strip()
        full_url = urljoin(page_url, href) if href else page_url
        parent = link.find_parent(["li", "tr", "p", "div"]) or link
        context = normalize_title(parent.get_text(" ", strip=True))
        raw_text = f"{title} {context}".strip()
        score, matched_labels, category = score_public_notice_relevance(title, raw_text)

        if score < 45:
            continue

        key = (title.lower(), full_url.lower())
        if key in seen:
            continue
        seen.add(key)

        entries.append({
            "lead_id": build_stable_lead_id(source_key, title, full_url),
            "title": title,
            "posted_date": extract_due_date(context),
            "due_date": extract_due_date(raw_text),
            "source_url": full_url,
            "raw_text": raw_text[:4000],
            "quality_score": min(100, max(score, compute_quality_score(title, extract_due_date(raw_text), source_name, "Statewide"))),
            "admin_notes": build_public_notice_admin_note(score, matched_labels, category, source_name),
            "access_notes": f"Imported from {source_name} for admin review before promotion.",
        })

    if entries:
        return entries[:80]

    cleaned = strip_html(html)
    fallback_entries = []
    for sentence in re.split(r"(?<=[.!?])\s+", cleaned):
        title = normalize_title(sentence)
        if len(title) < 25:
            continue
        score, matched_labels, category = score_public_notice_relevance(title, sentence)
        if score < 55:
            continue
        lead_id = build_stable_lead_id(source_key, title, page_url)
        fallback_entries.append({
            "lead_id": lead_id,
            "title": title,
            "posted_date": extract_due_date(sentence),
            "due_date": extract_due_date(sentence),
            "source_url": page_url,
            "raw_text": sentence[:4000],
            "quality_score": min(100, score),
            "admin_notes": build_public_notice_admin_note(score, matched_labels, category, source_name),
            "access_notes": f"Fallback statewide public notice extraction from {source_name}. Review before promotion.",
        })
    return fallback_entries[:40]


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


def manual_crawl_public_notice_source(source: dict):
    headers = {"User-Agent": "Mozilla/5.0 NJTransportationBids/1.0"}
    try:
        resp = requests.get(source["url"], headers=headers, timeout=30)
        resp.raise_for_status()
        entries = parse_public_notice_entries(resp.text, resp.url, source["source_key"], source["source_name"])
        inserted = upsert_leads(
            source_key=source["source_key"],
            source_id=source["source_id"],
            agency=source["agency"],
            county=source["county"],
            source_url=resp.url,
            titles=entries,
        )
        log_crawl_run(source["source_id"], source["source_name"], "Success", inserted, "Statewide public notice crawl completed")
        update_source_crawl_status(source["source_id"], "Success", inserted)
        return {"inserted": inserted, "titles": [entry["title"] for entry in entries]}
    except Exception as e:
        log_crawl_run(source["source_id"], source["source_name"], "Failed", 0, str(e))
        update_source_crawl_status(source["source_id"], "Failed", 0)
        raise


def manual_crawl_dos_public_notices():
    source = next(source for source in PUBLIC_NOTICE_SOURCES if source["source_id"] == "state-dos-public-notices")
    return manual_crawl_public_notice_source(source)


def manual_crawl_statewide_public_notices():
    results = []
    total_inserted = 0
    for source in PUBLIC_NOTICE_SOURCES:
        result = manual_crawl_public_notice_source(source)
        total_inserted += result["inserted"]
        results.append({
            "source_id": source["source_id"],
            "source_name": source["source_name"],
            "inserted": result["inserted"],
        })
    return {"inserted": total_inserted, "results": results}


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
            elif sid in get_public_notice_source_ids():
                source = next((notice_source for notice_source in PUBLIC_NOTICE_SOURCES if notice_source["source_id"] == sid), None)
                if not source:
                    continue
                result = manual_crawl_public_notice_source(source)
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
                        SELECT lead_id, source_id, title, agency, county, due_date, source_url,
                               access_type, platform_name, next_step, docs_path_note, addenda_note
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
                          AND LOWER(title) = LOWER(%s)
                          AND COALESCE(due_date, '') = COALESCE(%s, '')
                    """, (row[1], row[2], row[5]))
                    duplicate_count = cur.fetchone()[0]

                    if duplicate_count == 0:
                        opportunity_id = f"opp-{row[0]}"
                        cur.execute("""
                            INSERT INTO opportunities (
                                opportunity_id, title, agency, county, source_id, due_date, status,
                                opportunity_url, access_type, platform_name, next_step, docs_path_note, addenda_note
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (opportunity_id) DO UPDATE SET
                                title = EXCLUDED.title,
                                agency = EXCLUDED.agency,
                                county = EXCLUDED.county,
                                source_id = EXCLUDED.source_id,
                                due_date = EXCLUDED.due_date,
                                status = EXCLUDED.status,
                                opportunity_url = EXCLUDED.opportunity_url,
                                access_type = EXCLUDED.access_type,
                                platform_name = EXCLUDED.platform_name,
                                next_step = EXCLUDED.next_step,
                                docs_path_note = EXCLUDED.docs_path_note,
                                addenda_note = EXCLUDED.addenda_note
                        """, (
                            opportunity_id, row[2], row[3] or "Unknown Agency",
                            row[4], row[1], row[5], "Open", row[6],
                            row[7], row[8], row[9], row[10], row[11]
                        ))
                    cur.execute("UPDATE opportunity_leads SET status = 'Promoted' WHERE lead_id = %s", (lead_id,))

                elif action == "reject":
                    cur.execute("UPDATE opportunity_leads SET status = 'Rejected' WHERE lead_id = %s", (lead_id,))
                elif action == "reset":
                    cur.execute("UPDATE opportunity_leads SET status = 'New' WHERE lead_id = %s", (lead_id,))
        conn.commit()

    refresh_duplicate_flags()


def reject_lead(lead_id):
    bulk_update_leads([lead_id], "reject")


def reset_lead_to_new(lead_id):
    bulk_update_leads([lead_id], "reset")


def update_lead_notes(lead_id, admin_notes):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE opportunity_leads
                SET admin_notes = %s
                WHERE lead_id = %s
            """, (admin_notes.strip(), lead_id))
        conn.commit()


def mark_lead_duplicate(lead_id, is_duplicate: bool):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE opportunity_leads
                SET possible_duplicate = %s
                WHERE lead_id = %s
            """, (is_duplicate, lead_id))
        conn.commit()


def update_lead_access_info(
    lead_id: str,
    access_type: str,
    platform_name: str,
    next_step: str,
    docs_path_note: str,
    addenda_note: str,
    access_notes: str,
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE opportunity_leads
                SET access_type = %s,
                    platform_name = %s,
                    next_step = %s,
                    docs_path_note = %s,
                    addenda_note = %s,
                    access_notes = %s
                WHERE lead_id = %s
            """, (
                access_type.strip(),
                platform_name.strip(),
                next_step.strip(),
                docs_path_note.strip(),
                addenda_note.strip(),
                access_notes.strip(),
                lead_id,
            ))
        conn.commit()


def rerun_auto_guidance_for_lead(lead_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source_id, title, raw_text
                FROM opportunity_leads
                WHERE lead_id = %s
            """, (lead_id,))
            row = cur.fetchone()

            if not row:
                return

            source_id, title, raw_text = row
            guidance = classify_access_guidance(source_id, title, raw_text)

            cur.execute("""
                UPDATE opportunity_leads
                SET access_type = %s,
                    platform_name = %s,
                    next_step = %s,
                    docs_path_note = %s,
                    addenda_note = %s
                WHERE lead_id = %s
            """, (
                guidance["access_type"],
                guidance["platform_name"],
                guidance["next_step"],
                guidance["docs_path_note"],
                guidance["addenda_note"],
                lead_id,
            ))
        conn.commit()


def rerun_auto_guidance_for_new_leads():
    updated = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT lead_id, source_id, title, raw_text
                FROM opportunity_leads
                WHERE status = 'New'
            """)
            rows = cur.fetchall()

            for lead_id, source_id, title, raw_text in rows:
                guidance = classify_access_guidance(source_id, title, raw_text)
                cur.execute("""
                    UPDATE opportunity_leads
                    SET access_type = %s,
                        platform_name = %s,
                        next_step = %s,
                        docs_path_note = %s,
                        addenda_note = %s
                    WHERE lead_id = %s
                """, (
                    guidance["access_type"],
                    guidance["platform_name"],
                    guidance["next_step"],
                    guidance["docs_path_note"],
                    guidance["addenda_note"],
                    lead_id,
                ))
                updated += 1
        conn.commit()
    return updated


@app.on_event("startup")
def startup_event():
    init_db()
    refresh_duplicate_flags()


@app.get("/", response_class=HTMLResponse)
def home():
    sources = fetch_sources()
    summary = fetch_admin_summary()
    recent = fetch_recent_opportunities(limit=8)

    cards = ""
    for opp in recent:
        cards += f"""
        <div class="opp-card">
            <div class="opp-title"><a href="/opportunities/{opp['opportunity_id']}">{opp['title']}</a></div>
            <div class="opp-meta">{opp['agency'] or ''} • {opp['county'] or ''}</div>
            <div class="opp-badges">
                <span class="badge">{opp['access_type'] or 'Unknown access'}</span>
                <span class="badge">{opp['platform_name'] or 'Unknown platform'}</span>
            </div>
            <div class="opp-next">{opp['next_step'] or ''}</div>
        </div>
        """

    return f"""
    <html><head><title>NJ Transportation Bids</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; line-height: 1.5; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 1150px; margin: 0 auto; }}
      .hero {{ background: white; border: 1px solid #e5e7eb; border-radius: 18px; padding: 32px; margin-bottom: 24px; }}
      .hero h1 {{ margin-top: 0; }}
      .stats {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 18px 0; }}
      .stat {{ background: #f3f4f6; border-radius: 12px; padding: 16px; min-width: 180px; }}
      .nav {{ display: flex; gap: 12px; flex-wrap: wrap; margin-top: 20px; }}
      .nav a {{ display: inline-block; background: #0b57d0; color: white; padding: 10px 14px; border-radius: 10px; text-decoration: none; }}
      .nav a.secondary {{ background: #374151; }}
      .section {{ background: white; border: 1px solid #e5e7eb; border-radius: 18px; padding: 24px; margin-bottom: 20px; }}
      .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
      .opp-card {{ border: 1px solid #e5e7eb; border-radius: 14px; padding: 16px; background: #fafafa; }}
      .opp-title {{ font-weight: bold; margin-bottom: 8px; }}
      .opp-title a {{ color: #111827; text-decoration: none; }}
      .opp-meta {{ color: #4b5563; margin-bottom: 10px; }}
      .opp-badges {{ margin-bottom: 10px; }}
      .badge {{ display: inline-block; background: #e0e7ff; border-radius: 999px; padding: 4px 10px; margin-right: 8px; font-size: 12px; }}
      .opp-next {{ color: #1f2937; }}
    </style></head>
    <body><div class="wrap">
      <div class="hero">
        <h1>NJ Transportation Bids</h1>
        <p>Public transportation bid aggregation with official-path guidance for contractors.</p>
        <div class="stats">
          <div class="stat"><strong>{len(sources)}</strong><br>live source records</div>
          <div class="stat"><strong>{summary['opportunity_count']}</strong><br>published opportunities</div>
          <div class="stat"><strong>{summary['lead_count']}</strong><br>total leads</div>
          <div class="stat"><strong>{summary['access_populated_count']}</strong><br>leads with access info</div>
        </div>
        <div class="nav">
          <a href="/opportunities">Browse Opportunities</a>
          <a href="/sources">Browse Sources</a>
          <a href="/export/opportunities.csv" class="secondary">Export Opportunities CSV</a>
          <a href="/admin" class="secondary">Admin</a>
        </div>
      </div>

      <div class="section">
        <h2>How this site helps</h2>
        <div class="grid">
          <div>
            <strong>Find the job</strong><br>
            Search public transportation opportunities across major NJ agencies and authorities.
          </div>
          <div>
            <strong>Understand access</strong><br>
            See whether the job is public access, registration-based, or platform-driven.
          </div>
          <div>
            <strong>Get the official path</strong><br>
            Every opportunity points you to the official source and the next best step.
          </div>
          <div>
            <strong>Prepare for growth</strong><br>
            The structure is ready for alerts, networking, teaming, and contractor accounts later.
          </div>
        </div>
      </div>

      <div class="section">
        <h2>Recent opportunities</h2>
        <div class="grid">{cards}</div>
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
def api_opportunities(
    county: str | None = None,
    agency: str | None = None,
    source_id: str | None = None,
    q: str | None = None,
    access_type: str | None = None,
    platform_name: str | None = None,
):
    return JSONResponse(content=fetch_opportunities(county, agency, source_id, q, access_type, platform_name))


@app.get("/api/admin/summary")
def api_admin_summary(username: str = Depends(check_auth)):
    return JSONResponse(content=fetch_admin_summary())


@app.get("/api/admin/leads")
def api_admin_leads(
    username: str = Depends(check_auth),
    status_filter: str | None = Query(default=None, alias="status"),
    q: str | None = None,
    sort_by: str | None = None,
    source_id: str | None = None,
    public_notice_only: bool = False,
    duplicates_only: bool = False,
):
    return JSONResponse(content=fetch_leads(status_filter, q, sort_by, duplicates_only, source_id, public_notice_only))


@app.get("/api/admin/crawl-runs")
def api_admin_crawl_runs(username: str = Depends(check_auth)):
    return JSONResponse(content=fetch_crawl_runs())


@app.get("/export/opportunities.csv")
def export_opportunities_csv(
    county: str | None = None,
    agency: str | None = None,
    source_id: str | None = None,
    q: str | None = None,
    access_type: str | None = None,
    platform_name: str | None = None,
):
    opportunities = fetch_opportunities(county, agency, source_id, q, access_type, platform_name)
    rows = [
        [
            o["opportunity_id"], o["title"], o["agency"], o["county"], o["source_id"],
            o["source_name"], o["due_date"], o["status"], o["access_type"],
            o["platform_name"], o["next_step"], o["docs_path_note"],
            o["addenda_note"], o["opportunity_url"], o["created_at"]
        ]
        for o in opportunities
    ]
    return csv_response(
        "opportunities.csv",
        ["opportunity_id", "title", "agency", "county", "source_id", "source_name", "due_date", "status", "access_type", "platform_name", "next_step", "docs_path_note", "addenda_note", "opportunity_url", "created_at"],
        rows
    )


@app.get("/admin/export/leads.csv")
def export_leads_csv(
    username: str = Depends(check_auth),
    status_filter: str | None = Query(default=None, alias="status"),
    q: str | None = None,
    sort_by: str | None = None,
    source_id: str | None = None,
    public_notice_only: bool = False,
    duplicates_only: bool = False,
):
    leads = fetch_leads(status_filter, q, sort_by, duplicates_only, source_id, public_notice_only)
    rows = [
        [
            l["lead_id"], l["title"], l["source_id"], l["source_name"], l["agency"], l["county"],
            l["due_date"], l["status"], l["duplicate_key"], l["possible_duplicate"],
            l["quality_score"], l["access_type"], l["platform_name"], l["next_step"],
            l["docs_path_note"], l["addenda_note"], l["access_notes"], l["admin_notes"],
            l["source_url"], l["created_at"]
        ]
        for l in leads
    ]
    return csv_response(
        "leads.csv",
        ["lead_id", "title", "source_id", "source_name", "agency", "county", "due_date", "status", "duplicate_key", "possible_duplicate", "quality_score", "access_type", "platform_name", "next_step", "docs_path_note", "addenda_note", "access_notes", "admin_notes", "source_url", "created_at"],
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


@app.post("/admin/crawl/dos-public-notices")
def admin_crawl_dos_public_notices(username: str = Depends(check_auth)):
    manual_crawl_dos_public_notices()
    return RedirectResponse(url="/admin/leads?source_id=state-dos-public-notices&sort_by=quality", status_code=303)


@app.post("/admin/crawl/statewide-public-notices")
def admin_crawl_statewide_public_notices(username: str = Depends(check_auth)):
    manual_crawl_statewide_public_notices()
    return RedirectResponse(url="/admin/leads?public_notice_only=true&sort_by=quality", status_code=303)


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
    return_source_id: str | None = Form(default=None),
    return_public_notice_only: bool = Form(default=False),
):
    promote_lead(lead_id)
    return RedirectResponse(url=build_redirect_url("/admin/leads", return_status, return_q, return_sort_by, return_source_id, return_public_notice_only), status_code=303)


@app.post("/admin/leads/{lead_id}/reject")
def admin_reject_lead(
    lead_id: str,
    username: str = Depends(check_auth),
    return_status: str | None = Form(default=None),
    return_q: str | None = Form(default=None),
    return_sort_by: str | None = Form(default=None),
    return_source_id: str | None = Form(default=None),
    return_public_notice_only: bool = Form(default=False),
):
    reject_lead(lead_id)
    return RedirectResponse(url=build_redirect_url("/admin/leads", return_status, return_q, return_sort_by, return_source_id, return_public_notice_only), status_code=303)


@app.post("/admin/leads/{lead_id}/reset")
def admin_reset_lead(
    lead_id: str,
    username: str = Depends(check_auth),
    return_status: str | None = Form(default=None),
    return_q: str | None = Form(default=None),
    return_sort_by: str | None = Form(default=None),
    return_source_id: str | None = Form(default=None),
    return_public_notice_only: bool = Form(default=False),
):
    reset_lead_to_new(lead_id)
    return RedirectResponse(url=build_redirect_url("/admin/leads", return_status, return_q, return_sort_by, return_source_id, return_public_notice_only), status_code=303)


@app.post("/admin/leads/bulk")
def admin_bulk_leads_action(
    username: str = Depends(check_auth),
    action: str = Form(...),
    selected_lead_ids: list[str] = Form(default=[]),
    return_status: str | None = Form(default=None),
    return_q: str | None = Form(default=None),
    return_sort_by: str | None = Form(default=None),
    return_source_id: str | None = Form(default=None),
    return_public_notice_only: bool = Form(default=False),
):
    if action in {"promote", "reject", "reset"} and selected_lead_ids:
        bulk_update_leads(selected_lead_ids, action)
    return RedirectResponse(url=build_redirect_url("/admin/leads", return_status, return_q, return_sort_by, return_source_id, return_public_notice_only), status_code=303)


@app.post("/admin/leads/{lead_id}/notes")
def admin_update_lead_notes(
    lead_id: str,
    username: str = Depends(check_auth),
    admin_notes: str = Form(default=""),
    return_status: str | None = Form(default=None),
    return_q: str | None = Form(default=None),
    return_sort_by: str | None = Form(default=None),
    return_source_id: str | None = Form(default=None),
    return_public_notice_only: bool = Form(default=False),
):
    update_lead_notes(lead_id, admin_notes)
    return RedirectResponse(url=build_redirect_url("/admin/leads", return_status, return_q, return_sort_by, return_source_id, return_public_notice_only), status_code=303)


@app.post("/admin/leads/{lead_id}/access")
def admin_update_lead_access(
    lead_id: str,
    username: str = Depends(check_auth),
    access_type: str = Form(...),
    platform_name: str = Form(...),
    next_step: str = Form(default=""),
    docs_path_note: str = Form(default=""),
    addenda_note: str = Form(default=""),
    access_notes: str = Form(default=""),
    return_status: str | None = Form(default=None),
    return_q: str | None = Form(default=None),
    return_sort_by: str | None = Form(default=None),
    return_source_id: str | None = Form(default=None),
    return_public_notice_only: bool = Form(default=False),
):
    update_lead_access_info(
        lead_id,
        access_type,
        platform_name,
        next_step,
        docs_path_note,
        addenda_note,
        access_notes,
    )
    return RedirectResponse(url=build_redirect_url("/admin/leads", return_status, return_q, return_sort_by, return_source_id, return_public_notice_only), status_code=303)


@app.post("/admin/leads/{lead_id}/auto-guidance")
def admin_rerun_auto_guidance_for_lead(
    lead_id: str,
    username: str = Depends(check_auth),
    return_status: str | None = Form(default=None),
    return_q: str | None = Form(default=None),
    return_sort_by: str | None = Form(default=None),
    return_source_id: str | None = Form(default=None),
    return_public_notice_only: bool = Form(default=False),
):
    rerun_auto_guidance_for_lead(lead_id)
    return RedirectResponse(
        url=build_redirect_url("/admin/leads", return_status, return_q, return_sort_by, return_source_id, return_public_notice_only),
        status_code=303,
    )


@app.post("/admin/leads/auto-guidance/new")
def admin_rerun_auto_guidance_for_new(
    username: str = Depends(check_auth),
):
    rerun_auto_guidance_for_new_leads()
    return RedirectResponse(url="/admin/leads?status=New", status_code=303)


@app.post("/admin/leads/{lead_id}/mark-duplicate")
def admin_mark_duplicate(
    lead_id: str,
    username: str = Depends(check_auth),
    return_status: str | None = Form(default=None),
    return_q: str | None = Form(default=None),
    return_sort_by: str | None = Form(default=None),
):
    mark_lead_duplicate(lead_id, True)
    return RedirectResponse(url=build_redirect_url("/admin/duplicates", return_status, return_q, return_sort_by), status_code=303)


@app.post("/admin/leads/{lead_id}/clear-duplicate")
def admin_clear_duplicate(
    lead_id: str,
    username: str = Depends(check_auth),
    return_status: str | None = Form(default=None),
    return_q: str | None = Form(default=None),
    return_sort_by: str | None = Form(default=None),
):
    mark_lead_duplicate(lead_id, False)
    return RedirectResponse(url=build_redirect_url("/admin/duplicates", return_status, return_q, return_sort_by), status_code=303)


@app.get("/sources", response_class=HTMLResponse)
def sources_page():
    sources = fetch_sources()
    items = ""
    for row in sources:
        defaults = get_source_defaults(row["source_id"])
        items += f"""
        <tr>
            <td><a href="/sources/{row['source_id']}">{row['source_name']}</a></td>
            <td>{row['entity_type'] or ''}</td>
            <td>{row['county'] or ''}</td>
            <td>{row['priority_tier'] or ''}</td>
            <td>{defaults['access_type']}</td>
            <td>{defaults['platform_name']}</td>
        </tr>
        """

    return f"""
    <html><head><title>Sources</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 1300px; margin: 0 auto; }}
      table {{ border-collapse: collapse; width: 100%; background: white; }}
      th, td {{ border: 1px solid #e5e7eb; padding: 10px; text-align: left; vertical-align: top; }}
      th {{ background: #f3f4f6; }}
      a {{ color: #0b57d0; text-decoration: none; }}
    </style></head>
    <body><div class="wrap">
      <a href="/">← Back to home</a>
      <h1>Registry Sources</h1>
      <p>{len(sources)} sources currently loaded</p>
      <table>
        <thead><tr><th>Source Name</th><th>Entity Type</th><th>County</th><th>Priority</th><th>Default Access</th><th>Default Platform</th></tr></thead>
        <tbody>{items}</tbody>
      </table>
    </div></body></html>
    """


@app.get("/sources/{source_id}", response_class=HTMLResponse)
def source_detail_page(source_id: str):
    detail = fetch_source_detail(source_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Source not found")

    source = detail["source"]
    defaults = detail["defaults"]

    lead_rows = ""
    for row in detail["recent_leads"]:
        lead_rows += f"""
        <tr>
            <td>{row['title']}</td>
            <td>{row['due_date'] or ''}</td>
            <td>{row['status'] or ''}</td>
            <td>{row['access_type'] or ''}</td>
            <td>{row['platform_name'] or ''}</td>
        </tr>
        """

    opp_rows = ""
    for row in detail["recent_opportunities"]:
        opp_rows += f"""
        <tr>
            <td><a href="/opportunities/{row['opportunity_id']}">{row['title']}</a></td>
            <td>{row['due_date'] or ''}</td>
            <td>{row['status'] or ''}</td>
            <td>{row['access_type'] or ''}</td>
            <td>{row['platform_name'] or ''}</td>
        </tr>
        """

    return f"""
    <html><head><title>{source['source_name']}</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; background:#f8fafc; color:#111827; }}
      .wrap {{ max-width: 1250px; margin: 0 auto; }}
      .card {{ background:white; border:1px solid #e5e7eb; border-radius:16px; padding:24px; margin-bottom:18px; }}
      .stats {{ display:flex; gap:16px; flex-wrap:wrap; margin-top:12px; }}
      .stat {{ background:#f3f4f6; border-radius:12px; padding:14px; min-width:170px; }}
      table {{ border-collapse: collapse; width: 100%; background: white; }}
      th, td {{ border:1px solid #e5e7eb; padding:10px; text-align:left; vertical-align:top; }}
      th {{ background:#f3f4f6; }}
      a {{ color:#0b57d0; text-decoration:none; }}
    </style></head>
    <body><div class="wrap">
      <div class="card">
        <a href="/sources">← Back to sources</a>
        <h1>{source['source_name']}</h1>
        <p><strong>Entity type:</strong> {source['entity_type'] or ''}</p>
        <p><strong>County:</strong> {source['county'] or ''}</p>
        <p><strong>Official source:</strong> <a href="{source['source_url']}" target="_blank">{source['source_url']}</a></p>

        <div class="stats">
          <div class="stat"><strong>{detail['lead_count']}</strong><br>total leads</div>
          <div class="stat"><strong>{detail['new_lead_count']}</strong><br>new leads</div>
          <div class="stat"><strong>{detail['opportunity_count']}</strong><br>published opportunities</div>
        </div>
      </div>

      <div class="card">
        <h2>Default access guidance</h2>
        <p><strong>Access:</strong> {defaults['access_type']}</p>
        <p><strong>Platform:</strong> {defaults['platform_name']}</p>
        <p><strong>Next step:</strong> {defaults['next_step']}</p>
        <p><strong>Bid docs:</strong> {defaults['docs_path_note']}</p>
        <p><strong>Addenda:</strong> {defaults['addenda_note']}</p>
      </div>

      <div class="card">
        <h2>Recent published opportunities</h2>
        <table>
          <thead><tr><th>Title</th><th>Due Date</th><th>Status</th><th>Access</th><th>Platform</th></tr></thead>
          <tbody>{opp_rows}</tbody>
        </table>
      </div>

      <div class="card">
        <h2>Recent leads</h2>
        <table>
          <thead><tr><th>Title</th><th>Due Date</th><th>Status</th><th>Access</th><th>Platform</th></tr></thead>
          <tbody>{lead_rows}</tbody>
        </table>
      </div>
    </div></body></html>
    """


@app.get("/opportunities", response_class=HTMLResponse)
def opportunities_page(
    county: str | None = None,
    agency: str | None = None,
    source_id: str | None = None,
    q: str | None = None,
    access_type: str | None = None,
    platform_name: str | None = None,
):
    opportunities = fetch_opportunities(county, agency, source_id, q, access_type, platform_name)
    sources = fetch_sources()

    counties = sorted({s["county"] for s in sources if s["county"]})
    agencies = sorted({o["agency"] for o in fetch_opportunities() if o["agency"]})
    source_opts = sorted({(s["source_id"], s["source_name"]) for s in sources})

    county_options = "<option value=''>All counties</option>" + "".join(
        f"<option value='{c}' {'selected' if county == c else ''}>{c}</option>" for c in counties
    )
    agency_options = "<option value=''>All agencies</option>" + "".join(
        f"<option value='{a}' {'selected' if agency == a else ''}>{a}</option>" for a in agencies
    )
    source_options = "<option value=''>All sources</option>" + "".join(
        f"<option value='{sid}' {'selected' if source_id == sid else ''}>{sname}</option>" for sid, sname in source_opts
    )
    access_options = "<option value=''>All access types</option>" + "".join(
        f"<option value='{a}' {'selected' if access_type == a else ''}>{a}</option>" for a in ACCESS_TYPE_OPTIONS
    )
    platform_options = "<option value=''>All platforms</option>" + "".join(
        f"<option value='{p}' {'selected' if platform_name == p else ''}>{p}</option>" for p in PLATFORM_NAME_OPTIONS
    )

    cards = ""
    for row in opportunities:
        cards += f"""
        <div class="opp-card">
            <div class="opp-title"><a href="/opportunities/{row['opportunity_id']}">{row['title']}</a></div>
            <div class="opp-meta">{row['agency'] or ''} • {row['county'] or ''}</div>
            <div class="opp-badges">
                <span class="badge">{row['access_type'] or 'Unknown access'}</span>
                <span class="badge">{row['platform_name'] or 'Unknown platform'}</span>
            </div>
            <div class="opp-next"><strong>Next step:</strong> {row['next_step'] or ''}</div>
            <div class="opp-footer">
                <span>Due: {row['due_date'] or 'Not listed'}</span>
                <span><a href="{row['opportunity_url']}" target="_blank">Official source</a></span>
            </div>
        </div>
        """

    return f"""
    <html><head><title>Opportunities</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 1400px; margin: 0 auto; }}
      .filters {{ background: white; border: 1px solid #e5e7eb; padding: 16px; border-radius: 12px; margin-bottom: 20px; }}
      .filters input, .filters select {{ margin-right: 10px; margin-bottom:10px; padding: 8px; }}
      .filters button {{ padding: 8px 12px; }}
      .tools a {{ display:inline-block; margin-bottom:16px; color:#0b57d0; text-decoration:none; }}
      .grid {{ display:grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
      .opp-card {{ background:white; border:1px solid #e5e7eb; border-radius:16px; padding:18px; }}
      .opp-title {{ font-weight:bold; font-size:18px; margin-bottom:8px; }}
      .opp-title a {{ color:#111827; text-decoration:none; }}
      .opp-meta {{ color:#4b5563; margin-bottom:10px; }}
      .opp-badges {{ margin-bottom:10px; }}
      .badge {{ display:inline-block; background:#e0e7ff; border-radius:999px; padding:4px 10px; margin-right:8px; font-size:12px; }}
      .opp-next {{ margin-bottom:12px; }}
      .opp-footer {{ display:flex; justify-content:space-between; gap:12px; font-size:14px; color:#374151; }}
      a {{ color:#0b57d0; text-decoration:none; }}
    </style></head>
    <body><div class="wrap">
      <a href="/">← Back to home</a>
      <h1>Opportunities</h1>
      <p>{len(opportunities)} published opportunities currently shown</p>

      <div class="tools">
        <a href="/export/opportunities.csv">Export opportunities CSV</a>
      </div>

      <form method="get" action="/opportunities" class="filters">
        <input type="text" name="q" placeholder="Search title / agency / county" value="{q or ''}">
        <select name="county">{county_options}</select>
        <select name="agency">{agency_options}</select>
        <select name="source_id">{source_options}</select>
        <select name="access_type">{access_options}</select>
        <select name="platform_name">{platform_options}</select>
        <button type="submit">Filter</button>
      </form>

      <div class="grid">{cards}</div>
    </div></body></html>
    """


@app.get("/opportunities/{opportunity_id}", response_class=HTMLResponse)
def opportunity_detail_page(opportunity_id: str):
    opp = fetch_opportunity_by_id(opportunity_id)
    if not opp:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    source_link = f"/sources/{opp['source_id']}" if opp["source_id"] else "/sources"

    return f"""
    <html><head><title>{opp['title']}</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 950px; margin: 0 auto; }}
      .card {{ background: white; border: 1px solid #e5e7eb; border-radius: 16px; padding: 28px; margin-bottom: 18px; }}
      a {{ color: #0b57d0; text-decoration: none; }}
      .row {{ margin-bottom: 12px; }}
      .label {{ font-weight: bold; display: inline-block; min-width: 160px; }}
      .box {{ background:#f9fafb; border:1px solid #e5e7eb; border-radius:12px; padding:20px; margin-top:18px; }}
      .cta a {{ display:inline-block; background:#0b57d0; color:white; padding:10px 14px; border-radius:10px; margin-right:10px; text-decoration:none; }}
      .badge {{ display:inline-block; background:#e0e7ff; border-radius:999px; padding:4px 10px; margin-right:8px; font-size:12px; }}
    </style></head>
    <body><div class="wrap">
      <div class="card">
        <a href="/opportunities">← Back to opportunities</a>
        <h1>{opp['title']}</h1>
        <div class="row"><span class="label">Agency:</span> {opp['agency'] or ''}</div>
        <div class="row"><span class="label">County:</span> {opp['county'] or ''}</div>
        <div class="row"><span class="label">Source:</span> <a href="{source_link}">{opp['source_name'] or ''}</a></div>
        <div class="row"><span class="label">Due Date:</span> {opp['due_date'] or ''}</div>
        <div class="row"><span class="label">Status:</span> {opp['status'] or ''}</div>
        <div class="row"><span class="label">Published:</span> {opp['created_at']}</div>
      </div>

      <div class="card box">
        <h2>Official path</h2>
        <div class="row">
          <span class="badge">{opp['access_type'] or 'Unknown access'}</span>
          <span class="badge">{opp['platform_name'] or 'Unknown platform'}</span>
        </div>
        <div class="row"><span class="label">Next step:</span> {opp['next_step'] or ''}</div>
        <div class="row"><span class="label">Bid docs:</span> {opp['docs_path_note'] or ''}</div>
        <div class="row"><span class="label">Addenda:</span> {opp['addenda_note'] or ''}</div>
        <div class="cta" style="margin-top:18px;">
          <a href="{opp['opportunity_url']}" target="_blank">Open Official Source</a>
          <a href="/sources/{opp['source_id']}">View Source Details</a>
        </div>
      </div>
    </div></body></html>
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
        <div class="stat"><strong>{summary['duplicate_lead_count']}</strong><br>possible duplicates</div>
        <div class="stat"><strong>{summary['access_populated_count']}</strong><br>leads with access info</div>
        <div class="stat"><strong>{summary['opportunity_access_count']}</strong><br>opportunities with access info</div>
      </div>

      <div class="grid" style="margin-top:20px;">
        <div class="panel">
          <h3>Manual crawl controls</h3>
          <form action="/admin/crawl/statewide-public-notices" method="post"><button class="button" type="submit">Run Statewide Public Notice Crawl</button></form>
          <form action="/admin/crawl/njdot-construction" method="post"><button class="button" type="submit">Run NJDOT Construction Crawl</button></form>
          <form action="/admin/crawl/njdot-profserv" method="post"><button class="button" type="submit">Run NJDOT Professional Services Crawl</button></form>
          <form action="/admin/crawl/njta" method="post"><button class="button" type="submit">Run NJTA Crawl</button></form>
          <form action="/admin/crawl/dos-public-notices" method="post"><button class="button" type="submit">Run DOS Public Notices Crawl</button></form>
          <form action="/admin/crawl/monmouth" method="post"><button class="button" type="submit">Run Monmouth County Crawl</button></form>
          <form action="/admin/crawl/njtransit" method="post"><button class="button" type="submit">Run NJ TRANSIT Crawl</button></form>
          <form action="/admin/crawl/drjtbc-construction" method="post"><button class="button" type="submit">Run DRJTBC Construction Crawl</button></form>
          <form action="/admin/crawl/drjtbc-profserv" method="post"><button class="button" type="submit">Run DRJTBC Prof Services Crawl</button></form>
          <form action="/admin/crawl/run-enabled" method="post"><button class="button" type="submit">Run All Enabled Crawlers</button></form>
          <form action="/admin/leads/auto-guidance/new" method="post"><button class="button" type="submit">Re-run Auto Guidance on New Leads</button></form>
        </div>
        <div class="panel"><h3>Latest crawl runs</h3><ul>{crawl_items}</ul></div>
      </div>

      <h3>Admin links</h3>
      <p><a href="/admin/sources">View source crawl status page</a></p>
      <p><a href="/admin/leads">View admin leads page</a></p>
      <p><a href="/admin/leads?public_notice_only=true&sort_by=quality">Review statewide public notice leads</a></p>
      <p><a href="/admin/duplicates">View duplicate review queue</a></p>
      <p><a href="/admin/export/leads.csv">Export leads CSV</a></p>
      <p><a href="/export/opportunities.csv">Export opportunities CSV</a></p>
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
        defaults = get_source_defaults(row["source_id"])
        crawl_enabled_text = "Yes" if row["crawl_enabled"] else "No"
        items += f"""
        <tr>
            <td><a href="/sources/{row['source_id']}">{row['source_name']}</a></td>
            <td>{row['source_id']}</td>
            <td>{row['county'] or ''}</td>
            <td>{crawl_enabled_text}</td>
            <td>{row['crawl_method'] or ''}</td>
            <td>{defaults['access_type']}</td>
            <td>{defaults['platform_name']}</td>
            <td>{row['last_crawl_at'] or ''}</td>
            <td>{row['last_crawl_status'] or ''}</td>
            <td>{row['last_leads_found'] or 0}</td>
        </tr>
        """

    return f"""
    <html><head><title>Admin Sources</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 1600px; margin: 0 auto; }}
      table {{ border-collapse: collapse; width: 100%; background: white; }}
      th, td {{ border: 1px solid #e5e7eb; padding: 10px; text-align: left; vertical-align: top; }}
      th {{ background: #f3f4f6; }}
      a {{ color: #0b57d0; text-decoration: none; }}
    </style></head>
    <body><div class="wrap">
      <a href="/admin">← Back to admin</a>
      <h1>Source Crawl Status</h1>
      <table>
        <thead><tr><th>Source Name</th><th>Source ID</th><th>County</th><th>Crawl Enabled</th><th>Crawl Method</th><th>Default Access</th><th>Default Platform</th><th>Last Crawl At</th><th>Last Status</th><th>Last Leads Found</th></tr></thead>
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
    source_id: str | None = None,
    public_notice_only: bool = False,
):
    leads = fetch_leads(status_filter, q, sort_by, duplicates_only=False, source_filter=source_id, public_notice_only=public_notice_only)
    summary = fetch_admin_summary()
    sources = fetch_sources()

    current_status = status_filter or ""
    current_q = q or ""
    current_sort = sort_by or ""
    current_source_id = source_id or ""
    current_public_notice_only = public_notice_only
    source_options = "<option value=''>All sources</option>" + "".join(
        f"<option value='{row['source_id']}' {'selected' if current_source_id == row['source_id'] else ''}>{row['source_name']}</option>"
        for row in sources
    )

    items = ""
    for row in leads:
        duplicate_badge = "Yes" if row["possible_duplicate"] else ""
        quality_badge = row["quality_score"]

        if row["status"] == "New":
            workflow_html = f"""
                <button type="submit" formaction="/admin/leads/{row['lead_id']}/promote" formmethod="post" style="background:#0b57d0;color:white;border:none;padding:8px 10px;border-radius:8px;cursor:pointer;margin-right:6px;">Promote</button>
                <button type="submit" formaction="/admin/leads/{row['lead_id']}/reject" formmethod="post" style="background:#b91c1c;color:white;border:none;padding:8px 10px;border-radius:8px;cursor:pointer;margin-right:6px;">Reject</button>
            """
        else:
            workflow_html = f"""
                <button type="submit" formaction="/admin/leads/{row['lead_id']}/reset" formmethod="post" style="background:#374151;color:white;border:none;padding:8px 10px;border-radius:8px;cursor:pointer;margin-right:6px;">Reset to New</button>
            """

        workflow_html += f"""
            <button type="submit" formaction="/admin/leads/{row['lead_id']}/auto-guidance" formmethod="post" style="background:#2563eb;color:white;border:none;padding:8px 10px;border-radius:8px;cursor:pointer;margin-right:6px;">Re-run Auto Guidance</button>
            <button type="submit" formaction="/admin/leads/{row['lead_id']}/mark-duplicate" formmethod="post" style="background:#7c3aed;color:white;border:none;padding:8px 10px;border-radius:8px;cursor:pointer;margin-right:6px;">Mark Duplicate</button>
        """

        access_type_options = "".join(
            f"<option value='{opt}' {'selected' if row['access_type'] == opt else ''}>{opt}</option>"
            for opt in ACCESS_TYPE_OPTIONS
        )
        platform_name_options = "".join(
            f"<option value='{opt}' {'selected' if row['platform_name'] == opt else ''}>{opt}</option>"
            for opt in PLATFORM_NAME_OPTIONS
        )

        guidance_form = f"""
            <form action="/admin/leads/{row['lead_id']}/access" method="post" style="display:block; margin-top:8px;">
                <input type="hidden" name="return_status" value="{current_status}">
                <input type="hidden" name="return_q" value="{current_q}">
                <input type="hidden" name="return_sort_by" value="{current_sort}">
                <input type="hidden" name="return_source_id" value="{current_source_id}">
                <input type="hidden" name="return_public_notice_only" value="{str(current_public_notice_only).lower()}">
                <div style="margin-bottom:6px;">
                    <select name="access_type" style="width:210px;padding:6px;">{access_type_options}</select>
                </div>
                <div style="margin-bottom:6px;">
                    <select name="platform_name" style="width:210px;padding:6px;">{platform_name_options}</select>
                </div>
                <div style="margin-bottom:6px;">
                    <input type="text" name="next_step" value="{row['next_step'] or ''}" placeholder="next step" style="width:320px;padding:6px;">
                </div>
                <div style="margin-bottom:6px;">
                    <input type="text" name="docs_path_note" value="{row['docs_path_note'] or ''}" placeholder="docs path note" style="width:320px;padding:6px;">
                </div>
                <div style="margin-bottom:6px;">
                    <input type="text" name="addenda_note" value="{row['addenda_note'] or ''}" placeholder="addenda note" style="width:320px;padding:6px;">
                </div>
                <div style="margin-bottom:6px;">
                    <input type="text" name="access_notes" value="{row['access_notes'] or ''}" placeholder="internal access notes" style="width:320px;padding:6px;">
                </div>
                <button type="submit" style="padding:6px 8px;">Save access info</button>
            </form>
        """

        notes_form = f"""
            <form action="/admin/leads/{row['lead_id']}/notes" method="post" style="display:block; margin-top:8px;">
                <input type="hidden" name="return_status" value="{current_status}">
                <input type="hidden" name="return_q" value="{current_q}">
                <input type="hidden" name="return_sort_by" value="{current_sort}">
                <input type="hidden" name="return_source_id" value="{current_source_id}">
                <input type="hidden" name="return_public_notice_only" value="{str(current_public_notice_only).lower()}">
                <input type="text" name="admin_notes" value="{row['admin_notes']}" placeholder="admin notes" style="width:220px;padding:6px;">
                <button type="submit" style="padding:6px 8px;">Save note</button>
            </form>
        """

        hidden_inputs = f"""
            <input type="hidden" name="return_status" value="{current_status}">
            <input type="hidden" name="return_q" value="{current_q}">
            <input type="hidden" name="return_sort_by" value="{current_sort}">
            <input type="hidden" name="return_source_id" value="{current_source_id}">
            <input type="hidden" name="return_public_notice_only" value="{str(current_public_notice_only).lower()}">
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
            <td>{duplicate_badge}</td>
            <td>{quality_badge}</td>
            <td>{row['access_type'] or ''}</td>
            <td>{row['platform_name'] or ''}</td>
            <td>{row['next_step'] or ''}</td>
            <td>{row['docs_path_note'] or ''}</td>
            <td>{row['addenda_note'] or ''}</td>
            <td>{row['access_notes'] or ''}</td>
            <td>{row['duplicate_key'] or ''}</td>
            <td><a href="{row['source_url']}" target="_blank">source</a></td>
            <td>
                <form method="post" style="display:block;">
                    {hidden_inputs}
                    {workflow_html}
                </form>
                {guidance_form}
                {notes_form}
            </td>
        </tr>
        """

    export_url = build_redirect_url("/admin/export/leads.csv", status_filter, q, sort_by, source_id, public_notice_only)

    return f"""
    <html><head><title>Admin Leads</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 2200px; margin: 0 auto; }}
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
      <p>{len(leads)} leads currently shown</p>

      <div class="filters">
        <a href="/admin/leads">All ({summary['lead_count']})</a>
        <a href="/admin/leads?status=New">New ({summary['new_lead_count']})</a>
        <a href="/admin/leads?status=Promoted">Promoted ({summary['promoted_lead_count']})</a>
        <a href="/admin/leads?status=Rejected">Rejected ({summary['rejected_lead_count']})</a>
        <a href="/admin/leads?public_notice_only=true&sort_by=quality">Public Notices Review</a>
        <a href="/admin/duplicates">Duplicates ({summary['duplicate_lead_count']})</a>
        <a href="{export_url}">Export filtered leads CSV</a>
      </div>

      <form method="get" action="/admin/leads" class="searchbar">
        <input type="text" name="q" placeholder="Search title / source / agency / county / notes" value="{current_q}">
        <select name="status">
          <option value="" {"selected" if not current_status else ""}>All statuses</option>
          <option value="New" {"selected" if current_status == "New" else ""}>New</option>
          <option value="Promoted" {"selected" if current_status == "Promoted" else ""}>Promoted</option>
          <option value="Rejected" {"selected" if current_status == "Rejected" else ""}>Rejected</option>
        </select>
        <select name="source_id">{source_options}</select>
        <label style="display:inline-block; margin-right:8px;">
          <input type="checkbox" name="public_notice_only" value="true" {"checked" if current_public_notice_only else ""}>
          Public notices only
        </label>
        <select name="sort_by">
          <option value="" {"selected" if not current_sort else ""}>Default sort</option>
          <option value="due_date" {"selected" if current_sort == "due_date" else ""}>Sort by due date</option>
          <option value="quality" {"selected" if current_sort == "quality" else ""}>Sort by quality</option>
        </select>
        <button type="submit">Apply</button>
      </form>

      <form method="post" action="/admin/leads/bulk">
        <input type="hidden" name="return_status" value="{current_status}">
        <input type="hidden" name="return_q" value="{current_q}">
        <input type="hidden" name="return_sort_by" value="{current_sort}">
        <input type="hidden" name="return_source_id" value="{current_source_id}">
        <input type="hidden" name="return_public_notice_only" value="{str(current_public_notice_only).lower()}">

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
              <th>Possible Duplicate</th>
              <th>Quality</th>
              <th>Access</th>
              <th>Platform</th>
              <th>Next Step</th>
              <th>Docs Path</th>
              <th>Addenda</th>
              <th>Access Notes</th>
              <th>Duplicate Key</th>
              <th>Link</th>
              <th>Actions / Editing</th>
            </tr>
          </thead>
          <tbody>{items}</tbody>
        </table>
      </form>
    </div></body></html>
    """


@app.get("/admin/duplicates", response_class=HTMLResponse)
def admin_duplicates_page(
    username: str = Depends(check_auth),
    q: str | None = None,
    sort_by: str | None = None,
):
    leads = fetch_leads(None, q, sort_by, duplicates_only=True)
    current_q = q or ""
    current_sort = sort_by or ""

    items = ""
    for row in leads:
        items += f"""
        <tr>
            <td>{row['title']}</td>
            <td>{row['source_name']}</td>
            <td>{row['lead_id']}</td>
            <td>{row['due_date'] or ''}</td>
            <td>{row['quality_score']}</td>
            <td>{row['access_type'] or ''}</td>
            <td>{row['platform_name'] or ''}</td>
            <td>{row['duplicate_key'] or ''}</td>
            <td>{row['status'] or ''}</td>
            <td><a href="{row['source_url']}" target="_blank">source</a></td>
            <td>
                <form action="/admin/leads/{row['lead_id']}/clear-duplicate" method="post" style="display:inline-block;">
                    <input type="hidden" name="return_q" value="{current_q}">
                    <input type="hidden" name="return_sort_by" value="{current_sort}">
                    <button type="submit" style="padding:8px 10px;">Clear Duplicate Flag</button>
                </form>
            </td>
        </tr>
        """

    return f"""
    <html><head><title>Duplicate Review Queue</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
      .wrap {{ max-width: 1700px; margin: 0 auto; }}
      table {{ border-collapse: collapse; width: 100%; background: white; }}
      th, td {{ border: 1px solid #e5e7eb; padding: 10px; text-align: left; vertical-align: top; }}
      th {{ background: #f3f4f6; }}
      .searchbar {{ background:white; border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin-bottom:16px; }}
      .searchbar input, .searchbar select {{ margin-right:8px; padding:8px; }}
      a {{ color: #0b57d0; text-decoration: none; }}
    </style></head>
    <body><div class="wrap">
      <a href="/admin">← Back to admin</a>
      <h1>Duplicate Review Queue</h1>
      <p>{len(leads)} leads flagged as possible duplicates</p>

      <form method="get" action="/admin/duplicates" class="searchbar">
        <input type="text" name="q" placeholder="Search duplicates" value="{current_q}">
        <select name="sort_by">
          <option value="" {"selected" if not current_sort else ""}>Default sort</option>
          <option value="due_date" {"selected" if current_sort == "due_date" else ""}>Sort by due date</option>
          <option value="quality" {"selected" if current_sort == "quality" else ""}>Sort by quality</option>
        </select>
        <button type="submit">Apply</button>
      </form>

      <table>
        <thead>
          <tr>
            <th>Title</th>
            <th>Source</th>
            <th>Lead ID</th>
            <th>Due Date</th>
            <th>Quality</th>
            <th>Access</th>
            <th>Platform</th>
            <th>Duplicate Key</th>
            <th>Status</th>
            <th>Link</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>{items}</tbody>
      </table>
    </div></body></html>
    """


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "10000"))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=False)
