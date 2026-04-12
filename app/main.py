import os
import re
import secrets
import psycopg2
import requests
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

app = FastAPI(title="NJ Bid Registry")
security = HTTPBasic()

NJDOT_CONSTRUCTION_URL = "https://www.nj.gov/transportation/business/procurement/ConstrServ/curradvproj.shtm"


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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                INSERT INTO registry_sources (
                    source_id,
                    source_name,
                    entity_type,
                    county,
                    source_url,
                    priority_tier,
                    website_ready
                )
                VALUES
                ('state-njdot-construction','NJDOT Construction Services','State Agency','Statewide','https://www.nj.gov/transportation/business/procurement/ConstrServ/curradvproj.shtm','Tier 1','Yes'),
                ('state-njdot-profserv','NJDOT Professional Services','State Agency','Statewide','https://www.nj.gov/transportation/business/procurement/ProfServ/CurrentSolic.shtm','Tier 1','Yes'),
                ('state-njta','NJ Turnpike Authority Current Solicitations','Transportation Authority','Statewide','https://www.njta.gov/business-hub/current-solicitations/','Tier 1','Yes'),
                ('state-njtransit','NJ TRANSIT Procurement Calendar','Transit Agency','Statewide','https://www.njtransit.com/procurement/calendar','Tier 1','Yes'),
                ('state-sjta','South Jersey Transportation Authority Legal Notices','Transportation Authority','Atlantic','https://www.sjta.com/legal-notices','Tier 1','Yes'),
                ('state-drjtbc-construction','DRJTBC Notice To Contractors','Bi-State Authority','Warren/Hunterdon/Mercer','https://www.drjtbc.org/construction-services/notice-to-contractors/','Tier 1','Yes'),
                ('state-drjtbc-profserv','DRJTBC Current Procurements','Bi-State Authority','Warren/Hunterdon/Mercer','https://www.drjtbc.org/professional-services/current/','Tier 1','Yes'),
                ('state-panynj-construction','Port Authority Construction Opportunities','Bi-State Authority','Hudson/Essex/Union','https://www.panynj.gov/port-authority/en/business-opportunities/solicitations-advertisements/Construction.html','Tier 1','Yes'),
                ('state-panynj-profserv','Port Authority Professional Services','Bi-State Authority','Hudson/Essex/Union','https://www.panynj.gov/port-authority/en/business-opportunities/solicitations-advertisements/professional-services.html','Tier 1','Yes'),
                ('county-monmouth','Monmouth County Purchasing','County','Monmouth','https://pol.co.monmouth.nj.us/','Tier 1','Yes'),
                ('county-atlantic','Atlantic County Open Bids','County','Atlantic','https://www.atlanticcountynj.gov/government/county-departments/department-of-administrative-services/division-of-budget-and-purchasing/open-bids','Tier 1','Yes'),
                ('county-bergen','Bergen County Bids','County','Bergen','https://bergenbids.com/','Tier 1','Yes'),
                ('county-burlington','Burlington County Bid Solicitations','County','Burlington','https://www.co.burlington.nj.us/490/Bid-Solicitations','Tier 1','Yes'),
                ('county-camden','Camden County Procurements','County','Camden','https://procurements.camdencounty.com/','Tier 1','Yes'),
                ('county-cape-may','Cape May County Bids and RFPs','County','Cape May','https://capemaycountynj.gov/1072/Bids-and-RFPs','Tier 2','Yes'),
                ('county-cumberland','Cumberland County Bids','County','Cumberland','https://www.cumberlandcountynj.gov/bids','Tier 2','Yes'),
                ('county-essex','Essex County Procurement','County','Essex','https://www.essexcountynjprocure.org/bids/search?rfp_filter_status=current','Tier 1','Yes'),
                ('county-gloucester','Gloucester County Bids','County','Gloucester','https://www.gloucestercountynj.gov/Bids.aspx','Tier 2','Yes'),
                ('county-hudson','Hudson County Purchasing','County','Hudson','https://www.hcnj.us/finance/purchasing/','Tier 1','Yes'),
                ('county-hunterdon','Hunterdon County Bids','County','Hunterdon','https://www.co.hunterdon.nj.us/Bids.aspx','Tier 2','Yes'),
                ('county-mercer','Mercer County Bidding Opportunities','County','Mercer','https://www.mercercounty.org/departments/purchasing/bidding-opportunities','Tier 1','Yes'),
                ('county-middlesex','Middlesex County Improvement Authority Opportunities','County','Middlesex','https://www.middlesexcountynj.gov/government/departments/department-of-economic-development/middlesex-county-improvement-authority/current-bidding-opportunities','Tier 1','Yes'),
                ('county-morris','Morris County Bids and Quotes','County','Morris','https://www.morriscountynj.gov/Departments/Purchasing/Bids-and-Quotes','Tier 1','Yes'),
                ('county-ocean','Ocean County Purchasing','County','Ocean','https://www.co.ocean.nj.us/oc/purchasing/frmhomepdept.aspx','Tier 1','Yes'),
                ('county-union','Union County Invitations to Bid','County','Union','https://ucnj.org/vendor-opportunities/invitations-to-bid/current/','Tier 1','Yes')
                ON CONFLICT (source_id) DO NOTHING;
            """)

            cur.execute("""
                INSERT INTO opportunities (
                    opportunity_id,
                    title,
                    agency,
                    county,
                    source_id,
                    due_date,
                    status,
                    opportunity_url
                )
                VALUES
                ('opp-njdot-001','Sample NJDOT Construction Opportunity','NJDOT Construction Services','Statewide','state-njdot-construction','2026-05-15','Open','https://www.nj.gov/transportation/business/procurement/ConstrServ/curradvproj.shtm'),
                ('opp-njta-001','Sample NJTA Professional Services Opportunity','NJ Turnpike Authority Current Solicitations','Statewide','state-njta','2026-05-20','Open','https://www.njta.gov/business-hub/current-solicitations/'),
                ('opp-monmouth-001','Sample Monmouth County Intersection Improvement Opportunity','Monmouth County Purchasing','Monmouth','county-monmouth','2026-05-10','Open','https://pol.co.monmouth.nj.us/')
                ON CONFLICT (opportunity_id) DO NOTHING;
            """)
        conn.commit()


def fetch_sources():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source_id, source_name, entity_type, county, source_url, priority_tier, website_ready
                FROM registry_sources
                ORDER BY source_name
                LIMIT 100
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
        }
        for row in rows
    ]


def fetch_opportunities():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT opportunity_id, title, agency, county, source_id, due_date, status, opportunity_url
                FROM opportunities
                ORDER BY due_date
                LIMIT 100
            """)
            rows = cur.fetchall()

    return [
        {
            "opportunity_id": row[0],
            "title": row[1],
            "agency": row[2],
            "county": row[3],
            "source_id": row[4],
            "due_date": row[5],
            "status": row[6],
            "opportunity_url": row[7],
        }
        for row in rows
    ]


def fetch_leads():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT lead_id, source_id, title, agency, county, posted_date, due_date, status, source_url, created_at
                FROM opportunity_leads
                ORDER BY created_at DESC, title
                LIMIT 100
            """)
            rows = cur.fetchall()

    return [
        {
            "lead_id": row[0],
            "source_id": row[1],
            "title": row[2],
            "agency": row[3],
            "county": row[4],
            "posted_date": row[5],
            "due_date": row[6],
            "status": row[7],
            "source_url": row[8],
            "created_at": str(row[9]),
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

            cur.execute("""
                SELECT COALESCE(entity_type, 'Unknown'), COUNT(*)
                FROM registry_sources
                GROUP BY COALESCE(entity_type, 'Unknown')
                ORDER BY COUNT(*) DESC, COALESCE(entity_type, 'Unknown')
            """)
            entity_rows = cur.fetchall()

            cur.execute("""
                SELECT COALESCE(county, 'Unknown'), COUNT(*)
                FROM registry_sources
                GROUP BY COALESCE(county, 'Unknown')
                ORDER BY COUNT(*) DESC, COALESCE(county, 'Unknown')
                LIMIT 10
            """)
            county_rows = cur.fetchall()

    return {
        "source_count": source_count,
        "opportunity_count": opportunity_count,
        "lead_count": lead_count,
        "by_entity_type": [{"entity_type": row[0], "count": row[1]} for row in entity_rows],
        "top_counties": [{"county": row[0], "count": row[1]} for row in county_rows],
    }


def strip_html(text: str) -> str:
    text = re.sub(r"<script.*?</script>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;|&#160;", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_due_dates(text: str):
    pattern = r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
    return re.findall(pattern, text)


def manual_crawl_njdot_construction():
    headers = {"User-Agent": "Mozilla/5.0 NJTransportationBids/1.0"}
    resp = requests.get(NJDOT_CONSTRUCTION_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    cleaned = strip_html(resp.text)
    if not cleaned:
        return {"inserted": 0, "titles": []}

    titles = []
    for match in re.finditer(r"(Contract [A-Z0-9\-.]+.*?)(?=Contract [A-Z0-9\-.]+|$)", cleaned, flags=re.I):
        chunk = match.group(1).strip()
        if len(chunk) > 30:
            titles.append(chunk)

    if not titles:
        sentences = re.split(r"(?<=[.!?])\s+", cleaned)
        for sentence in sentences:
            if "contract" in sentence.lower() or "proposal" in sentence.lower():
                if len(sentence.strip()) > 30:
                    titles.append(sentence.strip())

    deduped = []
    seen = set()
    for title in titles:
        short = title[:220].strip()
        if short and short not in seen:
            seen.add(short)
            deduped.append(short)

    deduped = deduped[:15]

    inserted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for idx, title in enumerate(deduped, start=1):
                lead_id = f"lead-njdot-construction-{idx}"
                due_date_match = re.search(
                    r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}",
                    title,
                    flags=re.I,
                )
                due_date = due_date_match.group(0) if due_date_match else None

                cur.execute("""
                    INSERT INTO opportunity_leads (
                        lead_id,
                        source_id,
                        title,
                        agency,
                        county,
                        posted_date,
                        due_date,
                        status,
                        source_url,
                        raw_text
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (lead_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        agency = EXCLUDED.agency,
                        county = EXCLUDED.county,
                        due_date = EXCLUDED.due_date,
                        status = EXCLUDED.status,
                        source_url = EXCLUDED.source_url,
                        raw_text = EXCLUDED.raw_text
                """, (
                    lead_id,
                    "state-njdot-construction",
                    title,
                    "NJDOT Construction Services",
                    "Statewide",
                    None,
                    due_date,
                    "New",
                    NJDOT_CONSTRUCTION_URL,
                    title
                ))
                inserted += 1
        conn.commit()

    return {"inserted": inserted, "titles": deduped}


@app.on_event("startup")
def startup_event():
    init_db()


@app.get("/", response_class=HTMLResponse)
def home():
    sources = fetch_sources()
    opportunities = fetch_opportunities()
    summary = fetch_admin_summary()

    return f"""
    <html>
      <head>
        <title>NJ Transportation Bids</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 40px; line-height: 1.5; background: #f8fafc; color: #111827; }}
          .wrap {{ max-width: 960px; margin: 0 auto; }}
          .hero {{ background: white; border: 1px solid #e5e7eb; border-radius: 16px; padding: 28px; margin-bottom: 24px; }}
          .stats {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 18px 0; }}
          .stat {{ background: #f3f4f6; border-radius: 12px; padding: 16px; min-width: 180px; }}
          .nav {{ display: flex; gap: 12px; flex-wrap: wrap; margin-top: 20px; }}
          .nav a {{ display: inline-block; background: #0b57d0; color: white; padding: 10px 14px; border-radius: 10px; text-decoration: none; }}
          .nav a.secondary {{ background: #374151; }}
          .section {{ background: white; border: 1px solid #e5e7eb; border-radius: 16px; padding: 24px; }}
          ul {{ padding-left: 18px; }}
          li {{ margin-bottom: 8px; }}
          .muted {{ color: #4b5563; }}
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="hero">
            <h1>NJ Transportation Bids</h1>
            <p class="muted">
              Live New Jersey transportation bid registry with database-backed sources, opportunities, and lead staging.
            </p>

            <div class="stats">
              <div class="stat">
                <strong>{len(sources)}</strong><br>
                live source records
              </div>
              <div class="stat">
                <strong>{len(opportunities)}</strong><br>
                live opportunity records
              </div>
              <div class="stat">
                <strong>{summary['lead_count']}</strong><br>
                lead records
              </div>
            </div>

            <div class="nav">
              <a href="/sources">View Sources</a>
              <a href="/opportunities">View Opportunities</a>
              <a href="/admin" class="secondary">Admin</a>
              <a href="/health" class="secondary">Health</a>
              <a href="/ready" class="secondary">Readiness</a>
            </div>
          </div>

          <div class="section">
            <h2>What is live now</h2>
            <ul>
              <li>Custom domain connected</li>
              <li>Health and readiness checks passing</li>
              <li>Database-backed source registry working</li>
              <li>Database-backed opportunities page working</li>
              <li>Lead staging table and admin leads page ready</li>
            </ul>
          </div>
        </div>
      </body>
    </html>
    """


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/ready")
def ready():
    return {"ok": True}


@app.get("/debug-auth")
def debug_auth():
    return {
        "admin_username_set": bool(os.environ.get("ADMIN_USERNAME")),
        "admin_password_set": bool(os.environ.get("ADMIN_PASSWORD")),
        "admin_username_length": len(os.environ.get("ADMIN_USERNAME", "")),
        "admin_password_length": len(os.environ.get("ADMIN_PASSWORD", "")),
    }


@app.get("/api/sources")
def api_sources():
    return JSONResponse(content=fetch_sources())


@app.get("/api/opportunities")
def api_opportunities():
    return JSONResponse(content=fetch_opportunities())


@app.get("/api/admin/summary")
def api_admin_summary(username: str = Depends(check_auth)):
    return JSONResponse(content=fetch_admin_summary())


@app.get("/api/admin/leads")
def api_admin_leads(username: str = Depends(check_auth)):
    return JSONResponse(content=fetch_leads())


@app.post("/admin/crawl/njdot-construction")
def admin_crawl_njdot_construction(username: str = Depends(check_auth)):
    manual_crawl_njdot_construction()
    return RedirectResponse(url="/admin/leads", status_code=303)


@app.get("/sources", response_class=HTMLResponse)
def sources_page():
    sources = fetch_sources()

    items = ""
    for row in sources:
        items += f"""
        <tr>
            <td><a href="{row['source_url']}" target="_blank">{row['source_name']}</a></td>
            <td>{row['entity_type'] or ''}</td>
            <td>{row['county'] or ''}</td>
            <td>{row['priority_tier'] or ''}</td>
            <td>{row['website_ready'] or ''}</td>
        </tr>
        """

    return f"""
    <html>
      <head>
        <title>Sources</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
          .wrap {{ max-width: 1100px; margin: 0 auto; }}
          .top {{ margin-bottom: 24px; }}
          .top a {{ color: #0b57d0; text-decoration: none; }}
          table {{ border-collapse: collapse; width: 100%; background: white; }}
          th, td {{ border: 1px solid #e5e7eb; padding: 10px; text-align: left; }}
          th {{ background: #f3f4f6; }}
          h1 {{ margin-bottom: 6px; }}
          .muted {{ color: #4b5563; }}
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="top">
            <a href="/">← Back to home</a>
            <h1>Registry Sources</h1>
            <p class="muted">{len(sources)} sources currently loaded</p>
          </div>

          <table>
            <thead>
              <tr>
                <th>Source Name</th>
                <th>Entity Type</th>
                <th>County</th>
                <th>Priority</th>
                <th>Website Ready</th>
              </tr>
            </thead>
            <tbody>
              {items}
            </tbody>
          </table>
        </div>
      </body>
    </html>
    """


@app.get("/opportunities", response_class=HTMLResponse)
def opportunities_page():
    opportunities = fetch_opportunities()

    items = ""
    for row in opportunities:
        items += f"""
        <tr>
            <td><a href="{row['opportunity_url']}" target="_blank">{row['title']}</a></td>
            <td>{row['agency'] or ''}</td>
            <td>{row['county'] or ''}</td>
            <td>{row['due_date'] or ''}</td>
            <td>{row['status'] or ''}</td>
        </tr>
        """

    return f"""
    <html>
      <head>
        <title>Opportunities</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
          .wrap {{ max-width: 1100px; margin: 0 auto; }}
          .top {{ margin-bottom: 24px; }}
          .top a {{ color: #0b57d0; text-decoration: none; }}
          table {{ border-collapse: collapse; width: 100%; background: white; }}
          th, td {{ border: 1px solid #e5e7eb; padding: 10px; text-align: left; }}
          th {{ background: #f3f4f6; }}
          h1 {{ margin-bottom: 6px; }}
          .muted {{ color: #4b5563; }}
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="top">
            <a href="/">← Back to home</a>
            <h1>Opportunities</h1>
            <p class="muted">{len(opportunities)} opportunities currently loaded</p>
          </div>

          <table>
            <thead>
              <tr>
                <th>Title</th>
                <th>Agency</th>
                <th>County</th>
                <th>Due Date</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {items}
            </tbody>
          </table>
        </div>
      </body>
    </html>
    """


@app.get("/admin", response_class=HTMLResponse)
def admin_page(username: str = Depends(check_auth)):
    summary = fetch_admin_summary()

    entity_items = ""
    for row in summary["by_entity_type"]:
        entity_items += f"<li>{row['entity_type']}: {row['count']}</li>"

    county_items = ""
    for row in summary["top_counties"]:
        county_items += f"<li>{row['county']}: {row['count']}</li>"

    return f"""
    <html>
      <head>
        <title>Admin</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
          .wrap {{ max-width: 1000px; margin: 0 auto; }}
          .card {{ background: white; border: 1px solid #e5e7eb; border-radius: 16px; padding: 28px; }}
          .stats {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 18px 0 24px 0; }}
          .stat {{ background: #f3f4f6; border-radius: 12px; padding: 16px; min-width: 180px; }}
          .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
          .panel {{ background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 12px; padding: 18px; }}
          .nav a, a {{ color: #0b57d0; text-decoration: none; }}
          .button {{ display: inline-block; background: #0b57d0; color: white; padding: 10px 14px; border: none; border-radius: 10px; cursor: pointer; }}
          form {{ margin: 0; }}
          ul {{ padding-left: 18px; }}
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="card">
            <a href="/">← Back to home</a>
            <h1>Admin Dashboard</h1>
            <p>Signed in as <strong>{username}</strong></p>

            <div class="stats">
              <div class="stat">
                <strong>{summary['source_count']}</strong><br>
                source records
              </div>
              <div class="stat">
                <strong>{summary['opportunity_count']}</strong><br>
                published opportunities
              </div>
              <div class="stat">
                <strong>{summary['lead_count']}</strong><br>
                opportunity leads
              </div>
            </div>

            <div class="grid">
              <div class="panel">
                <h3>Sources by entity type</h3>
                <ul>{entity_items}</ul>
              </div>

              <div class="panel">
                <h3>Top counties</h3>
                <ul>{county_items}</ul>
              </div>
            </div>

            <h3>Manual crawl controls</h3>
            <form action="/admin/crawl/njdot-construction" method="post">
              <button class="button" type="submit">Run NJDOT Construction Crawl</button>
            </form>

            <h3>Admin links</h3>
            <p><a href="/admin/leads">View admin leads page</a></p>
            <p><a href="/api/admin/summary">Admin summary JSON</a></p>
            <p><a href="/api/admin/leads">Admin leads JSON</a></p>
            <p><a href="/sources">View sources page</a></p>
            <p><a href="/opportunities">View opportunities page</a></p>
          </div>
        </div>
      </body>
    </html>
    """


@app.get("/admin/leads", response_class=HTMLResponse)
def admin_leads_page(username: str = Depends(check_auth)):
    leads = fetch_leads()

    items = ""
    for row in leads:
        items += f"""
        <tr>
            <td>{row['title']}</td>
            <td>{row['source_id']}</td>
            <td>{row['agency'] or ''}</td>
            <td>{row['county'] or ''}</td>
            <td>{row['due_date'] or ''}</td>
            <td>{row['status'] or ''}</td>
            <td><a href="{row['source_url']}" target="_blank">source</a></td>
        </tr>
        """

    return f"""
    <html>
      <head>
        <title>Admin Leads</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
          .wrap {{ max-width: 1200px; margin: 0 auto; }}
          .top {{ margin-bottom: 24px; }}
          .top a {{ color: #0b57d0; text-decoration: none; }}
          table {{ border-collapse: collapse; width: 100%; background: white; }}
          th, td {{ border: 1px solid #e5e7eb; padding: 10px; text-align: left; vertical-align: top; }}
          th {{ background: #f3f4f6; }}
          .muted {{ color: #4b5563; }}
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="top">
            <a href="/admin">← Back to admin</a>
            <h1>Admin Leads</h1>
            <p class="muted">{len(leads)} leads currently loaded</p>
          </div>

          <table>
            <thead>
              <tr>
                <th>Title</th>
                <th>Source ID</th>
                <th>Agency</th>
                <th>County</th>
                <th>Due Date</th>
                <th>Status</th>
                <th>Link</th>
              </tr>
            </thead>
            <tbody>
              {items}
            </tbody>
          </table>
        </div>
      </body>
    </html>
    """
