import os
import secrets
import psycopg2
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

app = FastAPI(title="NJ Bid Registry")
security = HTTPBasic()


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
                (
                    'state-njdot-construction',
                    'NJDOT Construction Services',
                    'State Agency',
                    'Statewide',
                    'https://www.nj.gov/transportation/business/procurement/ConstrServ/curradvproj.shtm',
                    'Tier 1',
                    'Yes'
                ),
                (
                    'state-njdot-profserv',
                    'NJDOT Professional Services',
                    'State Agency',
                    'Statewide',
                    'https://www.nj.gov/transportation/business/procurement/ProfServ/CurrentSolic.shtm',
                    'Tier 1',
                    'Yes'
                ),
                (
                    'state-njta',
                    'NJ Turnpike Authority Current Solicitations',
                    'Transportation Authority',
                    'Statewide',
                    'https://www.njta.gov/business-hub/current-solicitations/',
                    'Tier 1',
                    'Yes'
                ),
                (
                    'state-njtransit',
                    'NJ TRANSIT Procurement Calendar',
                    'Transit Agency',
                    'Statewide',
                    'https://www.njtransit.com/procurement/calendar',
                    'Tier 1',
                    'Yes'
                ),
                (
                    'state-sjta',
                    'South Jersey Transportation Authority Legal Notices',
                    'Transportation Authority',
                    'Atlantic',
                    'https://www.sjta.com/legal-notices',
                    'Tier 1',
                    'Yes'
                ),
                (
                    'state-drjtbc-construction',
                    'DRJTBC Notice To Contractors',
                    'Bi-State Authority',
                    'Warren/Hunterdon/Mercer',
                    'https://www.drjtbc.org/construction-services/notice-to-contractors/',
                    'Tier 1',
                    'Yes'
                ),
                (
                    'state-drjtbc-profserv',
                    'DRJTBC Current Procurements',
                    'Bi-State Authority',
                    'Warren/Hunterdon/Mercer',
                    'https://www.drjtbc.org/professional-services/current/',
                    'Tier 1',
                    'Yes'
                ),
                (
                    'state-panynj-construction',
                    'Port Authority Construction Opportunities',
                    'Bi-State Authority',
                    'Hudson/Essex/Union',
                    'https://www.panynj.gov/port-authority/en/business-opportunities/solicitations-advertisements/Construction.html',
                    'Tier 1',
                    'Yes'
                ),
                (
                    'state-panynj-profserv',
                    'Port Authority Professional Services',
                    'Bi-State Authority',
                    'Hudson/Essex/Union',
                    'https://www.panynj.gov/port-authority/en/business-opportunities/solicitations-advertisements/professional-services.html',
                    'Tier 1',
                    'Yes'
                ),
                (
                    'county-monmouth',
                    'Monmouth County Purchasing',
                    'County',
                    'Monmouth',
                    'https://pol.co.monmouth.nj.us/',
                    'Tier 1',
                    'Yes'
                )
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
                (
                    'opp-njdot-001',
                    'Sample NJDOT Construction Opportunity',
                    'NJDOT Construction Services',
                    'Statewide',
                    'state-njdot-construction',
                    '2026-05-15',
                    'Open',
                    'https://www.nj.gov/transportation/business/procurement/ConstrServ/curradvproj.shtm'
                ),
                (
                    'opp-njta-001',
                    'Sample NJTA Professional Services Opportunity',
                    'NJ Turnpike Authority Current Solicitations',
                    'Statewide',
                    'state-njta',
                    '2026-05-20',
                    'Open',
                    'https://www.njta.gov/business-hub/current-solicitations/'
                ),
                (
                    'opp-monmouth-001',
                    'Sample Monmouth County Intersection Improvement Opportunity',
                    'Monmouth County Purchasing',
                    'Monmouth',
                    'county-monmouth',
                    '2026-05-10',
                    'Open',
                    'https://pol.co.monmouth.nj.us/'
                )
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
                LIMIT 50
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
                LIMIT 50
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


@app.on_event("startup")
def startup_event():
    init_db()


@app.get("/", response_class=HTMLResponse)
def home():
    sources = fetch_sources()
    opportunities = fetch_opportunities()

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
              Live New Jersey transportation bid registry with database-backed sources and opportunities.
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
                <strong>Postgres</strong><br>
                connected and working
              </div>
            </div>

            <div class="nav">
              <a href="/sources">View Sources</a>
              <a href="/opportunities">View Opportunities</a>
              <a href="/api/sources" class="secondary">Sources JSON</a>
              <a href="/api/opportunities" class="secondary">Opportunities JSON</a>
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
              <li>Admin route available with auth</li>
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


@app.get("/api/sources")
def api_sources():
    return JSONResponse(content=fetch_sources())


@app.get("/api/opportunities")
def api_opportunities():
    return JSONResponse(content=fetch_opportunities())


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
    sources = fetch_sources()
    opportunities = fetch_opportunities()

    return f"""
    <html>
      <head>
        <title>Admin</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 40px; background: #f8fafc; color: #111827; }}
          .wrap {{ max-width: 900px; margin: 0 auto; }}
          .card {{ background: white; border: 1px solid #e5e7eb; border-radius: 16px; padding: 28px; }}
          .stats {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 18px 0; }}
          .stat {{ background: #f3f4f6; border-radius: 12px; padding: 16px; min-width: 180px; }}
          a {{ color: #0b57d0; text-decoration: none; }}
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="card">
            <a href="/">← Back to home</a>
            <h1>Admin</h1>
            <p>Signed in as <strong>{username}</strong></p>

            <div class="stats">
              <div class="stat">
                <strong>{len(sources)}</strong><br>
                source records
              </div>
              <div class="stat">
                <strong>{len(opportunities)}</strong><br>
                opportunity records
              </div>
            </div>

            <p><a href="/api/sources">View sources JSON</a></p>
            <p><a href="/api/opportunities">View opportunities JSON</a></p>
            <p><a href="/sources">View sources page</a></p>
            <p><a href="/opportunities">View opportunities page</a></p>
          </div>
        </div>
      </body>
    </html>
    """
