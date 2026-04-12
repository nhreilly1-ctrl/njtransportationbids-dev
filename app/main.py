import os
import psycopg2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="NJ Bid Registry")


def get_conn():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(database_url)


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
        conn.commit()


@app.on_event("startup")
def startup_event():
    init_db()


@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
      <head>
        <title>NJ Transportation Bids</title>
        <style>
          body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.5; }
          h1 { margin-bottom: 8px; }
          a { color: #0b57d0; text-decoration: none; }
          a:hover { text-decoration: underline; }
          .card { max-width: 800px; padding: 24px; border: 1px solid #ddd; border-radius: 12px; }
        </style>
      </head>
      <body>
        <div class="card">
          <h1>NJ Transportation Bids</h1>
          <p>The site is live and now connected to Postgres.</p>
          <p><a href="/health">Health check</a></p>
          <p><a href="/ready">Readiness check</a></p>
          <p><a href="/api/sources">View sources JSON</a></p>
          <p><a href="/sources">View sources page</a></p>
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
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source_id, source_name, entity_type, county, source_url, priority_tier, website_ready
                FROM registry_sources
                ORDER BY source_name
                LIMIT 50
            """)
            rows = cur.fetchall()

    data = []
    for row in rows:
        data.append({
            "source_id": row[0],
            "source_name": row[1],
            "entity_type": row[2],
            "county": row[3],
            "source_url": row[4],
            "priority_tier": row[5],
            "website_ready": row[6],
        })
    return JSONResponse(content=data)


@app.get("/sources", response_class=HTMLResponse)
def sources_page():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source_name, entity_type, county, priority_tier, source_url
                FROM registry_sources
                ORDER BY source_name
                LIMIT 50
            """)
            rows = cur.fetchall()

    items = ""
    for row in rows:
        items += f"""
        <tr>
            <td>{row[0] or ''}</td>
            <td>{row[1] or ''}</td>
            <td>{row[2] or ''}</td>
            <td>{row[3] or ''}</td>
            <td><a href="{row[4]}" target="_blank">link</a></td>
        </tr>
        """

    return f"""
    <html>
      <head>
        <title>Sources</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 40px; }}
          table {{ border-collapse: collapse; width: 100%; }}
          th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
          th {{ background: #f5f5f5; }}
        </style>
      </head>
      <body>
        <h1>Registry Sources</h1>
        <table>
          <thead>
            <tr>
              <th>Source Name</th>
              <th>Entity Type</th>
              <th>County</th>
              <th>Priority</th>
              <th>URL</th>
            </tr>
          </thead>
          <tbody>
            {items}
          </tbody>
        </table>
      </body>
    </html>
    """
