import csv
import functools
import hashlib
import json
import os
from datetime import date, datetime, timedelta
from io import StringIO

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, Response, jsonify, redirect, render_template, request, session, url_for


app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-in-prod")

from app.notice_routes import notice_bp
app.register_blueprint(notice_bp)

from app.network_routes import network_bp
app.register_blueprint(network_bp)

@app.context_processor
def inject_globals():
    return {"today_date": date.today().isoformat()}

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA = os.path.join(BASE, "data") if os.path.isdir(os.path.join(BASE, "data")) else os.path.join(BASE, "data_store")
OPP_F = os.path.join(DATA, "opportunities.json")
SRC_F = os.path.join(DATA, "sources.json")

ADMIN_USER = os.environ.get("ADMIN_USERNAME", "admin")
_admin_password = os.environ.get("ADMIN_PASSWORD")
ADMIN_HASH = os.environ.get("ADMIN_PASSWORD_HASH") or (
    hashlib.sha256(_admin_password.encode()).hexdigest()
    if _admin_password
    else hashlib.sha256(b"changeme").hexdigest()
)

SOURCE_TYPE_MAP = {
    "njdot construction": "construction",
    "notice to contractors": "construction",
    "port authority construction": "construction",
    "drjtbc notice to contractors": "construction",
    "njdot professional services": "professional_services",
    "njdot procurement notices": "professional_services",
    "port authority professional": "professional_services",
    "drjtbc current": "professional_services",
    "nj department of state legal": "public_notice",
    "nj department of state public": "public_notice",
    "south jersey transportation authority legal": "public_notice",
    "nj treasury legal": "public_notice",
    "city of trenton legal": "public_notice",
}

SOURCE_ID_FALLBACK_TYPES = {
    "state-njdot-construction": "construction",
    "state-njdot-profserv": "professional_services",
    "state-drjtbc-construction": "construction",
    "state-drjtbc-profserv": "professional_services",
    "state-njta": "construction",
}

TITLE_TYPE_RULES = [
    (
        "public_notice",
        [
            "notice to all",
            "notice to contractors",
            "legal notice",
            "notice of intent",
            "legal ad",
            "public notice",
            "prequalif",
            "pre-qualif",
            "pre qualif",
            "2025 eeo",
            "2026 eeo",
        ],
    ),
    (
        "professional_services",
        [
            "rfp ",
            "rfq ",
            "rfp-",
            "rfq-",
            "request for proposal",
            "request for qualif",
            "professional services",
            "engineering services",
            "design services",
            "inspection services",
            "planning services",
            "construction inspection",
            "structural evaluation",
            "underwater inspection",
            "consulting",
            "consultant",
            "feasibility",
            "alternatives analysis",
            "program management",
            "cpmc",
            "order for professional",
            "op no.",
            "ops no.",
            "tp-",
            "tp -",
        ],
    ),
    (
        "construction",
        [
            "bid no.",
            "bid no ",
            "bid number",
            "ifb ",
            "ifb no.",
            "invitation for bids",
            "contract no.",
            "contract no ",
            "contract number",
            "roadway improvement",
            "road improvement",
            "road resurfacing",
            "pavement",
            "milling",
            "resurfacing",
            "overlay",
            "bridge replacement",
            "bridge rehabilitation",
            "bridge repair",
            "drainage improvement",
            "drainage repair",
            "intersection improvement",
            "signal",
            "guide rail",
            "guardrail",
            "culvert",
            "maintenance contract",
            "snow removal",
            "construction",
        ],
    ),
]

PUBLIC_NOTICE_CONSTRUCTION_SIGNALS = [
    "construction",
    "roadway",
    "bridge",
    "pavement",
    "drainage",
    "intersection",
    "culvert",
    "resurfacing",
    "guide rail",
    "maintenance",
    "notice to contractors",
    "bid opening",
    "contract award",
    "t200.",
    "t100.",
    "p200.",
    "p500.",
]

PUBLIC_NOTICE_PROFSERV_SIGNALS = [
    "rfp",
    "rfq",
    "professional services",
    "engineering",
    "design",
    "inspection",
    "planning",
    "consultant",
    "program management",
    "cpmc",
    "feasibility",
    "alternatives analysis",
    "tp-",
    "ops no.",
    "op no.",
]

NOISE_PHRASES = [
    "sign in",
    "staff directory",
    "vendor portal",
    "how do i",
    "search home",
    "website sign",
    "government departments",
    "built to help vendors",
    "in order to maintain",
    "contract documents or any",
    "contract documents should",
    "contract awards",
    "notice to all",
    "procurement calendar",
    "professional services upcoming",
    "professional services /",
    "rfbs (request for bids) awarded",
    "rfbs (request for bids) upcoming",
    "rfps (request for proposals) fair",
    "rfpq",
    "bidder's application",
    "results of bid/rfp",
    "bids and tenders",
    "camden business improvement",
    "comprehensive bridge replacement and improvement plan",
    "government records - bridge",
    "construction and materials",
    "mobility and systems",
    "vendor/contractor assistance",
    "alternative project delivery",
]

OUT_OF_SCOPE = [
    "harley",
    "davidson",
    "motorcycle",
    "cannabis",
    "housing rehabilitation",
    "septic",
    "arboriculture",
    "arborist",
    "eeoc",
    "affordable housing",
    "small cities",
    "exhibition design",
    "black heritage",
    "historic marker",
    "landscape maintenance",
    "ev charging",
    "electric vehicle charging",
    "rfq #25-arch",
    "rfq #25-njbac",
    "rfq #cc120",
]

SOURCE_RULES = {
    "state-njdot-construction": {"score": 5.0, "mode": "trusted", "label": "Trusted"},
    "state-njdot-profserv": {"score": 5.0, "mode": "trusted", "label": "Trusted"},
    "state-drjtbc-construction": {"score": 5.0, "mode": "trusted", "label": "Trusted"},
    "state-drjtbc-profserv": {"score": 5.0, "mode": "trusted", "label": "Trusted"},
    "state-njta": {"score": 4.5, "mode": "trusted", "label": "Trusted"},
    "state-njtransit": {"score": 4.5, "mode": "trusted", "label": "Trusted"},
    "state-sjta": {"score": 4.5, "mode": "trusted", "label": "Trusted"},
    "state-panynj-construction": {"score": 4.5, "mode": "trusted", "label": "Trusted"},
    "state-panynj-profserv": {"score": 4.5, "mode": "trusted", "label": "Trusted"},
    "county-camden": {"score": 4.0, "mode": "ai_review", "label": "AI review"},
    "county-burlington": {"score": 4.0, "mode": "ai_review", "label": "AI review"},
    "municipal-jersey-city": {"score": 4.0, "mode": "ai_review", "label": "AI review"},
    "municipal-hoboken": {"score": 4.0, "mode": "ai_review", "label": "AI review"},
    "county-bergen": {"score": 3.5, "mode": "ai_review", "label": "AI review"},
    "county-essex": {"score": 3.5, "mode": "manual_review", "label": "Manual review"},
    "municipal-paterson": {"score": 3.5, "mode": "manual_review", "label": "Manual review"},
    "municipal-elizabeth": {"score": 3.5, "mode": "manual_review", "label": "Manual review"},
    "county-cape-may": {"score": 3.5, "mode": "manual_review", "label": "Manual review"},
    "county-hudson": {"score": 3.5, "mode": "manual_review", "label": "Manual review"},
    "municipal-camden": {"score": 3.5, "mode": "manual_review", "label": "Manual review"},
    "county-cumberland": {"score": 3.0, "mode": "manual_review", "label": "Manual review"},
    "county-gloucester": {"score": 3.0, "mode": "manual_review", "label": "Manual review"},
    "county-hunterdon": {"score": 3.0, "mode": "manual_review", "label": "Manual review"},
    "municipal-newark": {"score": 2.0, "mode": "metadata_only", "label": "Metadata only"},
    "county-atlantic": {"score": 1.0, "mode": "disabled", "label": "Disabled"},
    "county-mercer": {"score": 1.0, "mode": "disabled", "label": "Disabled"},
    "municipal-trenton": {"score": 1.0, "mode": "disabled", "label": "Disabled"},
}

DEFAULT_SOURCE_RULE = {"score": 2.5, "mode": "manual_review", "label": "Manual review"}


def _check_pw(password: str) -> bool:
    return hashlib.sha256(password.encode()).hexdigest() == ADMIN_HASH


def admin_required(view):
    @functools.wraps(view)
    def wrapper(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login", next=request.path))
        return view(*args, **kwargs)

    return wrapper


def use_db_backend() -> bool:
    return bool(os.environ.get("DATABASE_URL"))


def get_conn():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(db_url)


def init_db_schema() -> None:
    if not use_db_backend():
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS status_override TEXT;")
                cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS noise_flagged BOOLEAN DEFAULT FALSE;")
                cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS noise_reason TEXT;")
                cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS record_type_override TEXT;")
                cur.execute("ALTER TABLE opportunity_leads ADD COLUMN IF NOT EXISTS notice_subtype_override TEXT;")
            conn.commit()
    except Exception:
        pass


def load_json_file(path: str) -> list[dict]:
    if not os.path.isfile(path):
        return []
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def save_json_file(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(rows, handle, indent=2, default=str)


def source_tier(source_id: str | None, entity_type: str | None) -> str:
    source_id = (source_id or "").lower()
    entity = (entity_type or "").lower()
    if source_id.startswith("state-") or "state" in entity or "authority" in entity or "transit" in entity:
        return "state"
    if source_id.startswith("county-") or "county" in entity:
        return "county"
    return "municipal"


def source_rule_for(source_id: str | None) -> dict:
    return dict(SOURCE_RULES.get((source_id or "").lower(), DEFAULT_SOURCE_RULE))


def load_opps_from_db() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    l.lead_id::text AS id,
                    l.source_id,
                    l.title,
                    COALESCE(NULLIF(rs.source_name, ''), NULLIF(l.agency, ''), l.source_id) AS source_name,
                    NULLIF(l.agency, '') AS agency,
                    NULLIF(l.county, '') AS county,
                    l.due_date AS due_date_raw,
                    l.source_url AS official_url,
                    NULLIF(l.access_type, '') AS access_type,
                    NULLIF(l.platform_name, '') AS platform,
                    NULLIF(l.next_step, '') AS next_step,
                    NULLIF(l.docs_path_note, '') AS docs_path_note,
                    NULLIF(l.addenda_note, '') AS addenda_note,
                    COALESCE(l.status_override, '') AS status_override,
                    COALESCE(l.noise_flagged, FALSE) AS noise_flagged,
                    COALESCE(l.noise_reason, '') AS noise_reason,
                    COALESCE(l.admin_notes, '') AS admin_notes,
                    COALESCE(l.record_type_override, '') AS record_type_override,
                    COALESCE(l.notice_subtype_override, '') AS notice_subtype_override,
                    COALESCE(l.status, '') AS db_status,
                    COALESCE(l.raw_text, '') AS raw_text,
                    l.created_at
                FROM opportunity_leads l
                LEFT JOIN registry_sources rs ON rs.source_id = l.source_id
                WHERE COALESCE(l.status, '') != 'Rejected'
                ORDER BY l.created_at DESC NULLS LAST, l.title
                """
            )
            rows = cur.fetchall()
    return [dict(row) for row in rows]


def load_sources_from_db() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    source_id,
                    source_name,
                    entity_type,
                    county,
                    source_url,
                    last_crawl_at,
                    last_crawl_status,
                    last_leads_found
                FROM registry_sources
                ORDER BY source_name
                """
            )
            rows = cur.fetchall()
    sources = []
    for row in rows:
        item = dict(row)
        sources.append(
            {
                "id": item["source_id"],
                "name": item["source_name"],
                "tier": source_tier(item["source_id"], item.get("entity_type")),
                "source_rule": source_rule_for(item["source_id"])["mode"],
                "rule_label": source_rule_for(item["source_id"])["label"],
                "crawlability_score": source_rule_for(item["source_id"])["score"],
                "county": item.get("county"),
                "url": item.get("source_url"),
                "last_crawl": item["last_crawl_at"].isoformat(sep=" ", timespec="minutes") if item.get("last_crawl_at") else None,
                "last_status": item.get("last_crawl_status"),
                "last_leads_found": item.get("last_leads_found") or 0,
            }
        )
    return sources


def load_opps() -> list[dict]:
    if use_db_backend():
        return load_opps_from_db()
    return load_json_file(OPP_F)


def save_opps(opps: list[dict]) -> None:
    if not use_db_backend():
        save_json_file(OPP_F, opps)


def load_sources() -> list[dict]:
    if use_db_backend():
        return load_sources_from_db()
    return load_json_file(SRC_F)


def classify_record(opp: dict) -> tuple[str, str | None]:
    manual_type = (opp.get("record_type_override") or "").strip()
    manual_subtype = (opp.get("notice_subtype_override") or "").strip() or None
    if manual_type:
        return manual_type, manual_subtype

    title = (opp.get("title") or "").lower()
    src_name = (opp.get("source_name") or "").lower()
    source_id = (opp.get("source_id") or "").lower()

    src_type = None
    for key, value in SOURCE_TYPE_MAP.items():
        if key in src_name or key in source_id:
            src_type = value
            break

    title_type = None
    for record_type, keywords in TITLE_TYPE_RULES:
        if any(keyword in title for keyword in keywords):
            title_type = record_type
            break

    record_type = title_type or src_type or SOURCE_ID_FALLBACK_TYPES.get(source_id) or "uncategorized"
    notice_subtype = None
    if record_type == "public_notice":
        if any(keyword in title for keyword in PUBLIC_NOTICE_CONSTRUCTION_SIGNALS):
            notice_subtype = "construction"
        elif any(keyword in title for keyword in PUBLIC_NOTICE_PROFSERV_SIGNALS):
            notice_subtype = "professional_services"

    return record_type, notice_subtype


def update_leads(ids: list[str], action: str, record_type: str | None = None, notice_subtype: str | None = None) -> int:
    if not ids:
        return 0

    if not use_db_backend():
        opps = load_opps()
        changed = 0
        id_set = set(ids)
        for opp in opps:
            if opp.get("id") not in id_set:
                continue
            if action == "delete":
                opp["status_override"] = "deleted"
                opp["noise_flagged"] = False
            elif action == "noise":
                opp["status_override"] = "noise"
                opp["noise_flagged"] = True
            elif action == "approve":
                opp["status_override"] = "approved"
                opp["noise_flagged"] = False
                opp["noise_reason"] = ""
            elif action == "restore":
                opp["status_override"] = ""
                opp["noise_flagged"] = False
                opp["noise_reason"] = ""
            elif action == "set_type" and record_type:
                opp["record_type_override"] = record_type
                opp["notice_subtype_override"] = notice_subtype or ""
            changed += 1
        save_opps(opps)
        return changed

    with get_conn() as conn:
        with conn.cursor() as cur:
            if action == "set_type" and record_type:
                cur.execute(
                    """
                    UPDATE opportunity_leads
                    SET
                        record_type_override = %s,
                        notice_subtype_override = %s
                    WHERE lead_id = ANY(%s)
                    """,
                    (record_type, notice_subtype, ids),
                )
            else:
                mapping = {
                    "delete": ("deleted", False, None),
                    "noise": ("noise", True, None),
                    "approve": ("approved", False, ""),
                    "restore": ("", False, ""),
                }
                status_override, noise_flagged, noise_reason = mapping[action]
                cur.execute(
                    """
                    UPDATE opportunity_leads
                    SET
                        status_override = %s,
                        noise_flagged = %s,
                        noise_reason = CASE WHEN %s IS NULL THEN noise_reason ELSE %s END
                    WHERE lead_id = ANY(%s)
                    """,
                    (status_override, noise_flagged, noise_reason, noise_reason, ids),
                )
            changed = cur.rowcount
        conn.commit()
    return changed


def patch_lead(opp_id: str, patch: dict) -> bool:
    allowed = {
        "title": "title",
        "due_date_raw": "due_date",
        "county": "county",
        "official_url": "source_url",
        "access_type": "access_type",
        "platform": "platform_name",
        "next_step": "next_step",
        "docs_path_note": "docs_path_note",
        "addenda_note": "addenda_note",
        "status_override": "status_override",
        "noise_flagged": "noise_flagged",
        "noise_reason": "noise_reason",
        "record_type_override": "record_type_override",
        "notice_subtype_override": "notice_subtype_override",
    }

    if not use_db_backend():
        opps = load_opps()
        record = next((opp for opp in opps if opp.get("id") == opp_id), None)
        if not record:
            return False
        for key, value in patch.items():
            if key in allowed:
                record[key] = value
        save_opps(opps)
        return True

    assignments = []
    values = []
    for key, column in allowed.items():
        if key in patch:
            assignments.append(f"{column} = %s")
            values.append(patch[key])

    if not assignments:
        return False

    values.append(opp_id)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE opportunity_leads SET {', '.join(assignments)} WHERE lead_id = %s",
                values,
            )
            changed = cur.rowcount
        conn.commit()
    return bool(changed)


def clear_noise_flags() -> int:
    if not use_db_backend():
        opps = load_opps()
        count = 0
        for opp in opps:
            if opp.get("status_override"):
                continue
            if opp.get("noise_flagged") or opp.get("noise_reason"):
                opp["noise_flagged"] = False
                opp["noise_reason"] = ""
                count += 1
        save_opps(opps)
        return count

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE opportunity_leads
                SET noise_flagged = FALSE, noise_reason = ''
                WHERE COALESCE(status_override, '') = ''
                """
            )
            count = cur.rowcount
        conn.commit()
    return count


def noise_score(opp: dict) -> tuple[bool, str]:
    title = (opp.get("title") or "").lower()
    if len(title.split()) < 6:
        return True, "title too short"
    for phrase in NOISE_PHRASES:
        if phrase in title:
            return True, f"nav/boilerplate: {phrase}"
    for keyword in OUT_OF_SCOPE:
        if keyword in title:
            return True, f"out of scope: {keyword}"
    return False, ""


def parse_due(raw: str | None) -> date | None:
    if not raw:
        return None
    raw = str(raw).strip()
    if raw.lower() in {"", "not listed", "-", "unknown", "—"}:
        return None
    fmts = ["%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%d-%b-%Y", "%b. %d, %Y"]
    for fmt in fmts:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass
    return None


def enrich(opp: dict) -> dict:
    record = dict(opp)
    due = parse_due(record.get("due_date_raw") or record.get("due_date"))
    record["due_date_parsed"] = due.isoformat() if due else None
    rule = source_rule_for(record.get("source_id"))
    record["source_rule"] = rule["mode"]
    record["source_rule_label"] = rule["label"]
    record["crawlability_score"] = rule["score"]

    manual = record.get("status_override")
    today = date.today()
    if manual == "deleted":
        record["status"] = "deleted"
    elif manual == "noise":
        record["status"] = "noise"
    elif record["source_rule"] == "disabled":
        record["status"] = "disabled"
    else:
        is_noise, reason = noise_score(record)
        if record.get("noise_flagged"):
            record["status"] = "noise"
            record["noise_reason"] = record.get("noise_reason") or "manually flagged"
        elif is_noise and manual != "approved":
            record["status"] = "noise"
            record["noise_reason"] = reason
        elif due and due < today:
            record["status"] = "expired"
        elif manual == "approved":
            record["status"] = "open"
        elif due:
            if record["source_rule"] == "trusted":
                record["status"] = "open"
            elif record["source_rule"] == "ai_review":
                record["status"] = "ai_review"
            else:
                record["status"] = "review_required"
        else:
            record["status"] = "review_required"

    record_type, notice_subtype = classify_record(record)
    record["record_type"] = record_type
    record["notice_subtype"] = notice_subtype
    return record


def sort_opps(opps: list[dict]) -> list[dict]:
    return sorted(
        opps,
        key=lambda opp: (
            1 if not opp.get("due_date_parsed") else 0,
            opp.get("due_date_parsed") or "9999-12-31",
            (opp.get("title") or "").lower(),
        ),
    )


def group_by_urgency(opps: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    today = date.today()
    cutoff = today + timedelta(days=14)
    soon, later, nodate = [], [], []
    for opp in opps:
        if opp.get("due_date_parsed"):
            due = date.fromisoformat(opp["due_date_parsed"])
            if due <= cutoff:
                soon.append(opp)
            else:
                later.append(opp)
        else:
            nodate.append(opp)
    return soon, later, nodate


@app.route("/health")
def health():
    return jsonify({"ok": True})


@app.route("/")
def index():
    opps = [enrich(opp) for opp in load_opps()]
    opps = [opp for opp in opps if opp["status"] not in ("noise", "deleted", "disabled")]
    active = [opp for opp in opps if opp["status"] == "open"]
    expiring = [
        opp
        for opp in sort_opps(active)
        if opp.get("due_date_parsed")
        and (date.fromisoformat(opp["due_date_parsed"]) - date.today()).days <= 14
    ]
    stats = {
        "construction": len([opp for opp in active if opp["record_type"] == "construction"]),
        "professional_services": len([opp for opp in active if opp["record_type"] == "professional_services"]),
        "public_notice": len([opp for opp in active if opp["record_type"] == "public_notice"]),
        "sources": len(load_sources()),
    }
    return render_template("index.html", stats=stats, expiring=expiring[:8])


def _opp_list_view(record_type: str, notice_subtype: str | None = None) -> dict:
    opps = [enrich(opp) for opp in load_opps()]
    opps = [opp for opp in opps if opp["status"] != "deleted"]
    county = request.args.get("county", "")
    agency = request.args.get("agency", "")
    status = request.args.get("status", "active")
    q = request.args.get("q", "").lower()

    def keep(opp: dict) -> bool:
        if status == "active" and opp["status"] != "open":
            return False
        if status == "all" and opp["status"] not in ("open", "review_required", "ai_review"):
            return False
        if status == "review" and opp["status"] not in ("review_required", "ai_review"):
            return False
        if status == "expired" and opp["status"] != "expired":
            return False
        if opp["status"] in ("noise", "deleted", "disabled"):
            return False
        if opp["record_type"] != record_type:
            return False
        if notice_subtype and opp.get("notice_subtype") != notice_subtype:
            return False
        if county and (opp.get("county") or "").lower() != county.lower():
            return False
        if agency and (opp.get("source_name") or "").lower() != agency.lower():
            return False
        haystack = f"{opp.get('title', '')} {opp.get('source_name', '')} {opp.get('county', '')}".lower()
        if q and q not in haystack:
            return False
        return True

    filtered = sort_opps([opp for opp in opps if keep(opp)])
    soon, later, nodate = group_by_urgency(filtered)
    counties = sorted({opp.get("county", "") for opp in opps if opp.get("county")})
    agencies = sorted({opp.get("source_name", "") for opp in opps if opp.get("source_name")})
    today = date.today()
    soon_cutoff = today + timedelta(days=14)
    return {
        "soon": soon,
        "later": later,
        "nodate": nodate,
        "counties": counties,
        "agencies": agencies,
        "selected_county": county,
        "selected_agency": agency,
        "selected_status": status,
        "q": q,
        "total": len(filtered),
        "record_type": record_type,
        "notice_subtype": notice_subtype,
        "today": today.isoformat(),
        "soon_cutoff": soon_cutoff.isoformat(),
    }


@app.route("/bids/construction")
def bids_construction():
    ctx = _opp_list_view("construction")
    ctx["page_title"] = "Construction Bids"
    ctx["page_desc"] = "Formal bids for roadway, bridge, drainage, pavement, and related heavy highway construction work."
    return render_template("opportunity_list.html", **ctx)


@app.route("/bids/professional-services")
def bids_profserv():
    ctx = _opp_list_view("professional_services")
    ctx["page_title"] = "Professional Services"
    ctx["page_desc"] = "RFPs and RFQs for engineering, design, inspection, planning, and related consulting services."
    return render_template("opportunity_list.html", **ctx)


@app.route("/opportunities")
def opportunities():
    return redirect(url_for("bids_construction"))


@app.route("/opportunities/<opp_id>")
def opportunity_detail(opp_id: str):
    opp = next((enrich(item) for item in load_opps() if str(item.get("id")) == opp_id), None)
    if not opp or opp["status"] == "deleted":
        return "Not found", 404
    return render_template("opportunity_detail.html", opp=opp)


@app.route("/sources")
def sources():
    sources = load_sources()
    opps = [enrich(opp) for opp in load_opps()]
    for source in sources:
        source_id = source.get("id")
        related = [opp for opp in opps if opp.get("source_id") == source_id]
        source["total"] = len(related)
        source["noise"] = len([opp for opp in related if opp["status"] == "noise"])
        source["expired"] = len([opp for opp in related if opp["status"] == "expired"])
        source["open"] = len([opp for opp in related if opp["status"] == "open"])
        source["review_required"] = len([opp for opp in related if opp["status"] == "review_required"])
        source["ai_review"] = len([opp for opp in related if opp["status"] == "ai_review"])
        ratio = source["noise"] / max(source["total"], 1)
        source["health"] = "bad" if ratio > 0.4 else "warn" if ratio > 0.15 else "good"
    sources = sorted(sources, key=lambda s: (-s.get("crawlability_score", 0), s.get("name", "").lower()))
    return render_template("sources.html", sources=sources)


@app.route("/export/opportunities.csv")
def export_csv():
    ids = request.args.get("ids", "")
    selected = {item for item in ids.split(",") if item}
    opps = [enrich(opp) for opp in load_opps()]
    opps = [opp for opp in opps if opp["status"] == "open"]
    if selected:
        opps = [opp for opp in opps if str(opp.get("id")) in selected]

    buf = StringIO()
    fields = [
        "id",
        "title",
        "source_name",
        "county",
        "record_type",
        "notice_subtype",
        "due_date_raw",
        "due_date_parsed",
        "status",
        "access_type",
        "platform",
        "official_url",
    ]
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(opps)
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": 'attachment; filename="njtbids-opportunities.csv"'},
    )


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    error = None
    if request.method == "POST":
        if request.form.get("username") == ADMIN_USER and _check_pw(request.form.get("password", "")):
            session["admin"] = True
            session.permanent = False
            return redirect(request.args.get("next") or url_for("admin_dashboard"))
        error = "Invalid username or password."
    return render_template("admin_login.html", error=error)


@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("index"))


@app.route("/admin")
@admin_required
def admin_dashboard():
    opps = [enrich(opp) for opp in load_opps()]
    opps = [opp for opp in opps if opp["status"] not in ("deleted", "disabled")]
    active = [opp for opp in opps if opp["status"] == "open"]
    stats = {
        "open": len([opp for opp in opps if opp["status"] == "open"]),
        "review_required": len([opp for opp in opps if opp["status"] == "review_required"]),
        "ai_review": len([opp for opp in opps if opp["status"] == "ai_review"]),
        "total": len(opps),
        "noise": len([opp for opp in opps if opp["status"] == "noise"]),
        "expired": len([opp for opp in opps if opp["status"] == "expired"]),
        "construction": len([opp for opp in active if opp["record_type"] == "construction"]),
        "profserv": len([opp for opp in active if opp["record_type"] == "professional_services"]),
        "notices": len([opp for opp in active if opp["record_type"] == "public_notice"]),
        "uncat": len([opp for opp in active if opp["record_type"] == "uncategorized"]),
    }
    return render_template("admin_dashboard.html", stats=stats)


@app.route("/admin/records")
@admin_required
def admin_records():
    opps = [enrich(opp) for opp in load_opps()]
    opps = [opp for opp in opps if opp["status"] != "deleted"]
    filt = request.args.get("filter", "all")
    q = request.args.get("q", "").lower()
    source_name = request.args.get("source", "")
    selected_type = request.args.get("type", "")

    def keep(opp: dict) -> bool:
        if filt == "review" and opp["status"] not in ("review_required", "ai_review"):
            return False
        if filt == "noise" and opp["status"] != "noise":
            return False
        if filt == "expired" and opp["status"] != "expired":
            return False
        if filt == "nodate" and opp["status"] != "review_required":
            return False
        if filt == "ai" and opp["status"] != "ai_review":
            return False
        if filt == "uncat" and opp["record_type"] != "uncategorized":
            return False
        if selected_type and opp["record_type"] != selected_type:
            return False
        if source_name and opp.get("source_name", "") != source_name:
            return False
        haystack = f"{opp.get('title', '')} {opp.get('source_name', '')}".lower()
        if q and q not in haystack:
            return False
        return True

    filtered = [opp for opp in opps if keep(opp)]
    sources = sorted({opp.get("source_name", "") for opp in opps if opp.get("source_name")})
    return render_template(
        "admin_records.html",
        records=filtered,
        filt=filt,
        q=q,
        selected_source=source_name,
        selected_type=selected_type,
        sources=sources,
        total=len(filtered),
        all_total=len(opps),
    )


@app.route("/admin/api/bulk", methods=["POST"])
@admin_required
def admin_bulk():
    data = request.get_json() or {}
    action = data.get("action")
    ids = [str(item) for item in data.get("ids", [])]
    record_type = data.get("record_type")
    notice_subtype = data.get("notice_subtype")
    if action not in {"delete", "noise", "approve", "restore", "set_type"}:
        return jsonify({"ok": False, "msg": "Unknown action"}), 400
    if not ids:
        return jsonify({"ok": False, "msg": "No records selected"}), 400
    if action == "set_type" and not record_type:
        return jsonify({"ok": False, "msg": "No record type provided"}), 400
    changed = update_leads(ids, action, record_type=record_type, notice_subtype=notice_subtype)
    return jsonify({"ok": True, "changed": changed})


@app.route("/admin/api/record/<opp_id>", methods=["PATCH", "DELETE"])
@admin_required
def admin_record(opp_id: str):
    if request.method == "DELETE":
        changed = update_leads([opp_id], "delete")
        return jsonify({"ok": bool(changed)})

    patch = request.get_json() or {}
    ok = patch_lead(opp_id, patch)
    return jsonify({"ok": ok})


@app.route("/admin/api/rescore", methods=["POST"])
@admin_required
def admin_rescore():
    rescored = clear_noise_flags()
    return jsonify({"ok": True, "rescored": rescored})


@app.route("/admin/sources")
@admin_required
def admin_sources():
    sources = load_sources()
    opps = [enrich(opp) for opp in load_opps()]
    for source in sources:
        source_id = source.get("id")
        related = [opp for opp in opps if opp.get("source_id") == source_id]
        source["total"] = len(related)
        source["noise"] = len([opp for opp in related if opp["status"] == "noise"])
        source["expired"] = len([opp for opp in related if opp["status"] == "expired"])
        source["open"] = len([opp for opp in related if opp["status"] == "open"])
        source["review_required"] = len([opp for opp in related if opp["status"] == "review_required"])
        source["ai_review"] = len([opp for opp in related if opp["status"] == "ai_review"])
        ratio = source["noise"] / max(source["total"], 1)
        source["health"] = "bad" if ratio > 0.4 else "warn" if ratio > 0.15 else "good"
    sources = sorted(sources, key=lambda s: (-s.get("crawlability_score", 0), s.get("name", "").lower()))
    return render_template("admin_sources.html", sources=sources)


init_db_schema()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=True)
