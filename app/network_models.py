"""
network_models.py
-----------------
All data models and storage logic for the NJ Transportation Bids
network features: users/profiles, board posts, equipment listings,
bid results, prevailing wage, and resources.

Storage: flat JSON files in data/network/.
No ORM dependency — pure stdlib + flask.
Each collection is one JSON file: a list of dicts.
"""

import os, json, uuid, hashlib, re
from datetime import datetime, date, timedelta
from typing import Optional

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE    = os.path.join(os.path.dirname(__file__), "..")
NET_DIR = os.path.join(BASE, "data", "network")

def _path(name): return os.path.join(NET_DIR, f"{name}.json")

def _load(name):
    p = _path(name)
    if not os.path.exists(p): return []
    with open(p, encoding="utf-8") as f: return json.load(f)

def _save(name, data):
    os.makedirs(NET_DIR, exist_ok=True)
    with open(_path(name), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)

def _uid(): return str(uuid.uuid4())[:12]

def _now(): return datetime.utcnow().isoformat()

def _hash(pw): return hashlib.sha256(pw.encode()).hexdigest()

def _ago(iso):
    """Human-readable relative time from ISO string."""
    if not iso: return ""
    try:
        dt = datetime.fromisoformat(iso)
        diff = datetime.utcnow() - dt
        s = int(diff.total_seconds())
        if s < 60:   return "just now"
        if s < 3600: return f"{s//60}m ago"
        if s < 86400:return f"{s//3600}h ago"
        return f"{s//86400}d ago"
    except: return ""

# ══════════════════════════════════════════════════════════════════════════════
# USERS & PROFILES
# ══════════════════════════════════════════════════════════════════════════════

TRADES = [
    "Roadway / HMA paving", "Bridge & structures", "Drainage & utilities",
    "Concrete flatwork", "Earthwork & grading", "Electrical / signalization",
    "Guardrail & safety", "Landscaping & erosion control",
    "Engineering — structural", "Engineering — transportation",
    "Engineering — environmental", "Engineering — inspection / CEI",
    "Planning", "Survey", "Geotechnical", "Other"
]

NJDOT_PREQUAL_CODES = [
    "A-1 Roadway","B-1 Bridge","C-1 Electrical","C-2 Utilities",
    "C-6 Construction Inspection","C-7 Concrete","D-1 Demolition",
    "E-3 Environmental","H-1 Hydraulics","H-3 Stormwater",
    "I-1 Inspection","I-3 Movable Bridge Inspection","P-1 Planning",
    "R-1 Right of Way","S-1 Survey","T-1 Traffic Engineering",
    "T-7 Resilience","B-2 Marine Structures","G-1 Geotechnical",
    "Contractor — Roadway","Contractor — Bridge","Contractor — Electrical",
    "Contractor — Drainage","Contractor — HMA Paving",
]

CERTIFICATIONS = [
    "DBE (Disadvantaged Business Enterprise)",
    "SBE (Small Business Enterprise)",
    "WBE (Women-Owned Business Enterprise)",
    "MBE (Minority Business Enterprise)",
    "VBE (Veteran-Owned Business Enterprise)",
    "NJDOT Prequalified Contractor",
    "NJDOT Prequalified Consultant",
    "Port Authority Approved",
    "NJ Transit Approved",
]

def create_user(email, password, company, trade, county, role="member"):
    users = _load("users")
    if any(u["email"].lower() == email.lower() for u in users):
        return None, "Email already registered"
    user = {
        "id":          _uid(),
        "email":       email.lower().strip(),
        "pw_hash":     _hash(password),
        "company":     company.strip(),
        "role":        role,          # member | admin
        "created":     _now(),
        "last_login":  None,
        # profile fields (filled in later)
        "trade":           trade,
        "county":          county,
        "phone":           "",
        "website":         "",
        "bio":             "",
        "prequal_codes":   [],
        "certifications":  [],
        "union_status":    "",        # union | open shop | merit shop
        "bonding_single":  "",        # e.g. "$10M"
        "bonding_agg":     "",
        "notable_projects": [],       # list of {name, owner, value, year}
        "insurance_gl":    "",        # GL limit
        "insurance_workers":"",
        "active":          True,
    }
    users.append(user)
    _save("users", users)
    return user, None

def authenticate_user(email, password):
    users = _load("users")
    u = next((u for u in users if u["email"] == email.lower() and u["active"]), None)
    if not u or u["pw_hash"] != _hash(password):
        return None
    u["last_login"] = _now()
    _save("users", users)
    return u

def get_user(user_id):
    return next((u for u in _load("users") if u["id"] == user_id), None)

def get_user_by_email(email):
    return next((u for u in _load("users") if u["email"] == email.lower()), None)

def update_user(user_id, fields):
    users = _load("users")
    for u in users:
        if u["id"] == user_id:
            safe = {k: v for k, v in fields.items()
                    if k not in ("id","email","pw_hash","role","created")}
            u.update(safe)
    _save("users", users)

def change_password(user_id, old_pw, new_pw):
    users = _load("users")
    for u in users:
        if u["id"] == user_id:
            if u["pw_hash"] != _hash(old_pw):
                return False, "Current password incorrect"
            u["pw_hash"] = _hash(new_pw)
            _save("users", users)
            return True, None
    return False, "User not found"

# ══════════════════════════════════════════════════════════════════════════════
# BOARD POSTS (sub requests, teaming, available-for-sub, etc.)
# ══════════════════════════════════════════════════════════════════════════════

POST_TYPES = {
    "sub_request":   "Seeking subs",
    "available_sub": "Available as sub",
    "teaming":       "Teaming",
    "general":       "General",
}

TRADES_SHORT = [
    "HMA paving","Milling","Concrete structures","Drainage","Earthwork",
    "Electrical","Guardrail","Landscaping","Demolition","Survey",
    "Engineering","Inspection / CEI","Structural steel","Masonry","Other"
]

def create_post(user_id, post_type, title, body, county, trades,
                bid_opp_id=None, sub_due=None, owner_due=None,
                est_value=None, dbe_required=False, bond_required=False,
                contact_email=None):
    posts = _load("posts")
    post = {
        "id":            _uid(),
        "user_id":       user_id,
        "type":          post_type,
        "title":         title.strip(),
        "body":          body.strip(),
        "county":        county,
        "trades":        trades,          # list
        "bid_opp_id":    bid_opp_id,      # links to opportunity record
        "sub_due":       sub_due,
        "owner_due":     owner_due,
        "est_value":     est_value,
        "dbe_required":  dbe_required,
        "bond_required": bond_required,
        "contact_email": contact_email,
        "created":       _now(),
        "expires":       (datetime.utcnow() + timedelta(days=60)).isoformat(),
        "active":        True,
        "responses":     [],             # list of response dicts
        "flagged":       False,
    }
    posts.insert(0, post)
    _save("posts", posts)
    return post

def get_posts(post_type=None, county=None, trade=None, bid_opp_id=None,
              active_only=True, limit=50):
    posts = _load("posts")
    now   = datetime.utcnow().isoformat()
    out   = []
    for p in posts:
        if active_only and not p.get("active"):          continue
        if active_only and p.get("expires","z") < now:   continue
        if active_only and p.get("flagged"):              continue
        if post_type and p["type"] != post_type:          continue
        if county and county.lower() not in p.get("county","").lower(): continue
        if trade and trade not in p.get("trades",[]):     continue
        if bid_opp_id and p.get("bid_opp_id") != bid_opp_id: continue
        p["_ago"] = _ago(p["created"])
        out.append(p)
    return out[:limit]

def add_response(post_id, user_id, message, contact_email):
    posts = _load("posts")
    for p in posts:
        if p["id"] == post_id:
            p["responses"].append({
                "id":      _uid(),
                "user_id": user_id,
                "message": message.strip(),
                "contact": contact_email,
                "created": _now(),
            })
    _save("posts", posts)

def get_post(post_id):
    p = next((p for p in _load("posts") if p["id"] == post_id), None)
    if p: p["_ago"] = _ago(p.get("created",""))
    return p

def delete_post(post_id, user_id, is_admin=False):
    posts = _load("posts")
    posts = [p for p in posts
             if not (p["id"] == post_id and (p["user_id"] == user_id or is_admin))]
    _save("posts", posts)

# ══════════════════════════════════════════════════════════════════════════════
# EQUIPMENT LISTINGS
# ══════════════════════════════════════════════════════════════════════════════

EQUIP_TYPES = {
    "sale":    "For sale",
    "lease":   "For lease / rent",
    "wanted":  "Looking for",
}

EQUIP_CATEGORIES = [
    "Paver","Milling machine","Roller / compactor","Excavator","Dozer",
    "Motor grader","Loader","Dump truck","Crane","Drill rig",
    "Concrete pump","Form / shoring","Survey equipment",
    "Traffic control","Generator / lighting","Other"
]

def create_equipment(user_id, listing_type, category, title, description,
                     year, hours, price, price_period, county,
                     njdot_certified=False, condition=None, contact_email=None):
    items = _load("equipment")
    item = {
        "id":             _uid(),
        "user_id":        user_id,
        "listing_type":   listing_type,   # sale | lease | wanted
        "category":       category,
        "title":          title.strip(),
        "description":    description.strip(),
        "year":           year,
        "hours":          hours,
        "price":          price,          # numeric or None
        "price_period":   price_period,   # "" | "day" | "week" | "month"
        "county":         county,
        "njdot_certified":njdot_certified,
        "condition":      condition,      # excellent | good | fair | as-is
        "contact_email":  contact_email,
        "created":        _now(),
        "expires":        (datetime.utcnow() + timedelta(days=90)).isoformat(),
        "active":         True,
        "flagged":        False,
        "inquiries":      0,
    }
    items.insert(0, item)
    _save("equipment", items)
    return item

def get_equipment(listing_type=None, category=None, county=None,
                  active_only=True, limit=60):
    items = _load("equipment")
    now   = datetime.utcnow().isoformat()
    out   = []
    for i in items:
        if active_only and not i.get("active"):           continue
        if active_only and i.get("expires","z") < now:    continue
        if active_only and i.get("flagged"):               continue
        if listing_type and i["listing_type"] != listing_type: continue
        if category and i["category"] != category:         continue
        if county and county.lower() not in i.get("county","").lower(): continue
        i["_ago"] = _ago(i["created"])
        out.append(i)
    return out[:limit]

def get_equipment_item(item_id):
    i = next((i for i in _load("equipment") if i["id"] == item_id), None)
    if i: i["_ago"] = _ago(i.get("created",""))
    return i

def bump_inquiry(item_id):
    items = _load("equipment")
    for i in items:
        if i["id"] == item_id:
            i["inquiries"] = i.get("inquiries", 0) + 1
    _save("equipment", items)

# ══════════════════════════════════════════════════════════════════════════════
# BID RESULTS BOARD
# ══════════════════════════════════════════════════════════════════════════════

def submit_bid_result(user_id, contract_title, owner_agency, county,
                      bid_date, awarded_to, award_amount, low_bid,
                      second_bid, engineer_estimate, num_bidders,
                      contract_number, bid_opp_id=None, notes=None,
                      source_url=None):
    results = _load("bid_results")
    result = {
        "id":               _uid(),
        "submitted_by":     user_id,       # None = admin-posted
        "bid_opp_id":       bid_opp_id,
        "contract_title":   contract_title.strip(),
        "contract_number":  contract_number,
        "owner_agency":     owner_agency,
        "county":           county,
        "bid_date":         bid_date,
        "awarded_to":       awarded_to,
        "award_amount":     award_amount,   # numeric
        "low_bid":          low_bid,
        "second_bid":       second_bid,
        "engineer_estimate":engineer_estimate,
        "num_bidders":      num_bidders,
        "notes":            notes,
        "source_url":       source_url,
        "created":          _now(),
        "verified":         False,          # admin verifies before featuring
        "flagged":          False,
    }
    results.insert(0, result)
    _save("bid_results", results)
    return result

def get_bid_results(agency=None, county=None, year=None,
                    verified_only=False, limit=100):
    results = _load("bid_results")
    out = []
    for r in results:
        if r.get("flagged"):                                    continue
        if verified_only and not r.get("verified"):            continue
        if agency and agency.lower() not in r.get("owner_agency","").lower(): continue
        if county and county.lower() not in r.get("county","").lower():       continue
        if year and not str(r.get("bid_date","")).startswith(str(year)):       continue
        r["_ago"] = _ago(r["created"])
        out.append(r)
    return out[:limit]

def fmt_currency(val):
    try:
        return f"${int(val):,}"
    except: return str(val) if val else "—"

# ══════════════════════════════════════════════════════════════════════════════
# PREVAILING WAGE RESOURCE
# ══════════════════════════════════════════════════════════════════════════════
# Rates are curated reference data — not a live API pull.
# Admin updates these; users can view and discuss.

NJ_COUNTIES = [
    "Atlantic","Bergen","Burlington","Camden","Cape May","Cumberland",
    "Essex","Gloucester","Hudson","Hunterdon","Mercer","Middlesex",
    "Monmouth","Morris","Ocean","Passaic","Salem","Somerset",
    "Sussex","Union","Warren"
]

def get_wage_rates(trade=None, county=None):
    rates = _load("wage_rates")
    if trade:  rates = [r for r in rates if r.get("trade","").lower() == trade.lower()]
    if county: rates = [r for r in rates if r.get("county","").lower() == county.lower()]
    return rates

def upsert_wage_rate(trade, county, straight_time, overtime, fringe, craft, updated_by):
    rates = _load("wage_rates")
    existing = next((r for r in rates
                     if r["trade"].lower() == trade.lower()
                     and r["county"].lower() == county.lower()), None)
    if existing:
        existing.update({
            "straight_time": straight_time,
            "overtime":      overtime,
            "fringe":        fringe,
            "craft":         craft,
            "updated":       _now(),
            "updated_by":    updated_by,
        })
    else:
        rates.append({
            "id":            _uid(),
            "trade":         trade,
            "county":        county,
            "straight_time": straight_time,
            "overtime":      overtime,
            "fringe":        fringe,
            "craft":         craft,
            "updated":       _now(),
            "updated_by":    updated_by,
        })
    _save("wage_rates", rates)

# ══════════════════════════════════════════════════════════════════════════════
# RESOURCES (insurance / bonding reference content)
# ══════════════════════════════════════════════════════════════════════════════

def get_resources(category=None):
    items = _load("resources")
    if category:
        items = [r for r in items if r.get("category","").lower() == category.lower()]
    return [r for r in items if r.get("active", True)]

def create_resource(title, body, category, url=None, created_by="admin"):
    items = _load("resources")
    item = {
        "id":         _uid(),
        "title":      title.strip(),
        "body":       body.strip(),
        "category":   category,   # insurance | bonding | prevailing_wage | compliance
        "url":        url,
        "created":    _now(),
        "created_by": created_by,
        "active":     True,
    }
    items.insert(0, item)
    _save("resources", items)
    return item

# ══════════════════════════════════════════════════════════════════════════════
# SEED DATA  (runs once to populate reference data)
# ══════════════════════════════════════════════════════════════════════════════

def seed_resources():
    if _load("resources"): return   # already seeded
    resources = [
        # Insurance
        {
            "id": _uid(), "category": "insurance",
            "title": "NJDOT minimum insurance requirements for construction contractors",
            "body": (
                "NJDOT requires the following minimum coverage for all construction contracts:\n\n"
                "Commercial General Liability: $2M per occurrence / $5M aggregate (most contracts). "
                "Projects over $25M typically require $5M/$10M.\n\n"
                "Auto Liability: $1M combined single limit.\n\n"
                "Workers' Compensation: Statutory NJ limits. Employers liability minimum $1M.\n\n"
                "Umbrella / Excess: $5M–$10M depending on contract size.\n\n"
                "All policies must name the State of New Jersey, NJDOT, and the contractor as "
                "additional insured. Certificate holder language must reference the specific contract number. "
                "NJDOT requires 30-day notice of cancellation language.\n\n"
                "Professional Liability (for design-build or CEI): $1M–$2M per claim.\n\n"
                "Always verify requirements in the specific contract Special Provisions — these "
                "minimum standards are frequently increased for larger or more complex contracts."
            ),
            "url": "https://www.nj.gov/transportation/business/procurement/",
            "created": _now(), "created_by": "admin", "active": True,
        },
        {
            "id": _uid(), "category": "insurance",
            "title": "NJ Turnpike Authority and Garden State Parkway insurance requirements",
            "body": (
                "NJTA and GSP contracts typically require higher limits than NJDOT:\n\n"
                "CGL: $5M per occurrence / $10M aggregate for most construction contracts.\n\n"
                "Workers' Compensation: Statutory plus $2M employers liability.\n\n"
                "Umbrella: $10M minimum on major construction.\n\n"
                "Pollution Liability: Required if work involves fuel storage, excavation near "
                "contaminated sites, or hazmat. $2M minimum.\n\n"
                "Railroad Protective Liability: Required when working within 50 feet of active "
                "rail lines. $2M per occurrence / $6M aggregate — this is frequently underestimated "
                "on projects near NJ Transit or Amtrak right-of-way.\n\n"
                "DRJTBC contracts follow similar requirements. Always request the insurance exhibit "
                "from the procurement documents before finalizing your coverage."
            ),
            "url": "https://www.njta.gov/business-hub/",
            "created": _now(), "created_by": "admin", "active": True,
        },
        # Bonding
        {
            "id": _uid(), "category": "bonding",
            "title": "Understanding bonding requirements on NJ public contracts",
            "body": (
                "All NJ public construction contracts over $100,000 require a Performance Bond "
                "and Payment Bond, each equal to 100% of the contract amount. This is mandated by "
                "the NJ Local Public Contracts Law and the State contracts process.\n\n"
                "BID BOND: Most agencies require a bid bond or certified check equal to 10% of the "
                "bid amount, submitted with the bid. NJDOT accepts a bid bond from a Treasury-listed surety.\n\n"
                "PERFORMANCE BOND: 100% of contract. Must be from a surety licensed in NJ and "
                "listed on U.S. Treasury Circular 570.\n\n"
                "PAYMENT BOND: 100% of contract. Protects subcontractors and suppliers.\n\n"
                "SINGLE VS. AGGREGATE: NJDOT prequalification requires you to demonstrate bonding "
                "capacity. Your single job limit and aggregate limit are reviewed. A typical "
                "mid-size NJ contractor might carry $5M single / $15M aggregate.\n\n"
                "TIMING: Bonds must typically be submitted within 10 days of award. Have your "
                "surety relationship in place before you bid — last-minute bonding requests "
                "frequently cause problems.\n\n"
                "SBA SURETY BOND GUARANTEE: Small and DBE contractors may qualify for the SBA "
                "surety bond guarantee program, which can help secure bonds on contracts up to $9M "
                "(or $14M for certain contracts)."
            ),
            "url": "https://www.sba.gov/funding-programs/surety-bonds",
            "created": _now(), "created_by": "admin", "active": True,
        },
        {
            "id": _uid(), "category": "bonding",
            "title": "NJ surety bond markets — what underwriters look at",
            "body": (
                "Surety underwriters evaluate contractors differently from banks. Key factors:\n\n"
                "CHARACTER: Your track record of completing jobs and paying subs. Defaults, "
                "liens, and sub complaints follow you in the surety market for years.\n\n"
                "CAPACITY: Your physical ability to complete the work — equipment, workforce, "
                "management depth. For new contract types or regions, expect more scrutiny.\n\n"
                "CAPITAL: Reviewed via your most recent CPA-prepared financial statements. "
                "Sureties typically want to see working capital equal to 10–15% of the "
                "bond amount, and a net worth supporting your aggregate line.\n\n"
                "FINANCIAL STATEMENTS: Most NJ public agencies require CPA-reviewed or audited "
                "financials for prequalification. Keep these current — submitting fiscal year-end "
                "statements within 6 months is standard practice.\n\n"
                "INDEMNITY AGREEMENT: When you sign a general indemnity agreement with a surety, "
                "you and your principals pledge personal assets. Understand what you are signing.\n\n"
                "SUBCONTRACTOR BONDS: GCs on larger NJDOT projects are sometimes required to bond "
                "their subs. Build this cost into your sub pricing expectations."
            ),
            "url": None,
            "created": _now(), "created_by": "admin", "active": True,
        },
        # Prevailing Wage
        {
            "id": _uid(), "category": "prevailing_wage",
            "title": "NJ Prevailing Wage Act — who it applies to and how",
            "body": (
                "The New Jersey Prevailing Wage Act (NJSA 34:11-56.25 et seq.) requires payment "
                "of prevailing wages on all public works contracts over $16,000.\n\n"
                "WHO IT COVERS: All workers — including subcontractor employees — on projects "
                "funded by state, county, or municipal funds, or on public property.\n\n"
                "DETERMINING THE RATE: Rates are set by the NJ Department of Labor and Workforce "
                "Development, Division of Wage and Hour Compliance. They publish craft-specific "
                "rates by county. Note that NJ rates differ from federal Davis-Bacon rates, "
                "and BOTH may apply on federally-funded projects.\n\n"
                "CERTIFIED PAYROLLS: Contractors must submit certified payroll records. NJDOT and "
                "most authorities use a standard form. Falsification is a criminal offense.\n\n"
                "FRINGE BENEFITS: The prevailing rate includes a base hourly wage plus fringe "
                "benefits (health, pension, vacation). Union contractors typically satisfy the "
                "fringe through their benefit funds. Open-shop contractors must pay the full "
                "fringe — either into a bona fide plan or as additional cash wages.\n\n"
                "APPRENTICES: Registered apprentices may be paid at lower rates, but only within "
                "the ratio specified by the trade (typically 1 apprentice per 3 journeymen). "
                "Apprentices must be registered with the NJ Department of Labor.\n\n"
                "DBE COMPLIANCE INTERACTION: When a GC employs a DBE sub, the DBE must also "
                "pay prevailing wages. This is sometimes overlooked and creates audit exposure.\n\n"
                "PENALTIES: Underpayment triggers back wages, penalties, and potential debarment."
            ),
            "url": "https://www.nj.gov/labor/wagehour/content/prevailing_wage.html",
            "created": _now(), "created_by": "admin", "active": True,
        },
        {
            "id": _uid(), "category": "prevailing_wage",
            "title": "Federal Davis-Bacon on NJDOT and NJTA federally-funded projects",
            "body": (
                "When NJDOT or NJTA projects use federal funds (FHWA, FTA, etc.), Davis-Bacon "
                "Act prevailing wages apply in addition to NJ state prevailing wages.\n\n"
                "WHICH RATE APPLIES: When both NJ and federal rates apply to the same work, "
                "you must pay whichever is higher for each classification. This comparison must "
                "be done classification by classification — you cannot average across trades.\n\n"
                "WAGE DECISIONS: Federal wage decisions are incorporated into the contract "
                "Special Provisions and are project-specific. Look for the WD number (Wage "
                "Decision) referenced in the bid documents. These are set by the Department "
                "of Labor's Wage and Hour Division.\n\n"
                "CERTIFIED PAYROLLS: Federal projects require submission through the DOL's "
                "LCPtracker or similar approved system. NJDOT and NJTA use LCPtracker on "
                "most federally-funded construction contracts.\n\n"
                "EEO REQUIREMENTS: Federal contracts also trigger FHWA Form 1391 (EEO "
                "workforce reporting) and specific utilization goals by trade. On-the-job "
                "training requirements may also apply.\n\n"
                "PRACTICAL NOTE: If you have not used LCPtracker before, plan time to set "
                "up your company and learn the system before your first payroll is due."
            ),
            "url": "https://www.dol.gov/agencies/whd/government-contracts/construction",
            "created": _now(), "created_by": "admin", "active": True,
        },
        # Compliance
        {
            "id": _uid(), "category": "compliance",
            "title": "DBE program compliance on NJDOT contracts — what GCs need to know",
            "body": (
                "NJDOT administers a federally-required DBE (Disadvantaged Business Enterprise) "
                "program on all USDOT-assisted contracts.\n\n"
                "CONTRACT GOALS: Each contract has a DBE participation goal, expressed as a "
                "percentage of the contract value. The goal is set during procurement and "
                "must be met or a good faith effort documented.\n\n"
                "CERTIFIED DBEs: Firms must be certified through the NJ Unified Certification "
                "Program (NJ UCP). Certification takes 90 days on average — subs need to be "
                "certified before contract award, not during.\n\n"
                "COUNTING CREDIT: Only the work actually performed by the DBE counts. If a DBE "
                "subs out more than 30% of its contract, the pass-through portion does not count "
                "toward the goal.\n\n"
                "COMMERCIALLY USEFUL FUNCTION (CUF): The DBE must perform a genuine role. "
                "NJDOT conducts CUF reviews — a DBE that shows up only to sign paperwork while "
                "another firm does the work is a compliance violation with serious consequences.\n\n"
                "MONTHLY REPORTING: GCs must submit monthly DBE payment reports. Unexplained "
                "gaps between committed and actual DBE payments trigger audits.\n\n"
                "SUBSTITUTIONS: If a DBE cannot perform, you must request approval for "
                "substitution and document good faith efforts to replace with another DBE."
            ),
            "url": "https://www.nj.gov/transportation/business/civilrights/dbe.shtm",
            "created": _now(), "created_by": "admin", "active": True,
        },
    ]
    _save("resources", resources)

def seed_wage_rates():
    if _load("wage_rates"): return
    # Representative NJ prevailing wage rates (approximate 2024 published rates)
    # GCs should always verify against current NJ DOL publications
    rates = []
    base_rates = {
        "Carpenter":              (58.20, 87.30, 38.45, "Carpenter"),
        "Laborer — general":      (44.75, 67.13, 31.20, "Laborer"),
        "Operating engineer":     (67.80, 101.70, 42.10, "Operating Engineer"),
        "Ironworker — structural":(73.40, 110.10, 51.30, "Ironworker"),
        "Ironworker — rebar":     (68.90, 103.35, 48.70, "Ironworker"),
        "Cement mason":           (62.15, 93.23, 39.80, "Cement Mason"),
        "Teamster":               (51.30, 76.95, 33.60, "Teamster"),
        "Electrician":            (81.50, 122.25, 52.40, "IBEW"),
        "Painter":                (56.80, 85.20, 37.90, "Painter"),
        "Plumber / pipefitter":   (79.60, 119.40, 53.20, "Plumber"),
        "Traffic control":        (32.50, 48.75, 18.40, "Laborer"),
        "Survey party chief":     (69.20, 103.80, 41.50, "Survey"),
        "Survey instrument":      (58.40, 87.60, 36.70, "Survey"),
    }
    # County multipliers (Northern NJ typically higher)
    county_mult = {
        "Bergen":1.04,"Essex":1.04,"Hudson":1.04,"Passaic":1.03,
        "Union":1.03,"Middlesex":1.02,"Monmouth":1.01,"Morris":1.02,
        "Somerset":1.01,"Mercer":1.00,"Ocean":0.99,"Burlington":0.99,
        "Camden":0.99,"Gloucester":0.98,"Atlantic":0.97,"Cape May":0.97,
        "Cumberland":0.96,"Salem":0.96,"Hunterdon":1.00,"Sussex":1.00,
        "Warren":1.00,
    }
    for county, mult in county_mult.items():
        for trade, (st, ot, fringe, craft) in base_rates.items():
            rates.append({
                "id": _uid(),
                "trade": trade, "county": county,
                "straight_time": round(st * mult, 2),
                "overtime":      round(ot * mult, 2),
                "fringe":        round(fringe * mult, 2),
                "craft":         craft,
                "updated":       "2024-07-01T00:00:00",
                "updated_by":    "admin",
            })
    _save("wage_rates", rates)
