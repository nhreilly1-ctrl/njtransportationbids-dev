"""
network_routes.py
-----------------
All Flask routes for the NJ Transportation Bids network features.
Register with:  app.register_blueprint(network_bp)
in app/main.py
"""

from flask import (Blueprint, render_template, request, redirect,
                   url_for, session, flash, jsonify, abort)
from functools import wraps
from .network_models import (
    # users
    create_user, authenticate_user, get_user, update_user,
    change_password, TRADES, NJDOT_PREQUAL_CODES, CERTIFICATIONS,
    NJ_COUNTIES,
    # posts
    create_post, get_posts, get_post, add_response, delete_post,
    POST_TYPES, TRADES_SHORT,
    # equipment
    create_equipment, get_equipment, get_equipment_item, bump_inquiry,
    EQUIP_TYPES, EQUIP_CATEGORIES,
    # bid results
    submit_bid_result, get_bid_results, fmt_currency,
    # wage / resources
    get_wage_rates, get_resources, seed_resources, seed_wage_rates,
)

network_bp = Blueprint("network", __name__, url_prefix="/network")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _users_cache():
    """Return {user_id: user_dict} for all registered users."""
    import json, os
    path = os.path.join(os.path.dirname(__file__), "..", "data", "network", "users.json")
    if not os.path.exists(path):
        return {}
    users = json.load(open(path, encoding="utf-8"))
    return {u["id"]: u for u in users}


# ── Auth helpers ──────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            flash("Sign in to access that page.", "info")
            return redirect(url_for("network.login", next=request.path))
        return f(*args, **kwargs)
    return wrapper

def current_user():
    uid = session.get("user_id")
    return get_user(uid) if uid else None

def is_admin():
    u = current_user()
    return u and u.get("role") == "admin"


# ── Seed on first import ──────────────────────────────────────────────────────
seed_resources()
seed_wage_rates()


# ══════════════════════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════════════════════

@network_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email","").strip()
        pw    = request.form.get("password","")
        user  = authenticate_user(email, pw)
        if user:
            session["user_id"]      = user["id"]
            session["user_company"] = user["company"]
            return redirect(request.args.get("next") or url_for("network.board"))
        flash("Invalid email or password.", "error")
    return render_template("network/login.html", next=request.args.get("next",""))


@network_bp.route("/logout")
def logout():
    session.pop("user_id", None)
    session.pop("user_company", None)
    return redirect(url_for("network.board"))


@network_bp.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        email   = request.form.get("email","").strip()
        pw      = request.form.get("password","")
        pw2     = request.form.get("password2","")
        company = request.form.get("company","").strip()
        trade   = request.form.get("trade","")
        county  = request.form.get("county","")
        if pw != pw2:
            flash("Passwords do not match.", "error")
        elif len(pw) < 8:
            flash("Password must be at least 8 characters.", "error")
        elif not company:
            flash("Company name is required.", "error")
        else:
            user, err = create_user(email, pw, company, trade, county)
            if err:
                flash(err, "error")
            else:
                session["user_id"]      = user["id"]
                session["user_company"] = user["company"]
                flash("Welcome to the NJ Transportation Bids network.", "success")
                return redirect(url_for("network.profile_edit"))
    return render_template("network/register.html",
                           trades=TRADES, counties=NJ_COUNTIES)


# ══════════════════════════════════════════════════════════════════════════════
# USER PROFILE
# ══════════════════════════════════════════════════════════════════════════════

@network_bp.route("/profile")
@login_required
def profile_view():
    user = current_user()
    return render_template("network/profile_view.html", user=user)


@network_bp.route("/profile/edit", methods=["GET", "POST"])
@login_required
def profile_edit():
    user = current_user()
    if request.method == "POST":
        f = request.form
        projects = []
        for i in range(10):
            name = f.get(f"proj_name_{i}","").strip()
            if name:
                projects.append({
                    "name":  name,
                    "owner": f.get(f"proj_owner_{i}","").strip(),
                    "value": f.get(f"proj_value_{i}","").strip(),
                    "year":  f.get(f"proj_year_{i}","").strip(),
                })
        update_user(user["id"], {
            "company":         f.get("company","").strip(),
            "phone":           f.get("phone","").strip(),
            "website":         f.get("website","").strip(),
            "bio":             f.get("bio","").strip(),
            "trade":           f.get("trade",""),
            "county":          f.get("county",""),
            "union_status":    f.get("union_status",""),
            "bonding_single":  f.get("bonding_single","").strip(),
            "bonding_agg":     f.get("bonding_agg","").strip(),
            "insurance_gl":    f.get("insurance_gl","").strip(),
            "insurance_workers":f.get("insurance_workers","").strip(),
            "prequal_codes":   f.getlist("prequal_codes"),
            "certifications":  f.getlist("certifications"),
            "notable_projects":projects,
        })
        flash("Profile updated.", "success")
        return redirect(url_for("network.profile_view"))
    return render_template("network/profile_edit.html",
                           user=user, trades=TRADES, counties=NJ_COUNTIES,
                           prequal_codes=NJDOT_PREQUAL_CODES,
                           certifications=CERTIFICATIONS)


@network_bp.route("/profile/password", methods=["POST"])
@login_required
def profile_password():
    user = current_user()
    ok, err = change_password(
        user["id"],
        request.form.get("old_password",""),
        request.form.get("new_password",""),
    )
    if ok:
        flash("Password changed.", "success")
    else:
        flash(err, "error")
    return redirect(url_for("network.profile_edit"))


# ══════════════════════════════════════════════════════════════════════════════
# BOARD
# ══════════════════════════════════════════════════════════════════════════════

@network_bp.route("/")
@network_bp.route("/board")
def board():
    post_type = request.args.get("type","")
    county    = request.args.get("county","")
    trade     = request.args.get("trade","")
    posts = get_posts(
        post_type=post_type or None,
        county=county or None,
        trade=trade or None,
        limit=60,
    )
    users_cache = _users_cache()

    return render_template("network/board.html",
                           posts=posts, users_cache=users_cache,
                           post_types=POST_TYPES,
                           trades=TRADES_SHORT,
                           counties=NJ_COUNTIES,
                           selected_type=post_type,
                           selected_county=county,
                           selected_trade=trade,
                           user=current_user())


@network_bp.route("/board/post", methods=["GET","POST"])
@login_required
def board_post():
    user = current_user()
    if request.method == "POST":
        f = request.form
        post = create_post(
            user_id=user["id"],
            post_type=f.get("post_type","general"),
            title=f.get("title","").strip(),
            body=f.get("body","").strip(),
            county=f.get("county",""),
            trades=f.getlist("trades"),
            bid_opp_id=f.get("bid_opp_id","") or None,
            sub_due=f.get("sub_due","") or None,
            owner_due=f.get("owner_due","") or None,
            est_value=f.get("est_value","") or None,
            dbe_required="dbe_required" in f,
            bond_required="bond_required" in f,
            contact_email=f.get("contact_email","").strip() or user["email"],
        )
        flash("Post published.", "success")
        return redirect(url_for("network.board_detail", post_id=post["id"]))
    return render_template("network/board_post.html",
                           post_types=POST_TYPES,
                           trades=TRADES_SHORT,
                           counties=NJ_COUNTIES,
                           user=user)


@network_bp.route("/board/<post_id>")
def board_detail(post_id):
    post = get_post(post_id)
    if not post:
        abort(404)
    users_cache = _users_cache()
    return render_template("network/board_detail.html",
                           post=post, user=current_user(),
                           users_cache=users_cache)


@network_bp.route("/board/<post_id>/respond", methods=["POST"])
@login_required
def board_respond(post_id):
    user = current_user()
    add_response(
        post_id=post_id,
        user_id=user["id"],
        message=request.form.get("message","").strip(),
        contact_email=request.form.get("contact_email","").strip() or user["email"],
    )
    flash("Response submitted. The poster will see your message.", "success")
    return redirect(url_for("network.board_detail", post_id=post_id))


@network_bp.route("/board/<post_id>/delete", methods=["POST"])
@login_required
def board_delete(post_id):
    user = current_user()
    delete_post(post_id, user["id"], is_admin=is_admin())
    flash("Post removed.", "success")
    return redirect(url_for("network.board"))


# ══════════════════════════════════════════════════════════════════════════════
# EQUIPMENT
# ══════════════════════════════════════════════════════════════════════════════

@network_bp.route("/equipment")
def equipment():
    listing_type = request.args.get("type","")
    category     = request.args.get("category","")
    county       = request.args.get("county","")
    items = get_equipment(
        listing_type=listing_type or None,
        category=category or None,
        county=county or None,
    )
    return render_template("network/equipment.html",
                           items=items,
                           equip_types=EQUIP_TYPES,
                           categories=EQUIP_CATEGORIES,
                           counties=NJ_COUNTIES,
                           selected_type=listing_type,
                           selected_category=category,
                           selected_county=county,
                           user=current_user())


@network_bp.route("/equipment/new", methods=["GET","POST"])
@login_required
def equipment_new():
    user = current_user()
    if request.method == "POST":
        f = request.form
        price_raw = f.get("price","").replace("$","").replace(",","").strip()
        try: price = float(price_raw)
        except: price = None
        item = create_equipment(
            user_id=user["id"],
            listing_type=f.get("listing_type","sale"),
            category=f.get("category","Other"),
            title=f.get("title","").strip(),
            description=f.get("description","").strip(),
            year=f.get("year","").strip(),
            hours=f.get("hours","").strip(),
            price=price,
            price_period=f.get("price_period",""),
            county=f.get("county",""),
            njdot_certified="njdot_certified" in f,
            condition=f.get("condition",""),
            contact_email=f.get("contact_email","").strip() or user["email"],
        )
        flash("Listing posted.", "success")
        return redirect(url_for("network.equipment_detail", item_id=item["id"]))
    return render_template("network/equipment_new.html",
                           equip_types=EQUIP_TYPES,
                           categories=EQUIP_CATEGORIES,
                           counties=NJ_COUNTIES,
                           user=user)


@network_bp.route("/equipment/<item_id>")
def equipment_detail(item_id):
    item = get_equipment_item(item_id)
    if not item:
        abort(404)
    poster = _users_cache().get(item["user_id"])
    return render_template("network/equipment_detail.html",
                           item=item, poster=poster, user=current_user(),
                           fmt_currency=fmt_currency)


@network_bp.route("/equipment/<item_id>/inquire", methods=["POST"])
@login_required
def equipment_inquire(item_id):
    bump_inquiry(item_id)
    flash("Inquiry recorded. Contact info is on the listing.", "success")
    return redirect(url_for("network.equipment_detail", item_id=item_id))


# ══════════════════════════════════════════════════════════════════════════════
# BID RESULTS
# ══════════════════════════════════════════════════════════════════════════════

@network_bp.route("/results")
def bid_results():
    agency = request.args.get("agency","")
    county = request.args.get("county","")
    year   = request.args.get("year","")
    results = get_bid_results(
        agency=agency or None,
        county=county or None,
        year=year or None,
    )
    return render_template("network/bid_results.html",
                           results=results,
                           counties=NJ_COUNTIES,
                           selected_agency=agency,
                           selected_county=county,
                           selected_year=year,
                           fmt_currency=fmt_currency,
                           user=current_user())


@network_bp.route("/results/submit", methods=["GET","POST"])
@login_required
def bid_results_submit():
    user = current_user()
    if request.method == "POST":
        f = request.form
        def _num(k):
            v = f.get(k,"").replace("$","").replace(",","").strip()
            try: return float(v)
            except: return None
        submit_bid_result(
            user_id=user["id"],
            contract_title=f.get("contract_title","").strip(),
            owner_agency=f.get("owner_agency","").strip(),
            county=f.get("county",""),
            bid_date=f.get("bid_date",""),
            awarded_to=f.get("awarded_to","").strip(),
            award_amount=_num("award_amount"),
            low_bid=_num("low_bid"),
            second_bid=_num("second_bid"),
            engineer_estimate=_num("engineer_estimate"),
            num_bidders=int(f.get("num_bidders",0) or 0),
            contract_number=f.get("contract_number","").strip(),
            bid_opp_id=f.get("bid_opp_id","") or None,
            notes=f.get("notes","").strip() or None,
            source_url=f.get("source_url","").strip() or None,
        )
        flash("Result submitted — thank you. It will appear after a quick review.", "success")
        return redirect(url_for("network.bid_results"))
    return render_template("network/bid_results_submit.html",
                           counties=NJ_COUNTIES, user=user)


# ══════════════════════════════════════════════════════════════════════════════
# PREVAILING WAGE
# ══════════════════════════════════════════════════════════════════════════════

@network_bp.route("/wages")
def wages():
    county = request.args.get("county", NJ_COUNTIES[0])
    trade  = request.args.get("trade","")
    rates  = get_wage_rates(trade=trade or None, county=county or None)
    trades_list = sorted(set(r["trade"] for r in get_wage_rates()))
    return render_template("network/wages.html",
                           rates=rates,
                           trades_list=trades_list,
                           counties=NJ_COUNTIES,
                           selected_county=county,
                           selected_trade=trade,
                           user=current_user())


# ══════════════════════════════════════════════════════════════════════════════
# RESOURCES (insurance, bonding, compliance)
# ══════════════════════════════════════════════════════════════════════════════

@network_bp.route("/resources")
def resources():
    category = request.args.get("category","")
    items    = get_resources(category=category or None)
    return render_template("network/resources.html",
                           items=items,
                           selected_category=category,
                           user=current_user())


@network_bp.route("/resources/<item_id>")
def resource_detail(item_id):
    import json, os
    path = os.path.join(os.path.dirname(__file__), "..", "data", "network", "resources.json")
    items = json.load(open(path, encoding="utf-8")) if os.path.exists(path) else []
    item = next((r for r in items if r["id"] == item_id), None)
    if not item: abort(404)
    return render_template("network/resource_detail.html",
                           item=item, user=current_user())


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN — network moderation
# ══════════════════════════════════════════════════════════════════════════════

def net_admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("admin") and not is_admin():
            abort(403)
        return f(*args, **kwargs)
    return wrapper


@network_bp.route("/admin/results")
@net_admin_required
def admin_results():
    import json, os
    path = os.path.join(os.path.dirname(__file__), "..", "data", "network", "bid_results.json")
    results = json.load(open(path, encoding="utf-8")) if os.path.exists(path) else []
    return render_template("network/admin_results.html",
                           results=results, fmt_currency=fmt_currency)


@network_bp.route("/admin/results/<result_id>/verify", methods=["POST"])
@net_admin_required
def admin_verify_result(result_id):
    import json, os
    path = os.path.join(os.path.dirname(__file__), "..", "data", "network", "bid_results.json")
    results = json.load(open(path, encoding="utf-8")) if os.path.exists(path) else []
    for r in results:
        if r["id"] == result_id:
            r["verified"] = True
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    return jsonify({"ok": True})


@network_bp.route("/admin/results/<result_id>/delete", methods=["POST"])
@net_admin_required
def admin_delete_result(result_id):
    import json, os
    path = os.path.join(os.path.dirname(__file__), "..", "data", "network", "bid_results.json")
    results = json.load(open(path, encoding="utf-8")) if os.path.exists(path) else []
    results = [r for r in results if r["id"] != result_id]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    return jsonify({"ok": True})
