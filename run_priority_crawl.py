from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import os
import secrets
import psycopg2
import requests
from bs4 import BeautifulSoup
from psycopg2.extras import RealDictCursor

from app.core.filters import clean_title, is_garbage_title


HEADERS = {
    "User-Agent": "NJTransportationBidsBot/0.3 (+manual-priority-crawl)"
}

PRIORITY_TIERS = ("Tier 1", "Tier 2")

SOURCE_RULES = {
    "state-njdot-construction": "trusted",
    "state-njdot-profserv": "trusted",
    "state-drjtbc-construction": "trusted",
    "state-drjtbc-profserv": "trusted",
    "state-njta": "trusted",
    "state-njtransit": "trusted",
    "state-panynj-construction": "trusted",
    "state-panynj-profserv": "trusted",
    "county-camden": "ai_review",
    "county-burlington": "ai_review",
    "municipal-jersey-city": "ai_review",
    "municipal-hoboken": "ai_review",
    "county-bergen": "ai_review",
    "county-essex": "manual_review",
    "municipal-paterson": "manual_review",
    "municipal-elizabeth": "manual_review",
    "county-cape-may": "manual_review",
    "county-hudson": "manual_review",
    "municipal-camden": "manual_review",
    "county-cumberland": "manual_review",
    "county-gloucester": "manual_review",
    "county-hunterdon": "manual_review",
    "municipal-newark": "metadata_only",
    "county-atlantic": "disabled",
    "county-mercer": "disabled",
    "municipal-trenton": "disabled",
}


@dataclass
class CrawlResult:
    status: str
    records_found: int
    records_promoted: int


def get_conn():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set in this PowerShell window.")
    if db_url.startswith("postgresql+psycopg2://"):
        db_url = "postgresql://" + db_url[len("postgresql+psycopg2://"):]
    return psycopg2.connect(db_url)


def get_table_columns(conn, table_name: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        )
        return {row[0] for row in cur.fetchall()}


def pick_source_url(source: dict[str, Any], source_cols: set[str]) -> str | None:
    for key in (
        "source_url",
        "effective_notice_entry_url",
        "primary_procurement_url",
        "direct_legal_notice_url",
    ):
        if key in source_cols and source.get(key):
            return source[key]
    return None


def fetch_page(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def extract_items(html: str, base_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    selectors = [
        "table tr",
        ".bid-item",
        ".solicitation-row",
        ".views-row",
        "article",
        "li",
        "a[href]",
    ]

    for selector in selectors:
        for node in soup.select(selector)[:100]:
            title = ""
            href = None

            link = node if getattr(node, "name", "") == "a" else node.select_one("a[href]")
            if link:
                href = link.get("href")
                title = link.get_text(" ", strip=True)

            if not title:
                title = node.get_text(" ", strip=True)

            title = clean_title(title)[:300]
            if is_garbage_title(title):
                continue

            if not href:
                href = base_url
            href = urljoin(base_url, href)

            key = (title.lower(), href)
            if key in seen:
                continue
            seen.add(key)
            items.append({"title": title, "url": href})

    return items[:50]


def load_sources(conn, source_cols: set[str]) -> list[dict[str, Any]]:
    select_fields = ["source_id"]
    for field in (
        "source_name",
        "county",
        "entity_type",
        "source_url",
        "effective_notice_entry_url",
        "primary_procurement_url",
        "direct_legal_notice_url",
        "priority_tier",
        "source_status",
        "import_enabled",
    ):
        if field in source_cols:
            select_fields.append(field)

    where_clauses: list[str] = []
    params: list[Any] = []

    if "import_enabled" in source_cols:
        where_clauses.append("COALESCE(import_enabled, TRUE) IS TRUE")
    if "source_status" in source_cols:
        where_clauses.append("COALESCE(source_status, '') != 'Inactive'")
    if "priority_tier" in source_cols:
        where_clauses.append("priority_tier = ANY(%s)")
        params.append(list(PRIORITY_TIERS))

    query = f"SELECT {', '.join(select_fields)} FROM registry_sources"
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    if "source_name" in source_cols:
        query += " ORDER BY source_name"
    query += " LIMIT 20"

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        rows = [dict(row) for row in cur.fetchall()]
        filtered: list[dict[str, Any]] = []
        for row in rows:
            mode = SOURCE_RULES.get((row.get("source_id") or "").lower(), "manual_review")
            if mode in {"disabled", "metadata_only"}:
                continue
            filtered.append(row)
        return filtered


def get_existing_keys(conn, lead_cols: set[str], source_id: str) -> tuple[set[str], set[str]]:
    title_col = "notice_title" if "notice_title" in lead_cols else "title"
    url_col = "notice_url" if "notice_url" in lead_cols else "source_url"
    if title_col not in lead_cols or url_col not in lead_cols:
        return set(), set()

    query = f"""
        SELECT COALESCE({title_col}, ''), COALESCE({url_col}, '')
        FROM opportunity_leads
        WHERE source_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (source_id,))
        rows = cur.fetchall()
        titles = {clean_title(row[0]).lower() for row in rows if row[0]}
        urls = {row[1] for row in rows if row[1]}
        return titles, urls


def build_insert_payload(
    *,
    lead_cols: set[str],
    source: dict[str, Any],
    item: dict[str, str],
) -> dict[str, Any]:
    payload: dict[str, Any] = {"source_id": source["source_id"]}
    title = clean_title(item["title"])
    url = item["url"]
    source_name = source.get("source_name") or source["source_id"]

    if "notice_title" in lead_cols:
        payload["notice_title"] = title
        payload["notice_url"] = url
        if "official_url" in lead_cols:
            payload["official_url"] = url
        if "owner_name" in lead_cols:
            payload["owner_name"] = source_name
        if "owner_type" in lead_cols:
            payload["owner_type"] = source.get("entity_type")
        if "promotion_decision" in lead_cols:
            payload["promotion_decision"] = "Review"
        if "lead_status" in lead_cols:
            payload["lead_status"] = "new"
        if "verification_status" in lead_cols:
            payload["verification_status"] = "Unknown"
    else:
        payload["title"] = title
        if "source_url" in lead_cols:
            payload["source_url"] = url
        if "agency" in lead_cols:
            payload["agency"] = source_name
        if "status" in lead_cols:
            payload["status"] = "Review"

    if "county" in lead_cols:
        payload["county"] = source.get("county")
    if "raw_text" in lead_cols:
        payload["raw_text"] = title
    if "created_at" in lead_cols:
        payload["created_at"] = datetime.now(timezone.utc)
    if "next_step" in lead_cols:
        payload["next_step"] = "Open the official source and review the bid notice."

    return {k: v for k, v in payload.items() if k in lead_cols}


def get_column_data_type(conn, table_name: str, column_name: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
            """,
            (table_name, column_name),
        )
        row = cur.fetchone()
        return row[0] if row else None


def build_lead_id(conn, source_id: str) -> str | int:
    lead_id_type = get_column_data_type(conn, "opportunity_leads", "lead_id")
    if lead_id_type in {"integer", "bigint", "smallint"}:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(MAX(CAST(lead_id AS bigint)), 0) + 1
                FROM opportunity_leads
                """
            )
            return int(cur.fetchone()[0])
    return f"lead-{source_id}-{secrets.token_hex(8)}"


def insert_lead(conn, payload: dict[str, Any], lead_cols: set[str], source_id: str) -> None:
    if "lead_id" in lead_cols and "lead_id" not in payload:
        payload["lead_id"] = build_lead_id(conn, source_id)
    columns = list(payload.keys())
    values = [payload[col] for col in columns]
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"""
        INSERT INTO opportunity_leads ({', '.join(columns)})
        VALUES ({placeholders})
    """
    with conn.cursor() as cur:
        cur.execute(query, values)


def update_source_status(
    conn,
    *,
    source_id: str,
    source_cols: set[str],
    records_found: int,
    status: str,
) -> None:
    assignments: list[str] = []
    params: list[Any] = []

    if "last_crawl_at" in source_cols:
        assignments.append("last_crawl_at = %s")
        params.append(datetime.now(timezone.utc))
    if "last_crawl_status" in source_cols:
        assignments.append("last_crawl_status = %s")
        params.append(status)
    if "last_leads_found" in source_cols:
        assignments.append("last_leads_found = %s")
        params.append(records_found)

    if not assignments:
        return

    params.append(source_id)
    query = f"""
        UPDATE registry_sources
        SET {', '.join(assignments)}
        WHERE source_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, params)


def run_source_crawl(conn, source: dict[str, Any], *, source_cols: set[str], lead_cols: set[str]) -> CrawlResult:
    base_url = pick_source_url(source, source_cols)
    if not base_url:
        update_source_status(conn, source_id=source["source_id"], source_cols=source_cols, records_found=0, status="missing_url")
        conn.commit()
        return CrawlResult(status="missing_url", records_found=0, records_promoted=0)

    html = fetch_page(base_url)
    items = extract_items(html, base_url)
    existing_titles, existing_urls = get_existing_keys(conn, lead_cols, source["source_id"])

    inserted = 0
    for item in items:
        title_key = clean_title(item["title"]).lower()
        url_key = item["url"]
        if title_key in existing_titles or url_key in existing_urls:
            continue
        payload = build_insert_payload(lead_cols=lead_cols, source=source, item=item)
        if not payload:
            continue
        insert_lead(conn, payload, lead_cols, source["source_id"])
        existing_titles.add(title_key)
        existing_urls.add(url_key)
        inserted += 1

    update_source_status(
        conn,
        source_id=source["source_id"],
        source_cols=source_cols,
        records_found=len(items),
        status="success",
    )
    conn.commit()
    return CrawlResult(status="success", records_found=len(items), records_promoted=inserted)


def main() -> None:
    conn = get_conn()
    try:
        source_cols = get_table_columns(conn, "registry_sources")
        lead_cols = get_table_columns(conn, "opportunity_leads")
        sources = load_sources(conn, source_cols)
        for source in sources:
            try:
                result = run_source_crawl(conn, source, source_cols=source_cols, lead_cols=lead_cols)
            except Exception as exc:
                conn.rollback()
                update_source_status(
                    conn,
                    source_id=source["source_id"],
                    source_cols=source_cols,
                    records_found=0,
                    status="error",
                )
                conn.commit()
                print(source["source_id"], "error", 0, 0, str(exc)[:160])
            else:
                print(source["source_id"], result.status, result.records_found, result.records_promoted)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
