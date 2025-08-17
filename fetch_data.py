#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Property Finder scraper (Python 3.9+)
- Resolves building IDs via the official locations API.
- Discovers Next.js buildId from the HTML.
- Crawls /search JSON for Buy or Rent with pagination.
- Enriches DLD/RERA permit from search JSON or listing page HTML.
- Exports CSV + JSONL per building with a custom column order.

Examples:
  python fetch_data.py --mode buildings --buildings "prive residence"
  python fetch_data.py --mode search --buildings "prive residence" --category buy --out ./out
"""

import argparse
import csv
import json
import math
import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
import requests
from openpyxl import Workbook

BASE_URL = "https://www.propertyfinder.ae"
SESSION_TIMEOUT = 30
REQUEST_RETRIES = 5
REQUEST_BACKOFF = 1.7  # seconds, exponential

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# ---- CSV priority (your exact requested order) ----
# CSV header order: these 6 first, exactly in this order
CSV_PRIORITY = [
    "dld_permit_number",
    "permit_modified",
    "price_currency",
    "price_value",
    "share_url",
    "size_unit",
    "size_value",
]

@dataclass
class LocationHit:
    id: int
    name: str
    url_slug: str
    url_city_slug: str
    location_type: str
    path_name: str

def _req(session: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    extra_headers = kwargs.pop("headers", None)
    merged_headers = dict(HEADERS)
    if extra_headers:
        merged_headers.update(extra_headers)

    last_exc = None
    resp = None
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            resp = session.request(method, url, timeout=SESSION_TIMEOUT, headers=merged_headers, **kwargs)
            if 200 <= resp.status_code < 300:
                return resp
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep((REQUEST_BACKOFF ** attempt) + random.uniform(0.2, 0.8))
                continue
            break
        except requests.RequestException as e:
            last_exc = e
            if attempt < REQUEST_RETRIES:
                time.sleep((REQUEST_BACKOFF ** attempt) + random.uniform(0.2, 0.8))
                continue
            raise
    if resp is not None:
        resp.raise_for_status()
    if last_exc:
        raise last_exc
    raise RuntimeError(f"Request failed: {method} {url}")

def search_locations(session: requests.Session, query: str, limit: int = 20) -> List[LocationHit]:
    params = {"locale": "en", "filters.name": query, "pagination.limit": str(limit)}
    url = f"{BASE_URL}/api/pwa/locations?{urlencode(params)}"
    resp = _req(session, "GET", url)
    data = resp.json()
    hits = []
    for item in data.get("data", {}).get("attributes", []):
        hits.append(
            LocationHit(
                id=int(item.get("id")),
                name=item.get("name") or "",
                url_slug=item.get("url_slug") or "",
                url_city_slug=item.get("url_city_slug") or "",
                location_type=item.get("location_type") or "",
                path_name=item.get("path_name") or "",
            )
        )
    return hits

def pick_best_tower(hits: List[LocationHit]) -> Optional[LocationHit]:
    if not hits: return None
    towers = [h for h in hits if h.location_type == "TOWER"]
    return towers[0] if towers else hits[0]

def extract_next_data_from_html(html: str) -> Optional[Dict]:
    m = re.search(r'id="__NEXT_DATA__"[^>]*>\s*({.*?})\s*</script>', html, re.DOTALL)
    if not m: return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None

def pagejson_from_html(html: str) -> Optional[Dict]:
    nd = extract_next_data_from_html(html)
    if not nd: return None
    props = nd.get("props") or {}
    page_props = props.get("pageProps")
    if not page_props: return None
    return {"pageProps": page_props}

def extract_build_id_from_html(html: str) -> Optional[str]:
    m = re.search(r'id="__NEXT_DATA__"[^>]*>\s*({.*?})\s*</script>', html, re.DOTALL)
    if m:
        try:
            obj = json.loads(m.group(1))
            build_id = obj.get("buildId") or obj.get("buildid") or obj.get("buildID")
            if build_id:
                return build_id
        except json.JSONDecodeError:
            pass
    m2 = re.search(r'buildId"\s*:\s*"([^"]+)"', html)
    if m2:
        return m2.group(1)
    return None

def discover_build_id(session: requests.Session, query_params: Dict[str, str]) -> str:
    url = f"{BASE_URL}/en/search?{urlencode(query_params)}"
    resp = _req(session, "GET", url)
    build_id = extract_build_id_from_html(resp.text)
    if not build_id:
        raise RuntimeError("Could not extract Next.js buildId from search HTML.")
    return build_id

def fetch_search_page(session: requests.Session, build_id: str, query_params: Dict[str, str]) -> Dict:
    json_url = f"{BASE_URL}/_next/data/{build_id}/en/search.json?{urlencode(query_params)}"
    try:
        resp = _req(session, "GET", json_url, headers={"x-nextjs-data": "1", "Accept": "application/json"})
        return resp.json()
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (429, 500, 502, 503, 504):
            html_url = f"{BASE_URL}/en/search?{urlencode(query_params)}"
            html_resp = _req(session, "GET", html_url, headers={"Accept": "text/html,application/xhtml+xml"})
            pj = pagejson_from_html(html_resp.text)
            if pj:
                return pj
        raise

def iter_listings_from_page(page_json: Dict) -> Iterable[Dict]:
    page_props = page_json.get("pageProps", {})
    search_result = page_props.get("searchResult", {}) or page_json.get("searchResult", {})
    listings = search_result.get("listings", [])
    for item in listings:
        if item.get("listing_type") == "property" and item.get("property"):
            yield item["property"]

# ---- Permit helpers ----
PERMIT_KEYS = (
    "rera_permit_number","reraPermitNumber",
    "permit_number","permitNumber",
    "trakheesi_permit_number","trakheesiPermitNumber",
    "rera",
)
def permit_from_dict(p: Dict) -> Optional[str]:
    for k in PERMIT_KEYS:
        v = p.get(k)
        if v:
            return str(v)
    return None

PERMIT_REGEXES = [
    re.compile(r'(?:Trakheesi|Permit)\s*(?:No\.?|Number)?\s*[:#]?\s*([0-9\-]{5,})', re.I),
    re.compile(r'"permitNumber"\s*:\s*"([^"]+)"'),
    re.compile(r'"reraPermitNumber"\s*:\s*"([^"]+)"'),
    re.compile(r'"rera"\s*:\s*"([^"]+)"'),
]
def extract_permit_from_html(html: str) -> Optional[str]:
    for rx in PERMIT_REGEXES[1:]:
        m = rx.search(html)
        if m: return m.group(1).strip()
    m = PERMIT_REGEXES[0].search(html)
    if m: return m.group(1).strip()
    return None

def fetch_permit_from_details(session: requests.Session, details_path: Optional[str], share_url: Optional[str]) -> Optional[str]:
    url = None
    if details_path:
        url = details_path if details_path.startswith("http") else f"{BASE_URL}{details_path}"
    elif share_url:
        url = share_url
    if not url: return None
    try:
        r = _req(session, "GET", url, headers={"Accept": "text/html,application/xhtml+xml"})
        return extract_permit_from_html(r.text)
    except Exception:
        return None

def normalize_listing(p: Dict) -> Dict:
    loc = p.get("location") or {}
    price = p.get("price") or {}
    size = p.get("size") or {}
    agent = p.get("agent") or {}
    broker = p.get("broker") or {}
    permit = permit_from_dict(p)
    return {
        "id": p.get("id"),
        "title": p.get("title"),
        "price_value": price.get("value"),
        "price_currency": price.get("currency"),
        "category_id": p.get("category_id"),
        "bedrooms": p.get("bedrooms"),
        "bathrooms": p.get("bathrooms"),
        "size_value": size.get("value"),
        "size_unit": size.get("unit"),
        "furnished": p.get("furnished"),
        "is_verified": p.get("is_verified"),
        "listed_date": p.get("listed_date"),
        "rera": p.get("rera"),
        "dld_permit_number": permit,
        "permit_modified": permit[2:] if permit and len(permit) > 2 else permit,
        "share_url": p.get("share_url"),
        "details_path": p.get("details_path"),
        "images_count": p.get("images_count"),
        "location_id": loc.get("id"),
        "location_name": loc.get("name"),
        "location_full": loc.get("full_name"),
        "location_slug": loc.get("slug"),
        "location_path": loc.get("path"),
        "location_type": loc.get("type"),
        "agent_id": agent.get("id"),
        "agent_name": agent.get("name"),
        "agent_email": agent.get("email"),
        "agent_phone": next((x.get("value") for x in (p.get("contact_options") or []) if x.get("type") == "phone"), None),
        "broker_id": broker.get("id"),
        "broker_name": broker.get("name"),
    }

def save_outputs(basepath: Path, listings: List[Dict]) -> Tuple[Path, Path]:
    """
    Saves JSONL + XLSX (with preferred column order).
    Preferred order first, then the rest (alphabetical).
    """
    basepath.parent.mkdir(parents=True, exist_ok=True)
    xlsx_path = basepath.with_suffix(".xlsx")
    jsonl_path = basepath.with_suffix(".jsonl")

    # --- JSONL (unchanged convenience output) ---
    with jsonl_path.open("w", encoding="utf-8") as f:
        for row in listings:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    # --- Compute column order for XLSX ---
    # Preferred first:
    preferred = [
        "dld_permit_number",
        "permit_modified",
        "price_currency",
        "price_value",
        "share_url",
        "size_unit",
        "size_value",
    ]
    # Whatever else (alphabetical), excluding the preferred ones:
    all_keys = sorted({k for r in listings for k in r.keys()})
    rest = [k for k in all_keys if k not in preferred]
    field_order = preferred + rest

    # --- Write XLSX ---
    wb = Workbook()
    ws = wb.active
    ws.title = "listings"

    # Header
    ws.append(field_order)

    # Rows
    for row in listings:
        ws.append([row.get(k, "") for k in field_order])

    wb.save(xlsx_path)
    return xlsx_path, jsonl_path


def get_pagination_info(page_json: Dict) -> Tuple[int, Optional[int], Optional[int]]:
    def _to_int(x, default=None):
        try: return int(x)
        except Exception: return default
    sr = page_json.get("pageProps", {}).get("searchResult", {})
    if not isinstance(sr, dict):
        sr = page_json.get("searchResult", {}) or {}
    pag = sr.get("pagination") or {}
    total_pages = _to_int(pag.get("total_pages")) or _to_int(pag.get("totalPages"))
    per_page = _to_int(pag.get("per_page")) or _to_int(pag.get("perPage"))
    total = _to_int(pag.get("total")) or _to_int(sr.get("total")) or _to_int(sr.get("listings_count"))
    if not total_pages and total and per_page:
        total_pages = max(1, math.ceil(total / per_page))
    if not total_pages and isinstance(pag.get("pages"), list) and pag["pages"]:
        try:
            cand = []
            for p in pag["pages"]:
                if isinstance(p, dict) and "page" in p:
                    cand.append(_to_int(p["page"]))
            cand = [c for c in cand if c]
            if cand: total_pages = max(cand)
        except Exception: pass
    if not total_pages: total_pages = 1
    if not per_page: per_page = 25
    return int(total_pages), int(per_page) if per_page else None, int(total) if total else None

def crawl_building(session: requests.Session, building: LocationHit, category: str, max_pages: int, out_dir: Path, enrich_permit: bool = True) -> Tuple[str, int]:
    c_map = {"buy": "1", "rent": "2"}
    if category not in c_map:
        raise ValueError("category must be 'buy' or 'rent'")
    query = {"l": str(building.id), "c": c_map[category], "fu": "0", "ob": "mr"}
    if category == "rent": query["rp"] = "y"

    build_id = discover_build_id(session, query)
    all_rows: List[Dict] = []

    q1 = dict(query); q1["page"] = "1"
    page1_json = fetch_search_page(session, build_id, q1)
    page1_rows = [normalize_listing(p) for p in iter_listings_from_page(page1_json)]
    all_rows.extend(page1_rows)

    total_pages, per_page, total = get_pagination_info(page1_json)
    last_page = min(total_pages, max_pages)
    print(f"[{building.name} / {category}] page 1: {len(page1_rows)} listings | per_page≈{per_page} total≈{total} total_pages={total_pages} (capped to {last_page})")

    counts_missing = (total is None)
    if last_page <= 1 and counts_missing and max_pages > 1:
        print(f"[{building.name} / {category}] Pagination unknown; probing up to {max_pages} pages…")
        for page_num in range(2, max_pages + 1):
            print(f"Probing page {page_num}/{max_pages} …")
            qn = dict(query); qn["page"] = str(page_num)
            try:
                page_json = fetch_search_page(session, build_id, qn)
            except requests.HTTPError as e:
                sc = getattr(e.response, "status_code", None)
                if sc in (404, 429, 500, 502, 503, 504):
                    print(f"Stopping probe on HTTP {sc} at page {page_num}.")
                    break
                raise
            rows = [normalize_listing(p) for p in iter_listings_from_page(page_json)]
            print(f"  -> got {len(rows)} listings")
            if not rows: break
            all_rows.extend(rows)
            time.sleep(0.6)
    else:
        if last_page > 1:
            for page_num in range(2, last_page + 1):
                print(f"Fetching page {page_num}/{last_page} for {building.name} ({category})…")
                qn = dict(query); qn["page"] = str(page_num)
                try:
                    page_json = fetch_search_page(session, build_id, qn)
                except requests.HTTPError as e:
                    sc = getattr(e.response, "status_code", None)
                    if sc in (404, 429, 500, 502, 503, 504):
                        print(f"Stopping early due to HTTP {sc} on page {page_num}.")
                        break
                    raise
                rows = [normalize_listing(p) for p in iter_listings_from_page(page_json)]
                print(f"  -> got {len(rows)} listings")
                if not rows: break
                all_rows.extend(rows)
                time.sleep(0.6)

    if enrich_permit:
        missing = [r for r in all_rows if not r.get("dld_permit_number")]
        if missing:
            print(f"[permit] Enriching {len(missing)} listings missing permit numbers …")
            filled = 0
            for r in missing:
                permit = fetch_permit_from_details(session, r.get("details_path"), r.get("share_url"))
                if permit:
                    r["dld_permit_number"] = permit
                    filled += 1
                time.sleep(0.4)
            print(f"[permit] Filled {filled}/{len(missing)}")

    print(f"Collected {len(all_rows)} total listings for {building.name} ({category}).")
    safe_name = re.sub(r"[^a-z0-9\-]+", "-", building.name.lower()).strip("-")
    stem = f"{safe_name}-{category}"
    basepath = (out_dir / stem)
    save_outputs(basepath, all_rows)
    return stem, len(all_rows)

def cmd_buildings(args: argparse.Namespace) -> None:
    buildings_arg = [s.strip() for s in args.buildings.split(",") if s.strip()]
    if not buildings_arg:
        print("Please provide at least one building name via --buildings", file=sys.stderr)
        sys.exit(2)
    with requests.Session() as session:
        for name in buildings_arg:
            hits = search_locations(session, name, limit=20)
            best = pick_best_tower(hits)
            if not best:
                print(f"[NOT FOUND] {name}")
                continue
            print(f"[FOUND] {name} -> id={best.id} type={best.location_type} name='{best.name}' path='{best.path_name}' slug='{best.url_slug}'")

def cmd_search(args: argparse.Namespace) -> None:
    buildings_arg = [s.strip() for s in args.buildings.split(",") if s.strip()]
    if not buildings_arg:
        print("Please provide at least one building name via --buildings", file=sys.stderr)
        sys.exit(2)
    out_dir = Path(args.out).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    with requests.Session() as session:
        for raw in buildings_arg:
            print(f"\n==> Resolving building: {raw}")
            hits = search_locations(session, raw, limit=20)
            best = pick_best_tower(hits)
            if not best:
                print(f"[SKIP] No location results for: {raw}")
                continue
            print(f"Using location: {best.name} (id {best.id}, type {best.location_type}, path '{best.path_name}')")
            try:
                stem, count = crawl_building(
                    session=session,
                    building=best,
                    category=args.category,
                    max_pages=args.max_pages_per_query,
                    out_dir=out_dir,
                    enrich_permit=not args.no_enrich_permit,
                )
                print(f"[DONE] {stem}: {count} listings saved to {out_dir}")
            except Exception as e:
                print(f"[ERROR] {best.name}: {e}", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(description="Property Finder scraper (buildings + search + permits)")
    sub = parser.add_subparsers(dest="mode", required=True)

    p_b = sub.add_parser("buildings", help="Resolve building IDs by name")
    p_b.add_argument("--buildings", required=True, help="Comma-separated building names (e.g., 'prive residence')")
    p_b.set_defaults(func=cmd_buildings)

    p_s = sub.add_parser("search", help="Crawl listings for buildings")
    p_s.add_argument("--buildings", required=True, help="Comma-separated building names")
    p_s.add_argument("--category", choices=["buy", "rent"], default="buy", help="Buy or rent")
    p_s.add_argument("--out", default="./out", help="Output folder")
    p_s.add_argument("--max-pages-per-query", type=int, default=10, help="Safety cap per building")
    p_s.add_argument("--no-enrich-permit", action="store_true",
                     help="Skip fetching each listing page to extract DLD/RERA permit if missing in search JSON")
    p_s.set_defaults(func=cmd_search)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
