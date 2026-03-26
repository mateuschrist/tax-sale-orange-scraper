import os
import re
import json
import time
import random
import logging
from io import BytesIO
from urllib.parse import urljoin, urlparse, parse_qs, quote

import requests
import pdfplumber
import pypdfium2
import pytesseract
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout


# =========================
# CONFIG (ENV)
# =========================
BASE_URL = "https://or.occompt.com"
LOGIN_URL = f"{BASE_URL}/recorder/web/login.jsp"
SEARCH_URL = f"{BASE_URL}/recorder/tdsmweb/applicationSearch.jsp"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
MAX_LOTS = int(os.getenv("MAX_LOTS", "1000"))
DEBUG_HTML = os.getenv("DEBUG_HTML", "false").lower() == "true"

RESTART_BROWSER_EVERY = int(os.getenv("RESTART_BROWSER_EVERY", "20"))

MAX_VIEWER_RETRIES = int(os.getenv("MAX_VIEWER_RETRIES", "3"))
MAX_WAIT = 60_000

OCR_MAX_PAGES = int(os.getenv("OCR_MAX_PAGES", "3"))
OCR_SCALE = float(os.getenv("OCR_SCALE", "2.2"))

SKIP_IF_ADDRESS_NOT_NUMBERED = os.getenv("SKIP_IF_ADDRESS_NOT_NUMBERED", "true").lower() == "true"
START_AFTER_LAST_NODE = os.getenv("START_AFTER_LAST_NODE", "false").lower() == "true"

SUPABASE_URL = (os.getenv("SUPABASE_URL", "") or "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or "").strip()
STATE_KEY = (os.getenv("STATE_KEY", "orange_taxdeed") or "orange_taxdeed").strip()
USE_STATE = bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)

APP_API_BASE = (os.getenv("APP_API_BASE", "") or "").strip().rstrip("/")
APP_API_TOKEN = (os.getenv("APP_API_TOKEN", "") or "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

TESSERACT_CMD = (os.getenv("TESSERACT_CMD", "") or "").strip()
if TESSERACT_CMD:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("taxdeed-orange-scraper")


# =========================
# HELPERS
# =========================
def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def normalize_money_to_float(value):
    if value in (None, ""):
        return None
    raw = re.sub(r"[^\d.]", "", str(value))
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def normalize_bid_for_payload(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    cleaned = re.sub(r"[^0-9.]", "", s.replace(",", ""))
    return cleaned if cleaned else None


def normalize_sale_date_value(value: str | None) -> str | None:
    v = clean_text(value or "")
    if not v:
        return None
    if v.lower() in ("null", "none", "n/a", "na", "not assigned"):
        return None
    return v


def must_be_pdf(headers: dict) -> bool:
    ct = (headers.get("content-type") or "").lower()
    return "application/pdf" in ct or ct.endswith("/pdf")


def is_check_human(url: str) -> bool:
    return "/recorder/web/checkHuman.jsp" in (url or "")


def human_backoff(idx: int, attempt: int):
    base = min(5 + idx * 0.15 + attempt * 2.0, 22)
    sleep_s = base + random.uniform(0.6, 3.0)
    log.warning("Backoff %.1fs (idx=%d attempt=%d)", sleep_s, idx, attempt)
    time.sleep(sleep_s)


def wait_network(page, timeout=20_000):
    try:
        page.wait_for_load_state("networkidle", timeout=timeout)
    except PWTimeout:
        pass


def click_any(page, selectors: list[str], label: str) -> bool:
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click()
                log.info("Clicked: %s (%s)", label, sel)
                return True
        except Exception:
            continue
    return False


def is_numbered_street_address(addr: str | None) -> bool:
    if not addr:
        return False
    a = addr.strip()
    return re.match(r"^\d{1,6}\s+\S", a) is not None


# =========================
# SUPABASE
# =========================
def sb_headers():
    if not USE_STATE:
        raise RuntimeError("Supabase not configured")
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation,resolution=merge-duplicates",
    }


def sb_get(url: str, timeout=30):
    return requests.get(url, headers=sb_headers(), timeout=timeout)


def sb_post(url: str, payload: dict, timeout=30):
    return requests.post(url, headers=sb_headers(), json=payload, timeout=timeout)


def sb_patch(url: str, payload: dict, timeout=30):
    return requests.patch(url, headers=sb_headers(), json=payload, timeout=timeout)


def sb_delete(url: str, timeout=30):
    return requests.delete(url, headers=sb_headers(), timeout=timeout)


def get_state_last_node() -> str | None:
    if not USE_STATE:
        return None

    url_a = f"{SUPABASE_URL}/rest/v1/scraper_state?scraper_name=eq.{STATE_KEY}&select=last_node"
    r = sb_get(url_a)
    if r.status_code == 200:
        arr = r.json()
        if arr:
            return arr[0].get("last_node")

    url_b = f"{SUPABASE_URL}/rest/v1/scraper_state?id=eq.{STATE_KEY}&select=last_node"
    r2 = sb_get(url_b)
    if r2.status_code == 200:
        arr = r2.json()
        if arr:
            return arr[0].get("last_node")

    return None


def set_state_last_node(last_node: str | None) -> None:
    if not USE_STATE:
        return

    url = f"{SUPABASE_URL}/rest/v1/scraper_state"

    payload_a = {"scraper_name": STATE_KEY, "last_node": last_node}
    r = sb_post(url, payload_a)
    if r.status_code in (200, 201, 204):
        return

    r = sb_patch(f"{SUPABASE_URL}/rest/v1/scraper_state?scraper_name=eq.{STATE_KEY}", {"last_node": last_node})
    if r.status_code in (200, 204):
        return

    payload_b = {"id": STATE_KEY, "last_node": last_node}
    r = sb_post(url, payload_b)
    if r.status_code in (200, 201, 204):
        return

    r = sb_patch(f"{SUPABASE_URL}/rest/v1/scraper_state?id=eq.{STATE_KEY}", {"last_node": last_node})
    if r.status_code in (200, 204):
        return

    raise RuntimeError("Could not update scraper_state")


def load_orange_index_from_supabase() -> dict:
    """
    Carrega tudo do Orange uma vez e monta índice em memória:
    key = (tax_sale_id, parcel_number)
    """
    if not USE_STATE:
        return {}

    index = {}
    offset = 0
    page_size = 1000

    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/properties"
            f"?county=eq.Orange"
            f"&select=id,node,tax_sale_id,parcel_number,sale_date,pdf_url,address,city,state_address,zip,opening_bid,deed_status,applicant_name,auction_source_url"
            f"&offset={offset}"
            f"&limit={page_size}"
        )

        r = sb_get(url, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"load_orange_index_from_supabase failed: {r.status_code} {r.text[:300]}")

        rows = r.json() or []
        if not rows:
            break

        for item in rows:
            key = (
                clean_text(item.get("tax_sale_id")),
                clean_text(item.get("parcel_number")),
            )
            if key[0] and key[1]:
                index[key] = item

        if len(rows) < page_size:
            break

        offset += page_size

    log.info("Loaded Orange index from Supabase: %s records", len(index))
    return index


def update_sale_date_only(record_id: str, sale_date: str | None) -> dict:
    url = f"{SUPABASE_URL}/rest/v1/properties?id=eq.{quote(str(record_id), safe='')}"
    payload = {"sale_date": normalize_sale_date_value(sale_date)}

    try:
        r = sb_patch(url, payload, timeout=20)
        ok = r.status_code in (200, 204)
        if ok:
            log.info("SUPABASE sale_date-only update OK id=%s sale_date=%s", record_id, payload["sale_date"])
        else:
            log.warning("SUPABASE sale_date-only update failed id=%s status=%s body=%s", record_id, r.status_code, r.text[:300])
        return {
            "sent": ok,
            "status_code": r.status_code,
            "response_text": r.text[:500],
        }
    except Exception as e:
        return {
            "sent": False,
            "status_code": None,
            "response_text": str(e),
        }


def payload_quality_score(payload: dict) -> int:
    fields = [
        "node",
        "parcel_number",
        "sale_date",
        "address",
        "city",
        "state_address",
        "zip",
        "pdf_url",
        "auction_source_url",
        "opening_bid",
        "deed_status",
    ]
    return sum(1 for f in fields if payload.get(f) not in (None, "", [], {}))


def payload_is_better_than_existing(payload: dict, existing: dict) -> bool:
    new_score = payload_quality_score(payload)
    old_score = payload_quality_score(existing)

    if new_score > old_score:
        return True

    important_fields = [
        "address",
        "city",
        "state_address",
        "zip",
        "pdf_url",
        "auction_source_url",
        "opening_bid",
        "deed_status",
        "applicant_name",
    ]

    for f in important_fields:
        old_val = clean_text(existing.get(f))
        new_val = clean_text(payload.get(f))
        if not old_val and new_val:
            return True

    return False


def supabase_upsert_property(payload: dict) -> dict:
    url = f"{SUPABASE_URL}/rest/v1/properties"

    try:
        r = sb_post(url, payload, timeout=30)
        ok = r.status_code in (200, 201)
        if ok:
            log.info("SUPABASE UPSERT OK node=%s", payload.get("node"))
        else:
            log.warning("SUPABASE UPSERT FAILED node=%s status=%s body=%s", payload.get("node"), r.status_code, r.text[:500])

        return {
            "sent": ok,
            "status_code": r.status_code,
            "node": payload.get("node"),
            "response_text": r.text[:1000],
        }
    except Exception as e:
        return {
            "sent": False,
            "status_code": None,
            "node": payload.get("node"),
            "response_text": str(e),
        }


def list_all_orange_nodes_from_supabase() -> list[str]:
    if not USE_STATE:
        return []

    nodes = []
    offset = 0
    page_size = 1000

    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/properties"
            f"?county=eq.Orange"
            f"&select=node"
            f"&offset={offset}"
            f"&limit={page_size}"
        )

        r = sb_get(url, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"list_all_orange_nodes_from_supabase failed: {r.status_code} {r.text[:300]}")

        rows = r.json() or []
        if not rows:
            break

        for item in rows:
            node = clean_text(item.get("node"))
            if node:
                nodes.append(node)

        if len(rows) < page_size:
            break

        offset += page_size

    return nodes


def delete_nodes_from_supabase(county: str, nodes: list[str]) -> dict:
    if not USE_STATE:
        return {
            "executed": False,
            "deleted_count": 0,
            "reason": "supabase not configured",
        }

    nodes = [clean_text(n) for n in nodes if clean_text(n)]
    if not nodes:
        return {
            "executed": True,
            "deleted_count": 0,
            "reason": "no nodes to delete",
        }

    deleted_count = 0
    errors = []
    batch_size = 100

    for i in range(0, len(nodes), batch_size):
        batch = nodes[i:i + batch_size]
        encoded = ",".join(f'"{quote(node, safe="")}"' for node in batch)

        url = (
            f"{SUPABASE_URL}/rest/v1/properties"
            f"?county=eq.{quote(county, safe='')}"
            f"&node=in.({encoded})"
        )

        try:
            r = sb_delete(url, timeout=60)
            if r.status_code in (200, 204):
                deleted_count += len(batch)
                log.info("Deleted %s stale nodes from Supabase", len(batch))
            else:
                errors.append(f"status={r.status_code} body={r.text[:400]}")
        except Exception as e:
            errors.append(str(e))

    return {
        "executed": True,
        "requested_delete_count": len(nodes),
        "deleted_count": deleted_count,
        "errors": errors,
    }


def reconcile_supabase_to_site(site_nodes_seen: set[str]) -> dict:
    try:
        existing_nodes = set(list_all_orange_nodes_from_supabase())
        site_nodes = {clean_text(x) for x in site_nodes_seen if clean_text(x)}

        to_delete = sorted(existing_nodes - site_nodes)

        log.info(
            "RECONCILE ORANGE: supabase=%s site=%s delete_candidates=%s",
            len(existing_nodes),
            len(site_nodes),
            len(to_delete),
        )

        delete_result = delete_nodes_from_supabase("Orange", to_delete)

        return {
            "executed": True,
            "supabase_nodes_count": len(existing_nodes),
            "site_nodes_count": len(site_nodes),
            "delete_candidates_count": len(to_delete),
            "delete_candidates_sample": to_delete[:50],
            "delete_result": delete_result,
        }
    except Exception as e:
        log.exception("reconcile_supabase_to_site failed: %s", str(e))
        return {
            "executed": False,
            "reason": str(e),
        }


# =========================
# APP INGEST
# =========================
def post_to_app(payload: dict) -> dict | None:
    if not SEND_TO_APP:
        log.info("SEND_TO_APP=False → skipping ingest")
        return None

    url = f"{APP_API_BASE}/api/ingest"
    headers = {"Authorization": f"Bearer {APP_API_TOKEN}"}

    last_err = None
    for attempt in range(1, 4):
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=30)
            log.info("INGEST attempt %d: %s", attempt, r.status_code)
            snippet = (r.text or "")[:250].replace("\n", " ")
            log.info("INGEST response snippet: %s", snippet)

            if r.status_code in (200, 201):
                try:
                    return r.json()
                except Exception:
                    return {"ok": True, "raw": r.text}

            if r.status_code == 401:
                log.error("INGEST unauthorized (401) → check APP_API_TOKEN")
                return None

            last_err = f"HTTP {r.status_code}: {r.text[:500]}"
        except Exception as e:
            last_err = str(e)

        time.sleep(1.0 * attempt)

    log.error("INGEST failed after retries: %s", last_err)
    return None


# =========================
# TABLE PARSE
# =========================
def parse_fields_from_row_text(row_text: str) -> dict:
    txt = row_text

    def pick(pattern):
        m = re.search(pattern, txt, re.I)
        return m.group(1).strip() if m else ""

    tax_sale_id = pick(r"Tax Sale\s+(\d{4}-\d+)")
    sale_date = pick(r"Sale Date:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})")
    status = pick(r"Status:\s*([A-Za-z ]+?)(?:\s+Parcel:|\s+Min Bid:|\s+High Bid:|$)")
    parcel = pick(r"Parcel:\s*([0-9A-Z\-]+)")
    min_bid = pick(r"Min Bid:\s*\$?\s*([0-9,]+\.\d{2}|[0-9,]+)")
    applicant = pick(r"Applicant Name:\s*(.+?)(?:\s+Status:|$)")

    return {
        "tax_sale_id": clean_text(tax_sale_id),
        "sale_date": normalize_sale_date_value(sale_date),
        "deed_status": clean_text(status),
        "parcel_number": clean_text(parcel),
        "opening_bid": clean_text(min_bid),
        "applicant_name": clean_text(applicant),
    }


def extract_lots_from_printable(page) -> list[dict]:
    lots = []
    links = page.locator("a:has-text('Tax Sale')")
    total = links.count()
    log.info("Tax Sale links found: %d", total)

    for i in range(total):
        a = links.nth(i)
        href = a.get_attribute("href")
        if not href:
            continue

        full = urljoin(page.url, href)
        q = parse_qs(urlparse(full).query)
        node = (q.get("node") or [None])[0]

        try:
            row_text = a.locator("xpath=ancestor::tr[1]").inner_text(timeout=2000)
        except Exception:
            row_text = ""

        fields = parse_fields_from_row_text(norm_ws(row_text))

        lots.append({
            "node": clean_text(node),
            "tax_sale_url": full,
            "row_text": norm_ws(row_text),
            "list_fields": fields,
        })

    return [l for l in lots if l.get("node")]


# =========================
# PDF TEXT + OCR
# =========================
def try_pdfplumber_text(pdf_bytes: bytes) -> str:
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            parts = []
            for p in pdf.pages:
                t = (p.extract_text() or "").strip()
                if t:
                    parts.append(t)
            return "\n".join(parts).strip()
    except Exception:
        return ""


def ocr_pdf_bytes(pdf_bytes: bytes, max_pages: int = 3, scale: float = 2.2) -> str:
    pdf = pypdfium2.PdfDocument(pdf_bytes)
    n_pages = len(pdf)
    pages_to_do = min(n_pages, max_pages)

    full_text = []
    for i in range(pages_to_do):
        page = pdf[i]
        bitmap = page.render(scale=scale)
        img = bitmap.to_pil()
        txt = pytesseract.image_to_string(img, config="--psm 6")
        if txt:
            full_text.append(txt)

    return "\n".join(full_text).strip()


def _extract_street_before_city(block: str, city_match_start: int) -> str | None:
    before = block[:city_match_start]
    lines = [ln.strip() for ln in before.splitlines() if ln.strip()]
    return lines[-1] if lines else None


def parse_best_address_from_text(text: str) -> dict:
    if not text:
        return {
            "address": None, "city": None, "state": None, "zip": None,
            "marker_used": None, "marker_found": False, "snippet": ""
        }

    markers = [
        ("ADDRESS_ON_RECORD", r"ADDRESS\s+ON\s+RECORD\s+ON\s+CURRENT\s+TAX\s+ROLL\s*[:\-]?"),
        ("PHYSICAL_ADDRESS", r"PHYSICAL\s+ADDRESS\s*[:\-]?"),
        ("TITLE_HOLDER_ADDRESS", r"TITLE\s+HOLDER\s+AND\s+ADDRESS\s+OF\s+RECORD\s*[:\-]?"),
    ]

    for marker_name, marker_re in markers:
        mm = re.search(marker_re, text, re.I)
        if not mm:
            continue

        after = text[mm.end():].strip()
        mcity = re.search(r"([A-Za-z .'-]+)\s*,\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)", after, re.I)
        if not mcity:
            continue

        street = _extract_street_before_city(after, mcity.start())
        return {
            "address": street,
            "city": mcity.group(1).title().strip(),
            "state": mcity.group(2).upper(),
            "zip": mcity.group(3),
            "marker_used": marker_name,
            "marker_found": True,
            "snippet": after[:700],
        }

    mcity = re.search(r"([A-Za-z .'-]+)\s*,\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)", text, re.I)
    if not mcity:
        return {
            "address": None, "city": None, "state": None, "zip": None,
            "marker_used": None, "marker_found": False, "snippet": text[:900]
        }

    street = _extract_street_before_city(text, mcity.start())
    return {
        "address": street,
        "city": mcity.group(1).title().strip(),
        "state": mcity.group(2).upper(),
        "zip": mcity.group(3),
        "marker_used": "FALLBACK_FIRST_MATCH",
        "marker_found": True,
        "snippet": text[max(0, mcity.start()-250):mcity.end()+250],
    }


# =========================
# PAYLOAD / DECISION
# =========================
def build_payload_from_detail(
    node: str,
    viewer_url: str,
    pdf_url: str,
    list_fields: dict,
    addr: dict,
) -> dict:
    notes = None
    if not addr.get("marker_found"):
        notes = f"Address marker not found via OCR first {OCR_MAX_PAGES} pages."

    return {
        "county": "Orange",
        "state": "FL",
        "node": node,
        "auction_source_url": viewer_url,
        "tax_sale_id": list_fields.get("tax_sale_id") or None,
        "parcel_number": list_fields.get("parcel_number") or None,
        "sale_date": normalize_sale_date_value(list_fields.get("sale_date")),
        "opening_bid": normalize_bid_for_payload(list_fields.get("opening_bid")),
        "deed_status": list_fields.get("deed_status") or None,
        "applicant_name": list_fields.get("applicant_name") or None,
        "pdf_url": pdf_url,
        "address": addr.get("address"),
        "city": addr.get("city"),
        "state_address": addr.get("state"),
        "zip": addr.get("zip"),
        "address_source_marker": addr.get("marker_used"),
        "status": "new",
        "notes": notes,
    }


def record_needs_enrichment(existing: dict) -> bool:
    important_fields = [
        "pdf_url",
        "address",
        "city",
        "state_address",
        "zip",
        "opening_bid",
        "deed_status",
        "applicant_name",
        "auction_source_url",
    ]
    return any(not clean_text(existing.get(f)) for f in important_fields)


def decide_list_action(list_fields: dict, existing: dict | None) -> dict:
    site_sale_date = normalize_sale_date_value(list_fields.get("sale_date"))
    tax_sale_id = clean_text(list_fields.get("tax_sale_id"))
    parcel_number = clean_text(list_fields.get("parcel_number"))

    if not existing:
        return {
            "action": "open_detail_and_upsert",
            "reason": "record not found",
            "tax_sale_id": tax_sale_id,
            "parcel_number": parcel_number,
            "site_sale_date": site_sale_date,
        }

    db_sale_date = normalize_sale_date_value(existing.get("sale_date"))

    if record_needs_enrichment(existing):
        return {
            "action": "open_detail_and_upsert",
            "reason": "record exists but needs enrichment",
            "tax_sale_id": tax_sale_id,
            "parcel_number": parcel_number,
            "site_sale_date": site_sale_date,
        }

    if db_sale_date == site_sale_date:
        return {
            "action": "skip",
            "reason": "sale_date unchanged and record already enriched",
            "tax_sale_id": tax_sale_id,
            "parcel_number": parcel_number,
            "site_sale_date": site_sale_date,
        }

    return {
        "action": "update_sale_date_only",
        "reason": f"sale_date changed from {db_sale_date} to {site_sale_date}",
        "tax_sale_id": tax_sale_id,
        "parcel_number": parcel_number,
        "site_sale_date": site_sale_date,
    }


# =========================
# BOOTSTRAP
# =========================
def bootstrap_to_printable(p, headless: bool):
    browser = p.chromium.launch(headless=headless)
    context = browser.new_context()
    page = context.new_page()

    log.info("OPEN: %s", LOGIN_URL)
    page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=MAX_WAIT)

    click_any(page, [
        "text=I Acknowledge",
        "button:has-text('I Acknowledge')",
        "a:has-text('I Acknowledge')",
        "input[value='I Acknowledge']",
    ], "I Acknowledge")
    wait_network(page)

    log.info("OPEN SEARCH: %s", SEARCH_URL)
    page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=MAX_WAIT)
    wait_network(page)

    if page.locator("select[name='DeedStatusID']").count() > 0:
        page.select_option("select[name='DeedStatusID']", value="AS")
        log.info("Set DeedStatusID=AS")
    else:
        log.warning("Dropdown DeedStatusID not found; selector may need update.")

    ok = click_any(page, [
        "input[type='submit'][value='Search']",
        "button:has-text('Search')",
        "text=Search"
    ], "Search")
    if not ok:
        raise RuntimeError("Could not click Search")

    page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
    wait_network(page, 30_000)

    ok = click_any(page, [
        "text=Printable Version",
        "a:has-text('Printable Version')",
        "button:has-text('Printable Version')"
    ], "Printable Version")
    if not ok:
        raise RuntimeError("Could not click Printable Version")

    page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
    wait_network(page, 30_000)

    printable_url = page.url
    log.info("After Printable URL: %s", printable_url)

    if DEBUG_HTML:
        log.info("Printable HTML length: %d", len(page.content()))

    return browser, context, page, printable_url


def safe_close(browser=None, context=None, page=None):
    try:
        if page:
            page.close()
    except Exception:
        pass
    try:
        if context:
            context.close()
    except Exception:
        pass
    try:
        if browser:
            browser.close()
    except Exception:
        pass


# =========================
# VIEWER
# =========================
def open_viewer_with_retry(page, printable_url: str, tax_sale_url: str, idx: int) -> str:
    viewer_url = ""
    for attempt in range(1, MAX_VIEWER_RETRIES + 1):
        page.goto(tax_sale_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
        wait_network(page)
        viewer_url = page.url
        log.info("Viewer URL: %s", viewer_url)

        if not is_check_human(viewer_url):
            return viewer_url

        log.warning("Hit checkHuman.jsp (attempt %d/%d).", attempt, MAX_VIEWER_RETRIES)
        human_backoff(idx, attempt)

        page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
        wait_network(page, 30_000)

    return viewer_url


# =========================
# MAIN
# =========================
def run():
    log.info("SEND_TO_APP=%s APP_API_BASE=%s", SEND_TO_APP, APP_API_BASE if APP_API_BASE else "(empty)")
    log.info("USE_STATE=%s STATE_KEY=%s START_AFTER_LAST_NODE=%s", USE_STATE, STATE_KEY, START_AFTER_LAST_NODE)
    log.info("MAX_LOTS=%s RESTART_BROWSER_EVERY=%s HEADLESS=%s", MAX_LOTS, RESTART_BROWSER_EVERY, HEADLESS)
    log.info("OCR_MAX_PAGES=%s OCR_SCALE=%s", OCR_MAX_PAGES, OCR_SCALE)

    last_node = None
    if USE_STATE:
        try:
            last_node = get_state_last_node()
            log.info("STATE last_node=%s", last_node)
        except Exception as e:
            log.warning("STATE read failed: %s", str(e))

    supabase_index = {}
    if USE_STATE:
        try:
            supabase_index = load_orange_index_from_supabase()
        except Exception as e:
            log.exception("Failed loading Orange index from Supabase: %s", str(e))
            raise

    with sync_playwright() as p:
        browser = context = page = None
        printable_url = ""

        browser, context, page, printable_url = bootstrap_to_printable(p, HEADLESS)

        lots = extract_lots_from_printable(page)
        if not lots:
            safe_close(browser, context, page)
            raise RuntimeError("No lots found on printable page.")

        total_site_items = len(lots)
        log.info("Total lots found in Orange printable: %s", total_site_items)

        if START_AFTER_LAST_NODE and last_node:
            pos = next((i for i, l in enumerate(lots) if l["node"] == last_node), None)
            if pos is not None:
                lots = lots[pos + 1:]
                log.info("Continuing AFTER last_node. Remaining lots=%d", len(lots))
            else:
                log.info("last_node not found in current list → processing from top.")

        selected = lots[:MAX_LOTS]
        log.info("Selected lots count=%d", len(selected))
        log.info("First nodes: %s", [l["node"] for l in selected[:10]])

        results = []
        failures = []
        supabase_results = []
        seen_nodes_this_run = set()

        completed_all_selected = False
        can_delete_missing = False

        for idx, lot in enumerate(selected, start=1):
            if RESTART_BROWSER_EVERY > 0 and idx > 1 and (idx - 1) % RESTART_BROWSER_EVERY == 0:
                log.warning("Restarting browser after %d lots...", idx - 1)
                safe_close(browser, context, page)
                browser, context, page, printable_url = bootstrap_to_printable(p, HEADLESS)

            node = clean_text(lot["node"])
            row_text = lot["row_text"]
            tax_sale_url = lot["tax_sale_url"]
            list_fields = lot["list_fields"]

            seen_nodes_this_run.add(node)

            log.info("----- LOT %d/%d node=%s -----", idx, len(selected), node)
            log.info("Row text: %s", row_text[:220])

            try:
                key = (
                    clean_text(list_fields.get("tax_sale_id")),
                    clean_text(list_fields.get("parcel_number")),
                )
                existing = supabase_index.get(key)

                action_decision = decide_list_action(list_fields, existing)
                action = action_decision["action"]

                log.info("DECISION node=%s action=%s reason=%s", node, action, action_decision["reason"])

                if action == "skip":
                    continue

                if action == "update_sale_date_only":
                    if existing and existing.get("id"):
                        sb_result = update_sale_date_only(existing["id"], list_fields.get("sale_date"))
                        supabase_results.append({
                            "node": node,
                            "mode": "sale_date_only",
                            **sb_result,
                        })

                        if sb_result.get("sent"):
                            existing["sale_date"] = normalize_sale_date_value(list_fields.get("sale_date"))
                            supabase_index[key] = existing
                        continue

                viewer_url = open_viewer_with_retry(page, printable_url, tax_sale_url, idx)
                if is_check_human(viewer_url):
                    raise RuntimeError(f"Blocked by checkHuman.jsp after retries for node={node}")

                pdf_a = page.locator("a[href*='Property_Information.pdf']")
                href_pdf = pdf_a.first.get_attribute("href") if pdf_a.count() else None

                if not href_pdf:
                    viewer_html = page.content()
                    m = re.search(r'href="([^"]*Property_Information\.pdf[^"]*)"', viewer_html, re.I)
                    href_pdf = m.group(1) if m else None

                if not href_pdf:
                    raise RuntimeError(f"PDF link not found for node={node}")

                pdf_url = urljoin(viewer_url, href_pdf)
                log.info("PDF URL: %s", pdf_url)

                pdf_resp = context.request.get(pdf_url, timeout=MAX_WAIT)
                log.info("PDF HTTP status: %s", pdf_resp.status)
                log.info("PDF content-type: %s", pdf_resp.headers.get("content-type"))

                if not pdf_resp.ok:
                    preview = (pdf_resp.text() or "")[:600]
                    raise RuntimeError(f"PDF download failed for node={node}: {preview}")

                if not must_be_pdf(pdf_resp.headers):
                    preview = (pdf_resp.text() or "")[:800]
                    raise RuntimeError(f"Response is not PDF for node={node}: {preview}")

                pdf_bytes = pdf_resp.body()
                log.info("PDF bytes: %d", len(pdf_bytes))

                text = try_pdfplumber_text(pdf_bytes)
                if text:
                    log.info("pdfplumber text length: %d", len(text))
                    addr = parse_best_address_from_text(text)
                else:
                    log.info("pdfplumber empty. OCR first %d pages...", OCR_MAX_PAGES)
                    ocr_text = ocr_pdf_bytes(pdf_bytes, max_pages=OCR_MAX_PAGES, scale=OCR_SCALE)
                    log.info("OCR text length: %d", len(ocr_text))
                    addr = parse_best_address_from_text(ocr_text)

                if SKIP_IF_ADDRESS_NOT_NUMBERED:
                    if not is_numbered_street_address(addr.get("address")):
                        log.warning("Skipping node=%s because address is not numbered: %r", node, addr.get("address"))
                        page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                        wait_network(page, 30_000)
                        time.sleep(1.0)
                        continue

                payload = build_payload_from_detail(
                    node=node,
                    viewer_url=viewer_url,
                    pdf_url=pdf_url,
                    list_fields=list_fields,
                    addr=addr,
                )

                print("\n" + "=" * 100)
                print(f"RESULT LOT {idx}")
                print("=" * 100)
                print(json.dumps(payload, indent=2))

                sb_result = supabase_upsert_property(payload)
                supabase_results.append({
                    "node": node,
                    "mode": "full_upsert",
                    **sb_result,
                })

                if sb_result.get("sent"):
                    supabase_index[key] = {
                        "id": existing.get("id") if existing else None,
                        "node": payload.get("node"),
                        "tax_sale_id": payload.get("tax_sale_id"),
                        "parcel_number": payload.get("parcel_number"),
                        "sale_date": payload.get("sale_date"),
                        "pdf_url": payload.get("pdf_url"),
                        "address": payload.get("address"),
                        "city": payload.get("city"),
                        "state_address": payload.get("state_address"),
                        "zip": payload.get("zip"),
                        "opening_bid": payload.get("opening_bid"),
                        "deed_status": payload.get("deed_status"),
                        "applicant_name": payload.get("applicant_name"),
                        "auction_source_url": payload.get("auction_source_url"),
                    }

                ingest_ok = True
                ingest_result = None
                if SEND_TO_APP:
                    ingest_result = post_to_app(payload)
                    ingest_ok = bool(ingest_result)

                if ingest_result:
                    log.info("INGEST OK: %s", ingest_result)

                if ingest_ok:
                    try:
                        set_state_last_node(node)
                        log.info("STATE updated last_node=%s", node)
                    except Exception as e:
                        log.warning("STATE write failed: %s", str(e))

                results.append(payload)

            except Exception as e:
                log.exception("LOT FAILED node=%s error=%s", node, str(e))
                failures.append({
                    "node": node,
                    "error": str(e),
                })

            try:
                page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                wait_network(page, 30_000)
            except Exception:
                log.warning("Failed to return printable. Hard reset session.")
                safe_close(browser, context, page)
                browser, context, page, printable_url = bootstrap_to_printable(p, HEADLESS)

            time.sleep(1.0)

        completed_all_selected = (
            len(selected) > 0
            and len(seen_nodes_this_run) == len(selected)
            and len(failures) == 0
        )

        can_delete_missing = (
            completed_all_selected
            and len(selected) == total_site_items
        )

        if can_delete_missing:
            reconcile_result = reconcile_supabase_to_site(seen_nodes_this_run)
        else:
            reconcile_result = {
                "executed": False,
                "reason": (
                    f"safe delete blocked: "
                    f"completed_all_selected={completed_all_selected}, "
                    f"selected_count={len(selected)}, "
                    f"total_site_items={total_site_items}, "
                    f"seen_nodes={len(seen_nodes_this_run)}, "
                    f"failures_count={len(failures)}, "
                    f"MAX_LOTS={MAX_LOTS}"
                )
            }

        final_payload = {
            "source": "Orange",
            "mode": "orange_fast_precheck_in_memory_index_safe_delete",
            "total_site_items": total_site_items,
            "selected_count": len(selected),
            "seen_nodes_count": len(seen_nodes_this_run),
            "completed_all_selected": completed_all_selected,
            "can_delete_missing": can_delete_missing,
            "records_count": len(results),
            "failures_count": len(failures),
            "supabase_results": supabase_results,
            "reconcile_result": reconcile_result,
            "records": results,
            "failures": failures,
        }

        log.info("===== FINAL PAYLOAD =====")
        log.info(json.dumps(final_payload, indent=2))
        print(json.dumps(final_payload, indent=2))

        log.info("DONE.")
        safe_close(browser, context, page)


if __name__ == "__main__":
    run()