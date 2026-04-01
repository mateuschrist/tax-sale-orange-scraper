import os
import re
import json
import time
import random
import logging
from io import BytesIO
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, quote

import requests
import pdfplumber
import pypdfium2
import pytesseract
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

log = logging.getLogger("taxdeed-palmbeach")


BASE_URL = "https://taxdeed.mypalmbeachclerk.com"
STATUS_URL = "https://taxdeed.mypalmbeachclerk.com/#tabs-7"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

APP_API_BASE = (os.getenv("APP_API_BASE", "") or "").strip().rstrip("/")
APP_API_TOKEN = (os.getenv("APP_API_TOKEN", "") or "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

SUPABASE_URL = (os.getenv("SUPABASE_URL", "") or "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or "").strip()
CAN_CHECK_SUPABASE = bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)

OCR_SCALE = float(os.getenv("OCR_SCALE", "2.2"))
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
PALM_BEACH_MAX_CASES = int(os.getenv("PALM_BEACH_MAX_CASES", "500"))
PALM_BEACH_FROM_DATE = (os.getenv("PALM_BEACH_FROM_DATE", "") or "").strip()
PALM_BEACH_TO_DATE = (os.getenv("PALM_BEACH_TO_DATE", "") or "").strip()
SAFE_DELETE_ENABLED = os.getenv("PALM_BEACH_SAFE_DELETE_ENABLED", "true").lower() == "true"


# =========================
# GENERIC HELPERS
# =========================
def norm(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def clean_bid(v):
    if v is None:
        return None
    cleaned = re.sub(r"[^\d.]", "", str(v))
    return cleaned or None


def normalize_sale_date_value(value: str | None) -> str | None:
    v = norm(value or "")
    if not v:
        return None
    low = v.lower()
    if low in ("null", "none", "n/a", "na", "not assigned"):
        return None
    return v


def empty_addr():
    return {
        "address": None,
        "city": None,
        "state": None,
        "zip": None,
        "source": None,
    }


def normalize_property_address(addr: str) -> str:
    return norm(addr).replace(" ,", ",")


def is_po_box(addr: str) -> bool:
    if not addr:
        return False
    a = addr.upper().replace(".", "").replace("  ", " ")
    return "PO BOX" in a or "P O BOX" in a or "POST OFFICE BOX" in a


def looks_like_garbage_address(addr: str) -> bool:
    if not addr:
        return True

    a = norm(addr)
    upper = a.upper()

    if len(a) < 4 or len(a) > 120:
        return True

    if is_po_box(a):
        return True

    bad_markers = [
        "NOTE:",
        "LEGAL DESCRIPTION",
        "PCN",
        "PARCEL",
        "NAME LAST ASSESSED",
        "OFFICIAL RECORDS",
        "TAX ASSESSMENT",
        "BOOK/PAGE",
        "SALE DATE",
        "OWNER INFORMATION",
        "MAILING ADDRESS",
        "MUNICIPALITY",
        "SUBDIVISION",
    ]

    for marker in bad_markers:
        if marker in upper:
            return True

    return False


def is_valid_property_address(addr: str) -> bool:
    if not addr:
        return False

    a = normalize_property_address(addr)

    if looks_like_garbage_address(a):
        return False

    if re.match(r"^\d{1,6}\s+[A-Z0-9 .'\-#/]+$", a, re.I):
        return True

    if re.match(r"^[0-9A-Z .'\-#/]+$", a, re.I) and len(a.split()) <= 8:
        return True

    return False


def sanitize_address_payload(addr: dict) -> dict:
    if not addr:
        return empty_addr()

    street = addr.get("address")
    if not is_valid_property_address(street):
        return empty_addr()

    return {
        "address": normalize_property_address(addr.get("address")),
        "city": norm(addr.get("city")) if addr.get("city") else None,
        "state": addr.get("state"),
        "zip": addr.get("zip"),
        "source": addr.get("source"),
    }


def payload_quality_score(payload: dict) -> int:
    fields = [
        "node",
        "tax_sale_id",
        "parcel_number",
        "sale_date",
        "opening_bid",
        "deed_status",
        "applicant_name",
        "pdf_url",
        "auction_source_url",
        "address",
        "city",
        "state_address",
        "zip",
    ]
    return sum(1 for f in fields if payload.get(f) not in (None, "", []))


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
        old_val = norm(existing.get(f) or "")
        new_val = norm(payload.get(f) or "")
        if not old_val and new_val:
            return True

    return False


def human_pause(a=0.20, b=0.60):
    time.sleep(random.uniform(a, b))


def wait_network_quiet(page, timeout=10000):
    try:
        page.wait_for_load_state("networkidle", timeout=timeout)
    except PWTimeout:
        pass


def visible_elements(locator):
    out = []
    try:
        count = locator.count()
    except Exception:
        return out

    for i in range(count):
        item = locator.nth(i)
        try:
            if item.is_visible() and item.is_enabled():
                out.append(item)
        except Exception:
            continue
    return out


def human_fill(page, locator, value: str):
    locator.click(timeout=10000)
    human_pause(0.08, 0.16)

    try:
        locator.press("Control+A")
    except Exception:
        pass

    human_pause(0.04, 0.08)

    try:
        locator.press("Backspace")
    except Exception:
        pass

    human_pause(0.04, 0.08)
    page.keyboard.type(value, delay=35)
    human_pause(0.12, 0.25)


def build_search_dates():
    if PALM_BEACH_FROM_DATE and PALM_BEACH_TO_DATE:
        return PALM_BEACH_FROM_DATE, PALM_BEACH_TO_DATE

    today = date.today()
    future = today + timedelta(days=365)

    if os.name == "nt":
        return today.strftime("%#m/%#d/%Y"), future.strftime("%#m/%#d/%Y")
    return today.strftime("%-m/%-d/%Y"), future.strftime("%-m/%-d/%Y")


def send(payload):
    if not SEND_TO_APP:
        return True

    try:
        r = requests.post(
            f"{APP_API_BASE}/api/ingest",
            json=payload,
            headers={"Authorization": f"Bearer {APP_API_TOKEN}"},
            timeout=30,
        )
        log.info("INGEST status=%s node=%s", r.status_code, payload.get("node"))
        if r.text:
            log.info("INGEST response=%s", r.text[:250].replace("\n", " "))
        return r.status_code in (200, 201)
    except Exception as e:
        log.warning("INGEST failed node=%s error=%s", payload.get("node"), str(e))
        return False


# =========================
# SUPABASE
# =========================
def sb_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }


def supabase_fetch_all_palm_beach_records() -> List[dict]:
    if not CAN_CHECK_SUPABASE:
        return []

    all_rows = []
    offset = 0
    page_size = 1000

    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/properties"
            f"?county=eq.PalmBeach"
            f"&select=id,node,tax_sale_id,parcel_number,sale_date,address,city,state_address,zip,pdf_url,auction_source_url,opening_bid,deed_status,applicant_name,address_source_marker"
            f"&offset={offset}"
            f"&limit={page_size}"
        )

        try:
            r = requests.get(url, headers=sb_headers(), timeout=60)
            if r.status_code != 200:
                log.warning(
                    "supabase_fetch_all_palm_beach_records failed status=%s body=%s",
                    r.status_code,
                    r.text[:500],
                )
                break

            arr = r.json() or []
            if not arr:
                break

            all_rows.extend(arr)

            if len(arr) < page_size:
                break

            offset += page_size

        except Exception as e:
            log.warning("supabase_fetch_all_palm_beach_records exception: %s", str(e))
            break

    log.info("Loaded %s Palm Beach records from Supabase", len(all_rows))
    return all_rows


def build_supabase_indexes(rows: List[dict]) -> dict:
    by_node = {}
    by_tax_sale_parcel = {}

    for row in rows:
        node = norm(row.get("node") or "")
        tax_sale_id = norm(row.get("tax_sale_id") or "")
        parcel_number = norm(row.get("parcel_number") or "")

        if node:
            by_node[node] = row

        if tax_sale_id and parcel_number:
            by_tax_sale_parcel[(tax_sale_id, parcel_number)] = row

    return {
        "by_node": by_node,
        "by_tax_sale_parcel": by_tax_sale_parcel,
    }


def supabase_update_sale_date(record_id: str, sale_date: str | None) -> dict:
    if not CAN_CHECK_SUPABASE or not record_id:
        return {
            "sent": False,
            "status_code": None,
            "response_text": "missing config or record id",
        }

    url = f"{SUPABASE_URL}/rest/v1/properties?id=eq.{quote(str(record_id), safe='')}"
    headers = sb_headers()
    headers["Prefer"] = "return=representation"

    payload = {"sale_date": normalize_sale_date_value(sale_date)}

    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=30)
        ok = r.status_code in (200, 204)
        if ok:
            log.info("SUPABASE SALE_DATE UPDATE OK id=%s sale_date=%s", record_id, payload["sale_date"])
        else:
            log.warning(
                "SUPABASE SALE_DATE UPDATE FAILED id=%s status=%s body=%s",
                record_id,
                r.status_code,
                r.text[:500],
            )

        return {
            "sent": ok,
            "status_code": r.status_code,
            "response_text": r.text[:1000],
        }
    except Exception as e:
        log.warning("SUPABASE SALE_DATE UPDATE EXCEPTION id=%s error=%s", record_id, str(e))
        return {
            "sent": False,
            "status_code": None,
            "response_text": str(e),
        }


def supabase_insert_or_update_property(payload: dict) -> dict:
    if not CAN_CHECK_SUPABASE:
        return {
            "sent": False,
            "status_code": None,
            "node": payload.get("node"),
            "response_text": "SUPABASE not configured",
        }

    url = f"{SUPABASE_URL}/rest/v1/properties"
    headers = sb_headers()
    headers["Prefer"] = "return=representation,resolution=merge-duplicates"

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        ok = r.status_code in (200, 201)

        if ok:
            returned = None
            try:
                returned = r.json()
            except Exception:
                returned = None

            rec_id = None
            if isinstance(returned, list) and returned:
                rec_id = returned[0].get("id")

            log.info(
                "SUPABASE INSERT OK node=%s status=%s id=%s",
                payload.get("node"),
                r.status_code,
                rec_id,
            )
        else:
            log.warning(
                "SUPABASE UPSERT FAILED node=%s status=%s body=%s",
                payload.get("node"),
                r.status_code,
                r.text[:600],
            )

        return {
            "sent": ok,
            "status_code": r.status_code,
            "node": payload.get("node"),
            "response_text": r.text[:1000],
        }
    except Exception as e:
        log.warning("SUPABASE UPSERT EXCEPTION node=%s error=%s", payload.get("node"), str(e))
        return {
            "sent": False,
            "status_code": None,
            "node": payload.get("node"),
            "response_text": str(e),
        }


def supabase_delete_nodes(nodes: List[str]) -> dict:
    if not CAN_CHECK_SUPABASE:
        return {
            "executed": False,
            "deleted_count": 0,
            "reason": "SUPABASE not configured",
        }

    nodes = [str(x).strip() for x in nodes if str(x).strip()]
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

        try:
            quoted = ",".join(f'"{quote(node, safe="")}"' for node in batch)
            url = (
                f"{SUPABASE_URL}/rest/v1/properties"
                f"?county=eq.PalmBeach"
                f"&node=in.({quoted})"
            )

            r = requests.delete(url, headers=sb_headers(), timeout=60)
            if r.status_code in (200, 204):
                deleted_count += len(batch)
                log.info("SUPABASE DELETE batch ok count=%s", len(batch))
            else:
                msg = f"status={r.status_code} body={r.text[:500]}"
                errors.append(msg)
                log.warning("SUPABASE DELETE batch failed: %s", msg)

        except Exception as e:
            msg = str(e)
            errors.append(msg)
            log.warning("SUPABASE DELETE batch exception: %s", msg)

    return {
        "executed": True,
        "deleted_count": deleted_count,
        "requested_delete_count": len(nodes),
        "errors": errors,
    }


def reconcile_supabase_to_site(seen_nodes_this_run: set[str], indexes: dict) -> dict:
    try:
        supabase_nodes = set(indexes["by_node"].keys())
        site_nodes = {str(x).strip() for x in seen_nodes_this_run if str(x).strip()}
        to_delete = sorted(supabase_nodes - site_nodes)

        log.info(
            "RECONCILE PALM BEACH: supabase=%s site=%s delete=%s",
            len(supabase_nodes),
            len(site_nodes),
            len(to_delete),
        )

        delete_result = supabase_delete_nodes(to_delete)

        return {
            "executed": True,
            "supabase_nodes_count": len(supabase_nodes),
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
# PDF EXTRACTION
# =========================
def build_adaptive_page_order(total_pages: int) -> List[int]:
    preferred = [10, 11]
    seen = set()
    order = []

    for p in preferred:
        if 1 <= p <= total_pages and p not in seen:
            order.append(p)
            seen.add(p)

    offsets = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    bases = [10, 11]

    for off in offsets:
        for base in bases:
            for candidate in (base - off, base + off):
                if 1 <= candidate <= total_pages and candidate not in seen:
                    order.append(candidate)
                    seen.add(candidate)

    for p in range(1, total_pages + 1):
        if p not in seen:
            order.append(p)
            seen.add(p)

    return order


def read_single_pdf_page(pdf_bytes: bytes, page_num: int) -> str:
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            idx = page_num - 1
            if 0 <= idx < len(pdf.pages):
                return (pdf.pages[idx].extract_text() or "").strip()
    except Exception:
        pass
    return ""


def ocr_single_pdf_page(pdf_bytes: bytes, page_num: int) -> str:
    try:
        doc = pypdfium2.PdfDocument(pdf_bytes)
        idx = page_num - 1
        if 0 <= idx < len(doc):
            img = doc[idx].render(scale=OCR_SCALE).to_pil()
            return pytesseract.image_to_string(img, config="--psm 6").strip()
    except Exception:
        pass
    return ""


def parse_location_or_mailing_address(text: str) -> dict:
    if not text:
        return empty_addr()

    t = text.replace("\r", "\n")

    m_loc = re.search(r"Location Address\s*:\s*(.+)", t, re.I)
    if m_loc:
        street = normalize_property_address(m_loc.group(1))

        if is_valid_property_address(street):
            m_muni = re.search(r"Municipality\s*:\s*([A-Z][A-Z .'-]+)", t, re.I)
            municipality = norm(m_muni.group(1)).title() if m_muni else None

            m_city_zip = re.search(
                r"([A-Z][A-Z .'-]+)\s+FL\s+(\d{5})(?:-\d{4}|\s+\d{4})?",
                t,
                re.I
            )

            if m_city_zip:
                return {
                    "address": street,
                    "city": norm(m_city_zip.group(1)).title(),
                    "state": "FL",
                    "zip": m_city_zip.group(2),
                    "source": "PDF_LOCATION_ADDRESS",
                }

            return {
                "address": street,
                "city": municipality,
                "state": "FL" if municipality else None,
                "zip": None,
                "source": "PDF_LOCATION_ADDRESS",
            }

    m_mail = re.search(
        r"Mailing Address\s*\n+\s*(.+?)\s*\n+\s*([A-Z][A-Z ]+)\s+FL\s+(\d{5})(?:-\d{4}|\s+\d{4})?",
        t,
        re.I | re.S,
    )
    if m_mail:
        street = normalize_property_address(m_mail.group(1))
        city = norm(m_mail.group(2)).title()
        zip_code = m_mail.group(3)

        if is_valid_property_address(street):
            return {
                "address": street,
                "city": city,
                "state": "FL",
                "zip": zip_code,
                "source": "PDF_MAILING_ADDRESS",
            }

    return empty_addr()


def parse_you_entered_address(text: str) -> dict:
    if not text:
        return empty_addr()

    lines = [norm(x) for x in text.replace("\r", "\n").splitlines() if norm(x)]

    for i, line in enumerate(lines):
        if "you entered" in line.lower():
            window = lines[i:i + 12]

            for j in range(len(window) - 2, -1, -1):
                addr_line = window[j]
                if j + 1 >= len(window):
                    continue

                city_line = window[j + 1]

                m_city = re.search(
                    r"^([A-Z][A-Z .'-]+)\s+FL\s+(\d{5})(?:-\d{4})?$",
                    city_line,
                    re.I
                )
                if not m_city:
                    continue

                addr_line = normalize_property_address(addr_line)
                if not is_valid_property_address(addr_line):
                    continue

                return {
                    "address": addr_line,
                    "city": norm(m_city.group(1)).title(),
                    "state": "FL",
                    "zip": m_city.group(2),
                    "source": "PDF_USPS_YOU_ENTERED",
                }

    return empty_addr()


def parse_address_from_pdf_text(text: str) -> dict:
    if not text:
        return empty_addr()

    addr = parse_location_or_mailing_address(text)
    addr = sanitize_address_payload(addr)
    if addr.get("address"):
        return addr

    addr = parse_you_entered_address(text)
    addr = sanitize_address_payload(addr)
    if addr.get("address"):
        return addr

    return empty_addr()


def extract_pdf_addr(pdf_bytes: bytes) -> dict:
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            total_pages = len(pdf.pages)
    except Exception:
        total_pages = 0

    if total_pages <= 0:
        return empty_addr()

    page_order = build_adaptive_page_order(total_pages)
    log.info("Adaptive PDF page order: %s", page_order)

    for p in page_order:
        txt = read_single_pdf_page(pdf_bytes, p)
        if txt:
            addr = parse_address_from_pdf_text(txt)
            if addr.get("address"):
                log.info("Address found in PDF text on page %s (%s)", p, addr.get("source"))
                return addr

    for p in page_order:
        txt = ocr_single_pdf_page(pdf_bytes, p)
        if txt:
            addr = parse_address_from_pdf_text(txt)
            if addr.get("address"):
                log.info("Address found in PDF OCR on page %s (%s)", p, addr.get("source"))
                return addr

    return empty_addr()


# =========================
# PROPERTY APPRAISER FALLBACK
# =========================
def parse_address_from_property_appraiser_page(text: str) -> dict:
    if not text:
        return empty_addr()

    t = text.replace("\r", "\n")
    lines = [norm(x) for x in t.splitlines() if norm(x)]

    address = None
    municipality = None
    zip_code = None

    for i, line in enumerate(lines):
        upper = line.upper()

        if upper == "LOCATION ADDRESS" and i + 1 < len(lines):
            cand = normalize_property_address(lines[i + 1])
            if is_valid_property_address(cand):
                address = cand

        if upper == "MUNICIPALITY" and i + 1 < len(lines):
            municipality = norm(lines[i + 1]).title()

        if upper == "ZIP" and i + 1 < len(lines):
            m = re.search(r"(\d{5})", lines[i + 1])
            if m:
                zip_code = m.group(1)

    if address:
        return {
            "address": address,
            "city": municipality,
            "state": "FL",
            "zip": zip_code,
            "source": "PROPERTY_APPRAISER_LOCATION_ADDRESS",
        }

    return empty_addr()


def fetch_address_from_property_appraiser_url(browser, url: str) -> dict:
    page = browser.new_page()
    try:
        log.info("Final fallback opening Property Appraiser URL: %s", url)
        page.goto(url, wait_until="domcontentloaded", timeout=30000)

        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except PWTimeout:
            pass

        try:
            page.locator("text=LOCATION ADDRESS").first.wait_for(timeout=10000)
        except PWTimeout:
            log.info("LOCATION ADDRESS not found within 10s; parsing body anyway")

        page.wait_for_timeout(1200)

        body_text = page.locator("body").inner_text(timeout=10000)
        addr = parse_address_from_property_appraiser_page(body_text)

        if not addr.get("address"):
            log.info("Property Appraiser snippet: %s", body_text[:1200].replace("\n", " "))

        return addr

    except Exception as e:
        log.warning("Property Appraiser fallback failed: %s", str(e))
        return empty_addr()
    finally:
        try:
            page.close()
        except Exception:
            pass


# =========================
# SEARCH FLOW
# =========================
def find_from_to_inputs(page):
    from_loc = page.locator("#dateFromStatus, [name='dateFromStatus']")
    to_loc = page.locator("#dateToStatus, [name='dateToStatus']")

    from_items = visible_elements(from_loc)
    to_items = visible_elements(to_loc)

    if from_items and to_items:
        return from_items[0], to_items[0]

    text_inputs = visible_elements(page.locator("input[type='text']"))
    if len(text_inputs) >= 2:
        return text_inputs[0], text_inputs[1]

    return None, None


def click_search_for_status_resilient(page):
    selectors = [
        "button[name='buttonSubmitStatus']",
        "button:has-text('Search for Status')",
        "input[type='submit'][value='Search for Status']",
    ]

    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() == 0:
                continue

            target = loc.first
            try:
                target.scroll_into_view_if_needed(timeout=5000)
            except Exception:
                pass

            for _ in range(3):
                try:
                    target.click(timeout=10000)
                    return True
                except Exception:
                    pass
                try:
                    target.click(force=True, timeout=10000)
                    return True
                except Exception:
                    pass
                try:
                    target.evaluate("(el) => el.click()")
                    return True
                except Exception:
                    pass
        except Exception:
            continue

    return False


def do_status_search_like_human(page):
    from_date, to_date = build_search_dates()
    log.info("Palm Beach search window: %s -> %s", from_date, to_date)

    page.goto(STATUS_URL, wait_until="domcontentloaded", timeout=30000)
    wait_network_quiet(page, 10000)
    page.wait_for_timeout(1800)

    from_input, to_input = find_from_to_inputs(page)
    if from_input is None or to_input is None:
        raise RuntimeError("Could not locate dateFromStatus/dateToStatus inputs")

    human_fill(page, from_input, from_date)
    human_fill(page, to_input, to_date)

    clicked = click_search_for_status_resilient(page)
    if not clicked:
        raise RuntimeError("Could not click Search for Status button")

    wait_network_quiet(page, 15000)
    page.wait_for_timeout(2200)


def parse_summary_from_row_text(row_text: str) -> dict:
    txt = norm(row_text)

    def pick(pattern):
        m = re.search(pattern, txt, re.I)
        return norm(m.group(1)) if m else ""

    case_number = pick(r"(?:Case Number|Case)\s*[:#]?\s*([A-Z0-9\-\/]+)")
    parcel_number = pick(r"(?:Parcel ID|Parcel|PCN)\s*[:#]?\s*([A-Z0-9\-]+)")
    sale_date = pick(r"(?:Auction Date|Sale Date|Date)\s*[:#]?\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})")
    status = pick(r"Status\s*[:#]?\s*([A-Z ]+)")
    opening_bid = pick(r"(?:Opening Bid|Min Bid|Minimum Bid)\s*[:#]?\s*\$?\s*([0-9,]+\.\d{2}|[0-9,]+)")

    return {
        "tax_sale_id": case_number or None,
        "parcel_number": parcel_number or None,
        "sale_date": normalize_sale_date_value(sale_date),
        "deed_status": status or None,
        "opening_bid": clean_bid(opening_bid),
    }


def extract_case_rows_from_current_results(page) -> List[dict]:
    rows_out = []
    seen = set()

    rows = page.locator("tr[role='row'][id]")
    count = rows.count()

    for i in range(count):
        try:
            row = rows.nth(i)
            row_id = norm(row.get_attribute("id") or "")
            if not row_id:
                continue
            if row_id.lower() == "jqgfirstrow":
                continue
            if not row_id.isdigit():
                continue

            case_url = f"{BASE_URL}/Home/Details?id={row_id}"
            row_text = norm(row.inner_text() or "")
            parsed = parse_summary_from_row_text(row_text)

            key = (row_id, case_url)
            if key in seen:
                continue
            seen.add(key)

            rows_out.append({
                "row_id": row_id,
                "case_url": case_url,
                "row_text": row_text,
                "summary": parsed,
            })
        except Exception:
            continue

    return rows_out


def goto_next_results_page(page) -> bool:
    candidates = [
        "td#next_pager a",
        "td[id*='next'] a",
        "a[title='Next Page']",
        "a[aria-label='Next Page']",
        "span.ui-icon-seek-next",
        "a:has-text('Next')",
    ]

    for sel in candidates:
        try:
            loc = page.locator(sel)
            for i in range(loc.count()):
                item = loc.nth(i)
                try:
                    cls = (item.get_attribute("class") or "").lower()
                    aria = (item.get_attribute("aria-disabled") or "").lower()
                    if "disabled" in cls or aria == "true":
                        continue

                    if item.is_visible():
                        old_first = ""
                        try:
                            old_first = norm(page.locator("tr[role='row'][id]").nth(1).inner_text() or "")
                        except Exception:
                            pass

                        try:
                            item.scroll_into_view_if_needed(timeout=3000)
                        except Exception:
                            pass

                        try:
                            item.click(timeout=5000)
                        except Exception:
                            try:
                                item.click(force=True, timeout=5000)
                            except Exception:
                                continue

                        wait_network_quiet(page, 12000)
                        page.wait_for_timeout(1500)

                        new_first = ""
                        try:
                            new_first = norm(page.locator("tr[role='row'][id]").nth(1).inner_text() or "")
                        except Exception:
                            pass

                        if old_first and new_first and old_first != new_first:
                            return True

                        return True
                except Exception:
                    continue
        except Exception:
            continue

    return False


def discover_sale_rows(page) -> Tuple[List[dict], int]:
    do_status_search_like_human(page)

    try:
        page.locator("tr[role='row'][id]").first.wait_for(timeout=10000)
    except Exception:
        pass

    page.wait_for_timeout(2000)

    all_rows = []
    seen_row_ids = set()
    pages_processed = 0

    for _ in range(50):
        page_rows = extract_case_rows_from_current_results(page)
        if not page_rows:
            break

        pages_processed += 1

        for row in page_rows:
            row_id = row["row_id"]
            if row_id not in seen_row_ids:
                seen_row_ids.add(row_id)
                all_rows.append(row)

        if PALM_BEACH_MAX_CASES > 0 and len(all_rows) >= PALM_BEACH_MAX_CASES:
            return all_rows[:PALM_BEACH_MAX_CASES], pages_processed

        moved = goto_next_results_page(page)
        if not moved:
            break

    return all_rows[:PALM_BEACH_MAX_CASES] if PALM_BEACH_MAX_CASES > 0 else all_rows, pages_processed


# =========================
# CASE DETAIL HTML
# =========================
def is_valid_case(html: str) -> bool:
    t = html.lower()
    return (
        "case number" in t and
        "parcel id" in t and
        "auction date" in t
    )


def parse_case(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")

    def pick(label):
        m = re.search(rf"{re.escape(label)}\s*\n\s*(.+)", text, re.I)
        return norm(m.group(1)) if m else None

    tax_url = None
    pdf_url = None
    property_appraiser_url = None

    for a in soup.find_all("a", href=True):
        label = norm(a.get_text()).lower()
        href = urljoin(url, a["href"])

        if "tax collector" in label:
            tax_url = href

        if "tax certificate" in label:
            pdf_url = href

        if "property appraiser" in label:
            property_appraiser_url = href

    case_number = pick("Case Number")
    parcel_number = pick("Parcel ID")
    sale_date = pick("Auction Date")
    status = pick("Status")
    opening_bid = pick("Opening Bid")
    applicant = pick("Applicant Names")

    return {
        "node": case_number,
        "case": case_number,
        "parcel": parcel_number,
        "date": sale_date,
        "status": status,
        "bid": opening_bid,
        "applicant": applicant,
        "tax": tax_url,
        "pdf": pdf_url,
        "property_appraiser": property_appraiser_url,
    }


# =========================
# LIST PRECHECK DECISION
# =========================
def existing_record_needs_enrichment(existing: dict) -> bool:
    important_fields = [
        "pdf_url",
        "auction_source_url",
        "address",
        "city",
        "state_address",
        "zip",
        "opening_bid",
        "deed_status",
        "applicant_name",
    ]
    return any(not norm(existing.get(f) or "") for f in important_fields)


def decide_list_action(row: dict, indexes: dict) -> dict:
    summary = row.get("summary") or {}

    tax_sale_id = norm(summary.get("tax_sale_id") or "")
    parcel_number = norm(summary.get("parcel_number") or "")
    site_sale_date = normalize_sale_date_value(summary.get("sale_date"))

    existing = None

    if tax_sale_id and parcel_number:
        existing = indexes["by_tax_sale_parcel"].get((tax_sale_id, parcel_number))

    if not existing and tax_sale_id:
        existing = indexes["by_node"].get(tax_sale_id)

    if not existing:
        return {
            "action": "open_detail",
            "reason": "record not found in supabase",
            "existing": None,
            "tax_sale_id": tax_sale_id,
            "parcel_number": parcel_number,
            "site_sale_date": site_sale_date,
        }

    db_sale_date = normalize_sale_date_value(existing.get("sale_date"))

    if existing_record_needs_enrichment(existing):
        return {
            "action": "open_detail",
            "reason": "record exists but needs enrichment",
            "existing": existing,
            "tax_sale_id": tax_sale_id,
            "parcel_number": parcel_number,
            "site_sale_date": site_sale_date,
        }

    if db_sale_date == site_sale_date:
        return {
            "action": "skip",
            "reason": "sale_date unchanged",
            "existing": existing,
            "tax_sale_id": tax_sale_id,
            "parcel_number": parcel_number,
            "site_sale_date": site_sale_date,
        }

    return {
        "action": "update_sale_date_only",
        "reason": f"sale_date changed from {db_sale_date} to {site_sale_date}",
        "existing": existing,
        "tax_sale_id": tax_sale_id,
        "parcel_number": parcel_number,
        "site_sale_date": site_sale_date,
    }


# =========================
# PAYLOAD BUILD
# =========================
def build_payload_from_case(case: dict, addr: dict) -> dict:
    return {
        "county": "PalmBeach",
        "state": "FL",
        "node": case.get("case"),
        "auction_source_url": case.get("property_appraiser") or case.get("tax"),
        "tax_sale_id": case.get("case"),
        "parcel_number": case.get("parcel"),
        "sale_date": normalize_sale_date_value(case.get("date")),
        "opening_bid": clean_bid(case.get("bid")),
        "deed_status": case.get("status"),
        "applicant_name": case.get("applicant"),
        "pdf_url": case.get("pdf"),
        "address": addr.get("address"),
        "city": addr.get("city"),
        "state_address": addr.get("state"),
        "zip": addr.get("zip"),
        "address_source_marker": addr.get("source"),
        "status": "new",
        "notes": None,
    }


# =========================
# MAIN
# =========================
def run_palm_beach():
    log.info("=== Palm Beach FINAL V2 aligned with Miami/Orange logic ===")

    supabase_rows = supabase_fetch_all_palm_beach_records() if CAN_CHECK_SUPABASE else []
    indexes = build_supabase_indexes(supabase_rows)

    seen_nodes_this_run = set()
    results = []
    failures = []
    supabase_results = []

    fast_skips = 0
    sale_date_only_updates = 0
    detail_opens = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        try:
            discovered_rows, pages_processed = discover_sale_rows(page)
            log.info(
                "Discovered %s case rows from search across %s page(s)",
                len(discovered_rows),
                pages_processed,
            )
        finally:
            try:
                page.close()
            except Exception:
                pass

        if not discovered_rows:
            log.warning("No case rows discovered from search")
            try:
                browser.close()
            except Exception:
                pass
            return

        s = requests.Session()
        s.headers.update(HEADERS)

        processed_rows = 0
        resolved_nodes = set()

        for idx, row in enumerate(discovered_rows, start=1):
            row_id = norm(row.get("row_id") or "")
            case_url = row.get("case_url")
            summary = row.get("summary") or {}

            processed_rows += 1

            log.info(
                "Palm Beach row %s/%s → row_id=%s case_url=%s",
                idx,
                len(discovered_rows),
                row_id,
                case_url,
            )

            try:
                decision = decide_list_action(row, indexes)
                log.info(
                    "PRECHECK row_id=%s action=%s reason=%s",
                    row_id,
                    decision["action"],
                    decision["reason"],
                )

                tax_sale_id = norm(decision.get("tax_sale_id") or "")
                if tax_sale_id:
                    seen_nodes_this_run.add(tax_sale_id)

                if decision["action"] == "skip":
                    fast_skips += 1
                    if tax_sale_id:
                        resolved_nodes.add(tax_sale_id)
                    continue

                if decision["action"] == "update_sale_date_only":
                    existing = decision["existing"]
                    upd = supabase_update_sale_date(existing["id"], decision["site_sale_date"])
                    sale_date_only_updates += 1
                    supabase_results.append({
                        "node": existing.get("node"),
                        "mode": "update_sale_date_only",
                        **upd,
                    })

                    if upd.get("sent"):
                        existing["sale_date"] = decision["site_sale_date"]

                    if existing.get("node"):
                        resolved_nodes.add(norm(existing.get("node")))
                    continue

                detail_opens += 1

                r = s.get(case_url, timeout=30)
                if r.status_code != 200 or not is_valid_case(r.text):
                    raise RuntimeError(f"Invalid case detail page status={r.status_code}")

                case = parse_case(r.text, r.url)
                status_value = (case.get("status") or "").strip().upper()

                if status_value != "SALE":
                    log.info(
                        "SKIPPED non-SALE case after detail read → %s (%s)",
                        case.get("status"),
                        case.get("case"),
                    )
                    continue

                final_node = norm(case.get("case") or "")
                if not final_node:
                    raise RuntimeError("Missing final case number/node in detail page")

                seen_nodes_this_run.add(final_node)

                addr = empty_addr()

                if case.get("pdf"):
                    try:
                        pdf = s.get(case["pdf"], timeout=60)
                        if "pdf" in (pdf.headers.get("content-type") or "").lower():
                            addr = extract_pdf_addr(pdf.content)
                        else:
                            log.warning(
                                "Non-PDF response for case=%s: %s",
                                case.get("case"),
                                pdf.headers.get("content-type"),
                            )
                    except Exception as e:
                        log.warning("PDF read failed for case=%s: %s", case.get("case"), str(e))

                addr = sanitize_address_payload(addr)

                if not addr.get("address") and case.get("property_appraiser"):
                    log.info("Address missing after PDF → trying Property Appraiser fallback")
                    pa_addr = fetch_address_from_property_appraiser_url(browser, case["property_appraiser"])
                    pa_addr = sanitize_address_payload(pa_addr)
                    if pa_addr.get("address"):
                        addr = pa_addr
                        log.info("Address found from Property Appraiser fallback")

                payload = build_payload_from_case(case, addr)
                results.append(payload)

                existing = None
                tax_sale_id = norm(payload.get("tax_sale_id") or "")
                parcel_number = norm(payload.get("parcel_number") or "")

                if tax_sale_id and parcel_number:
                    existing = indexes["by_tax_sale_parcel"].get((tax_sale_id, parcel_number))
                if not existing and final_node:
                    existing = indexes["by_node"].get(final_node)

                if existing and not payload_is_better_than_existing(payload, existing):
                    log.info("DETAIL READ but payload not better → skip upsert node=%s", final_node)
                    resolved_nodes.add(final_node)
                    continue

                sb_result = supabase_insert_or_update_property(payload)
                supabase_results.append({
                    "node": final_node,
                    "mode": "full_upsert",
                    **sb_result,
                })

                if sb_result.get("sent"):
                    if existing and existing.get("id"):
                        payload["id"] = existing.get("id")

                    indexes["by_node"][final_node] = payload
                    if tax_sale_id and parcel_number:
                        indexes["by_tax_sale_parcel"][(tax_sale_id, parcel_number)] = payload

                    send(payload)

                resolved_nodes.add(final_node)
                time.sleep(0.8)

            except Exception as e:
                log.error("ERROR row_id=%s url=%s: %s", row_id, case_url, str(e))
                failures.append({
                    "row_id": row_id,
                    "case_url": case_url,
                    "error": str(e),
                })

        expected_total_items = len(discovered_rows)
        completed_all_pages = processed_rows == expected_total_items

        can_delete_missing = (
            SAFE_DELETE_ENABLED
            and completed_all_pages
            and expected_total_items > 0
            and len(failures) == 0
            and len(seen_nodes_this_run) > 0
        )

        if can_delete_missing:
            reconcile_result = reconcile_supabase_to_site(seen_nodes_this_run, indexes)
        else:
            reconcile_result = {
                "executed": False,
                "reason": (
                    f"safe delete blocked: "
                    f"SAFE_DELETE_ENABLED={SAFE_DELETE_ENABLED}, "
                    f"completed_all_pages={completed_all_pages}, "
                    f"expected_total_items={expected_total_items}, "
                    f"seen_nodes={len(seen_nodes_this_run)}, "
                    f"failures_count={len(failures)}"
                ),
            }

        final_payload = {
            "source": "PalmBeach",
            "mode": "final_v2_aligned_with_miami_orange",
            "expected_total_items": expected_total_items,
            "processed_rows": processed_rows,
            "seen_nodes_count": len(seen_nodes_this_run),
            "resolved_nodes_count": len(resolved_nodes),
            "fast_skips": fast_skips,
            "sale_date_only_updates": sale_date_only_updates,
            "detail_opens": detail_opens,
            "records_count": len(results),
            "failures_count": len(failures),
            "can_delete_missing": can_delete_missing,
            "supabase_results_count": len(supabase_results),
            "reconcile_result": reconcile_result,
            "records": results,
            "failures": failures,
        }

        log.info("===== FINAL PAYLOAD =====")
        log.info(json.dumps(final_payload, indent=2))
        print(json.dumps(final_payload, indent=2))

        try:
            browser.close()
        except Exception:
            pass


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    run_palm_beach()