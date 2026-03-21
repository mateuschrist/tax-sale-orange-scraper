import os
import re
import json
import time
import random
import logging
from io import BytesIO
from datetime import date, timedelta
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
STATE_FILE = os.getenv("PALM_BEACH_STATE_FILE", "state_palm_beach.json")

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
PALM_BEACH_MAX_CASES = int(os.getenv("PALM_BEACH_MAX_CASES", "500"))
PALM_BEACH_FROM_DATE = (os.getenv("PALM_BEACH_FROM_DATE", "") or "").strip()
PALM_BEACH_TO_DATE = (os.getenv("PALM_BEACH_TO_DATE", "") or "").strip()


# =========================
# STATE
# =========================
def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(data):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)


def get_seen_cases():
    st = load_state()
    seen = st.get("seen_case_numbers", [])
    if isinstance(seen, list):
        return set(str(x) for x in seen if x)
    return set()


def add_seen_case(case_number: str):
    if not case_number:
        return
    st = load_state()
    seen = set(st.get("seen_case_numbers", []))
    seen.add(str(case_number))
    st["seen_case_numbers"] = sorted(seen)
    st["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save_state(st)


# =========================
# HELPERS
# =========================
def norm(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def clean_bid(v):
    return re.sub(r"[^\d.]", "", str(v or ""))


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

    if len(a) < 4 or len(a) > 80:
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

    if upper.count(" ") > 12:
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

    if re.match(r"^[0-9A-Z .'\-#/]+$", a, re.I) and len(a.split()) <= 6:
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
        "address",
        "city",
        "state_address",
        "zip",
        "pdf_url",
        "auction_source_url",
        "parcel_number",
        "sale_date",
        "opening_bid",
        "deed_status",
        "applicant_name",
    ]
    score = 0
    for f in fields:
        v = payload.get(f)
        if v is not None and str(v).strip() != "":
            score += 1
    return score


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
        log.info("INGEST status=%s", r.status_code)
        if r.text:
            log.info("INGEST response=%s", r.text[:250].replace("\n", " "))
        return r.status_code in (200, 201)
    except Exception as e:
        log.warning("INGEST failed: %s", str(e))
        return False


def build_search_dates():
    if PALM_BEACH_FROM_DATE and PALM_BEACH_TO_DATE:
        return PALM_BEACH_FROM_DATE, PALM_BEACH_TO_DATE

    today = date.today()
    future = today + timedelta(days=365)

    if os.name == "nt":
        return today.strftime("%#m/%#d/%Y"), future.strftime("%#m/%#d/%Y")
    return today.strftime("%-m/%-d/%Y"), future.strftime("%-m/%-d/%Y")


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


def human_click(locator):
    try:
        locator.scroll_into_view_if_needed(timeout=5000)
    except Exception:
        pass

    human_pause(0.08, 0.18)

    try:
        locator.hover(timeout=5000)
        human_pause(0.05, 0.12)
    except Exception:
        pass

    locator.click(timeout=10000)
    human_pause(0.18, 0.35)


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


# =========================
# SUPABASE DEDUP
# =========================
def sb_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }


def supabase_find_existing_case(node: str):
    if not CAN_CHECK_SUPABASE or not node:
        return None

    url = (
        f"{SUPABASE_URL}/rest/v1/properties"
        f"?county=eq.PalmBeach"
        f"&node=eq.{quote(str(node), safe='')}"
        f"&select=id,node,parcel_number,sale_date,address,city,state_address,zip,pdf_url,auction_source_url,opening_bid,deed_status,applicant_name"
        f"&limit=1"
    )

    try:
        r = requests.get(url, headers=sb_headers(), timeout=30)
        if r.status_code == 200:
            arr = r.json()
            return arr[0] if arr else None
    except Exception as e:
        log.warning("supabase_find_existing_case failed: %s", str(e))

    return None


def supabase_find_existing_property(parcel_number: str, sale_date: str):
    if not CAN_CHECK_SUPABASE or not parcel_number or not sale_date:
        return None

    url = (
        f"{SUPABASE_URL}/rest/v1/properties"
        f"?county=eq.PalmBeach"
        f"&parcel_number=eq.{quote(str(parcel_number), safe='')}"
        f"&sale_date=eq.{quote(str(sale_date), safe='')}"
        f"&select=id,node,parcel_number,sale_date,address,city,state_address,zip,pdf_url,auction_source_url,opening_bid,deed_status,applicant_name"
        f"&limit=1"
    )

    try:
        r = requests.get(url, headers=sb_headers(), timeout=30)
        if r.status_code == 200:
            arr = r.json()
            return arr[0] if arr else None
    except Exception as e:
        log.warning("supabase_find_existing_property failed: %s", str(e))

    return None


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
    ]

    for f in important_fields:
        old_val = (existing.get(f) or "").strip() if existing.get(f) else ""
        new_val = (payload.get(f) or "").strip() if payload.get(f) else ""
        if not old_val and new_val:
            return True

    return False


def should_send_payload(payload: dict):
    node = payload.get("node")
    parcel = payload.get("parcel_number")
    sale_date = payload.get("sale_date")

    existing_case = supabase_find_existing_case(node)
    if existing_case:
        if payload_is_better_than_existing(payload, existing_case):
            return True, "existing case found, but new payload is better"
        return False, "duplicate case/node already exists"

    existing_prop = supabase_find_existing_property(parcel, sale_date)
    if existing_prop:
        if payload_is_better_than_existing(payload, existing_prop):
            return True, "existing parcel_number + sale_date found, but new payload is better"
        return False, "duplicate parcel_number + sale_date already exists"

    return True, "new record"


# =========================
# PDF EXTRACTION
# =========================
def build_adaptive_page_order(total_pages: int) -> list[int]:
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
            window = lines[i:i+12]

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
            log.info("LOCATION ADDRESS not explicitly found within 10s; parsing full body anyway")

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
    # 1) exact selector
    try:
        btn = page.locator("button[name='buttonSubmitStatus']")
        if btn.count() > 0:
            target = btn.first
            try:
                target.scroll_into_view_if_needed(timeout=5000)
            except Exception:
                pass

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
        pass

    # 2) fallback by text
    try:
        btn = page.locator("button:has-text('Search for Status')")
        if btn.count() > 0:
            target = btn.first
            try:
                target.scroll_into_view_if_needed(timeout=5000)
            except Exception:
                pass

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
        pass

    # 3) deep scan
    try:
        buttons = page.locator("button")
        count = buttons.count()
        for i in range(count):
            btn = buttons.nth(i)
            try:
                name = (btn.get_attribute("name") or "").strip()
                btype = (btn.get_attribute("type") or "").strip()
                text = (btn.inner_text() or "").strip()

                if (
                    name == "buttonSubmitStatus"
                    or (btype == "submit" and text == "Search for Status")
                    or text == "Search for Status"
                ):
                    try:
                        btn.scroll_into_view_if_needed(timeout=5000)
                    except Exception:
                        pass

                    try:
                        btn.click(timeout=10000)
                        return True
                    except Exception:
                        pass

                    try:
                        btn.click(force=True, timeout=10000)
                        return True
                    except Exception:
                        pass

                    try:
                        btn.evaluate("(el) => el.click()")
                        return True
                    except Exception:
                        pass
            except Exception:
                continue
    except Exception:
        pass

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


def extract_case_links_from_current_results(page) -> list[str]:
    hrefs = []
    anchors = page.locator("a[href*='/Home/Details?id=']")
    count = anchors.count()

    for i in range(count):
        try:
            href = anchors.nth(i).get_attribute("href")
            if href:
                hrefs.append(urljoin(BASE_URL, href))
        except Exception:
            continue

    out = []
    seen = set()
    for h in hrefs:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out


def goto_next_results_page(page) -> bool:
    candidates = [
        "a[title='Next Page']",
        "a[aria-label='Next Page']",
        "a:has-text('Next')",
        "span.ui-icon-seek-next",
    ]

    for sel in candidates:
        try:
            loc = page.locator(sel)
            items = visible_elements(loc)
            if items:
                for item in items:
                    try:
                        cls = (item.get_attribute("class") or "").lower()
                        aria = (item.get_attribute("aria-disabled") or "").lower()
                        if "disabled" in cls or aria == "true":
                            continue
                        human_click(item)
                        wait_network_quiet(page, 12000)
                        page.wait_for_timeout(1500)
                        return True
                    except Exception:
                        continue
        except Exception:
            continue

    return False


def discover_sale_case_links(page) -> list[str]:
    do_status_search_like_human(page)

    all_links = []
    seen = set()

    for _ in range(50):
        page_links = extract_case_links_from_current_results(page)

        for h in page_links:
            if h not in seen:
                seen.add(h)
                all_links.append(h)

        if PALM_BEACH_MAX_CASES > 0 and len(all_links) >= PALM_BEACH_MAX_CASES:
            return all_links[:PALM_BEACH_MAX_CASES]

        moved = goto_next_results_page(page)
        if not moved:
            break

    return all_links[:PALM_BEACH_MAX_CASES] if PALM_BEACH_MAX_CASES > 0 else all_links


# =========================
# CASE HTML
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

    return {
        "case": pick("Case Number"),
        "parcel": pick("Parcel ID"),
        "date": pick("Auction Date"),
        "status": pick("Status"),
        "bid": pick("Opening Bid"),
        "applicant": pick("Applicant Names"),
        "tax": tax_url,
        "pdf": pdf_url,
        "property_appraiser": property_appraiser_url,
    }


# =========================
# MAIN
# =========================
def run_palm_beach():
    log.info("=== Palm Beach V6.6 direct-status-dates + resilient search click ===")

    seen_cases = get_seen_cases()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        try:
            case_links = discover_sale_case_links(page)
            log.info("Discovered %s SALE case links from search", len(case_links))
        finally:
            try:
                page.close()
            except Exception:
                pass

        if not case_links:
            log.warning("No SALE case links discovered from search")
            try:
                browser.close()
            except Exception:
                pass
            return

        s = requests.Session()
        s.headers.update(HEADERS)

        for idx, case_url in enumerate(case_links, start=1):
            log.info("Palm Beach case %s/%s → %s", idx, len(case_links), case_url)

            try:
                r = s.get(case_url, timeout=30)

                if r.status_code != 200 or not is_valid_case(r.text):
                    log.warning("Invalid case page at %s", case_url)
                    continue

                case = parse_case(r.text, r.url)

                status_value = (case.get("status") or "").strip().upper()
                if status_value != "SALE":
                    log.info(
                        "SKIPPED case because status is not SALE → %s (%s)",
                        case.get("status"),
                        case.get("case"),
                    )
                    continue

                if case.get("case") in seen_cases:
                    log.info("SKIPPED case already seen in current state → %s", case.get("case"))
                    continue

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
                                pdf.headers.get("content-type")
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

                payload = {
                    "county": "PalmBeach",
                    "state": "FL",
                    "node": case.get("case"),
                    "auction_source_url": case.get("property_appraiser") or case.get("tax"),
                    "tax_sale_id": case.get("case"),
                    "parcel_number": case.get("parcel"),
                    "sale_date": case.get("date"),
                    "opening_bid": clean_bid(case.get("bid")),
                    "deed_status": case.get("status"),
                    "applicant_name": case.get("applicant"),
                    "pdf_url": case.get("pdf"),
                    "address": addr.get("address"),
                    "city": addr.get("city"),
                    "state_address": addr.get("state"),
                    "zip": addr.get("zip"),
                    "address_source": addr.get("source"),
                }

                print(json.dumps(payload, indent=2))

                should_send, reason = should_send_payload(payload)
                log.info("DEDUP CHECK case=%s → %s", case.get("case"), reason)

                if should_send:
                    if send(payload):
                        add_seen_case(case.get("case"))
                        seen_cases.add(case.get("case"))
                        log.info("SENT case=%s", case.get("case"))
                    else:
                        log.warning("SEND FAILED case=%s", case.get("case"))
                else:
                    add_seen_case(case.get("case"))
                    seen_cases.add(case.get("case"))
                    log.info("SKIPPED case=%s because duplicate", case.get("case"))

                time.sleep(1.0)

            except Exception as e:
                log.error("ERROR case url=%s: %s", case_url, str(e))

        try:
            browser.close()
        except Exception:
            pass
