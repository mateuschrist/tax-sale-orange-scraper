import os
import re
import json
import time
import logging
from io import BytesIO
from urllib.parse import urljoin, urlparse, parse_qs

import pdfplumber
import pytesseract
import pypdfium2
import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# CONFIG
# =========================
BASE_URL = "https://or.occompt.com"
LOGIN_URL = f"{BASE_URL}/recorder/web/login.jsp"
SEARCH_URL = f"{BASE_URL}/recorder/tdsmweb/applicationSearch.jsp"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
MAX_LOTS = int(os.getenv("MAX_LOTS", "100"))  # pode aumentar
DEBUG_HTML = os.getenv("DEBUG_HTML", "false").lower() == "true"

# App/API (Vercel) ingest
APP_API_BASE = (os.getenv("APP_API_BASE", "") or "").rstrip("/")
APP_API_TOKEN = (os.getenv("APP_API_TOKEN", "") or "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

# Supabase state (memory)
SUPABASE_URL = (os.getenv("SUPABASE_URL", "") or "").strip()
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or "").strip()
STATE_KEY = (os.getenv("STATE_KEY", "orange_taxdeed") or "orange_taxdeed").strip()
USE_STATE = bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)

# Behavior
START_AFTER_LAST_NODE = os.getenv("START_AFTER_LAST_NODE", "true").lower() == "true"
OCR_MAX_PAGES = int(os.getenv("OCR_MAX_PAGES", "3"))
OCR_SCALE = float(os.getenv("OCR_SCALE", "2.2"))

MAX_WAIT = 60_000

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("taxdeed-orange-scraper")


# =========================
# PLAYWRIGHT HELPERS
# =========================
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


def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def must_be_pdf(headers: dict) -> bool:
    ct = (headers.get("content-type") or "").lower()
    return "application/pdf" in ct or ct.endswith("/pdf")


# =========================
# SUPABASE STATE (memory)
# =========================
def _sb_headers():
    if not USE_STATE:
        raise RuntimeError("Supabase state disabled (missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY).")
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation,resolution=merge-duplicates",
    }


def get_state_last_node() -> str | None:
    """
    Reads last_node from scraper_state table.
    Table schema:
      id text primary key,
      last_node text,
      updated_at timestamptz default now()
    """
    if not USE_STATE:
        return None

    url = f"{SUPABASE_URL}/rest/v1/scraper_state?id=eq.{STATE_KEY}&select=last_node"
    r = requests.get(url, headers=_sb_headers(), timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"GET scraper_state failed: {r.status_code} {r.text[:200]}")
    arr = r.json()
    if not arr:
        return None
    return arr[0].get("last_node")


def set_state_last_node(last_node: str | None) -> None:
    """Upserts last_node in scraper_state."""
    if not USE_STATE:
        return

    # POST upsert into scraper_state
    url = f"{SUPABASE_URL}/rest/v1/scraper_state"
    payload = {"id": STATE_KEY, "last_node": last_node}
    r = requests.post(url, headers=_sb_headers(), json=payload, timeout=30)

    # Supabase REST returns 201/200 for insert/upsert; some projects may return 204 for upsert
    if r.status_code in (200, 201, 204):
        return

    # fallback: PATCH by id
    url2 = f"{SUPABASE_URL}/rest/v1/scraper_state?id=eq.{STATE_KEY}"
    r2 = requests.patch(url2, headers=_sb_headers(), json={"last_node": last_node}, timeout=30)
    if r2.status_code not in (200, 204):
        raise RuntimeError(f"SET scraper_state failed: {r.status_code} {r.text[:200]} / PATCH {r2.status_code} {r2.text[:200]}")


# =========================
# APP INGEST (Vercel)
# =========================
def normalize_bid_for_payload(v):
    """Send as string without commas (backend normalizes too)."""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    cleaned = re.sub(r"[^0-9.]", "", s.replace(",", ""))
    return cleaned if cleaned else None
    SKIP_IF_ADDRESS_NOT_NUMBERED = os.getenv("SKIP_IF_ADDRESS_NOT_NUMBERED", "true").lower() == "true"

def is_numbered_street_address(addr: str | None) -> bool:
    if not addr:
        return False
    a = addr.strip()
    # Ex: 123 Main St, 12-34 (não comum), mas vamos ser diretos: só números no começo.
    return re.match(r"^\d{1,6}\s+\S", a) is not None


def post_to_app(payload: dict) -> dict | None:
    """POST payload to Vercel ingest endpoint. Retries lightly."""
    if not SEND_TO_APP:
        log.info("APP_API_BASE / APP_API_TOKEN not set → skipping send to app.")
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
                return r.json()

            if r.status_code == 401:
                log.error("INGEST unauthorized → check APP_API_TOKEN == Vercel INGEST_API_TOKEN.")
                return None

            last_err = f"HTTP {r.status_code}: {r.text[:500]}"
        except Exception as e:
            last_err = str(e)

        time.sleep(1.0 * attempt)

    log.error("INGEST failed after retries: %s", last_err)
    return None


# =========================
# TABLE PARSE (Printable row text)
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
        "tax_sale_id": tax_sale_id,
        "sale_date": sale_date,
        "deed_status": status,
        "parcel_number": parcel,
        "opening_bid": min_bid,
        "applicant_name": applicant,
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

        lots.append({
            "node": node,
            "tax_sale_url": full,
            "row_text": norm_ws(row_text),
        })

    return [l for l in lots if l.get("node")]


# =========================
# PDF TEXT + OCR (first N pages)
# =========================
def try_pdfplumber_text(pdf_bytes: bytes) -> str:
    """If PDF has embedded text, extract it."""
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
    """
    OCR first N pages using pypdfium2.page.render() (compatible).
    Some lots have the address on page 2 or 3.
    """
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
    """
    Priority markers:
    1) ADDRESS ON RECORD ON CURRENT TAX ROLL (physical property address)
    2) PHYSICAL ADDRESS
    3) TITLE HOLDER AND ADDRESS OF RECORD (owner mailing)
    Fallback: first City, ST ZIP found.
    """
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
        city = mcity.group(1).title().strip()
        state = mcity.group(2).upper()
        zipc = mcity.group(3)

        return {
            "address": street,
            "city": city,
            "state": state,
            "zip": zipc,
            "marker_used": marker_name,
            "marker_found": True,
            "snippet": after[:700]
        }

    # Fallback: first City, ST ZIP anywhere
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
        "snippet": text[max(0, mcity.start()-250):mcity.end()+250]
    }


# =========================
# MAIN
# =========================
def run():
    log.info("SEND_TO_APP=%s APP_API_BASE=%s", SEND_TO_APP, APP_API_BASE if APP_API_BASE else "(empty)")
    log.info("USE_STATE=%s STATE_KEY=%s", USE_STATE, STATE_KEY)

    last_node = None
    if USE_STATE:
        try:
            last_node = get_state_last_node()
            log.info("STATE last_node=%s", last_node)
        except Exception as e:
            log.warning("STATE read failed (continuing without state): %s", str(e))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        # 1) Login / Disclaimer
        log.info("OPEN: %s", LOGIN_URL)
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=MAX_WAIT)

        click_any(page, [
            "text=I Acknowledge",
            "button:has-text('I Acknowledge')",
            "a:has-text('I Acknowledge')",
            "input[value='I Acknowledge']",
        ], "I Acknowledge")
        wait_network(page)

        # 2) Search page
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
            log.error("Could not click Search.")
            browser.close()
            return

        page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
        wait_network(page, 30_000)
        log.info("After Search URL: %s", page.url)

        ok = click_any(page, [
            "text=Printable Version",
            "a:has-text('Printable Version')",
            "button:has-text('Printable Version')"
        ], "Printable Version")
        if not ok:
            log.error("Could not click Printable Version.")
            browser.close()
            return

        page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
        wait_network(page, 30_000)
        printable_url = page.url
        log.info("After Printable URL: %s", printable_url)

        if DEBUG_HTML:
            log.info("Printable HTML length: %d", len(page.content()))

        # 3) Capture lots once (avoid stale locators)
        lots = extract_lots_from_printable(page)
        if not lots:
            log.error("No lots found on printable page.")
            browser.close()
            return

        # If we have last_node and want to continue after it, slice list
        if START_AFTER_LAST_NODE and last_node:
            pos = next((i for i, l in enumerate(lots) if l["node"] == last_node), None)
            if pos is not None:
                lots = lots[pos + 1 :]
                log.info("Continuing AFTER last_node. Remaining lots=%d", len(lots))
            else:
                log.info("last_node not found in current list → processing from top.")

        selected = lots[:MAX_LOTS]
        log.info("Selected lots: %s", [l["node"] for l in selected])

        for idx, lot in enumerate(selected, start=1):
            node = lot["node"]
            row_text = lot["row_text"]
            tax_sale_url = lot["tax_sale_url"]
            fields = parse_fields_from_row_text(row_text)

            log.info("----- LOT %d/%d node=%s -----", idx, len(selected), node)
            log.info("Row text: %s", row_text[:220])

            try:
                # 4) Open viewer
                page.goto(tax_sale_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                wait_network(page)
                viewer_url = page.url
                log.info("Viewer URL: %s", viewer_url)

                # 5) Find PDF link inside viewer
                pdf_a = page.locator("a[href*='Property_Information.pdf']")
                href_pdf = pdf_a.first.get_attribute("href") if pdf_a.count() else None

                if not href_pdf:
                    viewer_html = page.content()
                    m = re.search(r'href="([^"]*Property_Information\.pdf[^"]*)"', viewer_html, re.I)
                    href_pdf = m.group(1) if m else None

                if not href_pdf:
                    log.error("PDF link not found in viewer for node=%s", node)
                    page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                    wait_network(page, 30_000)
                    continue

                pdf_url = urljoin(viewer_url, href_pdf)
                log.info("PDF URL: %s", pdf_url)

                # 6) Download PDF
                pdf_resp = context.request.get(pdf_url, timeout=MAX_WAIT)
                log.info("PDF HTTP status: %s", pdf_resp.status)
                log.info("PDF content-type: %s", pdf_resp.headers.get("content-type"))

                if not pdf_resp.ok:
                    preview = (pdf_resp.text() or "")[:600]
                    log.error("PDF download failed preview:\n%s", preview)
                    page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                    wait_network(page, 30_000)
                    continue

                if not must_be_pdf(pdf_resp.headers):
                    preview = (pdf_resp.text() or "")[:800]
                    log.error("Response is not PDF preview:\n%s", preview)
                    page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                    wait_network(page, 30_000)
                    continue

                pdf_bytes = pdf_resp.body()
                log.info("PDF bytes: %d", len(pdf_bytes))

                # 7) Extract address: text -> OCR (first N pages)
                text = try_pdfplumber_text(pdf_bytes)
                raw_text_for_debug = None

                if text:
                    log.info("pdfplumber text length: %d (using text parse)", len(text))
                    addr = parse_best_address_from_text(text)
                    raw_text_for_debug = text
                else:
                    log.info("pdfplumber returned empty. Running OCR on FIRST %d pages...", OCR_MAX_PAGES)
                    ocr_text = ocr_pdf_bytes(pdf_bytes, max_pages=OCR_MAX_PAGES, scale=OCR_SCALE)
                    log.info("OCR text length: %d", len(ocr_text))
                    addr = parse_best_address_from_text(ocr_text)
                    raw_text_for_debug = ocr_text

# ✅ Address filter: ignore "street-only" addresses (no house number)
if SKIP_IF_ADDRESS_NOT_NUMBERED:
    if not is_numbered_street_address(addr.get("address")):
        log.warning("Skipping node=%s because address is not numbered: %r", node, addr.get("address"))
        # volta pro printable e segue o próximo
        page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
        wait_network(page, 30_000)
        time.sleep(1.0)
        continue
        
                notes = None
                if not addr.get("marker_found"):
                    notes = f"Address marker not found via OCR first {OCR_MAX_PAGES} pages."

                payload = {
                    "county": "Orange",
                    "state": "FL",
                    "node": node,

                    "auction_source_url": viewer_url,

                    "tax_sale_id": fields.get("tax_sale_id") or None,
                    "parcel_number": fields.get("parcel_number") or None,
                    "sale_date": fields.get("sale_date") or None,
                    "opening_bid": normalize_bid_for_payload(fields.get("opening_bid")),
                    "deed_status": fields.get("deed_status") or None,
                    "applicant_name": fields.get("applicant_name") or None,

                    "pdf_url": pdf_url,
                    "address": addr.get("address"),
                    "city": addr.get("city"),
                    "state_address": addr.get("state"),
                    "zip": addr.get("zip"),
                    "address_source_marker": addr.get("marker_used"),

                    "status": "new",
                    "notes": notes,
                    # opcional futuro:
                    # "raw_ocr_text": raw_text_for_debug
                }

                print("\n" + "=" * 100)
                print(f"RESULT LOT {idx}")
                print("=" * 100)
                print(json.dumps(payload, indent=2))

                if not addr.get("marker_found"):
                    log.warning("Address marker not found. Snippet:\n%s", addr.get("snippet", "")[:900])

                # 8) Send to app
                ingest_ok = True
                ingest_result = None
                if SEND_TO_APP:
                    ingest_result = post_to_app(payload)
                    ingest_ok = bool(ingest_result)

                if ingest_result:
                    log.info("INGEST OK: %s", ingest_result)

                # 9) Update state only when ingest ok (or when SEND_TO_APP disabled)
                if ingest_ok:
                    try:
                        set_state_last_node(node)
                        log.info("STATE updated last_node=%s", node)
                    except Exception as e:
                        log.warning("STATE write failed: %s", str(e))

            except Exception as e:
                log.exception("LOT FAILED node=%s error=%s", node, str(e))

            # back to printable
            page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
            wait_network(page, 30_000)
            time.sleep(1.0)

        log.info("DONE.")
        browser.close()


if __name__ == "__main__":
    run()
