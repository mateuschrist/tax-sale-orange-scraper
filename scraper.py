import os
import re
import json
import time
import random
import logging
from io import BytesIO
from urllib.parse import urljoin, urlparse, parse_qs

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
MAX_LOTS = int(os.getenv("MAX_LOTS", "100"))
DEBUG_HTML = os.getenv("DEBUG_HTML", "false").lower() == "true"

# Restart browser every N lots (hard reset session)
RESTART_BROWSER_EVERY = int(os.getenv("RESTART_BROWSER_EVERY", "20"))

# Anti-bot
MAX_VIEWER_RETRIES = int(os.getenv("MAX_VIEWER_RETRIES", "3"))
MAX_WAIT = 60_000

# OCR
OCR_MAX_PAGES = int(os.getenv("OCR_MAX_PAGES", "3"))
OCR_SCALE = float(os.getenv("OCR_SCALE", "2.2"))

# Address filter (skip street-only)
SKIP_IF_ADDRESS_NOT_NUMBERED = os.getenv("SKIP_IF_ADDRESS_NOT_NUMBERED", "true").lower() == "true"

# State behavior
START_AFTER_LAST_NODE = os.getenv("START_AFTER_LAST_NODE", "true").lower() == "true"

# Supabase state via REST (optional)
SUPABASE_URL = (os.getenv("SUPABASE_URL", "") or "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or "").strip()
STATE_KEY = (os.getenv("STATE_KEY", "orange_taxdeed") or "orange_taxdeed").strip()
USE_STATE = bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)

# App/API ingest (Vercel) - IMPORTANT: base must be Vercel domain, no /api at end
APP_API_BASE = (os.getenv("APP_API_BASE", "") or "").strip().rstrip("/")
APP_API_TOKEN = (os.getenv("APP_API_TOKEN", "") or "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

# Optional: set tesseract executable explicitly if PATH issues
TESSERACT_CMD = (os.getenv("TESSERACT_CMD", "") or "").strip()
if TESSERACT_CMD:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

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


def is_check_human(url: str) -> bool:
    return "/recorder/web/checkHuman.jsp" in (url or "")


def human_backoff(idx: int, attempt: int):
    # pausa crescente + aleatória
    base = min(5 + idx * 0.15 + attempt * 2.0, 22)
    sleep_s = base + random.uniform(0.6, 3.0)
    log.warning("Backoff %.1fs (idx=%d attempt=%d)", sleep_s, idx, attempt)
    time.sleep(sleep_s)


# =========================
# ADDRESS FILTER
# =========================
def is_numbered_street_address(addr: str | None) -> bool:
    """
    True only if address begins with a house number, e.g.:
      "109 E Church St"
      "407 Dill Rd"
    False for street-only:
      "Dill Rd"
      "Forest City Rd"
    """
    if not addr:
        return False
    a = addr.strip()
    return re.match(r"^\d{1,6}\s+\S", a) is not None


# =========================
# SUPABASE STATE (optional)
# =========================
def _sb_headers():
    if not USE_STATE:
        raise RuntimeError("Supabase state disabled.")
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation,resolution=merge-duplicates",
    }


def _sb_get(url: str):
    return requests.get(url, headers=_sb_headers(), timeout=30)


def _sb_post(url: str, payload: dict):
    return requests.post(url, headers=_sb_headers(), json=payload, timeout=30)


def _sb_patch(url: str, payload: dict):
    return requests.patch(url, headers=_sb_headers(), json=payload, timeout=30)


def get_state_last_node() -> str | None:
    if not USE_STATE:
        return None

    # schema A: scraper_name
    url_a = f"{SUPABASE_URL}/rest/v1/scraper_state?scraper_name=eq.{STATE_KEY}&select=last_node"
    r = _sb_get(url_a)
    if r.status_code == 200:
        arr = r.json()
        if not arr:
            return None
        return arr[0].get("last_node")

    # schema B: id
    url_b = f"{SUPABASE_URL}/rest/v1/scraper_state?id=eq.{STATE_KEY}&select=last_node"
    r2 = _sb_get(url_b)
    if r2.status_code != 200:
        raise RuntimeError(f"GET scraper_state failed: {r2.status_code} {r2.text[:200]}")
    arr = r2.json()
    if not arr:
        return None
    return arr[0].get("last_node")


def set_state_last_node(last_node: str | None) -> None:
    if not USE_STATE:
        return

    url = f"{SUPABASE_URL}/rest/v1/scraper_state"
    payload_a = {"scraper_name": STATE_KEY, "last_node": last_node}

    r = _sb_post(url, payload_a)
    if r.status_code in (200, 201, 204):
        return

    # fallback patch schema A
    url_pa = f"{SUPABASE_URL}/rest/v1/scraper_state?scraper_name=eq.{STATE_KEY}"
    rpa = _sb_patch(url_pa, {"last_node": last_node})
    if rpa.status_code in (200, 204):
        return

    # schema B
    payload_b = {"id": STATE_KEY, "last_node": last_node}
    r2 = _sb_post(url, payload_b)
    if r2.status_code in (200, 201, 204):
        return

    url_pb = f"{SUPABASE_URL}/rest/v1/scraper_state?id=eq.{STATE_KEY}"
    rpb = _sb_patch(url_pb, {"last_node": last_node})
    if rpb.status_code not in (200, 204):
        raise RuntimeError(
            f"SET scraper_state failed: POST {r.status_code} {r.text[:200]} / "
            f"PATCHA {rpa.status_code} {rpa.text[:200]} / "
            f"POSTB {r2.status_code} {r2.text[:200]} / "
            f"PATCHB {rpb.status_code} {rpb.text[:200]}"
        )


# =========================
# APP INGEST (Vercel)
# =========================
def normalize_bid_for_payload(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    cleaned = re.sub(r"[^0-9.]", "", s.replace(",", ""))
    return cleaned if cleaned else None


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

    # fallback: first match anywhere
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
# BOOTSTRAP (fresh session)
# =========================
def bootstrap_to_printable(p, headless: bool):
    """
    Opens a brand new browser+context+page and navigates:
    login -> acknowledge -> search -> printable
    Returns: (browser, context, page, printable_url)
    """
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
# VIEWER OPEN WITH RETRY
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

        # reset by returning to printable
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

    # State
    last_node = None
    if USE_STATE:
        try:
            last_node = get_state_last_node()
            log.info("STATE last_node=%s", last_node)
        except Exception as e:
            log.warning("STATE read failed (continuing without state): %s", str(e))

    with sync_playwright() as p:
        browser = context = page = None
        printable_url = ""

        # Start fresh
        browser, context, page, printable_url = bootstrap_to_printable(p, HEADLESS)

        # Load lots
        lots = extract_lots_from_printable(page)
        if not lots:
            safe_close(browser, context, page)
            raise RuntimeError("No lots found on printable page.")

        # Continue after last_node?
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

        for idx, lot in enumerate(selected, start=1):

            # Restart browser every N lots
            if RESTART_BROWSER_EVERY > 0 and idx > 1 and (idx - 1) % RESTART_BROWSER_EVERY == 0:
                log.warning("Restarting browser after %d lots (hard reset session)...", idx - 1)
                safe_close(browser, context, page)
                browser, context, page, printable_url = bootstrap_to_printable(p, HEADLESS)

            node = lot["node"]
            row_text = lot["row_text"]
            tax_sale_url = lot["tax_sale_url"]
            fields = parse_fields_from_row_text(row_text)

            log.info("----- LOT %d/%d node=%s -----", idx, len(selected), node)
            log.info("Row text: %s", row_text[:220])

            try:
                # Open viewer with retry
                viewer_url = open_viewer_with_retry(page, printable_url, tax_sale_url, idx)
                if is_check_human(viewer_url):
                    log.error("Blocked by checkHuman.jsp after retries. Skipping node=%s", node)
                    # reset to printable and continue
                    page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                    wait_network(page, 30_000)
                    time.sleep(1.0)
                    continue

                # Find PDF link
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
                    time.sleep(1.0)
                    continue

                pdf_url = urljoin(viewer_url, href_pdf)
                log.info("PDF URL: %s", pdf_url)

                # Download PDF
                pdf_resp = context.request.get(pdf_url, timeout=MAX_WAIT)
                log.info("PDF HTTP status: %s", pdf_resp.status)
                log.info("PDF content-type: %s", pdf_resp.headers.get("content-type"))

                if not pdf_resp.ok:
                    preview = (pdf_resp.text() or "")[:600]
                    log.error("PDF download failed preview:\n%s", preview)
                    page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                    wait_network(page, 30_000)
                    time.sleep(1.0)
                    continue

                if not must_be_pdf(pdf_resp.headers):
                    preview = (pdf_resp.text() or "")[:800]
                    log.error("Response is not PDF preview:\n%s", preview)
                    page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                    wait_network(page, 30_000)
                    time.sleep(1.0)
                    continue

                pdf_bytes = pdf_resp.body()
                log.info("PDF bytes: %d", len(pdf_bytes))

                # Extract address
                text = try_pdfplumber_text(pdf_bytes)
                if text:
                    log.info("pdfplumber text length: %d (using text parse)", len(text))
                    addr = parse_best_address_from_text(text)
                else:
                    log.info("pdfplumber empty. OCR first %d pages...", OCR_MAX_PAGES)
                    try:
                        ocr_text = ocr_pdf_bytes(pdf_bytes, max_pages=OCR_MAX_PAGES, scale=OCR_SCALE)
                        log.info("OCR text length: %d", len(ocr_text))
                        addr = parse_best_address_from_text(ocr_text)
                    except Exception as e:
                        log.error("OCR failed: %s", str(e))
                        addr = {
                            "address": None, "city": None, "state": None, "zip": None,
                            "marker_used": None, "marker_found": False, "snippet": ""
                        }

                # Address filter
                if SKIP_IF_ADDRESS_NOT_NUMBERED:
                    if not is_numbered_street_address(addr.get("address")):
                        log.warning("Skipping node=%s because address is not numbered: %r", node, addr.get("address"))
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
                }

                print("\n" + "=" * 100)
                print(f"RESULT LOT {idx}")
                print("=" * 100)
                print(json.dumps(payload, indent=2))

                # Send to app
                ingest_ok = True
                ingest_result = None
                if SEND_TO_APP:
                    ingest_result = post_to_app(payload)
                    ingest_ok = bool(ingest_result)

                if ingest_result:
                    log.info("INGEST OK: %s", ingest_result)

                # Update state only if ingest ok OR ingest disabled
                if ingest_ok:
                    try:
                        set_state_last_node(node)
                        log.info("STATE updated last_node=%s", node)
                    except Exception as e:
                        log.warning("STATE write failed: %s", str(e))

            except Exception as e:
                log.exception("LOT FAILED node=%s error=%s", node, str(e))

            # Back to printable
            try:
                page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                wait_network(page, 30_000)
            except Exception:
                # if navigation fails, hard reset session
                log.warning("Failed to return printable. Hard reset session.")
                safe_close(browser, context, page)
                browser, context, page, printable_url = bootstrap_to_printable(p, HEADLESS)

            time.sleep(1.0)

        log.info("DONE.")
        safe_close(browser, context, page)


if __name__ == "__main__":
    run()