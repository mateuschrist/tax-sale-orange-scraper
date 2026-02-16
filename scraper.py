import os
import re
import json
import time
import logging
from io import BytesIO
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

import requests
import pdfplumber
import pytesseract
import pypdfium2
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# CONFIG
# =========================
BASE_URL = "https://or.occompt.com"
LOGIN_URL = f"{BASE_URL}/recorder/web/login.jsp"
SEARCH_URL = f"{BASE_URL}/recorder/tdsmweb/applicationSearch.jsp"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
DEBUG_HTML = os.getenv("DEBUG_HTML", "false").lower() == "true"

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
MAX_TOTAL = int(os.getenv("MAX_TOTAL", "0"))  # 0 = unlimited
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint_orange.json")

# App/API (Vercel)
APP_API_BASE = (os.getenv("APP_API_BASE", "") or "").rstrip("/")
APP_API_TOKEN = (os.getenv("APP_API_TOKEN", "") or "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

# Advanced sync options
MARK_REMOVED = os.getenv("MARK_REMOVED", "false").lower() == "true"
FETCH_EXISTING = os.getenv("FETCH_EXISTING", "true").lower() == "true"

MAX_WAIT = 60_000

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("orange-taxdeed-scraper")


# =========================
# CHECKPOINT
# =========================
def load_checkpoint() -> Dict:
    if not os.path.exists(CHECKPOINT_FILE):
        return {"cursor": 0, "last_run_at": None}
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"cursor": 0, "last_run_at": None}


def save_checkpoint(cp: Dict):
    cp["last_run_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(cp, f, indent=2)


# =========================
# HELPERS
# =========================
def wait_network(page, timeout=20_000):
    try:
        page.wait_for_load_state("networkidle", timeout=timeout)
    except PWTimeout:
        pass


def click_any(page, selectors: List[str], label: str) -> bool:
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


def normalize_bid_for_payload(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    cleaned = re.sub(r"[^0-9.]", "", s.replace(",", ""))
    return cleaned if cleaned else None


# =========================
# APP API HELPERS
# =========================
def app_headers():
    return {"Authorization": f"Bearer {APP_API_TOKEN}"} if APP_API_TOKEN else {}


def post_to_app_ingest(payload: dict) -> Optional[dict]:
    if not SEND_TO_APP:
        log.info("APP_API_BASE / APP_API_TOKEN not set → skipping send to app.")
        return None

    url = f"{APP_API_BASE}/api/ingest"
    last_err = None

    for attempt in range(1, 4):
        try:
            r = requests.post(url, json=payload, headers=app_headers(), timeout=40)
            log.info("INGEST attempt %d: %s", attempt, r.status_code)
            snippet = (r.text or "")[:250].replace("\n", " ")
            log.info("INGEST response: %s", snippet)

            if r.status_code in (200, 201):
                return r.json()

            if r.status_code == 401:
                log.error("INGEST unauthorized. Check APP_API_TOKEN == INGEST_API_TOKEN in Vercel.")
                return None

            last_err = f"HTTP {r.status_code}: {r.text[:500]}"
        except Exception as e:
            last_err = str(e)

        time.sleep(1.0 * attempt)

    log.error("INGEST failed after retries: %s", last_err)
    return None


def fetch_existing_nodes(limit=5000) -> List[str]:
    """
    Puxa lista de nodes do app para poder:
    - não ficar reprocessando tudo
    - marcar removidos (se MARK_REMOVED=true)
    """
    if not (APP_API_BASE and APP_API_TOKEN):
        return []

    url = f"{APP_API_BASE}/api/properties?limit={limit}"
    try:
        r = requests.get(url, headers=app_headers(), timeout=40)
        if r.status_code != 200:
            log.warning("fetch_existing_nodes HTTP %s", r.status_code)
            return []
        j = r.json()
        items = j.get("data") or []
        nodes = [it.get("node") for it in items if it.get("node")]
        return nodes
    except Exception as e:
        log.warning("fetch_existing_nodes failed: %s", e)
        return []


def mark_removed_nodes(nodes_to_mark: List[str]):
    """
    Marca nós como status='removed' (precisa seu backend aceitar update por node ou ter endpoint).
    Se você NÃO tiver endpoint pra isso, eu te passo abaixo a forma certa.
    """
    if not nodes_to_mark:
        return

    # Opção simples: reusar ingest com status=removed
    # (porque o /api/ingest upsert por node)
    for node in nodes_to_mark:
        payload = {
            "county": "Orange",
            "state": "FL",
            "node": node,
            "status": "removed",
            "notes": "Removed from source list (not found on latest scrape)."
        }
        post_to_app_ingest(payload)


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
        "tax_sale_id": tax_sale_id or None,
        "sale_date": sale_date or None,
        "deed_status": status or None,
        "parcel_number": parcel or None,
        "opening_bid": min_bid or None,
        "applicant_name": applicant or None,
    }


def extract_lots_from_printable(page) -> List[dict]:
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

        if node:
            lots.append({
                "node": node,
                "tax_sale_url": full,
                "row_text": norm_ws(row_text),
            })

    return lots


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


def _extract_street_before_city(block: str, city_match_start: int) -> Optional[str]:
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
            "snippet": after[:700]
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
        "snippet": text[max(0, mcity.start()-250):mcity.end()+250]
    }


# =========================
# MAIN
# =========================
def run():
    cp = load_checkpoint()
    cursor = int(cp.get("cursor", 0))

    log.info("SEND_TO_APP=%s APP_API_BASE=%s", SEND_TO_APP, APP_API_BASE or "(empty)")
    log.info("Checkpoint cursor=%s BATCH_SIZE=%s MAX_TOTAL=%s", cursor, BATCH_SIZE, MAX_TOTAL)

    existing_nodes = []
    if FETCH_EXISTING:
        existing_nodes = fetch_existing_nodes()
        log.info("Existing nodes fetched from app: %d", len(existing_nodes))

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

        # 2) Search
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

        # 3) Extract ALL lots
        lots = extract_lots_from_printable(page)
        if not lots:
            log.error("No lots found on printable page.")
            browser.close()
            return

        # Optional: compare for removed nodes
        current_nodes = [l["node"] for l in lots if l.get("node")]
        if MARK_REMOVED and existing_nodes:
            removed = sorted(list(set(existing_nodes) - set(current_nodes)))
            log.info("Nodes removed from source (will mark removed): %d", len(removed))
            # cuidado: isso pode ser muito grande. Recomendo limitar por batch em produção.
            mark_removed_nodes(removed)

        total_lots = len(lots)
        log.info("Total lots available: %d", total_lots)

        # 4) Select batch by cursor
        start = cursor
        end = min(cursor + BATCH_SIZE, total_lots)
        batch = lots[start:end]

        if not batch:
            log.info("No batch to process (cursor beyond end). Resetting cursor to 0.")
            cp["cursor"] = 0
            save_checkpoint(cp)
            browser.close()
            return

        # Limit total processing if MAX_TOTAL set
        processed_total = 0

        for idx, lot in enumerate(batch, start=1):
            node = lot["node"]
            row_text = lot["row_text"]
            tax_sale_url = lot["tax_sale_url"]

            fields = parse_fields_from_row_text(row_text)

            log.info("----- BATCH LOT %d/%d node=%s -----", idx, len(batch), node)

            try:
                # Open viewer
                page.goto(tax_sale_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                wait_network(page)
                viewer_url = page.url

                # Find PDF link
                pdf_a = page.locator("a[href*='Property_Information.pdf']")
                href_pdf = pdf_a.first.get_attribute("href") if pdf_a.count() else None
                if not href_pdf:
                    viewer_html = page.content()
                    m = re.search(r'href="([^"]*Property_Information\.pdf[^"]*)"', viewer_html, re.I)
                    href_pdf = m.group(1) if m else None

                if not href_pdf:
                    log.error("PDF link not found for node=%s", node)
                    continue

                pdf_url = urljoin(viewer_url, href_pdf)

                # Download PDF (Playwright request)
                pdf_resp = context.request.get(pdf_url, timeout=MAX_WAIT)

                if not pdf_resp.ok or not must_be_pdf(pdf_resp.headers):
                    log.error("PDF download failed node=%s status=%s", node, pdf_resp.status)
                    continue

                pdf_bytes = pdf_resp.body()

                # Extract address
                text = try_pdfplumber_text(pdf_bytes)
                if text:
                    addr = parse_best_address_from_text(text)
                else:
                    ocr_text = ocr_pdf_bytes(pdf_bytes, max_pages=3, scale=2.2)
                    addr = parse_best_address_from_text(ocr_text)

                notes = None
                if not addr.get("marker_found"):
                    notes = "Address marker not found via OCR first 3 pages."

                payload = {
                    "county": "Orange",
                    "state": "FL",
                    "node": node,
                    "tax_sale_id": fields.get("tax_sale_id"),
                    "parcel_number": fields.get("parcel_number"),
                    "sale_date": fields.get("sale_date"),
                    "opening_bid": normalize_bid_for_payload(fields.get("opening_bid")),
                    "deed_status": fields.get("deed_status"),
                    "applicant_name": fields.get("applicant_name"),
                    "pdf_url": pdf_url,
                    "address": addr.get("address"),
                    "city": addr.get("city"),
                    "state_address": addr.get("state"),
                    "zip": addr.get("zip"),
                    "address_source_marker": addr.get("marker_used"),
                    "status": "new",
                    "notes": notes,
                }

                # Send
                ingest_result = post_to_app_ingest(payload)
                if ingest_result:
                    log.info("INGEST OK node=%s => %s", node, ingest_result)

            except Exception as e:
                log.exception("LOT FAILED node=%s error=%s", node, str(e))

            finally:
                # back to printable (stability)
                page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                wait_network(page, 30_000)
                time.sleep(0.8)

            processed_total += 1
            if MAX_TOTAL and processed_total >= MAX_TOTAL:
                log.info("Reached MAX_TOTAL=%d; stopping early.", MAX_TOTAL)
                break

        # 5) Advance cursor and save checkpoint
        new_cursor = end
        if new_cursor >= total_lots:
            # done: reset cursor to 0 for next cycle
            new_cursor = 0

        cp["cursor"] = new_cursor
        save_checkpoint(cp)

        log.info("DONE batch. cursor moved %d -> %d", cursor, new_cursor)
        browser.close()


if __name__ == "__main__":
    run()
