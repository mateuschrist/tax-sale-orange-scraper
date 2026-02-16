import os
import re
import json
import time
import logging
from io import BytesIO
from datetime import datetime, timedelta, timezone
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
MAX_LOTS = int(os.getenv("MAX_LOTS", "99999"))  # agora é "limite de captura" (a lista), mas batch controla o processado
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
PAUSE_HOURS_ON_DONE = int(os.getenv("PAUSE_HOURS_ON_DONE", "20"))
MARK_REMOVED = os.getenv("MARK_REMOVED", "true").lower() == "true"

DEBUG_HTML = os.getenv("DEBUG_HTML", "false").lower() == "true"
MAX_WAIT = 60_000

# App/API (Vercel)
APP_API_BASE = (os.getenv("APP_API_BASE", "") or "").rstrip("/")
APP_API_TOKEN = (os.getenv("APP_API_TOKEN", "") or "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

SCRAPER_NAME = os.getenv("SCRAPER_NAME", "orange_taxdeed")
COUNTY = os.getenv("COUNTY", "Orange")
STATE = os.getenv("STATE", "FL")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("taxdeed-scraper-batch")


# =========================
# HELPERS
# =========================
def now_utc():
    return datetime.now(timezone.utc)

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

def normalize_bid_for_payload(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    cleaned = re.sub(r"[^0-9.]", "", s.replace(",", ""))
    return cleaned if cleaned else None

def api_headers():
    return {"Authorization": f"Bearer {APP_API_TOKEN}"}

def api_post(path: str, payload: dict, timeout=30):
    if not SEND_TO_APP:
        raise RuntimeError("APP_API_BASE / APP_API_TOKEN not set")
    url = f"{APP_API_BASE}{path}"
    r = requests.post(url, json=payload, headers=api_headers(), timeout=timeout)
    return r

def api_get(path: str, timeout=30):
    if not SEND_TO_APP:
        raise RuntimeError("APP_API_BASE / APP_API_TOKEN not set")
    url = f"{APP_API_BASE}{path}"
    r = requests.get(url, headers=api_headers(), timeout=timeout)
    return r


# =========================
# APP INTEGRATIONS
# =========================
def get_state():
    r = api_get(f"/api/scraper-state?scraper={SCRAPER_NAME}")
    if r.status_code != 200:
        raise RuntimeError(f"GET scraper-state failed: {r.status_code} {r.text[:200]}")
    return r.json().get("data")

def set_state(**kwargs):
    payload = {"scraper": SCRAPER_NAME, **kwargs}
    r = api_post("/api/scraper-state", payload)
    if r.status_code != 200:
        raise RuntimeError(f"POST scraper-state failed: {r.status_code} {r.text[:200]}")
    return r.json().get("data")

def run_start(run_id: str, found_total: int):
    payload = {"mode": "start", "scraper_name": SCRAPER_NAME, "run_id": run_id, "found_total": found_total}
    r = api_post("/api/scraper-run", payload)
    if r.status_code != 200:
        raise RuntimeError(f"run start failed: {r.status_code} {r.text[:200]}")

def run_finish(run_id: str, **stats):
    payload = {"mode": "finish", "scraper_name": SCRAPER_NAME, "run_id": run_id, **stats}
    r = api_post("/api/scraper-run", payload)
    if r.status_code != 200:
        raise RuntimeError(f"run finish failed: {r.status_code} {r.text[:200]}")

def existence_check(nodes: list[str]) -> set[str]:
    payload = {"county": COUNTY, "state": STATE, "nodes": nodes}
    r = api_post("/api/existence-check", payload)
    if r.status_code != 200:
        raise RuntimeError(f"existence-check failed: {r.status_code} {r.text[:200]}")
    existing = r.json().get("existing") or []
    return set(existing)

def mark_removed(current_nodes: list[str]) -> int:
    payload = {"county": COUNTY, "state": STATE, "current_nodes": current_nodes}
    r = api_post("/api/mark-removed", payload)
    if r.status_code != 200:
        raise RuntimeError(f"mark-removed failed: {r.status_code} {r.text[:200]}")
    return int(r.json().get("removed_marked") or 0)

def post_to_ingest(payload: dict) -> dict | None:
    url = f"{APP_API_BASE}/api/ingest"
    headers = api_headers()
    last_err = None
    for attempt in range(1, 4):
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=30)
            log.info("INGEST attempt %d: %s", attempt, r.status_code)
            if r.status_code in (200, 201):
                return r.json()
            if r.status_code == 401:
                log.error("INGEST unauthorized (check APP_API_TOKEN matches Vercel INGEST_API_TOKEN).")
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
        "tax_sale_id": tax_sale_id or None,
        "sale_date": sale_date or None,
        "deed_status": status or None,
        "parcel_number": parcel or None,
        "opening_bid": min_bid or None,
        "applicant_name": applicant or None,
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

    return lots


# =========================
# PDF TEXT (fallback) + OCR
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
        return {"address": None, "city": None, "state": None, "zip": None, "marker_used": None, "marker_found": False, "snippet": ""}

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
        return {"address": None, "city": None, "state": None, "zip": None, "marker_used": None, "marker_found": False, "snippet": text[:900]}

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
# MAIN
# =========================
def run():
    log.info("SEND_TO_APP=%s APP_API_BASE=%s", SEND_TO_APP, APP_API_BASE if APP_API_BASE else "(empty)")
    if not SEND_TO_APP:
        raise RuntimeError("Set APP_API_BASE + APP_API_TOKEN in GitHub Secrets to enable app sync.")

    # 0) Check memory (pause logic)
    state = get_state()
    if state:
        done = bool(state.get("done_for_today"))
        resume_after = state.get("resume_after")
        if done and resume_after:
            ra = datetime.fromisoformat(resume_after.replace("Z", "+00:00"))
            if now_utc() < ra:
                log.info("Done for today. Resume after: %s. Exiting.", resume_after)
                return

    run_id = f"{SCRAPER_NAME}_{now_utc().strftime('%Y%m%d_%H%M%S')}"

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

        # 3) Extract all lots on printable
        lots = extract_lots_from_printable(page)
        lots = [l for l in lots if l.get("node")]
        if not lots:
            log.error("No lots found on printable page.")
            browser.close()
            return

        # Apply MAX_LOTS cap only to the list (optional)
        if MAX_LOTS and len(lots) > MAX_LOTS:
            lots = lots[:MAX_LOTS]

        found_total = len(lots)
        run_start(run_id, found_total=found_total)

        # 4) mark removed (optional) using full current node list
        removed_marked = 0
        if MARK_REMOVED:
            try:
                current_nodes = [l["node"] for l in lots if l.get("node")]
                removed_marked = mark_removed(current_nodes)
                log.info("Removed marked: %d", removed_marked)
            except Exception as e:
                log.warning("mark_removed failed (non-fatal): %s", str(e))

        # 5) Determine which nodes already exist
        all_nodes = [l["node"] for l in lots]
        existing = existence_check(all_nodes)

        new_lots = [l for l in lots if l["node"] not in existing]
        log.info("Existing: %d | New candidates: %d", len(existing), len(new_lots))

        # 6) Batch select (BATCH_SIZE)
        batch = new_lots[:BATCH_SIZE]
        if not batch:
            # DONE: no new items
            resume_after = (now_utc() + timedelta(hours=PAUSE_HOURS_ON_DONE)).isoformat()
            set_state(
                last_run_id=run_id,
                last_run_at=now_utc().isoformat(),
                done_for_today=True,
                resume_after=resume_after,
                last_tax_sale_id=None,
                last_node=None,
            )
            run_finish(run_id, status="done", message="No new lots. Pausing.", processed=0, inserted=0, updated=0, skipped=0, removed_marked=removed_marked, found_total=found_total)
            log.info("No new lots. Pause until %s", resume_after)
            browser.close()
            return

        # 7) Process batch
        inserted = 0
        updated = 0
        skipped = 0
        processed = 0

        last_tax_sale_id = None
        last_node = None

        for idx, lot in enumerate(batch, start=1):
            node = lot["node"]
            row_text = lot["row_text"]
            tax_sale_url = lot["tax_sale_url"]

            fields = parse_fields_from_row_text(row_text)

            log.info("----- BATCH LOT %d/%d node=%s -----", idx, len(batch), node)
            log.info("Row text: %s", row_text[:220])

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
                    log.error("PDF link not found in viewer for node=%s", node)
                    skipped += 1
                    continue

                pdf_url = urljoin(viewer_url, href_pdf)

                # Download PDF
                pdf_resp = context.request.get(pdf_url, timeout=MAX_WAIT)
                if not pdf_resp.ok or not must_be_pdf(pdf_resp.headers):
                    log.error("PDF download failed or not PDF for node=%s status=%s", node, pdf_resp.status)
                    skipped += 1
                    continue

                pdf_bytes = pdf_resp.body()

                # Extract address: text -> OCR (3 pages)
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
                    "county": COUNTY,
                    "state": STATE,
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

                # Send to app ingest
                ingest_result = post_to_ingest(payload)
                processed += 1

                if ingest_result and ingest_result.get("action") == "created":
                    inserted += 1
                elif ingest_result and ingest_result.get("action") == "updated":
                    updated += 1

                # update checkpoint (memory)
                last_tax_sale_id = fields.get("tax_sale_id")
                last_node = node

            except Exception as e:
                log.exception("LOT FAILED node=%s error=%s", node, str(e))
                skipped += 1

            finally:
                # return printable to keep session stable
                page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                wait_network(page, 30_000)
                time.sleep(1.0)

        # 8) Save memory / state
        set_state(
            last_tax_sale_id=last_tax_sale_id,
            last_node=last_node,
            last_run_id=run_id,
            last_run_at=now_utc().isoformat(),
            done_for_today=False,
            resume_after=None,
        )

        run_finish(
            run_id,
            status="ok",
            message=f"Processed batch={len(batch)}",
            found_total=found_total,
            processed=processed,
            inserted=inserted,
            updated=updated,
            skipped=skipped,
            removed_marked=removed_marked,
        )

        log.info("DONE batch. processed=%d inserted=%d updated=%d skipped=%d removed_marked=%d",
                 processed, inserted, updated, skipped, removed_marked)

        browser.close()


if __name__ == "__main__":
    run()
