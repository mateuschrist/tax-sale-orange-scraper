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
from PIL import Image

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# CONFIG
# =========================
BASE_URL = "https://or.occompt.com"
LOGIN_URL = f"{BASE_URL}/recorder/web/login.jsp"
SEARCH_URL = f"{BASE_URL}/recorder/tdsmweb/applicationSearch.jsp"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
MAX_LOTS = int(os.getenv("MAX_LOTS", "3"))
DEBUG_HTML = os.getenv("DEBUG_HTML", "false").lower() == "true"

MAX_WAIT = 60_000

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("taxdeed-gh-ocr")


# =========================
# HELPERS
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
# TABLE PARSE (Printable row text)
# Example row text (from your logs):
# "Tax Sale 2023-17830 Sale Date: 03/12/2026 Applicant Name: ... Status: Active Sale Parcel: 01-23-... Min Bid: $3,432.29 ..."
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
        "opening_bid": min_bid,          # no site aparece como Min Bid
        "applicant_name": applicant,
    }


def extract_lots_from_printable(page) -> list[dict]:
    """
    Captura todos os lotes na página printable SEM depender de locator depois.
    Para cada link "Tax Sale", pega:
      - href (viewDoc.jsp?node=DOC...)
      - node
      - texto da linha (tr) pra extrair campos
    """
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
    """
    Se algum PDF vier com texto (raramente), tentamos extrair.
    """
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


def ocr_pdf_bytes(pdf_bytes: bytes, max_pages: int = 1, scale: float = 2.2) -> str:
    """
    OCR do PDF (escaneado).
    Para performance no GitHub, por padrão rodamos OCR só na 1ª página (max_pages=1),
    porque o endereço está nela.
    """
    pdf = pypdfium2.PdfDocument(pdf_bytes)
    n_pages = len(pdf)
    pages_to_do = min(n_pages, max_pages)

    full_text = []
    for i in range(pages_to_do):
        page = pdf[i]
        # render to PIL
        bitmap = page.render(scale=scale)
        img = bitmap.to_pil()

        # OCR
        txt = pytesseract.image_to_string(img)
        if txt:
            full_text.append(txt)

    return "\n".join(full_text).strip()


def parse_address_from_text(text: str) -> dict:
    """
    Extrai endereço usando marker tolerante.
    No seu PDF aparece: "ADDRESS ON RECORD ON CURRENT TAX ROLL:"
    OCR pode vir com variações, então usamos regex flexível.
    """
    if not text:
        return {"address": None, "city": None, "state": None, "zip": None, "marker_found": False, "snippet": ""}

    # marker flexível para OCR
    marker = re.search(r"ADDRESS\s+ON\s+RECORD\s+ON\s+CURRENT\s+TAX\s+ROLL\s*[:\-]?", text, re.I)
    if not marker:
        return {"address": None, "city": None, "state": None, "zip": None, "marker_found": False, "snippet": text[:900]}

    after = text[marker.end():].strip()
    lines = [ln.strip() for ln in after.splitlines() if ln.strip()]

    # geralmente:
    # line0 = street
    # line1 = "Orlando, FL 32833"
    street = lines[0] if len(lines) > 0 else None

    # procura city/state/zip no bloco "after"
    m = re.search(r"([A-Za-z .'-]+)\s*,\s*(FL)\s*(\d{5}(?:-\d{4})?)", after, re.I)
    city = m.group(1).strip().title() if m else None
    state = m.group(2).upper() if m else None
    zipc = m.group(3) if m else None

    return {
        "address": street,
        "city": city,
        "state": state,
        "zip": zipc,
        "marker_found": True,
        "snippet": after[:400]
    }


# =========================
# MAIN
# =========================
def run():
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

        # set DeedStatusID=AS
        if page.locator("select[name='DeedStatusID']").count() > 0:
            page.select_option("select[name='DeedStatusID']", value="AS")
            log.info("Set DeedStatusID=AS")
        else:
            log.warning("Dropdown DeedStatusID not found; selector may need update.")

        # click Search
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

        # click Printable Version
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
        lots = [l for l in lots if l.get("node")]  # keep only valid
        if not lots:
            log.error("No lots found on printable page.")
            browser.close()
            return

        selected = lots[:MAX_LOTS]
        log.info("Selected lots: %s", [l["node"] for l in selected])

        for idx, lot in enumerate(selected, start=1):
            node = lot["node"]
            row_text = lot["row_text"]
            tax_sale_url = lot["tax_sale_url"]

            fields = parse_fields_from_row_text(row_text)

            log.info("----- LOT %d/%d node=%s -----", idx, len(selected), node)
            log.info("Row text: %s", row_text[:220])

            # 4) Open viewer (same tab)
            page.goto(tax_sale_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
            wait_network(page)
            viewer_url = page.url
            log.info("Viewer URL: %s", viewer_url)

            # 5) Find PDF link in viewer
            pdf_a = page.locator("a[href*='Property_Information.pdf']")
            href_pdf = pdf_a.first.get_attribute("href") if pdf_a.count() else None

            if not href_pdf:
                # fallback via html
                viewer_html = page.content()
                m = re.search(r'href="([^"]*Property_Information\.pdf[^"]*)"', viewer_html, re.I)
                href_pdf = m.group(1) if m else None

            if not href_pdf:
                log.error("PDF link not found in viewer for node=%s", node)
                # return to printable
                page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
                wait_network(page, 30_000)
                continue

            pdf_url = urljoin(viewer_url, href_pdf)
            log.info("PDF URL: %s", pdf_url)

            # 6) Download PDF (session request)
            pdf_resp = context.request.get(pdf_url, timeout=MAX_WAIT)
            log.info("PDF HTTP status: %s", pdf_resp.status)
            log.info("PDF content-type: %s", pdf_resp.headers.get("content-type"))
            pdf_bytes = pdf_resp.body()
            log.info("PDF bytes: %d", len(pdf_bytes))

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

            # 7) Extract address: try text first, then OCR (real solution)
            text = try_pdfplumber_text(pdf_bytes)
            if text:
                log.info("pdfplumber text length: %d (using text parse)", len(text))
                addr = parse_address_from_text(text)
            else:
                log.info("pdfplumber returned empty. Running OCR on page 1...")
                ocr_text = ocr_pdf_bytes(pdf_bytes, max_pages=1, scale=2.2)
                log.info("OCR text length: %d", len(ocr_text))
                addr = parse_address_from_text(ocr_text)

            payload = {
                "county": "Orange",
                "state": "FL",
                "node": node,

                # from printable row
                "tax_sale_id": fields.get("tax_sale_id", ""),
                "parcel_number": fields.get("parcel_number", ""),
                "sale_date": fields.get("sale_date", ""),
                "opening_bid": fields.get("opening_bid", ""),
                "deed_status": fields.get("deed_status", ""),
                "applicant_name": fields.get("applicant_name", ""),

                # pdf link + extracted address
                "pdf_url": pdf_url,
                "address": addr.get("address"),
                "city": addr.get("city"),
                "state_address": addr.get("state"),  # keep separate if you want
                "zip": addr.get("zip"),
            }

            print("\n" + "=" * 100)
            print(f"RESULT LOT {idx}")
            print("=" * 100)
            print(json.dumps(payload, indent=2))

            if not addr.get("marker_found"):
                log.warning("Address marker not found. Snippet:\n%s", addr.get("snippet", "")[:900])

            # 8) Return to printable and continue
            page.goto(printable_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
            wait_network(page, 30_000)
            time.sleep(1.0)

        log.info("DONE.")
        browser.close()


if __name__ == "__main__":
    run()