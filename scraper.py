import os
import re
import json
import time
import logging
from io import BytesIO
from urllib.parse import urljoin

import pdfplumber
import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# ENV / CONFIG (GitHub safe)
# =========================
BASE_URL = "https://or.occompt.com"
LOGIN_URL = f"{BASE_URL}/recorder/web/login.jsp"
SEARCH_URL = f"{BASE_URL}/recorder/tdsmweb/applicationSearch.jsp"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
MAX_LOTS = int(os.getenv("MAX_LOTS", "3"))
DEBUG_HTML = os.getenv("DEBUG_HTML", "false").lower() == "true"

APP_API_BASE = os.getenv("APP_API_BASE")  # opcional
APP_API_TOKEN = os.getenv("APP_API_TOKEN")  # opcional
APP_API_ENDPOINT = f"{APP_API_BASE.rstrip('/')}/api/properties" if APP_API_BASE else None

MAX_WAIT = 60_000
ADDRESS_MARKER = "ADDRESS ON RECORD ON CURRENT TAX ROLL:"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("taxdeed-gh")

# =========================
# Helpers
# =========================
def click_first(page, selectors, label):
    """Try several selectors; click first that exists."""
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


def wait_network(page, ms=15_000):
    try:
        page.wait_for_load_state("networkidle", timeout=ms)
    except PWTimeout:
        pass


def extract_lot_fields_from_html(html: str) -> dict:
    """Light regex extraction; replace with selectors later if you want."""
    def rgx(pattern):
        m = re.search(pattern, html, re.I | re.S)
        return m.group(1).strip() if m else None

    return {
        "parcel_number": rgx(r"Parcel\s*Number.*?</[^>]+>\s*<[^>]+>\s*([^<]+)"),
        "sale_date": rgx(r"Sale\s*Date.*?</[^>]+>\s*<[^>]+>\s*([^<]+)"),
        "opening_bid": rgx(r"Opening\s*Bid.*?</[^>]+>\s*<[^>]+>\s*\$?\s*([^<]+)"),
        "application_number": rgx(r"Application\s*Number.*?</[^>]+>\s*<[^>]+>\s*([^<]+)"),
        "deed_status": rgx(r"Deed\s*Status.*?</[^>]+>\s*<[^>]+>\s*([^<]+)"),
        "homestead": rgx(r"Homestead.*?</[^>]+>\s*<[^>]+>\s*([^<]+)"),
    }


def parse_address_from_pdf_bytes(pdf_bytes: bytes) -> dict:
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        text = "\n".join([(p.extract_text() or "") for p in pdf.pages])

    if ADDRESS_MARKER not in text:
        return {
            "marker_found": False,
            "address": None, "city": None, "state": None, "zip": None,
            "text_snippet": (text[:800] if text else None)
        }

    after = text.split(ADDRESS_MARKER, 1)[1].strip()
    snippet = after[:700]
    lines = [ln.strip() for ln in snippet.splitlines() if ln.strip()]

    street = lines[0] if lines else None
    line2 = lines[1] if len(lines) > 1 else ""

    m = re.search(
        r"^(?P<city>[A-Z .'-]+),?\s+(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)$",
        line2.upper().strip()
    )
    if m:
        return {
            "marker_found": True,
            "address": street,
            "city": m.group("city").title().strip(),
            "state": m.group("state"),
            "zip": m.group("zip"),
            "text_snippet": snippet[:400]
        }

    # fallback
    joined = " | ".join(lines[:5]).upper()
    m2 = re.search(r"(?P<city>[A-Z .'-]+),?\s+(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)", joined)
    return {
        "marker_found": True,
        "address": street,
        "city": m2.group("city").title().strip() if m2 else None,
        "state": m2.group("state") if m2 else None,
        "zip": m2.group("zip") if m2 else None,
        "text_snippet": snippet[:400]
    }


def post_to_app(payload: dict):
    if not APP_API_ENDPOINT:
        return None, None

    headers = {"Content-Type": "application/json"}
    if APP_API_TOKEN:
        headers["Authorization"] = f"Bearer {APP_API_TOKEN}"

    r = requests.post(APP_API_ENDPOINT, headers=headers, data=json.dumps(payload), timeout=30)
    return r.status_code, r.text[:400]


def must_be_pdf(response) -> bool:
    # Playwright APIResponse has headers via response.headers
    ct = (response.headers.get("content-type") or "").lower()
    return "application/pdf" in ct or ct.endswith("/pdf")


# =========================
# Main scraper (GitHub)
# =========================
def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        log.info("OPEN: %s", LOGIN_URL)
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=MAX_WAIT)

        # Acknowledge (cookie/disclaimer)
        clicked_ack = click_first(
            page,
            [
                "text=I Acknowledge",
                "button:has-text('I Acknowledge')",
                "a:has-text('I Acknowledge')",
                "input[value='I Acknowledge']",
            ],
            "I Acknowledge"
        )
        if not clicked_ack:
            log.warning("Não achei o botão 'I Acknowledge' automaticamente. Pode precisar ajustar seletor.")

        wait_network(page)

        # Go to search page directly (more stable than relying on menu)
        log.info("OPEN SEARCH: %s", SEARCH_URL)
        page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=MAX_WAIT)
        wait_network(page)

        # Select DeedStatusID = AS
        set_ok = False
        for sel in ["select[name='DeedStatusID']", "select#DeedStatusID"]:
            try:
                if page.locator(sel).count() > 0:
                    page.select_option(sel, value="AS")
                    set_ok = True
                    log.info("Set DeedStatusID=AS using %s", sel)
                    break
            except Exception:
                continue
        if not set_ok:
            log.warning("Não consegui setar DeedStatusID=AS. Ajuste seletor do dropdown.")

        # Click Search
        clicked_search = click_first(
            page,
            [
                "input[type='submit'][value='Search']",
                "button:has-text('Search')",
                "text=Search"
            ],
            "Search"
        )
        if not clicked_search:
            log.error("Não consegui clicar Search. Ajuste seletor.")
            browser.close()
            return

        wait_network(page, 30_000)

        # Click Printable Version (may open popup or same page)
        printable_page = None
        log.info("Trying Printable Version...")
        try:
            with context.expect_page(timeout=8_000) as pop:
                clicked = click_first(
                    page,
                    [
                        "text=Printable Version",
                        "a:has-text('Printable Version')",
                        "button:has-text('Printable Version')"
                    ],
                    "Printable Version"
                )
            if clicked:
                printable_page = pop.value
        except Exception:
            # maybe same tab
            clicked = click_first(
                page,
                [
                    "text=Printable Version",
                    "a:has-text('Printable Version')",
                    "button:has-text('Printable Version')"
                ],
                "Printable Version"
            )
            printable_page = page if clicked else None

        if printable_page is None:
            log.error("Não consegui abrir Printable Version.")
            browser.close()
            return

        printable_page.bring_to_front()
        printable_page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
        wait_network(printable_page)

        if DEBUG_HTML:
            log.info("Printable HTML length: %d", len(printable_page.content()))

        # Locate "Tax Sale" links
        tax_links = printable_page.locator("a:has-text('Tax Sale')")
        total = tax_links.count()
        log.info("Tax Sale links found: %d", total)

        if total == 0:
            log.error("Nenhum 'Tax Sale' encontrado. Pode ser que a lista esteja vazia ou texto diferente.")
            browser.close()
            return

        n = min(total, MAX_LOTS)
        log.info("Processing %d lot(s)...", n)

        for i in range(n):
            log.info("----- LOT %d/%d -----", i + 1, n)

            # Open lot in new page to avoid history issues
            with context.expect_page() as pop:
                tax_links.nth(i).click()
            lot_page = pop.value
            lot_page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
            wait_network(lot_page)

            lot_url = lot_page.url
            log.info("Lot URL: %s", lot_url)

            lot_html = lot_page.content()
            if DEBUG_HTML:
                log.info("Lot HTML length: %d", len(lot_html))

            lot_data = extract_lot_fields_from_html(lot_html)

            # Open viewer
            vpi = lot_page.locator("a:has-text('View Property Information'), button:has-text('View Property Information')")
            if vpi.count() == 0:
                log.error("No 'View Property Information' found for lot %d.", i + 1)
                lot_page.close()
                continue

            with context.expect_page() as vpop:
                vpi.first.click()
            viewer = vpop.value
            viewer.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
            wait_network(viewer)

            log.info("Viewer URL: %s", viewer.url)

            # Find PDF link inside viewer
            pdf_a = viewer.locator("a[href*='Property_Information.pdf']")
            href = pdf_a.first.get_attribute("href") if pdf_a.count() else None

            if not href:
                # fallback search in html
                viewer_html = viewer.content()
                m = re.search(r'href="([^"]*Property_Information\.pdf[^"]*)"', viewer_html, re.I)
                href = m.group(1) if m else None

            if not href:
                log.error("PDF link not found in viewer for lot %d.", i + 1)
                viewer.close()
                lot_page.close()
                continue

            pdf_url = urljoin(viewer.url, href)
            log.info("PDF URL: %s", pdf_url)

            # Download PDF using same session/cookies
            pdf_resp = context.request.get(pdf_url, timeout=MAX_WAIT)

            log.info("PDF HTTP status: %s", pdf_resp.status)
            if not pdf_resp.ok:
                body_preview = (pdf_resp.text() or "")[:600]
                log.error("PDF download failed. Body preview:\n%s", body_preview)
                viewer.close()
                lot_page.close()
                continue

            # Validate content-type as PDF
            if not must_be_pdf(pdf_resp):
                preview = (pdf_resp.text() or "")[:800]
                log.error("PDF response is not application/pdf. Content preview:\n%s", preview)
                viewer.close()
                lot_page.close()
                continue

            pdf_bytes = pdf_resp.body()
            addr = parse_address_from_pdf_bytes(pdf_bytes)

            payload = {
                "county": "Orange",
                "state": "FL",
                "parcel_number": lot_data.get("parcel_number"),
                "sale_date": lot_data.get("sale_date"),
                "opening_bid": lot_data.get("opening_bid"),
                "application_number": lot_data.get("application_number"),
                "deed_status": lot_data.get("deed_status"),
                "homestead": lot_data.get("homestead"),
                "pdf_url": pdf_url,
                "address": addr.get("address"),
                "city": addr.get("city"),
                "state": addr.get("state") or "FL",
                "zip": addr.get("zip"),
            }

            # Print result to Actions logs (terminal)
            print("\n" + "=" * 100)
            print(f"RESULT LOT {i+1}")
            print("=" * 100)
            print(json.dumps(payload, indent=2))

            if not addr.get("marker_found"):
                log.warning("Marker not found in PDF. Snippet:\n%s", addr.get("text_snippet"))

            # Optional: post to your app
            if APP_API_ENDPOINT:
                status, body = post_to_app(payload)
                if status and 200 <= status < 300:
                    log.info("✅ Posted to app: HTTP %d", status)
                else:
                    log.warning("⚠️ App response: %s | %s", status, body)

            viewer.close()
            lot_page.close()

            # Rate limit small delay
            time.sleep(1.2)

        browser.close()
        log.info("DONE.")


if __name__ == "__main__":
    run()