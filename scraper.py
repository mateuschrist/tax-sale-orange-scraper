import os
import re
import json
import time
import logging
from io import BytesIO
from urllib.parse import urljoin

import pdfplumber
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE_URL = "https://or.occompt.com"
LOGIN_URL = f"{BASE_URL}/recorder/web/login.jsp"
SEARCH_URL = f"{BASE_URL}/recorder/tdsmweb/applicationSearch.jsp"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
MAX_LOTS = int(os.getenv("MAX_LOTS", "3"))
DEBUG_HTML = os.getenv("DEBUG_HTML", "false").lower() == "true"

MAX_WAIT = 60_000
ADDRESS_MARKER = "ADDRESS ON RECORD ON CURRENT TAX ROLL:"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("taxdeed-single-tab")


def wait_network(page, timeout=20_000):
    try:
        page.wait_for_load_state("networkidle", timeout=timeout)
    except PWTimeout:
        pass


def click(page, selector: str, label: str):
    page.wait_for_selector(selector, timeout=MAX_WAIT)
    page.click(selector)
    log.info("Clicked: %s (%s)", label, selector)


def click_any(page, selectors: list[str], label: str) -> bool:
    for sel in selectors:
        try:
            if page.locator(sel).count() > 0:
                page.click(sel)
                log.info("Clicked: %s (%s)", label, sel)
                return True
        except Exception:
            continue
    return False


def extract_lot_fields_from_html(html: str) -> dict:
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
        return {"marker_found": False, "address": None, "city": None, "state": None, "zip": None, "snippet": text[:800]}

    after = text.split(ADDRESS_MARKER, 1)[1].strip()
    snippet = after[:700]
    lines = [ln.strip() for ln in snippet.splitlines() if ln.strip()]

    street = lines[0] if lines else None
    line2 = lines[1] if len(lines) > 1 else ""

    m = re.search(r"^(?P<city>[A-Z .'-]+),?\s+(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)$",
                  line2.upper().strip())
    if m:
        return {
            "marker_found": True,
            "address": street,
            "city": m.group("city").title().strip(),
            "state": m.group("state"),
            "zip": m.group("zip"),
            "snippet": snippet[:400]
        }

    joined = " | ".join(lines[:5]).upper()
    m2 = re.search(r"(?P<city>[A-Z .'-]+),?\s+(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)", joined)
    return {
        "marker_found": True,
        "address": street,
        "city": m2.group("city").title().strip() if m2 else None,
        "state": m2.group("state") if m2 else None,
        "zip": m2.group("zip") if m2 else None,
        "snippet": snippet[:400]
    }


def must_be_pdf_headers(headers: dict) -> bool:
    ct = (headers.get("content-type") or "").lower()
    return "application/pdf" in ct or ct.endswith("/pdf")


def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        # 1) Login/Disclaimer
        log.info("OPEN: %s", LOGIN_URL)
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=MAX_WAIT)

        ack_ok = click_any(page, [
            "text=I Acknowledge",
            "button:has-text('I Acknowledge')",
            "a:has-text('I Acknowledge')",
            "input[value='I Acknowledge']",
        ], "I Acknowledge")

        if not ack_ok:
            log.warning("Não achei 'I Acknowledge' automaticamente.")
        wait_network(page)

        # 2) Search page
        log.info("OPEN SEARCH: %s", SEARCH_URL)
        page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=MAX_WAIT)
        wait_network(page)

        # Set DeedStatusID = AS
        set_ok = False
        for sel in ["select[name='DeedStatusID']", "select#DeedStatusID"]:
            try:
                if page.locator(sel).count() > 0:
                    page.select_option(sel, value="AS")
                    set_ok = True
                    log.info("Set DeedStatusID=AS using %s", sel)
                    break
            except Exception:
                pass
        if not set_ok:
            log.warning("Não consegui setar DeedStatusID=AS (ajustar seletor).")

        # Click Search (same tab navigation)
        before = page.url
        log.info("Before Search URL: %s", before)
        click_any(page, [
            "input[type='submit'][value='Search']",
            "button:has-text('Search')",
            "text=Search"
        ], "Search")

        # Esperar navegar / carregar resultados
        page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
        wait_network(page, 30_000)
        log.info("After Search URL: %s", page.url)

        # Click Printable Version (same tab navigation)
        before_print = page.url
        log.info("Before Printable URL: %s", before_print)

        ok = click_any(page, [
            "text=Printable Version",
            "a:has-text('Printable Version')",
            "button:has-text('Printable Version')"
        ], "Printable Version")

        if not ok:
            log.error("Não achei Printable Version.")
            browser.close()
            return

        # Esperar a navegação terminar
        page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
        wait_network(page, 30_000)
        log.info("After Printable URL: %s", page.url)

        if DEBUG_HTML:
            log.info("Printable HTML length: %d", len(page.content()))

        # 3) Links Tax Sale
        tax_links = page.locator("a:has-text('Tax Sale')")
        total = tax_links.count()
        log.info("Tax Sale links found: %d", total)

        if total == 0:
            log.error("Nenhum 'Tax Sale' encontrado. (texto pode ser diferente na página printable)")
            browser.close()
            return

        n = min(total, MAX_LOTS)

        for i in range(n):
            log.info("----- LOT %d/%d -----", i + 1, n)

            # Em single-tab: precisamos capturar o href e navegar
            href = tax_links.nth(i).get_attribute("href")
            if not href:
                log.error("Link Tax Sale sem href no índice %d", i)
                continue

            lot_url = urljoin(page.url, href)
            log.info("OPEN LOT: %s", lot_url)

            page.goto(lot_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
            wait_network(page)

            lot_html = page.content()
            lot_data = extract_lot_fields_from_html(lot_html)
            log.info("Lot extracted fields: %s", lot_data)

            # 4) View Property Information (viewer)
            vpi = page.locator("a:has-text('View Property Information'), button:has-text('View Property Information')")
            if vpi.count() == 0:
                log.error("Não achei 'View Property Information' no lote %d", i + 1)
                # voltar pro printable e continuar
                page.go_back()
                page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
                wait_network(page)
                continue

            # clicar e esperar carregar viewer na mesma aba
            vpi.first.click()
            page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
            wait_network(page)
            viewer_url = page.url
            log.info("Viewer URL: %s", viewer_url)

            # 5) Capturar href do PDF dentro do viewer
            pdf_a = page.locator("a[href*='Property_Information.pdf']")
            href_pdf = pdf_a.first.get_attribute("href") if pdf_a.count() else None

            if not href_pdf:
                viewer_html = page.content()
                m = re.search(r'href="([^"]*Property_Information\.pdf[^"]*)"', viewer_html, re.I)
                href_pdf = m.group(1) if m else None

            if not href_pdf:
                log.error("Não encontrei link do PDF no viewer.")
                # voltar para printable: 2 backs (viewer -> lot -> printable)
                page.go_back(); page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
                page.go_back(); page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
                wait_network(page)
                continue

            pdf_url = urljoin(viewer_url, href_pdf)
            log.info("PDF URL: %s", pdf_url)

            # 6) Baixar PDF via mesma sessão
            pdf_resp = context.request.get(pdf_url, timeout=MAX_WAIT)
            log.info("PDF HTTP status: %s", pdf_resp.status)

            if not pdf_resp.ok:
                preview = (pdf_resp.text() or "")[:600]
                log.error("PDF download failed. Preview:\n%s", preview)
                # voltar para printable
                page.go_back(); page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
                page.go_back(); page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
                wait_network(page)
                continue

            if not must_be_pdf_headers(pdf_resp.headers):
                preview = (pdf_resp.text() or "")[:800]
                log.error("PDF response not application/pdf. Preview:\n%s", preview)
                # voltar para printable
                page.go_back(); page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
                page.go_back(); page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
                wait_network(page)
                continue

            addr = parse_address_from_pdf_bytes(pdf_resp.body())

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

            print("\n" + "=" * 100)
            print(f"RESULT LOT {i+1}")
            print("=" * 100)
            print(json.dumps(payload, indent=2))

            if not addr.get("marker_found"):
                log.warning("Marker not found. Snippet:\n%s", addr.get("snippet"))

            # 7) Voltar para printable (viewer -> lot -> printable)
            page.go_back()
            page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
            page.go_back()
            page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
            wait_network(page)

            # pequeno delay para evitar rate-limit
            time.sleep(1.0)

        log.info("DONE.")
        browser.close()


if __name__ == "__main__":
    run()