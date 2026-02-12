import re
import sys
import logging
from io import BytesIO
from urllib.parse import urljoin

import pdfplumber
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# CONFIG
# =========================
BASE_URL = "https://or.occompt.com"
LOGIN_URL = f"{BASE_URL}/recorder/web/login.jsp"
LIST_URL = f"{BASE_URL}/recorder/tdsmweb/applicationSearchResults.jsp?searchId=0&printing=true"

MAX_WAIT = 60000
ADDRESS_MARKER = "ADDRESS ON RECORD ON CURRENT TAX ROLL:"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("taxdeed-test")


# =========================
# HELPERS
# =========================
def try_click_acknowledge(page) -> bool:
    candidates = [
        "text=I Acknowledge",
        "button:has-text('I Acknowledge')",
        "a:has-text('I Acknowledge')",
        "input[value='I Acknowledge']",
    ]
    for sel in candidates:
        try:
            if page.locator(sel).count() > 0:
                page.click(sel)
                return True
        except Exception:
            pass
    return False


def extract_from_lot_html(html: str) -> dict:
    """Regex simples para diagnóstico (rápido de ajustar)."""
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
    """Extrai address/city/state/zip do PDF."""
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        pages_text = []
        for p in pdf.pages:
            t = p.extract_text() or ""
            if t:
                pages_text.append(t)
        text = "\n".join(pages_text)

    if ADDRESS_MARKER not in text:
        return {
            "address": None, "city": None, "state": None, "zip": None,
            "marker_found": False,
            "text_snippet": text[:700] if text else None,
        }

    after = text.split(ADDRESS_MARKER, 1)[1].strip()
    snippet = after[:600]
    lines = [ln.strip() for ln in snippet.splitlines() if ln.strip()]

    street = lines[0] if lines else None
    line2 = lines[1] if len(lines) > 1 else ""

    m = re.search(
        r"^(?P<city>[A-Z .'-]+),?\s+(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)$",
        line2.upper().strip()
    )
    if m:
        return {
            "address": street,
            "city": m.group("city").title().strip(),
            "state": m.group("state"),
            "zip": m.group("zip"),
            "marker_found": True,
            "text_snippet": snippet[:400],
        }

    # fallback: busca city/state/zip em qualquer linha
    joined = " | ".join(lines[:5]).upper()
    m2 = re.search(r"(?P<city>[A-Z .'-]+),?\s+(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)", joined)
    return {
        "address": street,
        "city": m2.group("city").title().strip() if m2 else None,
        "state": m2.group("state") if m2 else None,
        "zip": m2.group("zip") if m2 else None,
        "marker_found": True,
        "text_snippet": snippet[:400],
    }


def pretty_print(title: str, data):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)
    if isinstance(data, dict):
        for k, v in data.items():
            print(f"{k}: {v}")
    else:
        print(data)


# =========================
# MAIN
# =========================
def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # headless=False pra você ver no teste
        context = browser.new_context()
        page = context.new_page()

        pretty_print("STEP 1 — OPEN LOGIN/DISCLAIMER", LOGIN_URL)
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=MAX_WAIT)

        ack = try_click_acknowledge(page)
        pretty_print("ACKNOWLEDGE CLICKED?", ack)

        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except PWTimeout:
            pass

        pretty_print("STEP 2 — OPEN LIST URL", LIST_URL)
        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=MAX_WAIT)

        pretty_print("CURRENT URL AFTER LIST NAV", page.url)

        if "login.jsp" in page.url:
            pretty_print(
                "ERROR",
                "Você caiu no login ao abrir a lista. Isso normalmente significa que o searchId=0 não está válido "
                "ou a sessão não foi aceita. O caminho robusto é automatizar o Search pra gerar um searchId válido."
            )
            browser.close()
            sys.exit(1)

        # Pega o PRIMEIRO link "Tax Sale"
        links = page.locator("a:has-text('Tax Sale')")
        total = links.count()
        pretty_print("FOUND 'Tax Sale' LINKS", total)

        if total == 0:
            pretty_print(
                "ERROR",
                "Nenhum link 'Tax Sale' encontrado na lista. Possíveis causas: página sem resultados ou texto diferente."
            )
            browser.close()
            sys.exit(1)

        pretty_print("STEP 3 — OPEN FIRST LOT (NEW TAB)", "Clicking first Tax Sale...")
        with context.expect_page() as pop:
            links.nth(0).click()
        lot_page = pop.value
        lot_page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)

        pretty_print("LOT PAGE URL", lot_page.url)

        lot_html = lot_page.content()
        lot_data = extract_from_lot_html(lot_html)
        pretty_print("LOT DATA (FROM HTML)", lot_data)

        # Abrir viewer "View Property Information"
        vpi = lot_page.locator("a:has-text('View Property Information'), button:has-text('View Property Information')")
        vpi_count = vpi.count()
        pretty_print("FOUND 'View Property Information' BUTTON/LINKS", vpi_count)

        if vpi_count == 0:
            pretty_print("ERROR", "Não achei 'View Property Information' no lote.")
            lot_page.close()
            browser.close()
            sys.exit(1)

        pretty_print("STEP 4 — OPEN VIEWER (NEW TAB)", "Clicking VPI...")
        with context.expect_page() as vpop:
            vpi.first.click()
        viewer = vpop.value
        viewer.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)

        pretty_print("VIEWER URL", viewer.url)

        # Captura link real do PDF dentro do viewer
        pdf_a = viewer.locator("a[href*='Property_Information.pdf']")
        count_pdf_links = pdf_a.count()
        pretty_print("FOUND PDF LINKS IN VIEWER", count_pdf_links)

        href = pdf_a.first.get_attribute("href") if count_pdf_links else None

        # fallback regex
        if not href:
            viewer_html = viewer.content()
            m = re.search(r'href="([^"]*Property_Information\.pdf[^"]*)"', viewer_html, re.I)
            href = m.group(1) if m else None

        pretty_print("PDF HREF (RAW)", href)

        if not href:
            pretty_print("ERROR", "Não foi possível capturar o href do Property_Information.pdf dentro do viewer.")
            viewer.close()
            lot_page.close()
            browser.close()
            sys.exit(1)

        pdf_url = urljoin(viewer.url, href)
        pretty_print("PDF URL (RESOLVED)", pdf_url)

        # Baixar PDF via sessão do Playwright (cookies OK)
        pretty_print("STEP 5 — DOWNLOAD PDF (SESSION REQUEST)", "Downloading...")
        resp = context.request.get(pdf_url, timeout=MAX_WAIT)
        pretty_print("PDF RESPONSE STATUS", resp.status)

        # Se não for 200, geralmente você caiu no disclaimer novamente
        if not resp.ok:
            body_preview = (resp.text() or "")[:800]
            pretty_print("PDF DOWNLOAD FAILED — BODY PREVIEW", body_preview)
            viewer.close()
            lot_page.close()
            browser.close()
            sys.exit(1)

        pdf_bytes = resp.body()
        pretty_print("PDF BYTES LENGTH", len(pdf_bytes))

        # Extrair endereço
        pretty_print("STEP 6 — PARSE PDF (ADDRESS)", "Parsing text...")
        addr = parse_address_from_pdf_bytes(pdf_bytes)
        pretty_print("ADDRESS PARSE RESULT", addr)

        # Resultado final
        final = {**lot_data, **{
            "pdf_url": pdf_url,
            "address": addr.get("address"),
            "city": addr.get("city"),
            "state": addr.get("state"),
            "zip": addr.get("zip"),
        }}
        pretty_print("✅ FINAL RESULT (ONE LOT)", final)

        # Fecha tudo
        viewer.close()
        lot_page.close()
        browser.close()


if __name__ == "__main__":
    main()