import os
import re
import json
import time
import logging
from io import BytesIO
from urllib.parse import urljoin, urlparse, parse_qs

import pytesseract
import pypdfium2
from PIL import Image


def ocr_pdf_bytes(pdf_bytes: bytes) -> str:
    pdf = pypdfium2.PdfDocument(pdf_bytes)
    pages = pdf.render(scale=2)  # melhora OCR
    full_text = ""

    for page in pages:
        img = Image.fromarray(page.to_numpy())
        txt = pytesseract.image_to_string(img)
        full_text += "\n" + txt

    return full_text
    
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
log = logging.getLogger("taxdeed-gh-fixed")


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


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def pdf_text_with_fallback(pdf_bytes: bytes) -> str:
    """Try extract_text; if empty, rebuild from extract_words."""
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        parts = []
        for page in pdf.pages:
            txt = page.extract_text() or ""
            txt = txt.strip()
            if txt:
                parts.append(txt)
                continue

            # fallback words
            words = page.extract_words() or []
            if words:
                line = " ".join([w.get("text", "") for w in words if w.get("text")])
                line = line.strip()
                if line:
                    parts.append(line)

        return "\n".join(parts).strip()


def parse_address_from_pdf(pdf_bytes: bytes) -> dict:
    text = pdf_text_with_fallback(pdf_bytes)
    if not text:
        return {"marker_found": False, "address": None, "city": None, "state": None, "zip": None, "snippet": ""}

    # marcador tolerante (ignora múltiplos espaços / quebras)
    marker_re = re.compile(r"ADDRESS\s+ON\s+RECORD\s+ON\s+CURRENT\s+TAX\s+ROLL\s*:", re.I)
    m = marker_re.search(text)
    if not m:
        return {"marker_found": False, "address": None, "city": None, "state": None, "zip": None, "snippet": text[:900]}

    after = text[m.end():].strip()
    snippet = after[:800]
    lines = [ln.strip() for ln in snippet.splitlines() if ln.strip()]

    street = lines[0] if lines else None
    line2 = lines[1] if len(lines) > 1 else ""

    line2u = normalize_ws(line2).upper()
    m2 = re.search(r"^(?P<city>[A-Z .'-]+),?\s+(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)$", line2u)
    if m2:
        return {
            "marker_found": True,
            "address": street,
            "city": m2.group("city").title().strip(),
            "state": m2.group("state"),
            "zip": m2.group("zip"),
            "snippet": snippet[:400]
        }

    joined = normalize_ws(" ".join(lines[:5])).upper()
    m3 = re.search(r"(?P<city>[A-Z .'-]+),?\s+(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)", joined)
    return {
        "marker_found": True,
        "address": street,
        "city": m3.group("city").title().strip() if m3 else None,
        "state": m3.group("state") if m3 else None,
        "zip": m3.group("zip") if m3 else None,
        "snippet": snippet[:400]
    }


def must_be_pdf(headers: dict) -> bool:
    ct = (headers.get("content-type") or "").lower()
    return "application/pdf" in ct or ct.endswith("/pdf")


def extract_rows_from_printable(page) -> list[dict]:
    """
    Extrai os dados diretamente da tabela printable.
    Como não temos o HTML aqui, fazemos um método robusto:
    - pegar todos os links Tax Sale e capturar a linha (tr) mais próxima
    - extrair o texto das células da linha como fallback
    """
    rows = []
    links = page.locator("a:has-text('Tax Sale')")
    total = links.count()
    log.info("Tax Sale links found: %d", total)

    for i in range(total):
        a = links.nth(i)
        href = a.get_attribute("href")
        if not href:
            continue

        # pega o texto do TR (linha) pra tentar extrair campos
        # (subimos para o TR mais próximo)
        try:
            tr_text = a.locator("xpath=ancestor::tr[1]").inner_text(timeout=2000)
        except Exception:
            tr_text = ""

        tr_text_clean = normalize_ws(tr_text)

        # tenta pegar “node=DOC...” do href
        full = urljoin(page.url, href)
        q = parse_qs(urlparse(full).query)
        node = (q.get("node") or [None])[0]

        rows.append({
            "node": node,
            "tax_sale_url": full,
            "row_text": tr_text_clean
        })

    return rows


def parse_fields_from_row_text(row_text: str) -> dict:
    """
    Parser “best-effort” baseado no texto completo da linha.
    Você pode refinar depois com colunas reais.
    """
    txt = row_text

    def pick(pattern):
        m = re.search(pattern, txt, re.I)
        return m.group(1).strip() if m else None

    # esses padrões podem precisar ajuste conforme o formato da tabela
    parcel = pick(r"Parcel\s*Number[:\s]*([0-9A-Z\-]+)")
    sale_date = pick(r"Sale\s*Date[:\s]*([0-9/]+)")
    opening_bid = pick(r"Opening\s*Bid[:\s]*\$?([0-9,]+\.\d{2}|[0-9,]+)")
    application = pick(r"Application\s*Number[:\s]*([0-9A-Z\-]+)")
    deed_status = pick(r"Deed\s*Status[:\s]*([A-Za-z ]+)")
    homestead = pick(r"Homestead[:\s]*(Yes|No|Y|N)")

    return {
        "parcel_number": parcel or "",
        "sale_date": sale_date or "",
        "opening_bid": opening_bid or "",
        "application_number": application or "",
        "deed_status": deed_status or "",
        "homestead": homestead or "",
    }


def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        # Login / disclaimer
        log.info("OPEN: %s", LOGIN_URL)
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=MAX_WAIT)

        click_any(page, [
            "text=I Acknowledge",
            "button:has-text('I Acknowledge')",
            "a:has-text('I Acknowledge')",
            "input[value='I Acknowledge']",
        ], "I Acknowledge")
        wait_network(page)

        # Search
        log.info("OPEN SEARCH: %s", SEARCH_URL)
        page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=MAX_WAIT)
        wait_network(page)

        # DeedStatusID=AS
        if page.locator("select[name='DeedStatusID']").count() > 0:
            page.select_option("select[name='DeedStatusID']", value="AS")
            log.info("Set DeedStatusID=AS")

        click_any(page, [
            "input[type='submit'][value='Search']",
            "button:has-text('Search')",
            "text=Search"
        ], "Search")
        page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
        wait_network(page, 30_000)
        log.info("After Search URL: %s", page.url)

        # Printable
        click_any(page, [
            "text=Printable Version",
            "a:has-text('Printable Version')",
            "button:has-text('Printable Version')"
        ], "Printable Version")
        page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
        wait_network(page, 30_000)
        log.info("After Printable URL: %s", page.url)

        if DEBUG_HTML:
            html_len = len(page.content())
            log.info("Printable HTML length: %d", html_len)

        # ✅ Capturar TODOS os lotes (href + texto da linha) antes de navegar
        all_rows = extract_rows_from_printable(page)
        if not all_rows:
            log.error("Nenhuma linha/lot encontrada na printable.")
            browser.close()
            return

        # Pegue os primeiros MAX_LOTS que tenham node
        selected = [r for r in all_rows if r.get("node")][:MAX_LOTS]
        log.info("Selected lots: %s", [r["node"] for r in selected])

        for idx, lot in enumerate(selected, start=1):
            node = lot["node"]
            tax_sale_url = lot["tax_sale_url"]
            row_text = lot["row_text"]

            fields = parse_fields_from_row_text(note:=row_text)
            log.info("----- LOT %d/%d node=%s -----", idx, len(selected), node)
            log.info("Row text: %s", note[:200])

            # Abre viewer (Tax Sale url é viewDoc.jsp?node=...)
            page.goto(tax_sale_url, wait_until="domcontentloaded", timeout=MAX_WAIT)
            wait_network(page)
            viewer_url = page.url
            log.info("Viewer URL: %s", viewer_url)

            # Link do PDF dentro do viewer
            pdf_a = page.locator("a[href*='Property_Information.pdf']")
            href_pdf = pdf_a.first.get_attribute("href") if pdf_a.count() else None

            if not href_pdf:
                viewer_html = page.content()
                m = re.search(r'href="([^"]*Property_Information\.pdf[^"]*)"', viewer_html, re.I)
                href_pdf = m.group(1) if m else None

            if not href_pdf:
                log.error("PDF link não encontrado no viewer.")
                # volta printable e segue
                page.go_back(); page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
                continue

            pdf_url = urljoin(viewer_url, href_pdf)
            log.info("PDF URL: %s", pdf_url)

            pdf_resp = context.request.get(pdf_url, timeout=MAX_WAIT)
            log.info("PDF HTTP status: %s", pdf_resp.status)

            if not pdf_resp.ok:
                preview = (pdf_resp.text() or "")[:600]
                log.error("PDF download failed preview:\n%s", preview)
                page.go_back(); page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
                continue

            if not must_be_pdf(pdf_resp.headers):
                preview = (pdf_resp.text() or "")[:800]
                log.error("Not a PDF response preview:\n%s", preview)
                page.go_back(); page.wait_for_load_state("domcontentloaded", timeout=MAX_WAIT)
                continue

            addr = parse_address_from_pdf(pdf_resp.body())

            payload = {
                "county": "Orange",
                "state": "FL",
                "node": node,
                "parcel_number": fields.get("parcel_number", ""),
                "sale_date": fields.get("sale_date", ""),
                "opening_bid": fields.get("opening_bid", ""),
                "application_number": fields.get("application_number", ""),
                "deed_status": fields.get("deed_status", ""),
                "homestead": fields.get("homestead", ""),
                "pdf_url": pdf_url,
                "address": addr.get("address"),
                "city": addr.get("city"),
                "state": addr.get("state") or "FL",
                "zip": addr.get("zip"),
            }

            print("\n" + "=" * 100)
            print(f"RESULT LOT {idx}")
            print("=" * 100)
            print(json.dumps(payload, indent=2))

            if not addr.get("marker_found"):
                log.warning("Marker not found. Snippet:\n%s", addr.get("snippet"))

            # Volta para printable (usamos goto para ser estável)
            # porque go_back depende do histórico do browser
            page.goto(f"{BASE_URL}/recorder/tdsmweb/applicationSearchResults.jsp?searchId=0&printing=true",
                      wait_until="domcontentloaded", timeout=MAX_WAIT)
            wait_network(page, 30_000)
            time.sleep(1.0)

        log.info("DONE.")
        browser.close()


if __name__ == "__main__":
    run()