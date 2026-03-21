import os
import re
import json
import time
import logging
from io import BytesIO
from urllib.parse import urljoin

import requests
import pdfplumber
import pypdfium2
import pytesseract
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

log = logging.getLogger("taxdeed-palmbeach")

BASE_URL = "https://taxdeed.mypalmbeachclerk.com"
DETAILS_URL = BASE_URL + "/Home/Details?id={id}"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

APP_API_BASE = (os.getenv("APP_API_BASE", "") or "").strip().rstrip("/")
APP_API_TOKEN = (os.getenv("APP_API_TOKEN", "") or "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

OCR_SCALE = float(os.getenv("OCR_SCALE", "2.2"))

# Observado nos PDFs Palm Beach
PB_PRIMARY_PAGES = [9, 10, 11, 12, 13]
PB_FALLBACK_PAGES = [8, 14]

PALM_BEACH_START_ID = int(os.getenv("PALM_BEACH_START_ID", "64600"))
PALM_BEACH_MAX_IDS = int(os.getenv("PALM_BEACH_MAX_IDS", "150"))

STATE_FILE = os.getenv("PALM_BEACH_STATE_FILE", "state_palm_beach.json")


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


def get_last():
    return load_state().get("last_pb_id")


def set_last(i: int):
    s = load_state()
    s["last_pb_id"] = i
    s["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save_state(s)


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


# =========================
# PDF EXTRACTION
# =========================
def read_pdf_pages(pdf_bytes: bytes, pages: list[int]) -> str:
    """
    pages em numeração humana: 1,2,3...
    """
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            out = []
            total = len(pdf.pages)
            for p in pages:
                idx = p - 1
                if 0 <= idx < total:
                    txt = (pdf.pages[idx].extract_text() or "").strip()
                    if txt:
                        out.append(txt)
            return "\n".join(out).strip()
    except Exception:
        return ""


def ocr_pdf_pages(pdf_bytes: bytes, pages: list[int]) -> str:
    try:
        doc = pypdfium2.PdfDocument(pdf_bytes)
        out = []
        total = len(doc)

        for p in pages:
            idx = p - 1
            if 0 <= idx < total:
                img = doc[idx].render(scale=OCR_SCALE).to_pil()
                txt = pytesseract.image_to_string(img, config="--psm 6")
                if txt:
                    out.append(txt)

        return "\n".join(out).strip()
    except Exception:
        return ""


def parse_address_from_pdf_text(text: str) -> dict:
    """
    Palm Beach:
    1) prioridade = Location Address
    2) fallback = Mailing Address
    """
    if not text:
        return empty_addr()

    t = text.replace("\r", "\n")

    # 1) Location Address: 1130 WYNNEWOOD DR
    m_loc = re.search(r"Location Address\s*:\s*(.+)", t, re.I)
    if m_loc:
        street = norm(m_loc.group(1))

        # busca cidade/zip em qualquer ponto do texto
        m_city_zip = re.search(
            r"([A-Z][A-Z ]+)\s+FL\s+(\d{5})(?:-\d{4}|\s+\d{4})?",
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

        # mesmo sem city/zip, retorna o endereço
        return {
            "address": street,
            "city": None,
            "state": "FL",
            "zip": None,
            "source": "PDF_LOCATION_ADDRESS",
        }

    # 2) Mailing Address block
    # Ex:
    # Mailing Address
    # 1130 WYNNEWOOD DR
    # WEST PALM BEACH FL 33417 5638
    m_mail = re.search(
        r"Mailing Address\s*\n+\s*(.+?)\s*\n+\s*([A-Z][A-Z ]+)\s+FL\s+(\d{5})(?:-\d{4}|\s+\d{4})?",
        t,
        re.I | re.S,
    )
    if m_mail:
        return {
            "address": norm(m_mail.group(1)),
            "city": norm(m_mail.group(2)).title(),
            "state": "FL",
            "zip": m_mail.group(3),
            "source": "PDF_MAILING_ADDRESS",
        }

    return empty_addr()


def extract_pdf_addr(pdf_bytes: bytes) -> dict:
    # 1) texto páginas principais
    txt = read_pdf_pages(pdf_bytes, PB_PRIMARY_PAGES)
    addr = parse_address_from_pdf_text(txt)
    if addr.get("address"):
        return addr

    # 2) texto páginas fallback
    txt2 = read_pdf_pages(pdf_bytes, PB_FALLBACK_PAGES)
    addr = parse_address_from_pdf_text(txt2)
    if addr.get("address"):
        return addr

    # 3) OCR principais
    ocr1 = ocr_pdf_pages(pdf_bytes, PB_PRIMARY_PAGES)
    addr = parse_address_from_pdf_text(ocr1)
    if addr.get("address"):
        return addr

    # 4) OCR fallback
    ocr2 = ocr_pdf_pages(pdf_bytes, PB_FALLBACK_PAGES)
    addr = parse_address_from_pdf_text(ocr2)
    if addr.get("address"):
        return addr

    return empty_addr()


# =========================
# TAX COLLECTOR FALLBACK (PLAYWRIGHT)
# =========================
def parse_tax_collector_text(body_text: str) -> dict:
    """
    Procura:
    Property Address:
    1130 WYNNEWOOD DR
    WEST PALM BEACH, FL 33417
    """
    if not body_text:
        return empty_addr()

    t = body_text.replace("\r", "\n")

    patterns = [
        r"Property Address\s*:\s*\n+\s*(.+?)\s*\n+\s*([A-Z][A-Z .'-]+),?\s*FL\s*(\d{5})",
        r"Property Address\s*\n+\s*(.+?)\s*\n+\s*([A-Z][A-Z .'-]+),?\s*FL\s*(\d{5})",
        r"Property Address\s*:\s*(.+?),?\s*([A-Z][A-Z .'-]+),?\s*FL\s*(\d{5})",
    ]

    for pat in patterns:
        m = re.search(pat, t, re.I | re.S)
        if m:
            return {
                "address": norm(m.group(1)),
                "city": norm(m.group(2)).title(),
                "state": "FL",
                "zip": m.group(3),
                "source": "TAX_COLLECTOR_PROPERTY_ADDRESS",
            }

    # fallback por varredura de linhas
    lines = [norm(x) for x in t.splitlines() if norm(x)]
    for i, line in enumerate(lines):
        if "property address" in line.lower():
            window = "\n".join(lines[i:i+8])
            m = re.search(
                r"(\d{1,6}\s+[A-Z0-9 .'\-#/]+)\s*\n+\s*([A-Z][A-Z .'-]+),?\s*FL\s*(\d{5})",
                window,
                re.I
            )
            if m:
                return {
                    "address": norm(m.group(1)),
                    "city": norm(m.group(2)).title(),
                    "state": "FL",
                    "zip": m.group(3),
                    "source": "TAX_COLLECTOR_PROPERTY_ADDRESS",
                }

    return empty_addr()


def extract_tax_addr_with_playwright(browser, url: str) -> dict:
    """
    Abre o Tax Collector, espera até 10s pelo carregamento,
    e extrai o campo Property Address da página renderizada.
    """
    page = browser.new_page()
    try:
        log.info("Tax Collector fallback open: %s", url)
        page.goto(url, wait_until="domcontentloaded", timeout=30000)

        try:
            page.locator("text=Property Address").first.wait_for(timeout=10000)
        except PWTimeout:
            log.info("Property Address not found within 10s; reading page anyway")

        page.wait_for_timeout(1500)

        body_text = page.locator("body").inner_text(timeout=10000)
        addr = parse_tax_collector_text(body_text)

        if not addr.get("address"):
            log.info("Tax Collector body snippet: %s", body_text[:1200].replace("\n", " "))

        return addr

    except Exception as e:
        log.warning("Tax Collector fallback failed: %s", str(e))
        return empty_addr()
    finally:
        try:
            page.close()
        except Exception:
            pass


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

    for a in soup.find_all("a", href=True):
        label = norm(a.get_text()).lower()
        href = urljoin(url, a["href"])

        if "tax collector" in label:
            tax_url = href

        if "tax certificate" in label:
            pdf_url = href

    return {
        "case": pick("Case Number"),
        "parcel": pick("Parcel ID"),
        "date": pick("Auction Date"),
        "status": pick("Status"),
        "bid": pick("Opening Bid"),
        "applicant": pick("Applicant Names"),
        "tax": tax_url,
        "pdf": pdf_url,
    }


# =========================
# MAIN
# =========================
def run_palm_beach():
    log.info("=== Palm Beach V4.2 ===")

    s = requests.Session()
    s.headers.update(HEADERS)

    start = (get_last() or (PALM_BEACH_START_ID - 1)) + 1
    end = start + PALM_BEACH_MAX_IDS

    invalid = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        for i in range(start, end):
            log.info("Palm Beach ID %s", i)

            try:
                r = s.get(DETAILS_URL.format(id=i), timeout=30)

                if r.status_code != 200 or not is_valid_case(r.text):
                    invalid += 1
                    if invalid > 40:
                        log.warning("Stop: dead range")
                        break
                    continue

                invalid = 0
                case = parse_case(r.text, r.url)

                addr = empty_addr()

                # 1) PDF primeiro
                if case.get("pdf"):
                    try:
                        pdf = s.get(case["pdf"], timeout=60)
                        if "pdf" in (pdf.headers.get("content-type") or "").lower():
                            addr = extract_pdf_addr(pdf.content)
                            if addr.get("address"):
                                log.info("Address found from PDF (%s)", addr.get("source"))
                    except Exception as e:
                        log.warning("PDF read failed for id=%s: %s", i, str(e))

                # 2) Se não trouxe address, tentar Tax Collector com espera de até 10s
                if not addr.get("address") and case.get("tax"):
                    log.info("PDF missing address → trying Tax Collector fallback")
                    tc_addr = extract_tax_addr_with_playwright(browser, case["tax"])
                    if tc_addr.get("address"):
                        addr = tc_addr
                        log.info("Address found from Tax Collector (%s)", addr.get("source"))

                payload = {
                    "county": "PalmBeach",
                    "state": "FL",
                    "node": case.get("case"),
                    "auction_source_url": case.get("tax"),   # conforme você pediu
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

                if send(payload):
                    set_last(i)

                time.sleep(1.5)

            except Exception as e:
                log.error("ERROR %s: %s", i, str(e))

        try:
            browser.close()
        except Exception:
            pass
