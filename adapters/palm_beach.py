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
# PDF EXTRACTION
# =========================
def build_adaptive_page_order(total_pages: int) -> list[int]:
    """
    Começa em 10 e 11, depois expande para perto.
    Usa numeração humana: 1..N
    """
    preferred = [10, 11]
    seen = set()
    order = []

    for p in preferred:
        if 1 <= p <= total_pages and p not in seen:
            order.append(p)
            seen.add(p)

    offsets = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    bases = [10, 11]

    for off in offsets:
        for base in bases:
            for candidate in (base - off, base + off):
                if 1 <= candidate <= total_pages and candidate not in seen:
                    order.append(candidate)
                    seen.add(candidate)

    for p in range(1, total_pages + 1):
        if p not in seen:
            order.append(p)
            seen.add(p)

    return order


def read_single_pdf_page(pdf_bytes: bytes, page_num: int) -> str:
    """
    page_num em numeração humana
    """
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            idx = page_num - 1
            if 0 <= idx < len(pdf.pages):
                return (pdf.pages[idx].extract_text() or "").strip()
    except Exception:
        pass
    return ""


def ocr_single_pdf_page(pdf_bytes: bytes, page_num: int) -> str:
    """
    page_num em numeração humana
    """
    try:
        doc = pypdfium2.PdfDocument(pdf_bytes)
        idx = page_num - 1
        if 0 <= idx < len(doc):
            img = doc[idx].render(scale=OCR_SCALE).to_pil()
            return pytesseract.image_to_string(img, config="--psm 6").strip()
    except Exception:
        pass
    return ""


def parse_address_from_pdf_text(text: str) -> dict:
    """
    Palm Beach:
    prioridade:
    1) Location Address
    2) Mailing Address
    3) Municipality como fallback de cidade
    """
    if not text:
        return empty_addr()

    t = text.replace("\r", "\n")

    # 1) PRIORIDADE: Location Address
    m_loc = re.search(r"Location Address\s*:\s*(.+)", t, re.I)
    if m_loc:
        street = norm(m_loc.group(1))

        # Municipality como cidade fallback
        m_muni = re.search(r"Municipality\s*:\s*([A-Z][A-Z .'-]+)", t, re.I)
        municipality = norm(m_muni.group(1)).title() if m_muni else None

        # tenta achar city/zip mais completos em qualquer ponto do texto
        m_city_zip = re.search(
            r"([A-Z][A-Z .'-]+)\s+FL\s+(\d{5})(?:-\d{4}|\s+\d{4})?",
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

        return {
            "address": street,
            "city": municipality,
            "state": "FL",
            "zip": None,
            "source": "PDF_LOCATION_ADDRESS",
        }

    # 2) FALLBACK: Mailing Address
    # Exemplo:
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
    """
    Busca adaptativa:
    1) tenta texto página a página, começando em 10 e 11
    2) se não achar, tenta OCR página a página na mesma ordem
    só retorna vazio depois de esgotar as páginas
    """
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            total_pages = len(pdf.pages)
    except Exception:
        total_pages = 0

    if total_pages <= 0:
        return empty_addr()

    page_order = build_adaptive_page_order(total_pages)
    log.info("Adaptive PDF page order: %s", page_order)

    # 1) Texto primeiro
    for p in page_order:
        txt = read_single_pdf_page(pdf_bytes, p)
        if txt:
            addr = parse_address_from_pdf_text(txt)
            if addr.get("address"):
                log.info("Address found in PDF text on page %s (%s)", p, addr.get("source"))
                return addr

    # 2) OCR depois
    for p in page_order:
        txt = ocr_single_pdf_page(pdf_bytes, p)
        if txt:
            addr = parse_address_from_pdf_text(txt)
            if addr.get("address"):
                log.info("Address found in PDF OCR on page %s (%s)", p, addr.get("source"))
                return addr

    return empty_addr()


# =========================
# MAIN
# =========================
def run_palm_beach():
    log.info("=== Palm Beach PDF-only adaptive mode ===")

    s = requests.Session()
    s.headers.update(HEADERS)

    start = (get_last() or (PALM_BEACH_START_ID - 1)) + 1
    end = start + PALM_BEACH_MAX_IDS

    invalid = 0

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

            if case.get("pdf"):
                try:
                    pdf = s.get(case["pdf"], timeout=60)
                    if "pdf" in (pdf.headers.get("content-type") or "").lower():
                        addr = extract_pdf_addr(pdf.content)
                    else:
                        log.warning(
                            "Non-PDF response for id=%s: %s",
                            i,
                            pdf.headers.get("content-type")
                        )
                except Exception as e:
                    log.warning("PDF read failed for id=%s: %s", i, str(e))

            payload = {
                "county": "PalmBeach",
                "state": "FL",
                "node": case.get("case"),
                "auction_source_url": case.get("tax"),
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
