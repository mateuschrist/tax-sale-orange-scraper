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

APP_API_BASE = (os.getenv("APP_API_BASE", "") or "").strip().rstrip("/")
APP_API_TOKEN = (os.getenv("APP_API_TOKEN", "") or "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

OCR_SCALE = float(os.getenv("OCR_SCALE", "2.2"))

# Páginas humanas observadas no PDF
PB_PRIMARY_PAGES = [10, 11, 12, 13]
PB_FALLBACK_PAGES = [9, 14]

# Controles do crawler por ID
PALM_BEACH_START_ID = int(os.getenv("PALM_BEACH_START_ID", "64000"))
PALM_BEACH_MAX_IDS = int(os.getenv("PALM_BEACH_MAX_IDS", "50"))
PALM_BEACH_STATE_FILE = os.getenv("PALM_BEACH_STATE_FILE", "state_palm_beach.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
}


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def normalize_bid_for_payload(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    cleaned = re.sub(r"[^0-9.]", "", s.replace(",", ""))
    return cleaned if cleaned else None


def load_state() -> dict:
    try:
        with open(PALM_BEACH_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(data: dict):
    tmp = PALM_BEACH_STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, PALM_BEACH_STATE_FILE)


def get_last_pb_id() -> int | None:
    st = load_state()
    v = st.get("last_pb_id")
    return int(v) if v is not None else None


def set_last_pb_id(pb_id: int):
    st = load_state()
    st["last_pb_id"] = pb_id
    st["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save_state(st)


def post_to_app(payload: dict) -> dict | None:
    if not SEND_TO_APP:
        log.info("APP_API_BASE / APP_API_TOKEN not set → skipping send to app.")
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
                return r.json()

            if r.status_code == 401:
                log.error("INGEST unauthorized → check APP_API_TOKEN == Vercel INGEST_API_TOKEN.")
                return None

            last_err = f"HTTP {r.status_code}: {r.text[:500]}"
        except Exception as e:
            last_err = str(e)

        time.sleep(1.0 * attempt)

    log.error("INGEST failed after retries: %s", last_err)
    return None


def extract_text_from_pdf_pages(pdf_bytes: bytes, human_pages: list[int]) -> str:
    """
    human_pages usa numeração humana: 1,2,3...
    """
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            parts = []
            total = len(pdf.pages)
            for p in human_pages:
                idx = p - 1
                if 0 <= idx < total:
                    txt = (pdf.pages[idx].extract_text() or "").strip()
                    if txt:
                        parts.append(txt)
            return "\n".join(parts).strip()
    except Exception:
        return ""


def ocr_pdf_pages(pdf_bytes: bytes, human_pages: list[int], scale: float = 2.2) -> str:
    pdf = pypdfium2.PdfDocument(pdf_bytes)
    total = len(pdf)
    parts = []

    for p in human_pages:
        idx = p - 1
        if 0 <= idx < total:
            page = pdf[idx]
            bitmap = page.render(scale=scale)
            img = bitmap.to_pil()
            txt = pytesseract.image_to_string(img, config="--psm 6")
            if txt:
                parts.append(txt)

    return "\n".join(parts).strip()


def parse_palm_beach_usps_block(text: str) -> dict:
    """
    Busca bloco tipo:
    You entered:
    1911 NW 150TH AVENUE STE 201
    HOLLYWOOD FL

    ...
    1911 NW 150TH AVE STE 201
    HOLLYWOOD FL 33028-2871
    """
    t = text.replace("\r", "\n")
    t = re.sub(r"[ \t]+", " ", t)

    # Tentativa 1: bloco "You entered"
    m = re.search(
        r"You entered:\s*\n?\s*(.+?)\s*\n\s*([A-Z .'-]+)\s+FL\s*(\d{5}(?:-\d{4})?)?",
        t,
        re.I,
    )
    if m:
        return {
            "address": normalize_ws(m.group(1)),
            "city": normalize_ws(m.group(2)).title(),
            "state": "FL",
            "zip": m.group(3).strip() if m.group(3) else None,
            "source": "YOU_ENTERED_BLOCK",
        }

    # Tentativa 2: linhas consecutivas endereço + city FL ZIP
    lines = [normalize_ws(x) for x in t.splitlines() if normalize_ws(x)]
    for i in range(len(lines) - 1):
        l1 = lines[i]
        l2 = lines[i + 1]

        if re.match(r"^\d{1,6}\s+", l1):
            m2 = re.search(r"^([A-Z .'-]+)\s+FL\s+(\d{5}(?:-\d{4})?)$", l2, re.I)
            if m2:
                return {
                    "address": l1,
                    "city": normalize_ws(m2.group(1)).title(),
                    "state": "FL",
                    "zip": m2.group(2),
                    "source": "ADDRESS_PLUS_CITY_LINE",
                }

    return {
        "address": None,
        "city": None,
        "state": None,
        "zip": None,
        "source": None,
    }


def extract_address_from_tax_certificate(pdf_bytes: bytes) -> dict:
    # 1) tenta texto páginas 10-13
    txt = extract_text_from_pdf_pages(pdf_bytes, PB_PRIMARY_PAGES)
    if txt:
        found = parse_palm_beach_usps_block(txt)
        if found["address"]:
            return found

    # 2) tenta texto páginas 9 e 14
    txt2 = extract_text_from_pdf_pages(pdf_bytes, PB_FALLBACK_PAGES)
    if txt2:
        found = parse_palm_beach_usps_block(txt2)
        if found["address"]:
            return found

    # 3) OCR páginas 10-13
    try:
        ocr1 = ocr_pdf_pages(pdf_bytes, PB_PRIMARY_PAGES, OCR_SCALE)
        if ocr1:
            found = parse_palm_beach_usps_block(ocr1)
            if found["address"]:
                return found
    except Exception as e:
        log.warning("OCR primary pages failed: %s", str(e))

    # 4) OCR páginas 9 e 14
    try:
        ocr2 = ocr_pdf_pages(pdf_bytes, PB_FALLBACK_PAGES, OCR_SCALE)
        if ocr2:
            found = parse_palm_beach_usps_block(ocr2)
            if found["address"]:
                return found
    except Exception as e:
        log.warning("OCR fallback pages failed: %s", str(e))

    return {
        "address": None,
        "city": None,
        "state": None,
        "zip": None,
        "source": None,
    }


def looks_like_case_page(html: str) -> bool:
    """
    Página válida de Case Details.
    """
    t = html.lower()
    return (
        "case details" in t
        or "case number" in t
        or "certificate" in t
        or "auction date" in t
    )


def extract_case_fields_from_html(html: str, current_url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    def pick(label: str):
        pattern = rf"{re.escape(label)}\s*\n?\s*(.+)"
        m = re.search(pattern, text, re.I)
        return normalize_ws(m.group(1)) if m else None

    tax_collector_url = None
    tax_certificate_url = None

    for a in soup.find_all("a", href=True):
        label = normalize_ws(a.get_text(" ", strip=True)).lower()
        href = urljoin(current_url, a["href"])

        if "click here to access the tax collector site" in label or "tax collector site" in label:
            tax_collector_url = href

        if "tax certificate" in label:
            tax_certificate_url = href

    # fallback pro PDF por href
    if not tax_certificate_url:
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if ".pdf" in href or "certificate" in href:
                tax_certificate_url = urljoin(current_url, a["href"])
                break

    return {
        "case_number": pick("Case Number"),
        "certificate": pick("Certificate"),
        "issued": pick("Issued"),
        "parcel_id": pick("Parcel ID"),
        "auction_date": pick("Auction Date"),
        "status": pick("Status"),
        "legal_description": pick("Legal Description"),
        "applicant_names": pick("Applicant Names"),
        "property_owners": pick("Property Owners"),
        "property_address_html": pick("Property Address"),
        "assessed_as": pick("Assessed As"),
        "opening_bid": pick("Opening Bid"),
        "high_bid": pick("High Bid"),
        "tax_collector_url": tax_collector_url,
        "tax_certificate_url": tax_certificate_url,
    }


def build_payload(case_fields: dict, address_data: dict, case_url: str, pdf_url: str | None) -> dict:
    return {
        "county": "PalmBeach",
        "state": "FL",
        "node": case_fields.get("case_number"),

        # conforme você pediu:
        # auction_source_url = link "Click here to access the Tax Collector site"
        "auction_source_url": case_fields.get("tax_collector_url"),

        "tax_sale_id": case_fields.get("case_number"),
        "parcel_number": case_fields.get("parcel_id"),
        "sale_date": case_fields.get("auction_date"),
        "opening_bid": normalize_bid_for_payload(case_fields.get("opening_bid")),
        "deed_status": case_fields.get("status"),
        "applicant_name": case_fields.get("applicant_names"),

        "pdf_url": pdf_url,
        "address": address_data.get("address"),
        "city": address_data.get("city"),
        "state_address": address_data.get("state"),
        "zip": address_data.get("zip"),
        "address_source_marker": address_data.get("source"),

        "status": "new",
        "notes": None,
    }


def fetch_case(session: requests.Session, pb_id: int) -> tuple[str, str] | tuple[None, None]:
    url = DETAILS_URL.format(id=pb_id)
    r = session.get(url, timeout=60)

    if r.status_code != 200:
        return None, None

    if not looks_like_case_page(r.text):
        return None, None

    return r.url, r.text


def run_palm_beach():
    log.info("=== Palm Beach start ===")

    s = requests.Session()
    s.headers.update(HEADERS)

    last_id = get_last_pb_id()
    start_id = (last_id + 1) if last_id else PALM_BEACH_START_ID
    end_id = start_id + PALM_BEACH_MAX_IDS

    log.info("Palm Beach scanning IDs from %s to %s", start_id, end_id - 1)

    for pb_id in range(start_id, end_id):
        log.info("Palm Beach id=%s", pb_id)

        try:
            case_url, html = fetch_case(s, pb_id)
            if not case_url or not html:
                log.info("Palm Beach id=%s not a valid case page", pb_id)
                continue

            case_fields = extract_case_fields_from_html(html, case_url)

            # Sem case number, não vale processar
            if not case_fields.get("case_number"):
                log.info("Palm Beach id=%s missing case number, skipping", pb_id)
                continue

            pdf_url = case_fields.get("tax_certificate_url")
            address_data = {
                "address": None,
                "city": None,
                "state": None,
                "zip": None,
                "source": None,
            }

            if pdf_url:
                log.info("Tax Certificate PDF found: %s", pdf_url)
                pdf_resp = s.get(pdf_url, timeout=120)
                pdf_resp.raise_for_status()

                content_type = (pdf_resp.headers.get("content-type") or "").lower()
                if "pdf" in content_type:
                    address_data = extract_address_from_tax_certificate(pdf_resp.content)
                else:
                    log.warning("Tax Certificate did not return a PDF: %s", content_type)
            else:
                log.warning("Tax Certificate PDF link not found for %s", case_url)

            payload = build_payload(case_fields, address_data, case_url, pdf_url)

            print("\n" + "=" * 100)
            print(f"PALM BEACH RESULT ID {pb_id}")
            print("=" * 100)
            print(json.dumps(payload, indent=2))

            if SEND_TO_APP:
                ingest_result = post_to_app(payload)
                if ingest_result:
                    log.info("INGEST OK: %s", ingest_result)
                    set_last_pb_id(pb_id)
                else:
                    log.warning("INGEST failed for Palm Beach id=%s", pb_id)
            else:
                # mesmo sem ingest, salva progresso local
                set_last_pb_id(pb_id)

            time.sleep(2.0)

        except Exception as e:
            log.exception("Palm Beach case failed for id=%s: %s", pb_id, str(e))
