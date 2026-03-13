import os
import re
import json
import time
import logging
from io import BytesIO
from urllib.parse import urljoin
from datetime import date, timedelta

import requests
import pdfplumber
import pypdfium2
import pytesseract
from bs4 import BeautifulSoup


log = logging.getLogger("taxdeed-palm-beach")

BASE_URL = "https://taxdeed.mypalmbeachclerk.com/"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
}

APP_API_BASE = (os.getenv("APP_API_BASE", "") or "").strip().rstrip("/")
APP_API_TOKEN = (os.getenv("APP_API_TOKEN", "") or "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

OCR_SCALE = float(os.getenv("OCR_SCALE", "2.2"))
PB_PRIMARY_PAGES = [10, 11, 12, 13]   # páginas humanas
PB_FALLBACK_PAGES = [9, 14]


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


def build_date_range():
    today = date.today()
    one_year = today + timedelta(days=365)
    return today.strftime("%m/%d/%Y"), one_year.strftime("%m/%d/%Y")


def extract_text_from_pdf_pages(pdf_bytes: bytes, human_pages: list[int]) -> str:
    """
    human_pages usa número humano: 1,2,3...
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


def extract_case_fields_from_html(html: str) -> dict:
    """
    Parser genérico da tela de detalhes, com estrutura:
    Label | Value
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    def pick(label: str):
        pattern = rf"{re.escape(label)}\s*\n?\s*(.+)"
        m = re.search(pattern, text, re.I)
        return normalize_ws(m.group(1)) if m else None

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
    }


def find_tax_certificate_link(case_html: str, current_url: str) -> str | None:
    soup = BeautifulSoup(case_html, "html.parser")

    # Procura link com texto "Tax Certificate"
    for a in soup.find_all("a", href=True):
        label = normalize_ws(a.get_text(" ", strip=True))
        if "tax certificate" in label.lower():
            return urljoin(current_url, a["href"])

    # fallback por href contendo certificate/pdf
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if "certificate" in href or ".pdf" in href:
            return urljoin(current_url, a["href"])

    return None


def parse_search_results_for_case_links(html: str, current_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = urljoin(current_url, href)
        txt = normalize_ws(a.get_text(" ", strip=True))

        # Ajuste isso se o site usar outro padrão
        if "case" in txt.lower() or "details" in txt.lower() or "auction" in href.lower():
            links.append(full)

    # remove duplicados preservando ordem
    seen = set()
    out = []
    for x in links:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def build_payload(case_fields: dict, address_data: dict, case_url: str, pdf_url: str | None) -> dict:
    return {
        "county": "PalmBeach",
        "state": "FL",
        "node": case_fields.get("case_number"),

        "auction_source_url": case_url,

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


def search_cases(session: requests.Session) -> list[str]:
    """
    Aqui está o único ponto que provavelmente vai precisar de ajuste fino
    depois que você me mandar o HTML real do form / submit.
    """
    from_date, to_date = build_date_range()

    # Primeiro GET na home
    r = session.get(BASE_URL, timeout=60)
    r.raise_for_status()

    # Payload genérico baseado no seu fluxo manual
    payload = {
        "status": "sale",
        "from": from_date,
        "to": to_date,
    }

    # ⚠️ Muito provavelmente esses nomes de campos terão que ser ajustados
    # depois que você me mandar o HTML real do formulário.
    r2 = session.post(BASE_URL, data=payload, timeout=60)
    r2.raise_for_status()

    links = parse_search_results_for_case_links(r2.text, r2.url)
    log.info("Palm Beach case links found: %d", len(links))
    return links


def run_palm_beach():
    log.info("=== Palm Beach start ===")

    s = requests.Session()
    s.headers.update(HEADERS)

    case_links = search_cases(s)
    if not case_links:
        log.warning("No Palm Beach case links found.")
        return

    for idx, case_url in enumerate(case_links, start=1):
        log.info("Palm Beach case %d/%d: %s", idx, len(case_links), case_url)

        try:
            r = s.get(case_url, timeout=60)
            r.raise_for_status()

            case_fields = extract_case_fields_from_html(r.text)
            pdf_link = find_tax_certificate_link(r.text, r.url)

            address_data = {
                "address": None,
                "city": None,
                "state": None,
                "zip": None,
                "source": None,
            }

            if pdf_link:
                log.info("Tax Certificate PDF found: %s", pdf_link)
                pdf_resp = s.get(pdf_link, timeout=120)
                pdf_resp.raise_for_status()

                content_type = (pdf_resp.headers.get("content-type") or "").lower()
                if "pdf" in content_type:
                    address_data = extract_address_from_tax_certificate(pdf_resp.content)
                else:
                    log.warning("Tax Certificate did not return a PDF: %s", content_type)
            else:
                log.warning("Tax Certificate PDF link not found for %s", case_url)

            payload = build_payload(case_fields, address_data, r.url, pdf_link)

            print("\n" + "=" * 100)
            print(f"PALM BEACH RESULT {idx}")
            print("=" * 100)
            print(json.dumps(payload, indent=2))

            if SEND_TO_APP:
                ingest_result = post_to_app(payload)
                if ingest_result:
                    log.info("INGEST OK: %s", ingest_result)

            time.sleep(2.0)

        except Exception as e:
            log.exception("Palm Beach case failed: %s", str(e))
