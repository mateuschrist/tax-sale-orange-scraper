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

HEADERS = {"User-Agent": "Mozilla/5.0"}

APP_API_BASE = os.getenv("APP_API_BASE", "").strip().rstrip("/")
APP_API_TOKEN = os.getenv("APP_API_TOKEN", "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

OCR_SCALE = 2.2

PB_PRIMARY = [10, 11, 12, 13]
PB_FALLBACK = [9, 14]

START_ID = int(os.getenv("PALM_BEACH_START_ID", "64600"))
MAX_IDS = int(os.getenv("PALM_BEACH_MAX_IDS", "200"))

STATE_FILE = "state_palm_beach.json"


# ================= STATE =================

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

def set_last(i):
    s = load_state()
    s["last_pb_id"] = i
    s["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save_state(s)


# ================= HELPERS =================

def norm(s):
    return re.sub(r"\s+", " ", (s or "")).strip()

def clean_bid(v):
    return re.sub(r"[^\d.]", "", v or "")

def post(payload):
    if not SEND_TO_APP:
        return True

    try:
        r = requests.post(
            f"{APP_API_BASE}/api/ingest",
            json=payload,
            headers={"Authorization": f"Bearer {APP_API_TOKEN}"},
            timeout=30
        )
        log.info("INGEST status=%s", r.status_code)
        if r.text:
            log.info("INGEST response=%s", r.text[:250].replace("\n", " "))
        return r.status_code in (200, 201)
    except Exception as e:
        log.warning("INGEST failed: %s", str(e))
        return False


# ================= PDF =================

def read_pdf(pdf_bytes, pages):
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            out = []
            for p in pages:
                idx = p - 1
                if 0 <= idx < len(pdf.pages):
                    t = pdf.pages[idx].extract_text()
                    if t:
                        out.append(t)
            return "\n".join(out)
    except Exception:
        return ""


def ocr_pdf(pdf_bytes, pages):
    doc = pypdfium2.PdfDocument(pdf_bytes)
    out = []
    for p in pages:
        idx = p - 1
        if 0 <= idx < len(doc):
            img = doc[idx].render(scale=OCR_SCALE).to_pil()
            out.append(pytesseract.image_to_string(img))
    return "\n".join(out)


def parse_address_from_usps_text(txt):
    # bloco "You entered:"
    m = re.search(
        r"You entered:\s*\n?\s*(.+?)\s*\n\s*([A-Z .'-]+)\s+FL\s*(\d{5}(?:-\d{4})?)?",
        txt,
        re.I
    )
    if m:
        return {
            "address": norm(m.group(1)),
            "city": norm(m.group(2)).title(),
            "state": "FL",
            "zip": m.group(3),
            "source": "PDF"
        }

    # fallback por duas linhas consecutivas
    lines = [norm(x) for x in txt.splitlines() if norm(x)]
    for i in range(len(lines) - 1):
        l1 = lines[i]
        l2 = lines[i + 1]
        if re.match(r"^\d{1,6}\s+", l1):
            m2 = re.search(r"^([A-Z .'-]+)\s+FL\s+(\d{5}(?:-\d{4})?)$", l2, re.I)
            if m2:
                return {
                    "address": l1,
                    "city": norm(m2.group(1)).title(),
                    "state": "FL",
                    "zip": m2.group(2),
                    "source": "PDF"
                }

    return None


def extract_pdf_addr(pdf_bytes):
    txt = read_pdf(pdf_bytes, PB_PRIMARY)
    res = parse_address_from_usps_text(txt)
    if res:
        return res

    txt = read_pdf(pdf_bytes, PB_FALLBACK)
    res = parse_address_from_usps_text(txt)
    if res:
        return res

    try:
        txt = ocr_pdf(pdf_bytes, PB_PRIMARY)
        res = parse_address_from_usps_text(txt)
        if res:
            return res
    except Exception as e:
        log.warning("OCR primary failed: %s", str(e))

    try:
        txt = ocr_pdf(pdf_bytes, PB_FALLBACK)
        res = parse_address_from_usps_text(txt)
        if res:
            return res
    except Exception as e:
        log.warning("OCR fallback failed: %s", str(e))

    return None


# ================= TAX COLLECTOR FALLBACK (PLAYWRIGHT) =================

def parse_property_address_text(text: str):
    """
    Procura o campo 'Property Address' na página renderizada do Tax Collector.
    """
    t = text.replace("\r", "\n")

    patterns = [
        # Property Address \n 123 MAIN ST \n CITY FL 33411
        r"Property Address\s*\n+\s*(.+?)\s*\n+\s*([A-Z .'-]+)\s+FL\s*(\d{5}(?:-\d{4})?)",
        # Property Address: 123 MAIN ST, CITY FL 33411
        r"Property Address\s*[:\-]?\s*(.+?),?\s+([A-Z .'-]+)\s+FL\s*(\d{5}(?:-\d{4})?)",
    ]

    for pat in patterns:
        m = re.search(pat, t, re.I)
        if m:
            return {
                "address": norm(m.group(1)),
                "city": norm(m.group(2)).title(),
                "state": "FL",
                "zip": m.group(3),
                "source": "TAX_COLLECTOR"
            }

    # fallback por linhas
    lines = [norm(x) for x in t.splitlines() if norm(x)]
    for i, line in enumerate(lines):
        if line.lower().startswith("property address"):
            window = "\n".join(lines[i:i+8])

            m = re.search(
                r"(\d{1,6}\s+[A-Z0-9 .'\-#/]+)\s*\n+\s*([A-Z .'-]+)\s+FL\s*(\d{5}(?:-\d{4})?)",
                window,
                re.I
            )
            if m:
                return {
                    "address": norm(m.group(1)),
                    "city": norm(m.group(2)).title(),
                    "state": "FL",
                    "zip": m.group(3),
                    "source": "TAX_COLLECTOR"
                }

    return None


def extract_tax_addr_with_playwright(browser, url: str):
    """
    Abre o Tax Collector, espera até 10s o campo 'Property Address' aparecer
    e extrai o endereço da página renderizada.
    """
    page = browser.new_page()
    try:
        log.info("Tax Collector fallback open: %s", url)
        page.goto(url, wait_until="domcontentloaded", timeout=30000)

        # aguarda renderizar o conteúdo
        try:
            page.locator("text=Property Address").first.wait_for(timeout=10000)
        except PWTimeout:
            log.info("Property Address text not found within 10s, collecting body anyway")

        # pequeno respiro extra para o conteúdo preencher
        page.wait_for_timeout(1500)

        body_text = page.locator("body").inner_text(timeout=10000)
        result = parse_property_address_text(body_text)
        if result:
            return result

        log.info("Tax Collector body snippet: %s", body_text[:600].replace("\n", " "))
        return None

    except Exception as e:
        log.warning("Tax Collector fallback failed: %s", str(e))
        return None
    finally:
        try:
            page.close()
        except Exception:
            pass


# ================= HTML =================

def is_valid(html):
    t = html.lower()
    return (
        "case number" in t and
        "parcel id" in t and
        "auction date" in t
    )


def parse_case(html, url):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")

    def pick(label):
        m = re.search(rf"{label}\s*\n\s*(.+)", text, re.I)
        return norm(m.group(1)) if m else None

    tax = None
    pdf = None

    for a in soup.find_all("a", href=True):
        label = norm(a.get_text()).lower()
        href = urljoin(url, a["href"])

        if "tax collector" in label:
            tax = href
        if "tax certificate" in label:
            pdf = href

    return {
        "case": pick("Case Number"),
        "parcel": pick("Parcel ID"),
        "date": pick("Auction Date"),
        "status": pick("Status"),
        "bid": pick("Opening Bid"),
        "applicant": pick("Applicant Names"),
        "tax": tax,
        "pdf": pdf
    }


# ================= MAIN =================

def run_palm_beach():
    log.info("=== Palm Beach V4.1 ===")

    s = requests.Session()
    s.headers.update(HEADERS)

    start = (get_last() or START_ID) + 1
    end = start + MAX_IDS

    invalid = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        for i in range(start, end):
            log.info("ID %s", i)

            try:
                r = s.get(DETAILS_URL.format(id=i), timeout=30)

                if r.status_code != 200 or not is_valid(r.text):
                    invalid += 1
                    if invalid > 40:
                        log.warning("Stop: dead range")
                        break
                    continue

                invalid = 0
                case = parse_case(r.text, r.url)

                addr = None

                # 1) PDF primeiro
                if case["pdf"]:
                    try:
                        pdf = s.get(case["pdf"], timeout=60)
                        if "pdf" in (pdf.headers.get("content-type") or "").lower():
                            addr = extract_pdf_addr(pdf.content)
                            if addr:
                                log.info("Address found from PDF")
                    except Exception as e:
                        log.warning("PDF read failed for id=%s: %s", i, str(e))

                # 2) Se PDF não trouxe endereço → abrir Tax Collector e esperar até 10s
                if (not addr or not addr.get("address")) and case["tax"]:
                    log.info("PDF missing address → trying Tax Collector fallback")
                    tc_addr = extract_tax_addr_with_playwright(browser, case["tax"])
                    if tc_addr:
                        addr = tc_addr
                        log.info("Address found from Tax Collector")

                if not addr:
                    addr = {
                        "address": None,
                        "city": None,
                        "state": None,
                        "zip": None,
                        "source": None
                    }

                payload = {
                    "county": "PalmBeach",
                    "state": "FL",
                    "node": case["case"],
                    "auction_source_url": case["tax"],  # Tax Collector site
                    "tax_sale_id": case["case"],
                    "parcel_number": case["parcel"],
                    "sale_date": case["date"],
                    "opening_bid": clean_bid(case["bid"]),
                    "deed_status": case["status"],
                    "applicant_name": case["applicant"],
                    "pdf_url": case["pdf"],
                    "address": addr["address"],
                    "city": addr["city"],
                    "state_address": addr["state"],
                    "zip": addr["zip"],
                    "address_source": addr["source"]
                }

                print(json.dumps(payload, indent=2))

                if post(payload):
                    set_last(i)

                time.sleep(1.5)

            except Exception as e:
                log.error("ERROR %s: %s", i, e)

        try:
            browser.close()
        except Exception:
            pass
