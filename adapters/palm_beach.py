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
        with open(STATE_FILE) as f:
            return json.load(f)
    except:
        return {}

def save_state(data):
    with open(STATE_FILE, "w") as f:
        json.dump(data, f)

def get_last():
    return load_state().get("last_pb_id")

def set_last(i):
    s = load_state()
    s["last_pb_id"] = i
    save_state(s)


# ================= HELPERS =================

def norm(s):
    return re.sub(r"\s+", " ", (s or "")).strip()

def clean_bid(v):
    return re.sub(r"[^\d.]", "", v or "")


# ================= PDF =================

def read_pdf(pdf_bytes, pages):
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            out = []
            for p in pages:
                if p-1 < len(pdf.pages):
                    t = pdf.pages[p-1].extract_text()
                    if t:
                        out.append(t)
            return "\n".join(out)
    except:
        return ""


def ocr_pdf(pdf_bytes, pages):
    doc = pypdfium2.PdfDocument(pdf_bytes)
    out = []
    for p in pages:
        if p-1 < len(doc):
            img = doc[p-1].render(scale=OCR_SCALE).to_pil()
            out.append(pytesseract.image_to_string(img))
    return "\n".join(out)


def parse_address(txt):
    m = re.search(
        r"You entered:\s*\n?\s*(.+?)\s*\n\s*([A-Z .'-]+)\s+FL\s*(\d{5})?",
        txt, re.I
    )
    if m:
        return {
            "address": norm(m.group(1)),
            "city": norm(m.group(2)).title(),
            "state": "FL",
            "zip": m.group(3),
            "source": "PDF"
        }
    return None


def extract_pdf_addr(pdf_bytes):
    txt = read_pdf(pdf_bytes, PB_PRIMARY)
    res = parse_address(txt)
    if res:
        return res

    txt = read_pdf(pdf_bytes, PB_FALLBACK)
    res = parse_address(txt)
    if res:
        return res

    try:
        txt = ocr_pdf(pdf_bytes, PB_PRIMARY)
        res = parse_address(txt)
        if res:
            return res
    except:
        pass

    return None


# ================= TAX COLLECTOR =================

def extract_tax_addr(session, url):
    try:
        r = session.get(url, timeout=30)
        txt = BeautifulSoup(r.text, "html.parser").get_text("\n")

        m = re.search(
            r"(\d{1,6}\s+[A-Z0-9 .'-]+)\s*\n\s*([A-Z .'-]+),?\s*FL\s*(\d{5})",
            txt, re.I
        )
        if m:
            return {
                "address": norm(m.group(1)),
                "city": norm(m.group(2)).title(),
                "state": "FL",
                "zip": m.group(3),
                "source": "TAX_COLLECTOR"
            }
    except Exception as e:
        log.warning("Tax collector fail: %s", e)

    return None


# ================= HTML =================

def is_valid(html):
    t = html.lower()
    return "case number" in t and "parcel id" in t


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


# ================= INGEST =================

def send(payload):
    if not SEND_TO_APP:
        return True

    try:
        r = requests.post(
            f"{APP_API_BASE}/api/ingest",
            json=payload,
            headers={"Authorization": f"Bearer {APP_API_TOKEN}"},
            timeout=30
        )
        return r.status_code in (200, 201)
    except:
        return False


# ================= MAIN =================

def run_palm_beach():
    log.info("=== Palm Beach V4 ===")

    s = requests.Session()
    s.headers.update(HEADERS)

    start = (get_last() or START_ID) + 1
    end = start + MAX_IDS

    invalid = 0

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

            # 1 PDF
            if case["pdf"]:
                pdf = s.get(case["pdf"], timeout=60)
                if "pdf" in (pdf.headers.get("content-type") or ""):
                    addr = extract_pdf_addr(pdf.content)

            # 2 Tax Collector fallback
            if not addr and case["tax"]:
                addr = extract_tax_addr(s, case["tax"])

            if not addr:
                addr = {"address": None, "city": None, "state": None, "zip": None, "source": None}

            payload = {
                "county": "PalmBeach",
                "state": "FL",
                "node": case["case"],
                "auction_source_url": case["tax"],
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

            if send(payload):
                set_last(i)

            time.sleep(1.5)

        except Exception as e:
            log.error("ERROR %s: %s", i, e)
