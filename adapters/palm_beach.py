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

PB_PRIMARY_PAGES = [10, 11, 12, 13]
PB_FALLBACK_PAGES = [9, 14]

PALM_BEACH_START_ID = int(os.getenv("PALM_BEACH_START_ID", "64600"))
PALM_BEACH_MAX_IDS = int(os.getenv("PALM_BEACH_MAX_IDS", "200"))
PALM_BEACH_STATE_FILE = os.getenv("PALM_BEACH_STATE_FILE", "state_palm_beach.json")

HEADERS = {"User-Agent": "Mozilla/5.0"}


# ================= STATE =================

def load_state():
    try:
        with open(PALM_BEACH_STATE_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

def save_state(data):
    with open(PALM_BEACH_STATE_FILE, "w") as f:
        json.dump(data, f, indent=2)

def get_last_id():
    return load_state().get("last_pb_id")

def set_last_id(val):
    s = load_state()
    s["last_pb_id"] = val
    s["updated_at"] = time.time()
    save_state(s)


# ================= UTILS =================

def normalize_ws(s):
    return re.sub(r"\s+", " ", (s or "")).strip()

def normalize_bid(v):
    if not v:
        return None
    return re.sub(r"[^\d.]", "", str(v))


# ================= PDF =================

def extract_text(pdf_bytes, pages):
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            out = []
            for p in pages:
                idx = p - 1
                if idx < len(pdf.pages):
                    txt = pdf.pages[idx].extract_text()
                    if txt:
                        out.append(txt)
            return "\n".join(out)
    except:
        return ""


def ocr_text(pdf_bytes, pages):
    doc = pypdfium2.PdfDocument(pdf_bytes)
    out = []
    for p in pages:
        idx = p - 1
        if idx < len(doc):
            img = doc[idx].render(scale=OCR_SCALE).to_pil()
            out.append(pytesseract.image_to_string(img))
    return "\n".join(out)


def parse_address(text):
    t = text.replace("\r", "\n")

    m = re.search(
        r"You entered:\s*\n?\s*(.+?)\s*\n\s*([A-Z .'-]+)\s+FL\s*(\d{5}(?:-\d{4})?)?",
        t, re.I
    )

    if m:
        return {
            "address": normalize_ws(m.group(1)),
            "city": normalize_ws(m.group(2)).title(),
            "state": "FL",
            "zip": m.group(3)
        }

    return {"address": None, "city": None, "state": None, "zip": None}


def extract_address(pdf_bytes):
    txt = extract_text(pdf_bytes, PB_PRIMARY_PAGES)
    res = parse_address(txt)
    if res["address"]:
        return res

    txt = extract_text(pdf_bytes, PB_FALLBACK_PAGES)
    res = parse_address(txt)
    if res["address"]:
        return res

    try:
        txt = ocr_text(pdf_bytes, PB_PRIMARY_PAGES)
        res = parse_address(txt)
        if res["address"]:
            return res
    except:
        pass

    return {"address": None, "city": None, "state": None, "zip": None}


# ================= HTML =================

def is_valid_case(html):
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
        return normalize_ws(m.group(1)) if m else None

    tax_collector = None
    pdf_link = None

    for a in soup.find_all("a", href=True):
        label = normalize_ws(a.get_text()).lower()
        href = urljoin(url, a["href"])

        if "tax collector" in label:
            tax_collector = href

        if "tax certificate" in label:
            pdf_link = href

    return {
        "case_number": pick("Case Number"),
        "parcel": pick("Parcel ID"),
        "date": pick("Auction Date"),
        "status": pick("Status"),
        "bid": pick("Opening Bid"),
        "applicant": pick("Applicant Names"),
        "tax_url": tax_collector,
        "pdf": pdf_link
    }


# ================= INGEST =================

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
        return r.status_code in (200, 201)
    except:
        return False


# ================= MAIN =================

def run_palm_beach():
    log.info("=== Palm Beach start ===")

    s = requests.Session()
    s.headers.update(HEADERS)

    last = get_last_id()
    start = last + 1 if last else PALM_BEACH_START_ID
    end = start + PALM_BEACH_MAX_IDS

    log.info("Scanning %s → %s", start, end)

    invalid = 0

    for i in range(start, end):
        url = DETAILS_URL.format(id=i)
        log.info("ID %s", i)

        try:
            r = s.get(url, timeout=30)

            if r.status_code != 200 or not is_valid_case(r.text):
                invalid += 1

                if invalid > 40:
                    log.warning("Too many invalid → stopping")
                    break

                continue

            invalid = 0

            case = parse_case(r.text, r.url)

            addr = {"address": None, "city": None, "state": None, "zip": None}

            if case["pdf"]:
                pdf = s.get(case["pdf"], timeout=60)
                if "pdf" in (pdf.headers.get("content-type") or ""):
                    addr = extract_address(pdf.content)

            payload = {
                "county": "PalmBeach",
                "state": "FL",
                "node": case["case_number"],
                "auction_source_url": case["tax_url"],
                "tax_sale_id": case["case_number"],
                "parcel_number": case["parcel"],
                "sale_date": case["date"],
                "opening_bid": normalize_bid(case["bid"]),
                "deed_status": case["status"],
                "applicant_name": case["applicant"],
                "pdf_url": case["pdf"],
                **addr
            }

            print(json.dumps(payload, indent=2))

            if post(payload):
                set_last_id(i)

            time.sleep(1.5)

        except Exception as e:
            log.error("ERROR ID %s: %s", i, str(e))
