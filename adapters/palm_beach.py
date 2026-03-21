# =========================
# PALM BEACH V6.5 FINAL
# =========================

import os
import re
import json
import time
import random
import logging
from io import BytesIO
from datetime import date, timedelta
from urllib.parse import urljoin, quote

import requests
import pdfplumber
import pypdfium2
import pytesseract
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

log = logging.getLogger("palmbeach")

BASE_URL = "https://taxdeed.mypalmbeachclerk.com"
STATUS_URL = BASE_URL + "/#tabs-7"

HEADERS = {"User-Agent": "Mozilla/5.0"}

APP_API_BASE = os.getenv("APP_API_BASE", "").strip()
APP_API_TOKEN = os.getenv("APP_API_TOKEN", "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
USE_SUPABASE = bool(SUPABASE_URL and SUPABASE_KEY)

HEADLESS = os.getenv("HEADLESS", "true") == "true"

# =========================
# HELPERS
# =========================

def norm(s):
    return re.sub(r"\s+", " ", (s or "")).strip()

def clean_bid(v):
    return re.sub(r"[^\d.]", "", str(v or ""))

def human_sleep(a=0.2, b=0.6):
    time.sleep(random.uniform(a, b))

# =========================
# DATE
# =========================

def build_dates():
    today = date.today()
    future = today + timedelta(days=365)
    return today.strftime("%m/%d/%Y"), future.strftime("%m/%d/%Y")

# =========================
# ADDRESS VALIDATION
# =========================

def valid_address(a):
    if not a:
        return False
    a = a.upper()
    if "PO BOX" in a:
        return False
    if len(a) < 5:
        return False
    if not re.search(r"\d", a):
        return False
    return True

# =========================
# SUPABASE CHECK
# =========================

def exists_supabase(parcel, date):
    if not USE_SUPABASE:
        return False

    url = f"{SUPABASE_URL}/rest/v1/properties"
    params = {
        "parcel_number": f"eq.{parcel}",
        "sale_date": f"eq.{date}",
        "select": "id",
        "limit": 1
    }

    r = requests.get(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }, params=params)

    return r.status_code == 200 and len(r.json()) > 0

# =========================
# PDF ADDRESS
# =========================

def extract_pdf_address(pdf_bytes):

    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            total = len(pdf.pages)
    except:
        return None

    order = list(range(9, min(14, total)+1)) + list(range(1, total+1))

    for p in order:
        try:
            text = pdfplumber.open(BytesIO(pdf_bytes)).pages[p-1].extract_text()
        except:
            continue

        if not text:
            continue

        m = re.search(r"Location Address:\s*(.+)", text)
        if m:
            addr = norm(m.group(1))
            if valid_address(addr):
                return addr

    return None

# =========================
# PROPERTY APPRAISER
# =========================

def fetch_pa_address(browser, url):
    page = browser.new_page()
    try:
        page.goto(url, timeout=30000)
        page.wait_for_timeout(1500)
        text = page.inner_text("body")

        m = re.search(r"LOCATION ADDRESS\s+(.+)", text)
        if m:
            addr = norm(m.group(1))
            if valid_address(addr):
                return addr

    except:
        pass
    finally:
        page.close()

    return None

# =========================
# SEARCH FLOW
# =========================

def run_search(page):

    from_date, to_date = build_dates()

    page.goto(STATUS_URL)
    page.wait_for_timeout(2000)

    # SELECT SALE
    select = page.locator("select").filter(has_text="SALE").first
    select.select_option(label="SALE")

    # DATES
    page.locator("#SearchSaleDateFrom").fill(from_date)
    page.locator("#SearchSaleDateTo").fill(to_date)

    # BUTTON (CORRIGIDO)
    page.locator("button[name='buttonSubmitStatus']").click()

    page.wait_for_timeout(3000)

# =========================
# CASE LINKS
# =========================

def get_links(page):

    links = []
    seen = set()

    for _ in range(30):

        anchors = page.locator("a[href*='Details?id=']")
        for i in range(anchors.count()):
            href = anchors.nth(i).get_attribute("href")
            if href:
                full = urljoin(BASE_URL, href)
                if full not in seen:
                    seen.add(full)
                    links.append(full)

        next_btn = page.locator("a:has-text('Next')")
        if next_btn.count() == 0:
            break

        next_btn.first.click()
        page.wait_for_timeout(2000)

    return links

# =========================
# CASE PARSE
# =========================

def parse_case(html, url):

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")

    def pick(label):
        m = re.search(rf"{label}\s*\n\s*(.+)", text)
        return norm(m.group(1)) if m else None

    pdf = None
    pa = None

    for a in soup.find_all("a", href=True):
        t = a.get_text().lower()
        if "certificate" in t:
            pdf = urljoin(url, a["href"])
        if "appraiser" in t:
            pa = urljoin(url, a["href"])

    return {
        "case": pick("Case Number"),
        "parcel": pick("Parcel ID"),
        "date": pick("Auction Date"),
        "status": pick("Status"),
        "bid": pick("Opening Bid"),
        "applicant": pick("Applicant Names"),
        "pdf": pdf,
        "pa": pa
    }

# =========================
# MAIN
# =========================

def run_palm_beach():

    log.info("=== PALM BEACH V6.5 FINAL ===")

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page()

        run_search(page)

        links = get_links(page)

        log.info(f"Found {len(links)} SALE cases")

        s = requests.Session()

        for link in links:

            r = s.get(link)
            if r.status_code != 200:
                continue

            case = parse_case(r.text, link)

            if case["status"] != "SALE":
                continue

            if exists_supabase(case["parcel"], case["date"]):
                continue

            address = None

            if case["pdf"]:
                try:
                    pdf = s.get(case["pdf"])
                    address = extract_pdf_address(pdf.content)
                except:
                    pass

            if not address and case["pa"]:
                address = fetch_pa_address(browser, case["pa"])

            payload = {
                "county": "PalmBeach",
                "state": "FL",
                "node": case["case"],
                "parcel_number": case["parcel"],
                "sale_date": case["date"],
                "opening_bid": clean_bid(case["bid"]),
                "deed_status": case["status"],
                "applicant_name": case["applicant"],
                "pdf_url": case["pdf"],
                "address": address
            }

            print(json.dumps(payload, indent=2))

            if SEND_TO_APP:
                requests.post(
                    f"{APP_API_BASE}/api/ingest",
                    json=payload,
                    headers={"Authorization": f"Bearer {APP_API_TOKEN}"}
                )

        browser.close()
