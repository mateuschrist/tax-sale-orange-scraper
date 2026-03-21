import os
import re
import json
import time
import logging
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

log = logging.getLogger("miami")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
STATE_FILE = "state_miami.json"
MIAMI_MAX_CASES = int(os.getenv("MIAMI_MAX_CASES", "200"))


# =========================
# STATE
# =========================
def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except:
        return {}


def save_state(st):
    with open(STATE_FILE, "w") as f:
        json.dump(st, f, indent=2)


def get_seen():
    return set(load_state().get("seen", []))


def add_seen(case):
    st = load_state()
    seen = set(st.get("seen", []))
    seen.add(case)
    st["seen"] = list(seen)
    save_state(st)


# =========================
# HELPERS
# =========================
def norm(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def clean_money(v):
    return re.sub(r"[^\d.]", "", str(v or ""))


def wait(page, ms=1500):
    page.wait_for_timeout(ms)


# =========================
# FILTER FLOW (EXATO)
# =========================
def run_filters(page):
    log.info("Opening Miami site...")
    page.goto(LIST_URL, timeout=30000)

    # esperar site carregar bem
    page.wait_for_load_state("networkidle")
    wait(page, 4000)

    # =====================
    # STEP 1: abrir dropdown
    # =====================
    page.wait_for_selector("#filterButtonStatus", timeout=20000)
    page.locator("#filterButtonStatus").click()
    wait(page, 1200)

    # =====================
    # STEP 2: active parent
    # =====================
    page.wait_for_selector("#caseStatus2", timeout=20000)
    page.locator("#caseStatus2").click()
    wait(page, 800)

    # =====================
    # STEP 3: active child
    # =====================
    page.wait_for_selector('a[data-statusid="192"][data-parentid="2"]', timeout=20000)
    page.locator('a[data-statusid="192"][data-parentid="2"]').click()
    wait(page, 800)

    # =====================
    # STEP 4: search
    # =====================
    page.wait_for_selector("button.filters-submit", timeout=20000)
    page.locator("button.filters-submit").click()

    log.info("Search executed, waiting results...")

    # site é lento — respeitar isso
    page.wait_for_load_state("networkidle")
    wait(page, 6000)


# =========================
# LIST EXTRACTION
# =========================
def extract_case_links(page):
    links = []

    rows = page.locator("tr.load-case.table-row.link[data-caseid]")
    count = rows.count()

    log.info(f"Rows found: {count}")

    for i in range(count):
        row = rows.nth(i)
        caseid = row.get_attribute("data-caseid")

        if caseid and caseid.isdigit():
            links.append(f"{BASE_URL}/public/cases/view/{caseid}")

    return links


# =========================
# CASE PARSE
# =========================
def pick(text, label):
    m = re.search(rf"{label}\s+(.+)", text, re.I)
    return norm(m.group(1)) if m else None


def parse_case(html, url):
    soup = BeautifulSoup(html, "html.parser")
    text = norm(soup.get_text("\n"))

    pa = soup.select_one("#propertyAppraiserLink")

    return {
        "case_number": pick(text, "Case Number"),
        "status": pick(text, "Case Status"),
        "opening_bid": pick(text, "Opening Bid"),
        "sale_date": pick(text, "Sale Date"),
        "address": pick(text, "Property Address"),
        "parcel": norm(pa.get_text()) if pa else None,
        "pa_url": pa["href"] if pa else None,
        "url": url,
    }


def valid_case(html):
    t = html.upper()
    return "CASE SUMMARY" in t


# =========================
# MAIN
# =========================
def run_miami():
    log.info("=== MIAMI V3 EXACT FLOW ===")

    seen = get_seen()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page()

        run_filters(page)

        case_links = extract_case_links(page)
        log.info(f"Cases found: {len(case_links)}")

        s = requests.Session()

        for i, link in enumerate(case_links):
            log.info(f"[{i+1}] {link}")

            try:
                r = s.get(link, timeout=20)

                if r.status_code != 200 or not valid_case(r.text):
                    continue

                data = parse_case(r.text, link)

                if data["status"] != "ACTIVE":
                    continue

                if data["case_number"] in seen:
                    continue

                payload = {
                    "county": "MiamiDade",
                    "state": "FL",
                    "node": data["case_number"],
                    "auction_source_url": data["url"],
                    "parcel_number": data["parcel"],
                    "sale_date": data["sale_date"],
                    "opening_bid": clean_money(data["opening_bid"]),
                    "deed_status": data["status"],
                    "address": data["address"],
                    "property_appraiser_url": data["pa_url"],
                }

                print(json.dumps(payload, indent=2))

                add_seen(data["case_number"])
                seen.add(data["case_number"])

                time.sleep(0.8)

            except Exception as e:
                log.error(f"ERROR: {e}")

        browser.close()
