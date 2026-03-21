import os
import re
import json
import time
import logging
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

log = logging.getLogger("miami")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"


# =========================
# STATE
# =========================
STATE_FILE = "state_miami.json"


def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except:
        return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


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


# =========================
# SEARCH FLOW (JS BASED)
# =========================
def run_filters(page):
    log.info("Opening Miami list page...")

    page.goto(LIST_URL, timeout=30000)
    page.wait_for_timeout(5000)

    # 🔥 abrir dropdown via JS
    page.evaluate("""
        document.querySelector('#filterButtonStatus').click()
    """)

    page.wait_for_timeout(1500)

    # 🔥 selecionar ACTIVE parent
    page.evaluate("""
        document.querySelector('#caseStatus2').click()
    """)

    page.wait_for_timeout(1000)

    # 🔥 selecionar ACTIVE child (192)
    page.evaluate("""
        document.querySelector('[data-statusid="192"]').click()
    """)

    page.wait_for_timeout(1000)

    # 🔥 clicar search via JS
    page.evaluate("""
        document.querySelector('.filters-submit').click()
    """)

    log.info("Search triggered... waiting results")

    page.wait_for_timeout(7000)


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
def parse_case(html, url):
    soup = BeautifulSoup(html, "html.parser")
    text = norm(soup.get_text("\n"))

    def pick(label):
        m = re.search(rf"{label}\s+(.+)", text, re.I)
        return norm(m.group(1)) if m else None

    pa = soup.select_one("#propertyAppraiserLink")

    return {
        "case_number": pick("Case Number"),
        "status": pick("Case Status"),
        "opening_bid": pick("Opening Bid"),
        "sale_date": pick("Sale Date"),
        "address": pick("Property Address"),
        "parcel": norm(pa.get_text()) if pa else None,
        "pa_url": pa["href"] if pa else None,
        "url": url
    }


def valid_case(html):
    return "CASE SUMMARY" in html.upper()


# =========================
# MAIN
# =========================
def run_miami():
    log.info("=== MIAMI V2 JS FLOW ===")

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
