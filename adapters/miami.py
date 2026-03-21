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
# 🔍 DOM SCANNER
# =========================
def scan_dom(page):
    data = page.evaluate("""
    () => {
        const result = {
            filter: null,
            parent: null,
            child: null,
            search: null
        };

        // FILTER BUTTON
        document.querySelectorAll("button").forEach(el => {
            const text = (el.innerText || "").trim();
            const cls = el.className || "";
            const id = el.id || "";

            if (
                id === "filterButtonStatus" ||
                cls.includes("filter-case-status") ||
                text.includes("Select Case Status")
            ) {
                result.filter = id ? "#" + id : el.tagName.toLowerCase();
            }
        });

        // ACTIVE PARENT
        document.querySelectorAll("a").forEach(el => {
            const id = el.id || "";
            const status = el.getAttribute("data-statusid") || "";

            if (id === "caseStatus2" || status === "2") {
                result.parent = id ? "#" + id : el.tagName.toLowerCase();
            }
        });

        // ACTIVE CHILD
        document.querySelectorAll("a").forEach(el => {
            const status = el.getAttribute("data-statusid") || "";
            const parent = el.getAttribute("data-parentid") || "";

            if (status === "192" && parent === "2") {
                result.child = el.tagName.toLowerCase() + '[data-statusid="192"]';
            }
        });

        // SEARCH BUTTON
        document.querySelectorAll("button").forEach(el => {
            const text = (el.innerText || "").trim();
            const cls = el.className || "";

            if (cls.includes("filters-submit") || text.includes("Search")) {
                result.search = el.tagName.toLowerCase() + ".filters-submit";
            }
        });

        return result;
    }
    """)
    return data


# =========================
# CLICK ENGINE
# =========================
def click_safe(page, selector, name):
    if not selector:
        raise Exception(f"{name} selector missing")

    log.info(f"Clicking {name}: {selector}")

    try:
        page.locator(selector).first.click(timeout=5000)
        return
    except:
        pass

    try:
        page.locator(selector).first.click(force=True)
        return
    except:
        pass

    page.evaluate(f"""
        (() => {{
            const el = document.querySelector("{selector}");
            if (el) el.click();
        }})()
    """)


# =========================
# FILTER FLOW
# =========================
def run_filters(page):
    log.info("Opening Miami...")
    page.goto(LIST_URL)

    page.wait_for_load_state("networkidle")
    wait(page, 5000)

    scan = scan_dom(page)
    log.info(f"SCAN RESULT: {scan}")

    click_safe(page, scan["filter"], "FILTER")
    wait(page, 1500)

    click_safe(page, scan["parent"], "ACTIVE PARENT")
    wait(page, 1000)

    click_safe(page, scan["child"], "ACTIVE CHILD")
    wait(page, 1000)

    click_safe(page, scan["search"], "SEARCH")

    log.info("Waiting results...")
    wait(page, 7000)


# =========================
# LIST EXTRACTION
# =========================
def extract_links(page):
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
        "case": pick(text, "Case Number"),
        "status": pick(text, "Case Status"),
        "bid": pick(text, "Opening Bid"),
        "date": pick(text, "Sale Date"),
        "address": pick(text, "Property Address"),
        "parcel": norm(pa.get_text()) if pa else None,
        "pa_url": pa["href"] if pa else None,
        "url": url
    }


def valid(html):
    return "CASE SUMMARY" in html.upper()


# =========================
# MAIN
# =========================
def run_miami():
    log.info("=== MIAMI V4 SCAN ENGINE ===")

    seen = get_seen()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page()

        run_filters(page)

        links = extract_links(page)
        log.info(f"Cases found: {len(links)}")

        s = requests.Session()

        for i, link in enumerate(links):
            log.info(f"[{i+1}] {link}")

            try:
                r = s.get(link, timeout=20)

                if r.status_code != 200 or not valid(r.text):
                    continue

                data = parse_case(r.text, link)

                if data["status"] != "ACTIVE":
                    continue

                if data["case"] in seen:
                    continue

                payload = {
                    "county": "MiamiDade",
                    "state": "FL",
                    "node": data["case"],
                    "auction_source_url": data["url"],
                    "parcel_number": data["parcel"],
                    "sale_date": data["date"],
                    "opening_bid": clean_money(data["bid"]),
                    "deed_status": data["status"],
                    "address": data["address"],
                    "property_appraiser_url": data["pa_url"]
                }

                print(json.dumps(payload, indent=2))

                add_seen(data["case"])
                seen.add(data["case"])

                time.sleep(0.8)

            except Exception as e:
                log.error(e)

        browser.close()
