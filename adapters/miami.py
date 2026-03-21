import os
import re
import json
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

log = logging.getLogger("miami")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
MIAMI_MAX_CASES = int(os.getenv("MIAMI_MAX_CASES", "300"))
STATE_FILE = os.getenv("MIAMI_STATE_FILE", "state_miami.json")

APP_API_BASE = (os.getenv("APP_API_BASE", "") or "").strip().rstrip("/")
APP_API_TOKEN = (os.getenv("APP_API_TOKEN", "") or "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

SUPABASE_URL = (os.getenv("SUPABASE_URL", "") or "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or "").strip()
CAN_CHECK_SUPABASE = bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)


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


def get_seen():
    st = load_state()
    seen = st.get("seen", [])
    if isinstance(seen, list):
        return set(str(x) for x in seen if x)
    return set()


def add_seen(case_number: str):
    if not case_number:
        return
    st = load_state()
    seen = set(st.get("seen", []))
    seen.add(str(case_number))
    st["seen"] = sorted(seen)
    st["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save_state(st)


# =========================
# HELPERS
# =========================
def norm(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def clean_money(v):
    return re.sub(r"[^\d.]", "", str(v or ""))


def wait_network_quiet(page, timeout=12000):
    try:
        page.wait_for_load_state("networkidle", timeout=timeout)
    except PWTimeout:
        pass


def click_resilient(page, selector: str, timeout: int = 15000):
    page.wait_for_selector(selector, state="attached", timeout=timeout)
    loc = page.locator(selector).first

    try:
        loc.scroll_into_view_if_needed(timeout=3000)
    except Exception:
        pass

    try:
        loc.click(timeout=5000)
        return
    except Exception:
        pass

    try:
        loc.click(force=True, timeout=5000)
        return
    except Exception:
        pass

    page.evaluate(
        """(sel) => {
            const el = document.querySelector(sel);
            if (!el) throw new Error(`selector not found: ${sel}`);
            el.click();
        }""",
        selector,
    )


# =========================
# SUPABASE DEDUP
# =========================
def sb_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }


def supabase_find_existing_case(node: str):
    if not CAN_CHECK_SUPABASE or not node:
        return None

    url = (
        f"{SUPABASE_URL}/rest/v1/properties"
        f"?county=eq.MiamiDade"
        f"&node=eq.{quote(str(node), safe='')}"
        f"&select=id,node,parcel_number,sale_date,address,opening_bid,deed_status,auction_source_url,property_appraiser_url,applicant_name"
        f"&limit=1"
    )

    try:
        r = requests.get(url, headers=sb_headers(), timeout=30)
        if r.status_code == 200:
            arr = r.json()
            return arr[0] if arr else None
    except Exception as e:
        log.warning("supabase_find_existing_case failed: %s", str(e))

    return None


def supabase_find_existing_property(parcel_number: str):
    if not CAN_CHECK_SUPABASE or not parcel_number:
        return None

    url = (
        f"{SUPABASE_URL}/rest/v1/properties"
        f"?county=eq.MiamiDade"
        f"&parcel_number=eq.{quote(str(parcel_number), safe='')}"
        f"&select=id,node,parcel_number,sale_date,address,opening_bid,deed_status,auction_source_url,property_appraiser_url,applicant_name"
        f"&limit=1"
    )

    try:
        r = requests.get(url, headers=sb_headers(), timeout=30)
        if r.status_code == 200:
            arr = r.json()
            return arr[0] if arr else None
    except Exception as e:
        log.warning("supabase_find_existing_property failed: %s", str(e))

    return None


def payload_quality_score(payload: dict) -> int:
    fields = [
        "address",
        "parcel_number",
        "sale_date",
        "opening_bid",
        "deed_status",
        "auction_source_url",
        "property_appraiser_url",
        "applicant_name",
    ]
    return sum(1 for f in fields if payload.get(f) not in (None, ""))


def payload_is_better_than_existing(payload: dict, existing: dict) -> bool:
    if payload_quality_score(payload) > payload_quality_score(existing):
        return True

    important = [
        "address",
        "opening_bid",
        "auction_source_url",
        "property_appraiser_url",
    ]
    for f in important:
        old_val = (existing.get(f) or "").strip() if existing.get(f) else ""
        new_val = (payload.get(f) or "").strip() if payload.get(f) else ""
        if not old_val and new_val:
            return True

    return False


def should_send_payload(payload: dict):
    node = payload.get("node")
    parcel = payload.get("parcel_number")

    existing_case = supabase_find_existing_case(node)
    if existing_case:
        if payload_is_better_than_existing(payload, existing_case):
            return True, "existing case found, but new payload is better"
        return False, "duplicate case/node already exists"

    existing_prop = supabase_find_existing_property(parcel)
    if existing_prop:
        if payload_is_better_than_existing(payload, existing_prop):
            return True, "existing parcel found, but new payload is better"
        return False, "duplicate parcel already exists"

    return True, "new record"


# =========================
# APP SEND
# =========================
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
# SEARCH FLOW
# =========================
def run_filters(page):
    log.info("Opening Miami list page...")
    page.goto(LIST_URL, wait_until="domcontentloaded", timeout=30000)
    wait_network_quiet(page, 15000)
    page.wait_for_timeout(5000)

    # Open case status dropdown
    click_resilient(page, "#filterButtonStatus")
    page.wait_for_timeout(1200)

    # Click Active parent
    click_resilient(page, "#caseStatus2")
    page.wait_for_timeout(1000)

    # Click Active child
    click_resilient(page, 'a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]')
    page.wait_for_timeout(1000)

    # Search
    click_resilient(page, "button.filters-submit")
    log.info("Search triggered... waiting results")
    wait_network_quiet(page, 15000)
    page.wait_for_timeout(7000)


# =========================
# LIST EXTRACTION
# =========================
def extract_case_links(page):
    links = []
    rows = page.locator("tr.load-case.table-row.link[data-caseid]")

    count = rows.count()
    log.info("Rows found on current page: %s", count)

    for i in range(count):
        try:
            row = rows.nth(i)
            caseid = (row.get_attribute("data-caseid") or "").strip()
            if caseid and caseid.isdigit():
                links.append(f"{BASE_URL}/public/cases/view/{caseid}")
        except Exception:
            continue

    return links


def goto_next_page(page):
    candidates = [
        'a[rel="next"]',
        ".pagination li.next a",
        '.pagination a:has-text("Next")',
        'a:has-text("Next")',
    ]

    for sel in candidates:
        loc = page.locator(sel)
        if loc.count() == 0:
            continue

        btn = loc.first
        try:
            cls = (btn.get_attribute("class") or "").lower()
            parent_cls = ""
            try:
                parent = btn.locator("xpath=ancestor::li[1]")
                if parent.count() > 0:
                    parent_cls = (parent.first.get_attribute("class") or "").lower()
            except Exception:
                pass

            if "disabled" in cls or "disabled" in parent_cls:
                continue

            try:
                btn.click(timeout=5000)
            except Exception:
                try:
                    btn.click(force=True, timeout=5000)
                except Exception:
                    continue

            wait_network_quiet(page, 12000)
            page.wait_for_timeout(4000)
            return True
        except Exception:
            continue

    return False


def discover_case_links(page):
    run_filters(page)

    try:
        page.wait_for_selector("tr.load-case.table-row.link[data-caseid]", timeout=15000)
    except Exception:
        pass

    page.wait_for_timeout(2500)

    all_links = []
    seen = set()

    for _ in range(50):
        page_links = extract_case_links(page)

        for link in page_links:
            if link not in seen:
                seen.add(link)
                all_links.append(link)

        if MIAMI_MAX_CASES > 0 and len(all_links) >= MIAMI_MAX_CASES:
            return all_links[:MIAMI_MAX_CASES]

        moved = goto_next_page(page)
        if not moved:
            break

    return all_links[:MIAMI_MAX_CASES] if MIAMI_MAX_CASES > 0 else all_links


# =========================
# CASE PARSE
# =========================
def pick_after_label(text: str, label: str):
    m = re.search(rf"{re.escape(label)}\s+(.+?)(?=(Tax Collector #|Applicant Number|Case Number|Case Status|Opening Bid:|Redemption Amount:|App Receive Date|Sale Date|Property Address|Legal Description|$))", text, re.I)
    return norm(m.group(1)) if m else None


def parse_case_page(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    text = norm(soup.get_text("\n"))

    tax_collector_number = pick_after_label(text, "Tax Collector #")
    applicant_number = pick_after_label(text, "Applicant Number")
    case_number = pick_after_label(text, "Case Number")
    case_status = pick_after_label(text, "Case Status")
    opening_bid = pick_after_label(text, "Opening Bid:")
    redemption_amount = pick_after_label(text, "Redemption Amount:")
    app_receive_date = pick_after_label(text, "App Receive Date")
    sale_date = pick_after_label(text, "Sale Date")
    property_address = pick_after_label(text, "Property Address")
    legal_description = pick_after_label(text, "Legal Description")

    property_appraiser_url = None
    parcel_number = None

    pa_link = soup.select_one("a#propertyAppraiserLink")
    if pa_link and pa_link.get("href"):
        property_appraiser_url = pa_link["href"]
        parcel_number = norm(pa_link.get_text())

    return {
        "case_number": case_number,
        "tax_collector_number": tax_collector_number,
        "applicant_number": applicant_number,
        "case_status": case_status,
        "opening_bid": opening_bid,
        "redemption_amount": redemption_amount,
        "app_receive_date": app_receive_date,
        "sale_date": sale_date,
        "property_address": property_address,
        "legal_description": legal_description,
        "parcel_number": parcel_number,
        "property_appraiser_url": property_appraiser_url,
        "applicant_name": None,
        "case_url": url,
    }


def valid_case(html):
    t = html.upper()
    return "CASE SUMMARY" in t and "CASE NUMBER" in t and "CASE STATUS" in t


# =========================
# MAIN
# =========================
def run_miami():
    log.info("=== MIAMI V2.1 resilient active flow ===")

    seen = get_seen()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page()

        try:
            case_links = discover_case_links(page)
            log.info("Cases found: %s", len(case_links))
        finally:
            try:
                page.close()
            except Exception:
                pass

        if not case_links:
            log.warning("No Miami-Dade case links discovered")
            try:
                browser.close()
            except Exception:
                pass
            return

        s = requests.Session()

        for i, link in enumerate(case_links, start=1):
            log.info("[%s/%s] %s", i, len(case_links), link)

            try:
                r = s.get(link, timeout=20)

                if r.status_code != 200 or not valid_case(r.text):
                    continue

                data = parse_case_page(r.text, link)

                if (data.get("case_status") or "").upper() != "ACTIVE":
                    continue

                case_number = data.get("case_number")
                if case_number in seen:
                    continue

                payload = {
                    "county": "MiamiDade",
                    "state": "FL",
                    "node": data.get("case_number"),
                    "auction_source_url": data.get("case_url"),
                    "tax_sale_id": data.get("case_number"),
                    "parcel_number": data.get("parcel_number"),
                    "sale_date": data.get("sale_date"),
                    "opening_bid": clean_money(data.get("opening_bid")),
                    "deed_status": data.get("case_status"),
                    "applicant_name": data.get("applicant_name"),
                    "property_appraiser_url": data.get("property_appraiser_url"),
                    "address": data.get("property_address"),
                    "city": None,
                    "state_address": "FL" if data.get("property_address") else None,
                    "zip": None,
                    "address_source": "CASE_SUMMARY_PROPERTY_ADDRESS" if data.get("property_address") else None,
                    "legal_description": data.get("legal_description"),
                    "tax_collector_number": data.get("tax_collector_number"),
                    "applicant_number": data.get("applicant_number"),
                    "redemption_amount": clean_money(data.get("redemption_amount")),
                    "app_receive_date": data.get("app_receive_date"),
                }

                print(json.dumps(payload, indent=2))

                should_send, reason = should_send_payload(payload)
                log.info("DEDUP CHECK case=%s → %s", case_number, reason)

                if should_send:
                    if send(payload):
                        add_seen(case_number)
                        seen.add(case_number)
                        log.info("SENT case=%s", case_number)
                    else:
                        log.warning("SEND FAILED case=%s", case_number)
                else:
                    add_seen(case_number)
                    seen.add(case_number)
                    log.info("SKIPPED case=%s because duplicate", case_number)

                time.sleep(0.8)

            except Exception as e:
                log.error("ERROR: %s", e)

        browser.close()
