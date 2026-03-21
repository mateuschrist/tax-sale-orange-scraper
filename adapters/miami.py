import os
import re
import json
import time
import random
import logging
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

log = logging.getLogger("taxdeed-miami")

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


def get_seen_cases():
    st = load_state()
    seen = st.get("seen_case_numbers", [])
    if isinstance(seen, list):
        return set(str(x) for x in seen if x)
    return set()


def add_seen_case(case_number: str):
    if not case_number:
        return
    st = load_state()
    seen = set(st.get("seen_case_numbers", []))
    seen.add(str(case_number))
    st["seen_case_numbers"] = sorted(seen)
    st["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save_state(st)


# =========================
# HELPERS
# =========================
def norm(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def clean_bid(v):
    return re.sub(r"[^\d.]", "", str(v or ""))


def human_pause(a=0.20, b=0.60):
    time.sleep(random.uniform(a, b))


def wait_network_quiet(page, timeout=12000):
    try:
        page.wait_for_load_state("networkidle", timeout=timeout)
    except PWTimeout:
        pass


def visible_elements(locator):
    out = []
    try:
        count = locator.count()
    except Exception:
        return out

    for i in range(count):
        item = locator.nth(i)
        try:
            if item.is_visible() and item.is_enabled():
                out.append(item)
        except Exception:
            continue
    return out


def human_click(locator):
    try:
        locator.scroll_into_view_if_needed(timeout=5000)
    except Exception:
        pass

    human_pause(0.08, 0.16)

    try:
        locator.hover(timeout=5000)
        human_pause(0.05, 0.12)
    except Exception:
        pass

    locator.click(timeout=10000)
    human_pause(0.15, 0.30)


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
# CASE PARSE
# =========================
def pick_after_label(text: str, label: str):
    m = re.search(rf"{re.escape(label)}\s+(.+)", text, re.I)
    return norm(m.group(1)) if m else None


def parse_case_page(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    text = norm(soup.get_text("\n"))

    # Header fields
    tax_collector_number = pick_after_label(text, "Tax Collector #")
    applicant_number = pick_after_label(text, "Applicant Number")
    case_number = pick_after_label(text, "Case Number")
    case_status = pick_after_label(text, "Case Status")
    opening_bid = pick_after_label(text, "Opening Bid:")
    redemption_amount = pick_after_label(text, "Redemption Amount:")

    # Summary fields
    app_receive_date = pick_after_label(text, "App Receive Date")
    sale_date = pick_after_label(text, "Sale Date")
    property_address = pick_after_label(text, "Property Address")
    legal_description = pick_after_label(text, "Legal Description")

    applicant_name = None
    # first version: applicant name may not be obvious on details page header, so leave blank unless found later

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
        "applicant_name": applicant_name,
        "case_url": url,
    }


def is_valid_case_page(html: str) -> bool:
    t = html.lower()
    return "case summary" in t and "case number" in t and "case status" in t


# =========================
# LIST FLOW
# =========================
def click_filter_status(page):
    btn = page.locator("#filterButtonStatus")
    if btn.count() == 0:
        raise RuntimeError("Could not locate filterButtonStatus")
    human_click(btn.first)
    page.wait_for_timeout(800)


def click_active_parent(page):
    btn = page.locator("#caseStatus2")
    if btn.count() == 0:
        raise RuntimeError("Could not locate caseStatus2")
    human_click(btn.first)
    page.wait_for_timeout(700)


def click_active_child(page):
    btn = page.locator("a.filter-status-nosub.status-sub[data-statusid='192'][data-parentid='2']")
    if btn.count() == 0:
        raise RuntimeError("Could not locate Active child status 192")
    human_click(btn.first)
    page.wait_for_timeout(700)


def click_search(page):
    btn = page.locator("button.filters-submit")
    if btn.count() == 0:
        raise RuntimeError("Could not locate filters-submit search button")
    human_click(btn.first)
    wait_network_quiet(page, 15000)
    # user already warned this page is slow
    page.wait_for_timeout(5000)


def do_active_search(page):
    page.goto(LIST_URL, wait_until="domcontentloaded", timeout=30000)
    wait_network_quiet(page, 12000)
    page.wait_for_timeout(2500)

    click_filter_status(page)
    click_active_parent(page)
    click_active_child(page)
    click_search(page)


def extract_case_links_from_current_results(page) -> list[str]:
    """
    Each row is like:
    <tr class="load-case table-row link" data-caseid="25207">
    """
    links = []
    seen = set()

    rows = page.locator("tr.load-case.table-row.link[data-caseid]")
    count = rows.count()

    for i in range(count):
        try:
            row = rows.nth(i)
            caseid = (row.get_attribute("data-caseid") or "").strip()
            if not caseid or not caseid.isdigit():
                continue

            full = f"{BASE_URL}/public/cases/view/{caseid}"
            if full not in seen:
                seen.add(full)
                links.append(full)
        except Exception:
            continue

    return links


def goto_next_results_page(page) -> bool:
    candidates = [
        "a[rel='next']",
        "a:has-text('Next')",
        ".pagination a:has-text('Next')",
        ".pagination li.next a",
    ]

    for sel in candidates:
        try:
            loc = page.locator(sel)
            items = visible_elements(loc)
            for item in items:
                try:
                    cls = (item.get_attribute("class") or "").lower()
                    if "disabled" in cls:
                        continue
                    human_click(item)
                    wait_network_quiet(page, 12000)
                    page.wait_for_timeout(3000)
                    return True
                except Exception:
                    continue
        except Exception:
            continue

    return False


def discover_case_links(page) -> list[str]:
    do_active_search(page)

    try:
        page.locator("tr.load-case.table-row.link[data-caseid]").first.wait_for(timeout=15000)
    except Exception:
        pass

    page.wait_for_timeout(2500)

    all_links = []
    seen = set()

    for _ in range(50):
        page_links = extract_case_links_from_current_results(page)

        for h in page_links:
            if h not in seen:
                seen.add(h)
                all_links.append(h)

        if MIAMI_MAX_CASES > 0 and len(all_links) >= MIAMI_MAX_CASES:
            return all_links[:MIAMI_MAX_CASES]

        moved = goto_next_results_page(page)
        if not moved:
            break

    return all_links[:MIAMI_MAX_CASES] if MIAMI_MAX_CASES > 0 else all_links


# =========================
# MAIN
# =========================
def run_miami():
    log.info("=== Miami-Dade V1 Active filter + case details ===")

    seen_cases = get_seen_cases()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        try:
            case_links = discover_case_links(page)
            log.info("Discovered %s Miami-Dade case links", len(case_links))
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
        s.headers.update(HEADERS)

        for idx, case_url in enumerate(case_links, start=1):
            log.info("Miami case %s/%s → %s", idx, len(case_links), case_url)

            try:
                r = s.get(case_url, timeout=30)
                if r.status_code != 200 or not is_valid_case_page(r.text):
                    log.warning("Invalid Miami case page at %s", case_url)
                    continue

                case = parse_case_page(r.text, r.url)

                status_value = (case.get("case_status") or "").strip().upper()
                if status_value != "ACTIVE":
                    log.info("SKIPPED because case status is not ACTIVE → %s (%s)", status_value, case.get("case_number"))
                    continue

                case_number = case.get("case_number")
                if case_number in seen_cases:
                    log.info("SKIPPED already seen in local state → %s", case_number)
                    continue

                payload = {
                    "county": "MiamiDade",
                    "state": "FL",
                    "node": case.get("case_number"),
                    "auction_source_url": case.get("case_url"),
                    "tax_sale_id": case.get("case_number"),
                    "parcel_number": case.get("parcel_number"),
                    "sale_date": case.get("sale_date"),
                    "opening_bid": clean_bid(case.get("opening_bid")),
                    "deed_status": case.get("case_status"),
                    "applicant_name": case.get("applicant_name"),
                    "property_appraiser_url": case.get("property_appraiser_url"),
                    "address": case.get("property_address"),
                    "city": None,
                    "state_address": "FL" if case.get("property_address") else None,
                    "zip": None,
                    "address_source": "CASE_SUMMARY_PROPERTY_ADDRESS" if case.get("property_address") else None,
                    "legal_description": case.get("legal_description"),
                    "tax_collector_number": case.get("tax_collector_number"),
                    "applicant_number": case.get("applicant_number"),
                    "redemption_amount": clean_bid(case.get("redemption_amount")),
                    "app_receive_date": case.get("app_receive_date"),
                }

                print(json.dumps(payload, indent=2))

                should_send, reason = should_send_payload(payload)
                log.info("DEDUP CHECK case=%s → %s", case_number, reason)

                if should_send:
                    if send(payload):
                        add_seen_case(case_number)
                        seen_cases.add(case_number)
                        log.info("SENT case=%s", case_number)
                    else:
                        log.warning("SEND FAILED case=%s", case_number)
                else:
                    add_seen_case(case_number)
                    seen_cases.add(case_number)
                    log.info("SKIPPED case=%s because duplicate", case_number)

                time.sleep(1.0)

            except Exception as e:
                log.error("ERROR Miami case url=%s: %s", case_url, str(e))

        try:
            browser.close()
        except Exception:
            pass
