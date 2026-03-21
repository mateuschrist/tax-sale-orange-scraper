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
MIAMI_MAX_CASES = int(os.getenv("MIAMI_MAX_CASES", "300"))


# =========================
# STATE
# =========================
def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(st):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(st, f, indent=2, ensure_ascii=False)


def get_seen():
    return set(load_state().get("seen", []))


def add_seen(case_number):
    st = load_state()
    seen = set(st.get("seen", []))
    seen.add(case_number)
    st["seen"] = sorted(seen)
    save_state(st)


# =========================
# HELPERS
# =========================
def norm(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def clean_money(v):
    return re.sub(r"[^\d.]", "", str(v or ""))


def click_safe(page, selector, name):
    try:
        page.click(selector, timeout=5000)
        log.info(f"{name} clicked (normal)")
        return True
    except Exception:
        pass

    try:
        page.locator(selector).first.click(force=True, timeout=5000)
        log.info(f"{name} clicked (force)")
        return True
    except Exception:
        pass

    try:
        page.evaluate(
            """(sel) => {
                const el = document.querySelector(sel);
                if (!el) throw new Error(`not found: ${sel}`);
                el.click();
            }""",
            selector,
        )
        log.info(f"{name} clicked (JS fallback)")
        return True
    except Exception as e:
        log.error(f"{name} FAILED: {e}")
        return False


def safe_text(el):
    try:
        return norm(el.inner_text())
    except Exception:
        return ""


def safe_attr(el, attr):
    try:
        return el.get_attribute(attr)
    except Exception:
        return None


# =========================
# SEARCH FLOW
# =========================
def force_exact_active_192(page):
    page.evaluate(
        """
        () => {
            document.querySelectorAll('a.filter-status-nosub.status-sub[data-parentid="2"]').forEach(el => {
                el.classList.remove('selected');
                const icon = el.querySelector('i');
                if (icon) {
                    icon.classList.remove('icon-ok-sign');
                    icon.classList.add('icon-circle-blank');
                }
            });

            const parent = document.querySelector('#caseStatus2');
            if (parent) {
                parent.classList.remove('selected');
                const icon = parent.querySelector('i');
                if (icon) {
                    icon.classList.remove('icon-ok-sign');
                    icon.classList.add('icon-circle-blank');
                }
            }

            const target = document.querySelector('a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]');
            if (!target) throw new Error('Active 192 target not found');

            target.classList.add('selected');
            const targetIcon = target.querySelector('i');
            if (targetIcon) {
                targetIcon.classList.remove('icon-circle-blank');
                targetIcon.classList.add('icon-ok-sign');
            }

            if (parent) {
                parent.classList.add('selected');
                const parentIcon = parent.querySelector('i');
                if (parentIcon) {
                    parentIcon.classList.remove('icon-circle-blank');
                    parentIcon.classList.add('icon-ok-sign');
                }
            }

            const group = document.querySelector('#caseFiltersStatus');
            if (group) group.classList.remove('open');

            const label = document.querySelector('#filterCaseStatusLabel');
            if (label) label.textContent = 'Select Case Status';
        }
        """
    )
    log.info("Forced exact status selection: only 192")


def run_search(page):
    log.info("Running Miami search flow...")

    page.wait_for_timeout(8000)

    click_safe(page, "a.filters-reset", "RESET FILTERS")
    page.wait_for_timeout(2000)

    click_safe(page, "#filterButtonStatus", "FILTER BUTTON")
    page.wait_for_timeout(1500)

    click_safe(page, "#caseStatus2", "ACTIVE PARENT")
    page.wait_for_timeout(1000)

    force_exact_active_192(page)
    page.wait_for_timeout(1500)

    click_safe(page, "button.filters-submit", "SEARCH BUTTON")
    log.info("Sleeping 40 seconds to let results load...")
    page.wait_for_timeout(40000)


# =========================
# PAGINATION + LIST
# =========================
def collect_page_case_links(page):
    rows = page.locator('tr.load-case.table-row.link[data-caseid]')
    count = rows.count()
    log.info(f"Rows on current page: {count}")

    links = []
    for i in range(count):
        row = rows.nth(i)
        caseid = safe_attr(row, "data-caseid")
        if caseid and caseid.isdigit():
            links.append((caseid, f"{BASE_URL}/public/cases/view/{caseid}"))
    return links


def click_pagination_link(page, label_text):
    links = page.locator("a")
    total = links.count()

    for i in range(total):
        a = links.nth(i)
        txt = safe_text(a)
        if txt == label_text:
            try:
                a.click(timeout=5000)
                log.info(f"Clicked pagination: {label_text}")
                return True
            except Exception:
                try:
                    a.click(force=True, timeout=5000)
                    log.info(f"Clicked pagination (force): {label_text}")
                    return True
                except Exception:
                    return False
    return False


def collect_all_case_links(page, max_cases):
    all_links = []
    seen_caseids = set()

    current_links = collect_page_case_links(page)
    for caseid, url in current_links:
        if caseid not in seen_caseids:
            seen_caseids.add(caseid)
            all_links.append((caseid, url))

    page_num = 2
    while len(all_links) < max_cases:
        label = f"Page {page_num}"
        found = False

        links = page.locator("a")
        total = links.count()
        for i in range(total):
            txt = safe_text(links.nth(i))
            if txt.startswith(label):
                found = True
                break

        if not found:
            break

        if not click_pagination_link(page, label):
            break

        page.wait_for_timeout(12000)

        current_links = collect_page_case_links(page)
        if not current_links:
            break

        for caseid, url in current_links:
            if caseid not in seen_caseids:
                seen_caseids.add(caseid)
                all_links.append((caseid, url))
                if len(all_links) >= max_cases:
                    break

        page_num += 1

    log.info(f"Total collected case links: {len(all_links)}")
    return all_links[:max_cases]


# =========================
# CASE PARSER
# =========================
def pick_label_value(text, label):
    pattern = rf"{re.escape(label)}\s*(.+)"
    m = re.search(pattern, text, re.I)
    return norm(m.group(1)) if m else None


def parse_case_page(html, url):
    soup = BeautifulSoup(html, "html.parser")
    text = norm(soup.get_text("\n"))

    pa_link = soup.select_one("#propertyAppraiserLink")
    parcel_link_text = norm(pa_link.get_text()) if pa_link else None
    pa_url = pa_link.get("href") if pa_link else None

    return {
        "url": url,
        "case_number": pick_label_value(text, "Case Number"),
        "status": pick_label_value(text, "Case Status"),
        "date_created": pick_label_value(text, "Date Created"),
        "application_number": pick_label_value(text, "Application Number"),
        "certificate_number": pick_label_value(text, "Certificate Number"),
        "parcel_number": parcel_link_text or pick_label_value(text, "Parcel Number"),
        "sale_date": pick_label_value(text, "Sale Date"),
        "property_address": pick_label_value(text, "Property Address"),
        "opening_bid": pick_label_value(text, "Opening Bid"),
        "property_appraiser_url": pa_url,
        "raw_text": text,
    }


def case_page_looks_valid(html):
    t = html.upper()
    return "CASE" in t and ("PARCEL" in t or "APPLICATION" in t)


def is_candidate_status(status):
    if not status:
        return False
    s = status.upper()
    blocked = [
        "COMPLETED",
        "REDEEMED",
        "SOLD BIDDER",
        "SOLD APPLICANT",
        "CANCELED",
        "TRANSFER",
        "TRANSFERED",
    ]
    return not any(b in s for b in blocked)


# =========================
# MAIN
# =========================
def run_miami():
    log.info("=== MIAMI OPERATIONAL SCRAPER ===")

    seen = get_seen()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chrome",
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/134.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="America/New_York",
        )

        page = context.new_page()
        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)

        run_search(page)

        case_links = collect_all_case_links(page, MIAMI_MAX_CASES)

        session = requests.Session()

        for idx, (caseid, url) in enumerate(case_links, start=1):
            log.info(f"[{idx}/{len(case_links)}] Reading case {caseid}")

            try:
                resp = session.get(url, timeout=30)
                if resp.status_code != 200:
                    log.warning(f"Case {caseid} returned status {resp.status_code}")
                    continue

                if not case_page_looks_valid(resp.text):
                    log.warning(f"Case {caseid} did not look like a valid case page")
                    continue

                data = parse_case_page(resp.text, url)
                case_number = data.get("case_number") or caseid

                if case_number in seen:
                    continue

                status = data.get("status")
                if not is_candidate_status(status):
                    log.info(f"Skipping case {case_number} due to status: {status}")
                    continue

                payload = {
                    "county": "MiamiDade",
                    "state": "FL",
                    "node": case_number,
                    "auction_source_url": data.get("url"),
                    "parcel_number": data.get("parcel_number"),
                    "sale_date": data.get("sale_date"),
                    "opening_bid": clean_money(data.get("opening_bid")),
                    "deed_status": status,
                    "address": data.get("property_address"),
                    "property_appraiser_url": data.get("property_appraiser_url"),
                    "application_number": data.get("application_number"),
                    "date_created": data.get("date_created"),
                }

                print(json.dumps(payload, indent=2, ensure_ascii=False))

                add_seen(case_number)
                seen.add(case_number)

                time.sleep(0.5)

            except Exception as e:
                log.error(f"Error reading case {caseid}: {e}")

        browser.close()