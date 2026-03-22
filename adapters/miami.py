import json
import logging
import os
import re
from typing import Dict, List, Optional
from urllib.parse import quote

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

log = logging.getLogger("miami")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"
AUCTION_URL = "https://www.miamidade.realforeclose.com/index.cfm"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
MAX_LOTS = int(os.getenv("MAX_LOTS", "100"))

APP_API_BASE = (os.getenv("APP_API_BASE", "") or "").strip().rstrip("/")
APP_API_TOKEN = (os.getenv("APP_API_TOKEN", "") or "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

SUPABASE_URL = (os.getenv("SUPABASE_URL", "") or "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or "").strip()
CAN_CHECK_SUPABASE = bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)


# =========================
# BASIC HELPERS
# =========================
def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def clean_multiline(value):
    if value is None:
        return ""
    lines = [re.sub(r"\s+", " ", x).strip(" ,") for x in str(value).splitlines()]
    lines = [x for x in lines if x]
    return ", ".join(lines)


def money_from_text(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"\$[\d,]+(?:\.\d{2})?", text)
    return m.group(0) if m else ""


def normalize_money(value: str) -> Optional[float]:
    if not value:
        return None
    raw = re.sub(r"[^\d.]", "", str(value))
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def payload_quality_score(payload: dict) -> int:
    fields = [
        "external_id",
        "case_number",
        "parcel_number",
        "property_address_case",
        "property_address_pa",
        "owner_name",
        "mailing_address",
        "auction_source_url",
        "opening_bid",
        "sale_date",
    ]
    return sum(1 for f in fields if payload.get(f) not in (None, "", [], {}))


# =========================
# APP INGEST
# =========================
def send_to_app(payload: dict) -> bool:
    if not SEND_TO_APP:
        log.info("APP SEND skipped: APP_API_BASE / APP_API_TOKEN not configured")
        return True

    try:
        r = requests.post(
            f"{APP_API_BASE}/api/ingest",
            json=payload,
            headers={"Authorization": f"Bearer {APP_API_TOKEN}"},
            timeout=30,
        )
        log.info("APP INGEST status=%s external_id=%s", r.status_code, payload.get("external_id"))
        if r.text:
            log.info("APP INGEST response=%s", r.text[:300].replace("\n", " "))
        return r.status_code in (200, 201)
    except Exception as e:
        log.warning("APP INGEST failed external_id=%s error=%s", payload.get("external_id"), str(e))
        return False


# =========================
# SUPABASE DEDUP
# =========================
def sb_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }


def supabase_find_existing_case(external_id: str):
    if not CAN_CHECK_SUPABASE or not external_id:
        return None

    url = (
        f"{SUPABASE_URL}/rest/v1/properties"
        f"?county=eq.Miami-Dade"
        f"&external_id=eq.{quote(str(external_id), safe='')}"
        f"&select=id,external_id,parcel_number,sale_date,address,city,state_address,zip,auction_source_url,opening_bid,deed_status,owner_name"
        f"&limit=1"
    )

    try:
        r = requests.get(url, headers=sb_headers(), timeout=30)
        if r.status_code == 200:
            arr = r.json()
            return arr[0] if arr else None
        log.warning("supabase_find_existing_case non-200 status=%s body=%s", r.status_code, r.text[:300])
    except Exception as e:
        log.warning("supabase_find_existing_case failed: %s", str(e))

    return None


def supabase_find_existing_property(parcel_number: str, sale_date: str):
    if not CAN_CHECK_SUPABASE or not parcel_number or not sale_date:
        return None

    url = (
        f"{SUPABASE_URL}/rest/v1/properties"
        f"?county=eq.Miami-Dade"
        f"&parcel_number=eq.{quote(str(parcel_number), safe='')}"
        f"&sale_date=eq.{quote(str(sale_date), safe='')}"
        f"&select=id,external_id,parcel_number,sale_date,address,city,state_address,zip,auction_source_url,opening_bid,deed_status,owner_name"
        f"&limit=1"
    )

    try:
        r = requests.get(url, headers=sb_headers(), timeout=30)
        if r.status_code == 200:
            arr = r.json()
            return arr[0] if arr else None
        log.warning("supabase_find_existing_property non-200 status=%s body=%s", r.status_code, r.text[:300])
    except Exception as e:
        log.warning("supabase_find_existing_property failed: %s", str(e))

    return None


def payload_is_better_than_existing(payload: dict, existing: dict) -> bool:
    new_score = payload_quality_score(payload)
    old_score = payload_quality_score(existing)

    if new_score > old_score:
        return True

    important_fields = [
        "address",
        "city",
        "state_address",
        "zip",
        "auction_source_url",
        "owner_name",
    ]

    for f in important_fields:
        old_val = (existing.get(f) or "").strip() if existing.get(f) else ""
        new_val = (payload.get(f) or "").strip() if payload.get(f) else ""
        if not old_val and new_val:
            return True

    return False


def should_send_payload(payload: dict):
    external_id = payload.get("external_id")
    parcel = payload.get("parcel_number")
    sale_date = payload.get("sale_date")

    existing_case = supabase_find_existing_case(external_id)
    if existing_case:
        if payload_is_better_than_existing(payload, existing_case):
            return True, "existing external_id found, but new payload is better"
        return False, "duplicate external_id already exists"

    existing_prop = supabase_find_existing_property(parcel, sale_date)
    if existing_prop:
        if payload_is_better_than_existing(payload, existing_prop):
            return True, "existing parcel_number + sale_date found, but new payload is better"
        return False, "duplicate parcel_number + sale_date already exists"

    return True, "new record"


def supabase_upsert_property(payload: dict) -> dict:
    if not CAN_CHECK_SUPABASE:
        return {
            "sent": False,
            "status_code": None,
            "external_id": payload.get("external_id"),
            "response_text": "SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not configured",
        }

    should_send, reason = should_send_payload(payload)
    log.info("SUPABASE DEDUP external_id=%s => %s", payload.get("external_id"), reason)

    if not should_send:
        return {
            "sent": False,
            "status_code": 200,
            "external_id": payload.get("external_id"),
            "response_text": reason,
        }

    url = f"{SUPABASE_URL}/rest/v1/properties"
    headers = sb_headers()
    headers["Prefer"] = "return=representation,resolution=merge-duplicates"

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        ok = r.status_code in (200, 201)

        if ok:
            log.info(
                "SUPABASE UPSERT OK status=%s external_id=%s",
                r.status_code,
                payload.get("external_id"),
            )
        else:
            log.error(
                "SUPABASE UPSERT FAILED status=%s external_id=%s body=%s",
                r.status_code,
                payload.get("external_id"),
                r.text[:600],
            )

        return {
            "sent": ok,
            "status_code": r.status_code,
            "external_id": payload.get("external_id"),
            "response_text": r.text[:1000],
        }
    except Exception as e:
        log.exception("SUPABASE UPSERT EXCEPTION external_id=%s error=%s", payload.get("external_id"), str(e))
        return {
            "sent": False,
            "status_code": None,
            "external_id": payload.get("external_id"),
            "response_text": str(e),
        }


# =========================
# UI HELPERS
# =========================
def click_safe(page, selector, name, timeout=6000):
    try:
        page.click(selector, timeout=timeout)
        log.info("%s clicked (normal)", name)
        return True
    except Exception:
        pass

    try:
        page.locator(selector).first.click(force=True, timeout=timeout)
        log.info("%s clicked (force)", name)
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
        log.info("%s clicked (JS fallback)", name)
        return True
    except Exception as e:
        log.error("%s FAILED: %s", name, e)
        return False


def click_element_handle_safe(el, page, name):
    try:
        el.click(timeout=6000)
        log.info("%s clicked (normal)", name)
        return True
    except Exception:
        pass

    try:
        el.click(force=True, timeout=6000)
        log.info("%s clicked (force)", name)
        return True
    except Exception:
        pass

    try:
        page.evaluate("(el) => el.click()", el)
        log.info("%s clicked (JS fallback)", name)
        return True
    except Exception as e:
        log.error("%s FAILED: %s", name, e)
        return False


def force_clear_all_active_statuses(page):
    page.evaluate(
        """
        () => {
            const children = document.querySelectorAll('a.filter-status-nosub.status-sub[data-parentid="2"]');
            children.forEach(el => {
                el.classList.remove('selected');
                const icon = el.querySelector('i');
                if (icon) {
                    icon.className = icon.className.replace(/\\bicon-ok-sign\\b/g, '').trim();
                    icon.className = icon.className.replace(/\\bicon-circle-blank\\b/g, '').trim();
                    icon.className = (icon.className + ' icon-circle-blank').trim();
                }
            });

            const parent = document.querySelector('#caseStatus2');
            if (parent) {
                parent.classList.remove('selected');
                const picon = parent.querySelector('i');
                if (picon) {
                    picon.className = picon.className.replace(/\\bicon-ok-sign\\b/g, '').trim();
                    picon.className = picon.className.replace(/\\bicon-circle-blank\\b/g, '').trim();
                    picon.className = (picon.className + ' icon-circle-blank').trim();
                }
            }

            const hidden = document.querySelector('#filterCaseStatus');
            if (hidden) hidden.value = '';

            const label = document.querySelector('#filterCaseStatusLabel');
            if (label) label.innerText = 'None Selected';
        }
        """
    )
    log.info("Cleared UI + hidden filterCaseStatus")


def force_select_only_192(page):
    page.evaluate(
        """
        () => {
            const el = document.querySelector('a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]');
            if (!el) throw new Error("Status 192 not found");

            el.classList.add('selected');

            const icon = el.querySelector('i');
            if (icon) {
                icon.className = icon.className.replace(/\\bicon-circle-blank\\b/g, '').trim();
                icon.className = icon.className.replace(/\\bicon-ok-sign\\b/g, '').trim();
                icon.className = (icon.className + ' icon-ok-sign').trim();
            }

            const parent = document.querySelector('#caseStatus2');
            if (parent) {
                parent.classList.add('selected');
                const picon = parent.querySelector('i');
                if (picon) {
                    picon.className = picon.className.replace(/\\bicon-circle-blank\\b/g, '').trim();
                    picon.className = picon.className.replace(/\\bicon-ok-sign\\b/g, '').trim();
                    picon.className = (picon.className + ' icon-ok-sign').trim();
                }
            }

            const hidden = document.querySelector('#filterCaseStatus');
            if (hidden) hidden.value = '192';

            const label = document.querySelector('#filterCaseStatusLabel');
            if (label) label.innerText = '1 Selected';
        }
        """
    )
    log.info("Forced UI + hidden filterCaseStatus=192")


def get_filter_state(page):
    return page.evaluate(
        """
        () => {
            const selected192 = document.querySelector('a[data-statusid="192"][data-parentid="2"]');
            const hidden = document.querySelector('#filterCaseStatus');
            const label = document.querySelector('#filterCaseStatusLabel');
            return {
                label: (label?.innerText || '').trim(),
                hidden_filterCaseStatus: hidden ? hidden.value : '',
                selected192_class: selected192 ? selected192.className : ''
            };
        }
        """
    )


def wait_for_case_rows(page, timeout_ms=25000):
    log.info("Waiting for Miami search results...")
    waited = 0
    step = 1000
    while waited < timeout_ms:
        rows = page.locator('tr.load-case.table-row.link[data-caseid]').count()
        if rows > 0:
            log.info("Rows after search: %s", rows)
            return rows
        page.wait_for_timeout(step)
        waited += step

    raise RuntimeError("No case rows found after Miami search")


def wait_for_page_change(page, previous_first_caseid: str, timeout_ms: int = 20000):
    log.info("Waiting for next Miami page to load...")
    waited = 0
    step = 1000

    while waited < timeout_ms:
        try:
            rows = page.locator('tr.load-case.table-row.link[data-caseid]')
            count = rows.count()
            if count > 0:
                first_caseid = rows.first.get_attribute("data-caseid") or ""
                if first_caseid and first_caseid != previous_first_caseid:
                    log.info("Next Miami page loaded. First case changed %s -> %s", previous_first_caseid, first_caseid)
                    page.wait_for_timeout(1500)
                    return True
        except Exception:
            pass

        page.wait_for_timeout(step)
        waited += step

    log.warning("Timed out waiting for next page change after %sms", timeout_ms)
    return False


def run_search_flow(page):
    log.info("Running Miami ACTIVE-only flow...")

    if not click_safe(page, "a.filters-reset", "RESET FILTERS"):
        raise RuntimeError("Could not reset filters")
    page.wait_for_timeout(1500)

    if not click_safe(page, "#filterButtonStatus", "FILTER BUTTON"):
        raise RuntimeError("Could not open filter button")
    page.wait_for_timeout(1000)

    force_clear_all_active_statuses(page)
    page.wait_for_timeout(400)

    force_select_only_192(page)
    page.wait_for_timeout(800)

    state = get_filter_state(page)
    log.info("SEARCH STATE BEFORE SUBMIT: %s", state)

    if not click_safe(page, "button.filters-submit", "SEARCH BUTTON"):
        raise RuntimeError("Could not click search")

    wait_for_case_rows(page)


def open_list_and_apply_filter(page):
    page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(6000)
    run_search_flow(page)


# =========================
# PAGINATION
# =========================
def get_results_summary(page) -> Dict:
    return page.evaluate(
        """
        () => {
            const bodyText = document.body.innerText || '';

            const pageLinks = Array.from(document.querySelectorAll('a'))
                .map(a => (a.innerText || '').trim())
                .filter(x => /^Page\\s+\\d+/i.test(x));

            const rows = document.querySelectorAll('tr.load-case.table-row.link[data-caseid]').length;

            const currentPageText =
                (document.querySelector('.pagination .active')?.innerText || '').trim() ||
                '';

            return {
                rows_on_page: rows,
                page_links: pageLinks,
                current_page_text: currentPageText,
                body_sample: bodyText.slice(0, 3000)
            };
        }
        """
    
    )


def collect_case_rows(page) -> List[Dict]:
    rows = page.locator('tr.load-case.table-row.link[data-caseid]')
    count = rows.count()
    log.info("Collecting case rows from current page: %s", count)

    items = []
    for i in range(count):
        row = rows.nth(i)
        items.append({
            "index": i,
            "caseid": row.get_attribute("data-caseid") or "",
            "row_text": clean_text(row.inner_text()),
        })
    return items


def parse_total_pages(page) -> int:
    summary = get_results_summary(page)
    links = summary.get("page_links", [])
    max_page = 1

    for txt in links:
        m = re.search(r"Page\s+(\d+)", txt, re.I)
        if m:
            max_page = max(max_page, int(m.group(1)))

    return max_page


def click_next_page(page) -> bool:
    current_rows = page.locator('tr.load-case.table-row.link[data-caseid]')
    previous_first_caseid = ""
    try:
        if current_rows.count() > 0:
            previous_first_caseid = current_rows.first.get_attribute("data-caseid") or ""
    except Exception:
        pass

    selectors = [
        "a[rel='next']",
        "a:has-text('Next')",
        ".pagination a.next",
    ]

    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                target = loc.first
                try:
                    target.scroll_into_view_if_needed(timeout=3000)
                except Exception:
                    pass

                try:
                    target.click(timeout=8000)
                except Exception:
                    try:
                        target.click(force=True, timeout=8000)
                    except Exception:
                        continue

                if wait_for_page_change(page, previous_first_caseid, timeout_ms=22000):
                    return True
        except Exception:
            continue

    try:
        page_links = page.locator("a")
        count = page_links.count()
        for i in range(count):
            a = page_links.nth(i)
            txt = clean_text(a.inner_text())
            if txt.lower() == "next":
                try:
                    a.scroll_into_view_if_needed(timeout=3000)
                except Exception:
                    pass

                try:
                    a.click(timeout=8000)
                except Exception:
                    try:
                        a.click(force=True, timeout=8000)
                    except Exception:
                        continue

                if wait_for_page_change(page, previous_first_caseid, timeout_ms=22000):
                    return True
    except Exception:
        pass

    return False


# =========================
# DETAIL PARSING
# =========================
def parse_row_text(row_text: str) -> Dict:
    parts = re.split(r"\s{2,}|\t", row_text)
    parts = [clean_text(x) for x in parts if clean_text(x)]
    payload = {
        "status": parts[0] if len(parts) > 0 else "",
        "case_number": parts[1] if len(parts) > 1 else "",
        "date_created": parts[2] if len(parts) > 2 else "",
        "application_number": parts[3] if len(parts) > 3 else "",
        "parcel_number": parts[4] if len(parts) > 4 else "",
        "sale_date": parts[5] if len(parts) > 5 else "",
    }
    return payload


def open_case_by_index(page, index: int) -> Dict:
    rows = page.locator('tr.load-case.table-row.link[data-caseid]')
    count = rows.count()
    if index >= count:
        raise RuntimeError(f"Index {index} out of range. Row count={count}")

    row = rows.nth(index)
    caseid = row.get_attribute("data-caseid") or ""
    row_text = clean_text(row.inner_text())

    handle = row.element_handle()
    if handle is None:
        raise RuntimeError(f"Could not get handle for case row {index}")

    if not click_element_handle_safe(handle, page, f"CASE ROW {caseid}"):
        raise RuntimeError(f"Could not open case detail for caseid {caseid}")

    log.info("Waiting for case detail to load...")
    page.wait_for_timeout(7000)

    try:
        page.wait_for_selector("text=CASE SUMMARY", timeout=15000)
    except PlaywrightTimeoutError:
        log.warning("CASE SUMMARY title not found; continuing with DOM parse attempt")

    return {"caseid": caseid, "row_text": row_text}


def parse_case_header(page):
    data = page.evaluate(
        """
        () => {
            const bodyText = document.body.innerText || '';

            function byLabel(label) {
                const nodes = Array.from(document.querySelectorAll('body *'));
                for (const node of nodes) {
                    const txt = (node.innerText || '').trim();
                    if (txt === label) {
                        const parent = node.parentElement;
                        if (parent) {
                            const parentText = (parent.innerText || '').trim();
                            if (parentText && parentText !== label) {
                                return parentText.replace(label, '').trim();
                            }
                        }
                        const next = node.nextElementSibling;
                        if (next) return (next.innerText || '').trim();
                    }
                }
                return '';
            }

            const parcelLink = document.querySelector('#propertyAppraiserLink');

            return {
                tax_collector_number: byLabel('Tax Collector #'),
                applicant_number: byLabel('Applicant Number'),
                case_number: byLabel('Case Number'),
                parcel_number: byLabel('Parcel Number') || (parcelLink ? (parcelLink.innerText || '').trim() : ''),
                case_status: byLabel('Case Status'),
                raw_body: bodyText,
                parcel_link: parcelLink ? {
                    text: (parcelLink.innerText || '').trim(),
                    href: parcelLink.getAttribute('href') || ''
                } : null
            };
        }
        """
    )

    raw_body = data.get("raw_body", "")
    data["redemption_amount"] = money_from_text(
        re.search(r"Redemption Amount:\s*(\$[\d,]+(?:\.\d{2})?)", raw_body).group(0)
        if re.search(r"Redemption Amount:\s*(\$[\d,]+(?:\.\d{2})?)", raw_body) else ""
    )
    if data["redemption_amount"]:
        data["redemption_amount"] = re.search(r"\$[\d,]+(?:\.\d{2})?", data["redemption_amount"]).group(0)

    data["opening_bid"] = money_from_text(
        re.search(r"Opening Bid:\s*(\$[\d,]+(?:\.\d{2})?)", raw_body).group(0)
        if re.search(r"Opening Bid:\s*(\$[\d,]+(?:\.\d{2})?)", raw_body) else ""
    )
    if data["opening_bid"]:
        data["opening_bid"] = re.search(r"\$[\d,]+(?:\.\d{2})?", data["opening_bid"]).group(0)

    return data


def parse_case_summary(page):
    data = page.evaluate(
        """
        () => {
            function byLabel(label) {
                const nodes = Array.from(document.querySelectorAll('body *'));
                for (const node of nodes) {
                    const txt = (node.innerText || '').trim();
                    if (txt === label) {
                        const parent = node.parentElement;
                        if (parent) {
                            const parentText = (parent.innerText || '').trim();
                            if (parentText && parentText !== label) {
                                return parentText.replace(label, '').trim();
                            }
                        }
                        const next = node.nextElementSibling;
                        if (next) return (next.innerText || '').trim();
                    }
                }
                return '';
            }

            return {
                app_receive_date: byLabel('App Receive Date'),
                sale_date: byLabel('Sale Date'),
                publish_dates: byLabel('Publish Date(s)'),
                property_address: byLabel('Property Address'),
                homestead: byLabel('Homestead'),
                legal_description: byLabel('Legal Description')
            };
        }
        """
    )

    data["property_address"] = clean_multiline(data.get("property_address", ""))
    pub = data.get("publish_dates", "")
    if pub:
        data["publish_dates_list"] = [clean_text(x) for x in pub.splitlines() if clean_text(x)]
    else:
        data["publish_dates_list"] = []

    return data


def extract_case_detail(page, base_case: Dict) -> Dict:
    header_data = parse_case_header(page)
    summary_data = parse_case_summary(page)

    detail = {
        **base_case,
        "header": {
            "tax_collector_number": clean_text(header_data.get("tax_collector_number", "")),
            "applicant_number": clean_text(header_data.get("applicant_number", "")),
            "case_number": clean_text(header_data.get("case_number", "")),
            "parcel_number": clean_text(header_data.get("parcel_number", "")),
            "case_status": clean_text(header_data.get("case_status", "")),
            "redemption_amount": clean_text(header_data.get("redemption_amount", "")),
            "opening_bid": clean_text(header_data.get("opening_bid", "")),
        },
        "case_summary": summary_data,
        "parcel_link": header_data.get("parcel_link"),
        "has_case_summary": "CASE SUMMARY" in header_data.get("raw_body", ""),
    }

    log.info("CASE DETAIL EXTRACTED: %s", json.dumps(detail, indent=2))
    return detail


def open_property_appraiser(context, parcel_href):
    if not parcel_href:
        raise RuntimeError("No parcel/property appraiser href found")

    log.info("Opening Property Appraiser URL: %s", parcel_href)
    pa_page = context.new_page()
    pa_page.goto(parcel_href, wait_until="domcontentloaded", timeout=60000)
    pa_page.wait_for_timeout(10000)
    return pa_page


def extract_property_appraiser(pa_page):
    data = pa_page.evaluate(
        """
        () => {
            function findField(label) {
                const nodes = Array.from(document.querySelectorAll('body *'));
                for (const el of nodes) {
                    const txt = (el.innerText || '').trim();
                    if (txt === label || txt.startsWith(label)) {
                        const parent = el.parentElement;
                        if (parent) {
                            const parentText = (parent.innerText || '').trim();
                            if (parentText && parentText !== label) {
                                return parentText.replace(label, '').trim();
                            }
                        }
                        const next = el.nextElementSibling;
                        if (next) return (next.innerText || '').trim();
                    }
                }
                return '';
            }

            return {
                folio: findField('Folio:'),
                subdivision: findField('Sub-Division:'),
                property_address: findField('Property Address'),
                owner: findField('Owner'),
                mailing_address: findField('Mailing Address'),
                pa_primary_zone: findField('PA Primary Zone'),
                primary_land_use: findField('Primary Land Use'),
                beds_baths_half: findField('Beds / Baths /Half')
            };
        }
        """
    )

    data["property_address"] = clean_multiline(data.get("property_address", ""))
    data["mailing_address"] = clean_multiline(data.get("mailing_address", ""))

    log.info("PROPERTY APPRAISER EXTRACTED: %s", json.dumps(data, indent=2))
    return data


def build_final_record(case_detail: Dict, pa_data: Dict) -> Dict:
    row_parsed = parse_row_text(case_detail.get("row_text", ""))
    header = case_detail.get("header", {})
    summary = case_detail.get("case_summary", {})
    parcel = case_detail.get("parcel_link", {}) or {}

    parcel_number = header.get("parcel_number") or row_parsed.get("parcel_number") or parcel.get("text", "")
    case_number = header.get("case_number") or row_parsed.get("case_number", "")

    record = {
        "source": "MiamiDade",
        "county": "Miami-Dade",
        "visible_in_app": True,
        "auction_status": "SOON",
        "auction_date": None,
        "auction_time": None,
        "auction_location": "Miami-Dade RealForeclose",
        "auction_url": AUCTION_URL,
        "external_id": f"{case_number}|{parcel_number}",
        "caseid": case_detail.get("caseid", ""),
        "case_number": case_number,
        "case_status": header.get("case_status") or row_parsed.get("status", ""),
        "tax_collector_number": header.get("tax_collector_number", ""),
        "application_number": row_parsed.get("application_number", ""),
        "applicant_number": header.get("applicant_number", ""),
        "parcel_number": parcel_number,
        "sale_date": summary.get("sale_date") or row_parsed.get("sale_date", ""),
        "app_receive_date": summary.get("app_receive_date", ""),
        "publish_dates": summary.get("publish_dates_list", []),
        "redemption_amount": header.get("redemption_amount", ""),
        "opening_bid": header.get("opening_bid", ""),
        "property_address_case": summary.get("property_address", ""),
        "property_address_pa": pa_data.get("property_address", ""),
        "legal_description": summary.get("legal_description", ""),
        "homestead": summary.get("homestead", ""),
        "folio": pa_data.get("folio", ""),
        "subdivision": pa_data.get("subdivision", ""),
        "owner": pa_data.get("owner", ""),
        "mailing_address": pa_data.get("mailing_address", ""),
        "pa_primary_zone": pa_data.get("pa_primary_zone", ""),
        "primary_land_use": pa_data.get("primary_land_use", ""),
        "beds_baths_half": pa_data.get("beds_baths_half", ""),
        "parcel_appraiser_url": parcel.get("href", ""),
    }
    return record


def build_properties_payload(record: dict) -> dict:
    addr_full = record.get("property_address_pa") or record.get("property_address_case") or ""

    city = None
    state_address = None
    zip_code = None

    m = re.search(r",\s*([A-Z ]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", addr_full, re.I)
    if m:
        city = clean_text(m.group(1)).title()
        state_address = clean_text(m.group(2)).upper()
        zip_code = clean_text(m.group(3))

    address_only = addr_full
    if m:
        address_only = addr_full[:m.start()].strip(" ,")

    return {
        "county": "Miami-Dade",
        "state": "FL",
        "node": record.get("caseid"),
        "external_id": record.get("external_id"),
        "auction_source_url": record.get("parcel_appraiser_url") or AUCTION_URL,
        "tax_sale_id": record.get("case_number"),
        "parcel_number": record.get("parcel_number"),
        "sale_date": record.get("sale_date"),
        "opening_bid": normalize_money(record.get("opening_bid")),
        "deed_status": record.get("case_status"),
        "applicant_name": record.get("applicant_number"),
        "owner_name": record.get("owner"),
        "address": address_only or None,
        "city": city,
        "state_address": state_address,
        "zip": zip_code,
        "mailing_address": record.get("mailing_address"),
        "legal_description": record.get("legal_description"),
        "homestead": record.get("homestead"),
        "folio": record.get("folio"),
        "property_address_case": record.get("property_address_case"),
        "property_address_pa": record.get("property_address_pa"),
        "auction_location": record.get("auction_location"),
        "auction_url": record.get("auction_url"),
        "redemption_amount": normalize_money(record.get("redemption_amount")),
    }


def scrape_case_by_index_on_current_page(context, page, index: int) -> Dict:
    base_case = open_case_by_index(page, index)
    case_detail = extract_case_detail(page, base_case)

    parcel_href = (case_detail.get("parcel_link") or {}).get("href", "")
    pa_data = {}
    if parcel_href:
        pa_page = open_property_appraiser(context, parcel_href)
        pa_data = extract_property_appraiser(pa_page)
        pa_page.close()
    else:
        log.warning("No parcel href found for case %s", base_case.get("caseid"))

    record = build_final_record(case_detail, pa_data)

    return {
        "case_detail": case_detail,
        "property_appraiser": pa_data,
        "record": record,
    }


# =========================
# MAIN
# =========================
def run_miami():
    log.info("=== MIAMI FINAL OPERATIONAL V3 + PAGE-BY-PAGE + SUPABASE ===")

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
            viewport={"width": 1366, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
        )

        page = context.new_page()

        open_list_and_apply_filter(page)
        total_pages = parse_total_pages(page)
        log.info("Detected total Miami pages: %s", total_pages)

        results = []
        failures = []
        supabase_results = []
        processed_caseids = set()

        total_processed = 0
        current_page_num = 1

        while current_page_num <= total_pages and total_processed < MAX_LOTS:
            rows = collect_case_rows(page)
            row_count = len(rows)

            if row_count == 0:
                log.warning("No rows found on page %s", current_page_num)
                break

            log.info("Processing all %s rows from page %s before moving forward", row_count, current_page_num)

            for idx, row_meta in enumerate(rows):
                if total_processed >= MAX_LOTS:
                    break

                caseid = row_meta.get("caseid")
                if not caseid or caseid in processed_caseids:
                    continue

                log.info(
                    "[%s/%s] Scraping caseid=%s page=%s row=%s ...",
                    total_processed + 1,
                    MAX_LOTS,
                    caseid,
                    current_page_num,
                    idx,
                )

                try:
                    result = scrape_case_by_index_on_current_page(context, page, idx)
                    record = result["record"]
                    results.append(record)
                    processed_caseids.add(caseid)
                    total_processed += 1

                    prop_payload = build_properties_payload(record)
                    sb_result = supabase_upsert_property(prop_payload)
                    supabase_results.append(sb_result)

                    send_to_app(prop_payload)

                    log.info("SUCCESS CASE %s: %s", total_processed, record.get("external_id"))

                except Exception as e:
                    log.exception("FAILED CASE caseid=%s page=%s row=%s: %s", caseid, current_page_num, idx, e)
                    failures.append({
                        "caseid": caseid,
                        "page_num": current_page_num,
                        "row_index": idx,
                        "error": str(e),
                    })

                if idx < row_count - 1 and total_processed < MAX_LOTS:
                    open_list_and_apply_filter(page)

                    for _ in range(1, current_page_num):
                        moved = click_next_page(page)
                        if not moved:
                            raise RuntimeError(f"Could not return to page {current_page_num} after scraping caseid={caseid}")

            if total_processed >= MAX_LOTS:
                break

            if current_page_num < total_pages:
                moved = click_next_page(page)
                if not moved:
                    log.warning("Could not navigate to page %s", current_page_num + 1)
                    break

            current_page_num += 1

        summary = get_results_summary(page)

        final_payload = {
            "source": "MiamiDade",
            "mode": "final_operational_v3_page_by_page_supabase",
            "total_pages_detected": total_pages,
            "rows_detected": len(results) + len(failures),
            "records_count": len(results),
            "failures_count": len(failures),
            "supabase_results": supabase_results,
            "page_summary": summary,
            "records": results,
            "failures": failures,
        }

        log.info("===== FINAL PAYLOAD =====")
        log.info(json.dumps(final_payload, indent=2))
        print(json.dumps(final_payload, indent=2))

        browser.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    run_miami()