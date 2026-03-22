import json
import logging
import os
import re
import time
from typing import Dict, List, Optional
from urllib.parse import quote

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

log = logging.getLogger("miami")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"
AUCTION_URL = "https://www.miamidade.realforeclose.com/index.cfm"

APP_API_BASE = (os.getenv("APP_API_BASE", "") or "").strip().rstrip("/")
APP_API_TOKEN = (os.getenv("APP_API_TOKEN", "") or "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

SUPABASE_URL = (os.getenv("SUPABASE_URL", "") or "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or "").strip()
CAN_CHECK_SUPABASE = bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
MAX_LOTS = int(os.getenv("MAX_LOTS", "100"))


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


def clean_bid(v):
    return re.sub(r"[^\d.]", "", str(v or ""))


def norm(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


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


def run_search_flow(page):
    log.info("Running Miami ACTIVE-only flow...")

    if not click_safe(page, "a.filters-reset", "RESET FILTERS"):
        raise RuntimeError("Could not reset filters")
    page.wait_for_timeout(2000)

    if not click_safe(page, "#filterButtonStatus", "FILTER BUTTON"):
        raise RuntimeError("Could not open filter button")
    page.wait_for_timeout(1200)

    force_clear_all_active_statuses(page)
    page.wait_for_timeout(600)

    force_select_only_192(page)
    page.wait_for_timeout(1000)

    state = get_filter_state(page)
    log.info("SEARCH STATE BEFORE SUBMIT: %s", state)

    if not click_safe(page, "button.filters-submit", "SEARCH BUTTON"):
        raise RuntimeError("Could not click search")

    wait_for_case_rows(page)


def get_results_summary(page) -> Dict:
    data = page.evaluate(
        """
        () => {
            const bodyText = document.body.innerText || '';

            const pageLinks = Array.from(document.querySelectorAll('a'))
                .map(a => (a.innerText || '').trim())
                .filter(x => /^Page\\s+\\d+/i.test(x));

            const rows = document.querySelectorAll('tr.load-case.table-row.link[data-caseid]').length;

            const hiddenPerPage = document.querySelector('#filterCasesPerPage');

            return {
                rows_on_page: rows,
                page_links: pageLinks,
                hidden_per_page: hiddenPerPage ? hiddenPerPage.value : '',
                body_sample: bodyText.slice(0, 3000)
            };
        }
        """
    )
    return data


def try_set_results_per_page(page, target="100") -> Dict:
    log.info("Trying to set results/page to %s...", target)

    success = page.evaluate(
        """
        (target) => {
            const sel = document.querySelector('#filterCasesPerPage');
            if (!sel) return {ok:false, reason:'filterCasesPerPage not found'};

            sel.value = String(target);

            sel.dispatchEvent(new Event('input', { bubbles: true }));
            sel.dispatchEvent(new Event('change', { bubbles: true }));

            return {ok:true, value: sel.value};
        }
        """,
        target,
    )

    log.info("Results/page set attempt result: %s", success)

    if not success.get("ok"):
        return {"ok": False, "target": target, "reason": success.get("reason", "unknown")}

    if not click_safe(page, "button.filters-submit", f"SEARCH BUTTON AFTER {target}/PAGE"):
        return {"ok": False, "target": target, "reason": "could not re-submit search"}

    wait_for_case_rows(page)

    page.wait_for_timeout(4000)
    summary = get_results_summary(page)

    rows = summary.get("rows_on_page", 0)
    hidden = summary.get("hidden_per_page", "")
    ok = (str(hidden) == str(target)) or (rows > 20 and str(target) == "100")

    result = {
        "ok": ok,
        "target": target,
        "rows_on_page": rows,
        "hidden_per_page": hidden,
        "page_links_count": len(summary.get("page_links", [])),
    }
    log.info("Results/page final state: %s", result)
    return result


def set_results_per_page_with_fallback(page):
    result_100 = try_set_results_per_page(page, "100")
    if result_100.get("ok"):
        return {"mode": "100", "details": result_100}

    log.info("Falling back to 20 results/page...")
    result_20 = try_set_results_per_page(page, "20")
    return {"mode": "20", "details": result_20}


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
    m_red = re.search(r"Redemption Amount:\s*(\$[\d,]+(?:\.\d{2})?)", raw_body)
    m_bid = re.search(r"Opening Bid:\s*(\$[\d,]+(?:\.\d{2})?)", raw_body)

    data["redemption_amount"] = m_red.group(1) if m_red else ""
    data["opening_bid"] = m_bid.group(1) if m_bid else ""

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
    data["publish_dates_list"] = [clean_text(x) for x in pub.splitlines() if clean_text(x)] if pub else []

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


def scrape_one_case(context, page, index: int) -> Dict:
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
# PAYLOAD / DEDUP / SEND
# =========================
def parse_address_components(full_address: str) -> Dict[str, Optional[str]]:
    out = {
        "address": None,
        "city": None,
        "state_address": None,
        "zip": None,
    }

    if not full_address:
        return out

    text = norm(full_address)

    m = re.match(
        r"^(.*?)(?:,\s*|\s+)([A-Za-z .'-]+),?\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$",
        text,
        re.I,
    )
    if m:
        out["address"] = norm(m.group(1))
        out["city"] = norm(m.group(2)).title()
        out["state_address"] = m.group(3).upper()
        out["zip"] = m.group(4)
        return out

    m2 = re.match(r"^(.*?)(?:,\s*|\s+)([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", text, re.I)
    if m2:
        out["address"] = norm(m2.group(1))
        out["state_address"] = m2.group(2).upper()
        out["zip"] = m2.group(3)
        return out

    parts = [x.strip() for x in text.split(",") if x.strip()]
    if len(parts) >= 2:
        out["address"] = parts[0]
        last = parts[-1]
        m3 = re.search(r"\b([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\b", last, re.I)
        if m3:
            out["state_address"] = m3.group(1).upper()
            out["zip"] = m3.group(2)
            city = re.sub(r"\b[A-Z]{2}\s+\d{5}(?:-\d{4})?\b", "", last, flags=re.I).strip(" ,")
            if city:
                out["city"] = city.title()
        elif len(parts) >= 3:
            out["city"] = parts[1].title()

    if not out["address"]:
        out["address"] = text

    return out


def choose_best_property_address(record: Dict) -> Dict[str, Optional[str]]:
    pa_addr = parse_address_components(record.get("property_address_pa", ""))
    case_addr = parse_address_components(record.get("property_address_case", ""))

    best = {
        "address": None,
        "city": None,
        "state_address": "FL",
        "zip": None,
        "address_source": None,
    }

    if pa_addr.get("address"):
        best["address"] = pa_addr.get("address")
        best["city"] = pa_addr.get("city")
        best["state_address"] = pa_addr.get("state_address") or "FL"
        best["zip"] = pa_addr.get("zip")
        best["address_source"] = "PROPERTY_APPRAISER"
        return best

    if case_addr.get("address"):
        best["address"] = case_addr.get("address")
        best["city"] = case_addr.get("city")
        best["state_address"] = case_addr.get("state_address") or "FL"
        best["zip"] = case_addr.get("zip")
        best["address_source"] = "CASE_SUMMARY"
        return best

    return best


def build_properties_payload(record: Dict) -> Dict:
    addr = choose_best_property_address(record)

    payload = {
        "county": "MiamiDade",
        "state": "FL",
        "node": record.get("case_number"),
        "auction_source_url": record.get("parcel_appraiser_url") or record.get("auction_url"),
        "tax_sale_id": record.get("case_number"),
        "parcel_number": record.get("parcel_number"),
        "sale_date": record.get("sale_date"),
        "opening_bid": clean_bid(record.get("opening_bid")),
        "deed_status": record.get("case_status"),
        "applicant_name": record.get("owner") or record.get("applicant_number"),
        "pdf_url": None,
        "address": addr.get("address"),
        "city": addr.get("city"),
        "state_address": addr.get("state_address"),
        "zip": addr.get("zip"),
        "address_source": addr.get("address_source"),
    }
    return payload


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
        f"&select=id,node,parcel_number,sale_date,address,city,state_address,zip,pdf_url,auction_source_url,opening_bid,deed_status,applicant_name"
        f"&limit=1"
    )

    try:
        r = requests.get(url, headers=sb_headers(), timeout=30)
        if r.status_code == 200:
            arr = r.json()
            return arr[0] if arr else None
        log.warning("supabase_find_existing_case unexpected status=%s body=%s", r.status_code, r.text[:300])
    except Exception as e:
        log.warning("supabase_find_existing_case failed: %s", str(e))

    return None


def supabase_find_existing_property(parcel_number: str, sale_date: str):
    if not CAN_CHECK_SUPABASE or not parcel_number or not sale_date:
        return None

    url = (
        f"{SUPABASE_URL}/rest/v1/properties"
        f"?county=eq.MiamiDade"
        f"&parcel_number=eq.{quote(str(parcel_number), safe='')}"
        f"&sale_date=eq.{quote(str(sale_date), safe='')}"
        f"&select=id,node,parcel_number,sale_date,address,city,state_address,zip,pdf_url,auction_source_url,opening_bid,deed_status,applicant_name"
        f"&limit=1"
    )

    try:
        r = requests.get(url, headers=sb_headers(), timeout=30)
        if r.status_code == 200:
            arr = r.json()
            return arr[0] if arr else None
        log.warning("supabase_find_existing_property unexpected status=%s body=%s", r.status_code, r.text[:300])
    except Exception as e:
        log.warning("supabase_find_existing_property failed: %s", str(e))

    return None


def payload_quality_score(payload: dict) -> int:
    fields = [
        "address",
        "city",
        "state_address",
        "zip",
        "pdf_url",
        "auction_source_url",
        "parcel_number",
        "sale_date",
        "opening_bid",
        "deed_status",
        "applicant_name",
    ]
    return sum(1 for f in fields if payload.get(f) not in (None, ""))


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
        "pdf_url",
        "auction_source_url",
    ]

    for f in important_fields:
        old_val = (existing.get(f) or "").strip() if existing.get(f) else ""
        new_val = (payload.get(f) or "").strip() if payload.get(f) else ""
        if not old_val and new_val:
            return True

    return False


def should_send_payload(payload: dict):
    node = payload.get("node")
    parcel = payload.get("parcel_number")
    sale_date = payload.get("sale_date")

    existing_case = supabase_find_existing_case(node)
    if existing_case:
        if payload_is_better_than_existing(payload, existing_case):
            return True, "existing case found, but new payload is better"
        return False, "duplicate case/node already exists"

    existing_prop = supabase_find_existing_property(parcel, sale_date)
    if existing_prop:
        if payload_is_better_than_existing(payload, existing_prop):
            return True, "existing parcel_number + sale_date found, but new payload is better"
        return False, "duplicate parcel_number + sale_date already exists"

    return True, "new record"


def upsert_property_to_supabase(payload: Dict) -> Dict:
    if not CAN_CHECK_SUPABASE:
        return {
            "sent": False,
            "status_code": None,
            "external_id": payload.get("node"),
            "response_text": "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing",
        }

    url = f"{SUPABASE_URL}/rest/v1/properties"

    headers = sb_headers()
    headers["Prefer"] = "resolution=merge-duplicates,return=representation"

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        ok = r.status_code in (200, 201)

        if ok:
            log.info("SUPABASE UPSERT OK: status=%s node=%s", r.status_code, payload.get("node"))
        else:
            log.error(
                "SUPABASE UPSERT FAILED: status=%s node=%s body=%s",
                r.status_code,
                payload.get("node"),
                r.text[:500],
            )

        return {
            "sent": ok,
            "status_code": r.status_code,
            "external_id": payload.get("node"),
            "response_text": r.text[:1000],
        }
    except Exception as e:
        log.exception("SUPABASE UPSERT EXCEPTION node=%s", payload.get("node"))
        return {
            "sent": False,
            "status_code": None,
            "external_id": payload.get("node"),
            "response_text": str(e),
        }


def send_to_app_and_supabase(payload: Dict) -> Dict:
    dedup_ok, reason = should_send_payload(payload)
    log.info("DEDUP CHECK node=%s -> %s", payload.get("node"), reason)

    if not dedup_ok:
        return {
            "sent": False,
            "status_code": None,
            "external_id": payload.get("node"),
            "response_text": reason,
        }

    app_ok = send(payload)
    if not app_ok:
        log.warning("APP INGEST FAILED node=%s", payload.get("node"))

    sb_result = upsert_property_to_supabase(payload)
    return sb_result


def run_miami():
    log.info("=== MIAMI FINAL OPERATIONAL V2 + APP + SUPABASE(PROPERTIES) ===")

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
        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(8000)

        run_search_flow(page)

        per_page_mode = set_results_per_page_with_fallback(page)
        summary = get_results_summary(page)
        case_rows = collect_case_rows(page)

        results = []
        failures = []
        supabase_results = []

        total_to_read = min(len(case_rows), MAX_LOTS)

        for i in range(total_to_read):
            log.info("[%s/%s] Scraping case row on current page...", i + 1, total_to_read)

            page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(6000)
            run_search_flow(page)
            set_results_per_page_with_fallback(page)
            page.wait_for_timeout(4000)

            try:
                result = scrape_one_case(context, page, i)
                record = result["record"]
                results.append(record)

                payload = build_properties_payload(record)
                sb_result = send_to_app_and_supabase(payload)
                supabase_results.append(sb_result)

                log.info("SUCCESS CASE %s: %s", i + 1, record.get("external_id"))
                time.sleep(0.5)

            except Exception as e:
                log.exception("FAILED CASE INDEX %s: %s", i, e)
                failures.append({
                    "index": i,
                    "error": str(e),
                })

        final_payload = {
            "source": "MiamiDade",
            "mode": "final_operational_v2_properties",
            "auction_url_default": AUCTION_URL,
            "results_per_page_mode": per_page_mode,
            "page_summary": summary,
            "rows_detected": len(case_rows),
            "records_count": len(results),
            "failures_count": len(failures),
            "supabase_results": supabase_results,
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