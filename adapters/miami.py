import json
import logging
import os
import re
from typing import Dict, List, Optional

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("miami")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"
AUCTION_URL = "https://www.miamidade.realforeclose.com/index.cfm"

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "tax_sales")


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


def click_safe(page, selector, name, timeout=6000):
    try:
        page.click(selector, timeout=timeout)
        log.info(f"{name} clicked (normal)")
        return True
    except Exception:
        pass

    try:
        page.locator(selector).first.click(force=True, timeout=timeout)
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


def click_element_handle_safe(el, page, name):
    try:
        el.click(timeout=6000)
        log.info(f"{name} clicked (normal)")
        return True
    except Exception:
        pass

    try:
        el.click(force=True, timeout=6000)
        log.info(f"{name} clicked (force)")
        return True
    except Exception:
        pass

    try:
        page.evaluate("(el) => el.click()", el)
        log.info(f"{name} clicked (JS fallback)")
        return True
    except Exception as e:
        log.error(f"{name} FAILED: {e}")
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
    log.info(f"SEARCH STATE BEFORE SUBMIT: {state}")

    if not click_safe(page, "button.filters-submit", "SEARCH BUTTON"):
        raise RuntimeError("Could not click search")

    wait_for_case_rows(page)


def wait_for_case_rows(page, timeout_ms=25000):
    log.info("Waiting for Miami search results...")
    waited = 0
    step = 1000
    while waited < timeout_ms:
        rows = page.locator('tr.load-case.table-row.link[data-caseid]').count()
        if rows > 0:
            log.info(f"Rows after search: {rows}")
            return rows
        page.wait_for_timeout(step)
        waited += step

    raise RuntimeError("No case rows found after Miami search")


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
    log.info(f"Trying to set results/page to {target}...")

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

    log.info(f"Results/page set attempt result: {success}")

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
    log.info(f"Results/page final state: {result}")
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
    log.info(f"Collecting case rows from current page: {count}")

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

    log.info(f"CASE DETAIL EXTRACTED: {json.dumps(detail, indent=2)}")
    return detail


def open_property_appraiser(context, parcel_href):
    if not parcel_href:
        raise RuntimeError("No parcel/property appraiser href found")

    log.info(f"Opening Property Appraiser URL: {parcel_href}")
    pa_page = context.new_page()
    pa_page.goto(parcel_href, wait_until="domcontentloaded", timeout=60000)
    pa_page.wait_for_timeout(10000)
    return pa_page


def extract_property_appraiser(pa_page):
    data = pa_page.evaluate(
        """
        () => {
            const bodyText = document.body.innerText || '';

            function afterLabel(label) {
                const idx = bodyText.indexOf(label);
                if (idx === -1) return '';
                const tail = bodyText.slice(idx + label.length).trim();
                return tail;
            }

            function extractBetween(startLabel, endLabels) {
                const idx = bodyText.indexOf(startLabel);
                if (idx === -1) return '';

                let tail = bodyText.slice(idx + startLabel.length).trim();
                let cut = tail.length;

                for (const endLabel of endLabels) {
                    const eidx = tail.indexOf(endLabel);
                    if (eidx !== -1 && eidx < cut) cut = eidx;
                }

                return tail.slice(0, cut).trim();
            }

            return {
                folio: extractBetween('Folio:', ['Sub-Division:', 'Property Address', 'Owner']),
                subdivision: extractBetween('Sub-Division:', ['Property Address', 'Owner', 'Mailing Address']),
                property_address: extractBetween('Property Address', ['Owner', 'Mailing Address', 'PA Primary Zone']),
                owner: extractBetween('Owner', ['Mailing Address', 'PA Primary Zone', 'Primary Land Use']),
                mailing_address: extractBetween('Mailing Address', ['PA Primary Zone', 'Primary Land Use', 'Beds / Baths /Half']),
                pa_primary_zone: extractBetween('PA Primary Zone', ['Primary Land Use', 'Beds / Baths /Half', 'Floors']),
                primary_land_use: extractBetween('Primary Land Use', ['Beds / Baths /Half', 'Floors', 'Living Units']),
                beds_baths_half: extractBetween('Beds / Baths /Half', ['Floors', 'Living Units', 'Actual Area'])
            };
        }
        """
    )

    data["folio"] = clean_text(data.get("folio", ""))
    data["subdivision"] = clean_text(data.get("subdivision", ""))
    data["property_address"] = clean_multiline(data.get("property_address", ""))
    data["owner"] = clean_text(data.get("owner", ""))
    data["mailing_address"] = clean_multiline(data.get("mailing_address", ""))
    data["pa_primary_zone"] = clean_text(data.get("pa_primary_zone", ""))
    data["primary_land_use"] = clean_text(data.get("primary_land_use", ""))
    data["beds_baths_half"] = clean_text(data.get("beds_baths_half", ""))

    log.info(f"PROPERTY APPRAISER EXTRACTED: {json.dumps(data, indent=2)}")
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


def sanitize_for_supabase(record: Dict) -> Dict:
    clean = dict(record)

    clean["visible_in_app"] = bool(clean.get("visible_in_app", True))

    if not isinstance(clean.get("publish_dates"), list):
        clean["publish_dates"] = []

    nullable_fields = [
        "auction_date",
        "auction_time",
        "sale_date",
        "app_receive_date",
        "redemption_amount",
        "opening_bid",
        "property_address_case",
        "property_address_pa",
        "legal_description",
        "homestead",
        "folio",
        "subdivision",
        "owner",
        "mailing_address",
        "pa_primary_zone",
        "primary_land_use",
        "beds_baths_half",
        "parcel_appraiser_url",
        "application_number",
        "applicant_number",
        "tax_collector_number",
    ]

    for field in nullable_fields:
        if clean.get(field, "") == "":
            clean[field] = None

    return clean


def supabase_rest_upsert(record: Dict) -> Dict:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        return {
            "sent": False,
            "reason": "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing",
            "external_id": record.get("external_id", ""),
        }

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?on_conflict=external_id"

    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }

    payload = [sanitize_for_supabase(record)]

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        ok = 200 <= resp.status_code < 300

        if ok:
            log.info(f"SUPABASE UPSERT OK: {record.get('external_id')}")
        else:
            log.error(
                f"SUPABASE UPSERT FAILED: status={resp.status_code} external_id={record.get('external_id')} body={resp.text}"
            )

        return {
            "sent": ok,
            "status_code": resp.status_code,
            "external_id": record.get("external_id", ""),
            "response_text": resp.text[:1000],
        }

    except Exception as e:
        log.exception(f"SUPABASE UPSERT EXCEPTION for {record.get('external_id')}: {e}")
        return {
            "sent": False,
            "reason": str(e),
            "external_id": record.get("external_id", ""),
        }


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
        log.warning(f"No parcel href found for case {base_case.get('caseid')}")

    record = build_final_record(case_detail, pa_data)
    return {
        "case_detail": case_detail,
        "property_appraiser": pa_data,
        "record": record,
    }


def run_miami():
    log.info("=== MIAMI FINAL OPERATIONAL V1 + SUPABASE REST ===")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chrome",
            headless=True,
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
        supabase_results = []
        failures = []

        total_to_read = min(len(case_rows), 100)

        # mantém a estratégia que estava funcionando:
        # reabre a lista a cada item e lê por índice
        for i in range(total_to_read):
            log.info(f"[{i+1}/{total_to_read}] Scraping case row on current page...")

            page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(6000)

            run_search_flow(page)
            set_results_per_page_with_fallback(page)
            page.wait_for_timeout(4000)

            try:
                result = scrape_one_case(context, page, i)
                record = result["record"]

                results.append(record)

                sb_result = supabase_rest_upsert(record)
                supabase_results.append(sb_result)

                log.info(f"SUCCESS CASE {i+1}: {record.get('external_id')}")

            except Exception as e:
                failures.append({
                    "index": i,
                    "error": str(e),
                })
                log.exception(f"FAILED CASE INDEX {i}: {e}")

        final_payload = {
            "source": "MiamiDade",
            "mode": "final_operational_v1_supabase_rest",
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
    run_miami()