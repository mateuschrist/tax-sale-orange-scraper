import json
import logging
import re
from playwright.sync_api import sync_playwright

log = logging.getLogger("miami")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"


def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def click_safe(page, selector, name, timeout=5000):
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
        el.click(timeout=5000)
        log.info(f"{name} clicked (normal)")
        return True
    except Exception:
        pass

    try:
        el.click(force=True, timeout=5000)
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


def wait_for_results(page):
    log.info("Waiting for Miami search results...")
    page.wait_for_timeout(15000)

    rows = page.locator('tr.load-case.table-row.link[data-caseid]')
    count = rows.count()
    log.info(f"Rows after search: {count}")

    if count == 0:
        raise RuntimeError("No case rows found after Miami search")


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


def run_search_flow(page):
    log.info("Running Miami ACTIVE-only flow...")

    if not click_safe(page, "a.filters-reset", "RESET FILTERS"):
        raise RuntimeError("Could not reset filters")
    page.wait_for_timeout(2000)

    if not click_safe(page, "#filterButtonStatus", "FILTER BUTTON"):
        raise RuntimeError("Could not open filter button")
    page.wait_for_timeout(1500)

    force_clear_all_active_statuses(page)
    page.wait_for_timeout(800)

    force_select_only_192(page)
    page.wait_for_timeout(1200)

    state = page.evaluate(
        """
        () => {
            const hidden = document.querySelector('#filterCaseStatus');
            const selected192 = document.querySelector('a[data-statusid="192"][data-parentid="2"]');
            return {
                label: (document.querySelector('#filterCaseStatusLabel')?.innerText || '').trim(),
                hidden_filterCaseStatus: hidden ? hidden.value : null,
                selected192_class: selected192 ? selected192.className : null
            };
        }
        """
    )
    log.info(f"SEARCH STATE BEFORE SUBMIT: {state}")

    if not click_safe(page, "button.filters-submit", "SEARCH BUTTON"):
        raise RuntimeError("Could not click search")

    wait_for_results(page)


def get_first_case_row(page):
    rows = page.locator('tr.load-case.table-row.link[data-caseid]')
    count = rows.count()

    if count == 0:
        raise RuntimeError("No case rows found")

    first = rows.nth(0)
    caseid = first.get_attribute("data-caseid")
    text = clean_text(first.inner_text())

    log.info(f"First case row -> caseid={caseid} text={text}")
    return first, caseid, text


def open_first_case_detail(page):
    row, caseid, row_text = get_first_case_row(page)

    # try clicking the row itself
    handle = row.element_handle()
    if handle is None:
        raise RuntimeError("Could not get element handle for first case row")

    if not click_element_handle_safe(handle, page, f"CASE ROW {caseid}"):
        raise RuntimeError(f"Could not open case detail for caseid {caseid}")

    log.info("Waiting for case detail to load...")
    page.wait_for_timeout(8000)

    # wait for top case summary area to appear
    page.wait_for_selector("text=CASE SUMMARY", timeout=20000)

    return {
        "caseid": caseid,
        "row_text": row_text,
    }


def parse_case_header(page):
    data = page.evaluate(
        """
        () => {
            function getCellValue(label) {
                const all = Array.from(document.querySelectorAll('body *'));
                const node = all.find(el => (el.innerText || '').trim() === label);
                if (!node) return '';
                const parent = node.parentElement;
                if (!parent) return '';
                return (parent.innerText || '').replace(label, '').trim();
            }

            const summaryText = (document.body.innerText || '');

            const parcelLink = document.querySelector('#propertyAppraiserLink');

            const header = {
                tax_collector_number: getCellValue('Tax Collector #'),
                applicant_number: getCellValue('Applicant Number'),
                case_number: getCellValue('Case Number'),
                parcel_number: getCellValue('Parcel Number'),
                case_status: getCellValue('Case Status'),
                redemption_amount: getCellValue('Redemption Amount:'),
                opening_bid: getCellValue('Opening Bid:')
            };

            // fallback parsing if direct capture misses
            if (!header.parcel_number && parcelLink) {
                header.parcel_number = (parcelLink.innerText || '').trim();
            }

            return {
                header,
                parcel_link: parcelLink ? {
                    text: (parcelLink.innerText || '').trim(),
                    href: parcelLink.getAttribute('href') || ''
                } : null,
                has_case_summary: summaryText.includes('CASE SUMMARY')
            };
        }
        """
    )
    return data


def parse_case_summary(page):
    data = page.evaluate(
        """
        () => {
            function findValueAfterLabel(label) {
                const rows = Array.from(document.querySelectorAll('body *'));
                for (const el of rows) {
                    const txt = (el.innerText || '').trim();
                    if (txt === label) {
                        let sib = el.nextElementSibling;
                        if (sib) return (sib.innerText || '').trim();

                        const parent = el.parentElement;
                        if (parent) {
                            const children = Array.from(parent.children);
                            const idx = children.indexOf(el);
                            if (idx >= 0 && children[idx + 1]) {
                                return (children[idx + 1].innerText || '').trim();
                            }
                        }
                    }
                }
                return '';
            }

            return {
                app_receive_date: findValueAfterLabel('App Receive Date'),
                sale_date: findValueAfterLabel('Sale Date'),
                publish_dates: findValueAfterLabel('Publish Date(s)'),
                property_address: findValueAfterLabel('Property Address'),
                homestead: findValueAfterLabel('Homestead'),
                legal_description: findValueAfterLabel('Legal Description')
            };
        }
        """
    )
    return data


def extract_case_detail(page, base_case):
    header_data = parse_case_header(page)
    summary_data = parse_case_summary(page)

    detail = {
        **base_case,
        "header": header_data.get("header", {}),
        "case_summary": summary_data,
        "parcel_link": header_data.get("parcel_link"),
        "has_case_summary": header_data.get("has_case_summary", False),
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
            function findField(label) {
                const nodes = Array.from(document.querySelectorAll('body *'));
                for (const el of nodes) {
                    const txt = (el.innerText || '').trim();
                    if (txt === label) {
                        const parent = el.parentElement;
                        if (!parent) continue;

                        const parentText = (parent.innerText || '').trim();
                        if (parentText && parentText !== label) {
                            return parentText.replace(label, '').trim();
                        }

                        let sib = el.nextElementSibling;
                        if (sib) return (sib.innerText || '').trim();
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

    log.info(f"PROPERTY APPRAISER EXTRACTED: {json.dumps(data, indent=2)}")
    return data


def run_miami():
    log.info("=== MIAMI ONE-CASE DETAIL TEST ===")

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

        base_case = open_first_case_detail(page)
        case_detail = extract_case_detail(page, base_case)

        parcel_link = case_detail.get("parcel_link", {}) or {}
        parcel_href = parcel_link.get("href", "")

        pa_data = {}
        if parcel_href:
            pa_page = open_property_appraiser(context, parcel_href)
            pa_data = extract_property_appraiser(pa_page)
            pa_page.close()
        else:
            log.warning("No parcel href found in case detail")

        final_payload = {
            "source": "MiamiDade",
            "mode": "one_case_detail_test",
            "case_detail": case_detail,
            "property_appraiser": pa_data,
        }

        log.info("===== FINAL PAYLOAD =====")
        log.info(json.dumps(final_payload, indent=2))

        print(json.dumps(final_payload, indent=2))

        browser.close()