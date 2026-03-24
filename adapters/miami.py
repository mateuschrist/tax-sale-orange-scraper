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
        "node",
        "parcel_number",
        "sale_date",
        "address",
        "city",
        "state_address",
        "zip",
        "pdf_url",
        "auction_source_url",
        "opening_bid",
        "deed_status",
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
        log.info("APP INGEST status=%s node=%s", r.status_code, payload.get("node"))
        if r.text:
            log.info("APP INGEST response=%s", r.text[:300].replace("\n", " "))
        return r.status_code in (200, 201)
    except Exception as e:
        log.warning("APP INGEST failed node=%s error=%s", payload.get("node"), str(e))
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


def supabase_find_existing_case(node: str):
    if not CAN_CHECK_SUPABASE or not node:
        return None

    url = (
        f"{SUPABASE_URL}/rest/v1/properties"
        f"?county=eq.Miami-Dade"
        f"&node=eq.{quote(str(node), safe='')}"
        f"&select=id,node,parcel_number,sale_date,address,city,state_address,zip,pdf_url,auction_source_url,opening_bid,deed_status,applicant_name"
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
        f"&select=id,node,parcel_number,sale_date,address,city,state_address,zip,pdf_url,auction_source_url,opening_bid,deed_status,applicant_name"
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
            return True, "existing node found, but new payload is better"
        return False, "duplicate node already exists"

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
            "node": payload.get("node"),
            "response_text": "SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not configured",
        }

    should_send, reason = should_send_payload(payload)
    log.info("SUPABASE DEDUP node=%s => %s", payload.get("node"), reason)

    if not should_send:
        return {
            "sent": False,
            "status_code": 200,
            "node": payload.get("node"),
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
                "SUPABASE UPSERT OK status=%s node=%s",
                r.status_code,
                payload.get("node"),
            )
        else:
            log.error(
                "SUPABASE UPSERT FAILED status=%s node=%s body=%s",
                r.status_code,
                payload.get("node"),
                r.text[:600],
            )

        return {
            "sent": ok,
            "status_code": r.status_code,
            "node": payload.get("node"),
            "response_text": r.text[:1000],
        }
    except Exception as e:
        log.exception("SUPABASE UPSERT EXCEPTION node=%s error=%s", payload.get("node"), str(e))
        return {
            "sent": False,
            "status_code": None,
            "node": payload.get("node"),
            "response_text": str(e),
        }


# =========================
# UI HELPERS
# =========================
def click_safe(page, selector, name, timeout=6000, allow_navigation=False):
    try:
        page.click(selector, timeout=timeout)
        log.info("%s clicked (normal)", name)

        if allow_navigation:
            try:
                page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception:
                pass

        return True
    except Exception as e:
        msg = str(e)

        # Se a navegação começou logo após o clique, consideramos sucesso
        if allow_navigation and (
            "Execution context was destroyed" in msg
            or "Target closed" in msg
            or "has been closed" in msg
            or "Most likely the page has been closed" in msg
        ):
            log.info("%s click triggered navigation (normal)", name)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception:
                pass
            return True

    try:
        page.locator(selector).first.click(force=True, timeout=timeout)
        log.info("%s clicked (force)", name)

        if allow_navigation:
            try:
                page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception:
                pass

        return True
    except Exception as e:
        msg = str(e)

        if allow_navigation and (
            "Execution context was destroyed" in msg
            or "Target closed" in msg
            or "has been closed" in msg
            or "Most likely the page has been closed" in msg
        ):
            log.info("%s click triggered navigation (force)", name)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception:
                pass
            return True

    # JS fallback só deve ser usado quando NÃO esperamos navegação
    if not allow_navigation:
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

    log.error("%s FAILED", name)
    return False

def click_search_button(page) -> bool:
    selectors = [
        "button.filters-submit",
        "text=Search",
    ]

    for sel in selectors:
        try:
            with page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
                page.locator(sel).first.click(timeout=5000)
            log.info("SEARCH BUTTON clicked with navigation: %s", sel)
            return True
        except Exception:
            pass

        try:
            page.locator(sel).first.click(timeout=5000)
            log.info("SEARCH BUTTON clicked without navigation: %s", sel)
            return True
        except Exception:
            pass

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

    if not click_search_button(page):
        raise RuntimeError("Could not click search")

    page.wait_for_timeout(3000)
    wait_for_case_rows(page)
    
    
def open_list_and_apply_filter(page):
    page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(6000)
    run_search_flow(page)


def return_to_results(page):
    try:
        page.go_back(wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(8000)
        wait_for_case_rows(page, timeout_ms=20000)
        return True
    except Exception as e:
        log.warning("go_back failed, reloading filtered list: %s", str(e))
        try:
            open_list_and_apply_filter(page)
            return True
        except Exception:
            return False


# =========================
# PAGINATION
# =========================
def get_results_summary(page) -> Dict:
    return page.evaluate(
        """
        () => {
            const bodyText = document.body.innerText || '';
            const rows = document.querySelectorAll('tr.load-case.table-row.link[data-caseid]').length;

            const pageLinks = Array.from(document.querySelectorAll('a, button, span, div, li'))
                .map(el => (el.innerText || '').replace(/\\s+/g, ' ').trim())
                .filter(x => /^Page\\s+\\d+/i.test(x));

            const m = bodyText.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);

            return {
                rows_on_page: rows,
                page_links: pageLinks,
                current_page_text: m ? `Page ${m[1]}/${m[2]}` : "",
                body_sample: bodyText.slice(0, 3000)
            };
        }
        """
    )


def get_active_page_number(page) -> int:
    try:
        active = page.evaluate(
            """
            () => {
                const body = document.body.innerText || '';
                const m = body.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);
                if (m) return parseInt(m[1], 10);

                const els = Array.from(document.querySelectorAll('button, a, div, span, li'));
                for (const el of els) {
                    const txt = (el.innerText || '').replace(/\\s+/g, ' ').trim();
                    const mm = txt.match(/^Page\\s+(\\d+)$/i);
                    if (mm) return parseInt(mm[1], 10);
                }

                return 1;
            }
            """
        )
        return int(active or 1)
    except Exception:
        return 1


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
    try:
        total = page.evaluate(
            """
            () => {
                const body = document.body.innerText || '';
                const m = body.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);
                if (m) return parseInt(m[2], 10);

                const allText = Array.from(document.querySelectorAll('a, button, span, div, li'))
                    .map(el => (el.innerText || '').replace(/\\s+/g, ' ').trim())
                    .filter(Boolean);

                let maxPage = 1;
                for (const txt of allText) {
                    const mm = txt.match(/^Page\\s+(\\d+)\\b/i);
                    if (mm) {
                        const n = parseInt(mm[1], 10);
                        if (n > maxPage) maxPage = n;
                    }
                }
                return maxPage;
            }
            """
        )
        return int(total or 1)
    except Exception:
        return 1


def wait_for_page_change(page, old_page: int, timeout_ms: int = 20000) -> bool:
    waited = 0
    step = 500

    while waited < timeout_ms:
        try:
            current = get_active_page_number(page)
            rows = page.locator('tr.load-case.table-row.link[data-caseid]').count()

            if current != old_page and rows > 0:
                log.info("Miami page changed successfully: %s -> %s", old_page, current)
                return True
        except Exception:
            pass

        page.wait_for_timeout(step)
        waited += step

    return False


def open_pager_dropdown(page) -> bool:
    selectors = [
        "text=/^Page\\s+\\d+$/",
        "text=/^Page\\s+\\d+\\s*$/",
        "text=/^Page\\s+\\d+\\s*▾?$/",
    ]

    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click(timeout=5000)
                page.wait_for_timeout(800)
                log.info("Opened pager dropdown using selector: %s", sel)
                return True
        except Exception:
            pass

    try:
        result = page.evaluate(
            """
            () => {
                function isVisible(el) {
                    if (!el) return false;
                    const s = window.getComputedStyle(el);
                    const r = el.getBoundingClientRect();
                    return s.display !== 'none' &&
                           s.visibility !== 'hidden' &&
                           r.width > 0 &&
                           r.height > 0;
                }

                const els = Array.from(document.querySelectorAll('a, button, div, span, td'))
                    .filter(isVisible);

                const target = els.find(el => {
                    const txt = (el.innerText || '').replace(/\\s+/g, ' ').trim();
                    return /^Page\\s+\\d+$/.test(txt);
                });

                if (!target) return { ok: false };

                const r = target.getBoundingClientRect();
                target.click();

                return {
                    ok: true,
                    x: r.left + r.width / 2,
                    y: r.top + r.height / 2,
                    text: (target.innerText || '').trim()
                };
            }
            """
        )

        if result and result.get("ok"):
            page.wait_for_timeout(800)
            log.info("Opened pager dropdown via JS click: %s", result)
            return True
    except Exception as e:
        log.warning("open_pager_dropdown JS failed: %s", str(e))

    return False


def click_page_option_from_dropdown(page, target_page: int) -> bool:
    old_page = get_active_page_number(page)

    try:
        result = page.evaluate(
            """
            (targetPage) => {
                function isVisible(el) {
                    if (!el) return false;
                    const s = window.getComputedStyle(el);
                    const r = el.getBoundingClientRect();
                    return s.display !== 'none' &&
                           s.visibility !== 'hidden' &&
                           r.width > 0 &&
                           r.height > 0;
                }

                function clean(txt) {
                    return (txt || '').replace(/\\s+/g, ' ').trim();
                }

                const all = Array.from(document.querySelectorAll('a, button, div, span, li, td'))
                    .filter(isVisible);

                const exactPrefix = `Page ${targetPage} `;
                const exactOnly = `Page ${targetPage}`;

                let candidates = all.filter(el => {
                    const txt = clean(el.innerText);
                    return txt === exactOnly || txt.startsWith(exactPrefix);
                });

                // prioriza texto com "(results ...)"
                candidates.sort((a, b) => {
                    const at = clean(a.innerText);
                    const bt = clean(b.innerText);
                    const ascore = at.includes('(results') ? 10 : 0;
                    const bscore = bt.includes('(results') ? 10 : 0;
                    return bscore - ascore;
                });

                if (!candidates.length) {
                    return { ok: false, reason: 'dropdown option not found' };
                }

                const target = candidates[0];
                const r = target.getBoundingClientRect();

                target.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
                target.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
                target.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
                target.click();

                return {
                    ok: true,
                    text: clean(target.innerText),
                    x: r.left + r.width / 2,
                    y: r.top + r.height / 2
                };
            }
            """,
            target_page,
        )

        log.info("Dropdown page option click result for page %s: %s", target_page, result)

        if not result or not result.get("ok"):
            return False

        page.wait_for_timeout(2000)

        if wait_for_page_change(page, old_page, timeout_ms=20000):
            return True

        try:
            page.mouse.click(result["x"], result["y"])
            page.wait_for_timeout(2000)
            if wait_for_page_change(page, old_page, timeout_ms=20000):
                return True
        except Exception:
            pass

    except Exception as e:
        log.warning("click_page_option_from_dropdown failed for page %s: %s", target_page, str(e))

    return False


def go_to_page_number(page, page_num: int) -> bool:
    current = get_active_page_number(page)

    if current == page_num:
        log.info("Already on page %s", page_num)
        return True

    for attempt in range(1, 4):
        log.info("Trying to navigate from page %s to page %s (attempt %s)", current, page_num, attempt)

        if not open_pager_dropdown(page):
            log.warning("Could not open pager dropdown on attempt %s", attempt)
            continue

        if click_page_option_from_dropdown(page, page_num):
            new_page = get_active_page_number(page)
            log.info("Navigation success: page %s -> %s", current, new_page)
            return new_page == page_num

        page.wait_for_timeout(1000)

    log.warning("Failed navigating from page %s to page %s", current, page_num)
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


def open_case_by_caseid(page, caseid: str) -> Dict:
    rows = page.locator(f'tr.load-case.table-row.link[data-caseid="{caseid}"]')
    count = rows.count()
    if count == 0:
        raise RuntimeError(f"Case row not found for caseid={caseid}")

    row = rows.first
    row_text = clean_text(row.inner_text())

    handle = row.element_handle()
    if handle is None:
        raise RuntimeError(f"Could not get handle for case row {caseid}")

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


def build_final_record(case_detail: Dict) -> Dict:
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
        "legal_description": summary.get("legal_description", ""),
        "homestead": summary.get("homestead", ""),
        "parcel_appraiser_url": parcel.get("href", ""),
    }
    return record


def build_properties_payload(record: dict) -> dict:
    addr_full = record.get("property_address_case") or ""

    city = None
    state_address = None
    zip_code = None

    m = re.search(r"^(.*?),\s*([A-Z][A-Z .]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", addr_full, re.I)
    if m:
        address_only = clean_text(m.group(1))
        city = clean_text(m.group(2)).title()
        state_address = clean_text(m.group(3)).upper()
        zip_code = clean_text(m.group(4))
    else:
        address_only = addr_full.strip(" ,") or None

    return {
        "county": "Miami-Dade",
        "state": "FL",
        "node": str(record.get("caseid") or ""),
        "pdf_url": record.get("parcel_appraiser_url") or None,
        "auction_source_url": AUCTION_URL,
        "tax_sale_id": record.get("case_number"),
        "parcel_number": record.get("parcel_number"),
        "sale_date": record.get("sale_date"),
        "opening_bid": normalize_money(record.get("opening_bid")),
        "deed_status": record.get("case_status"),
        "applicant_name": record.get("applicant_number"),
        "address": address_only,
        "city": city,
        "state_address": state_address,
        "zip": zip_code,
    }


# =========================
# MAIN
# =========================
def run_miami():
    log.info("=== MIAMI FINAL OPERATIONAL V5 + DIRECT PAGE LINKS + BACK NAV ===")

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
        summary = get_results_summary(page)

        log.info("Detected total Miami pages: %s", total_pages)

        results = []
        failures = []
        supabase_results = []
        total_processed = 0

        for page_num in range(1, total_pages + 1):
            if total_processed >= MAX_LOTS:
                break

            current_page = get_active_page_number(page)
            if current_page != page_num:
                ok = go_to_page_number(page, page_num)
                if not ok:
                    log.warning("Could not navigate to page %s", page_num)
                    break

            rows = collect_case_rows(page)
            if not rows:
                log.warning("No rows found on page %s", page_num)
                continue

            log.info("Processing all %s rows from page %s before moving forward", len(rows), page_num)

            page_caseids = [r.get("caseid") for r in rows if r.get("caseid")]

            for row_index, caseid in enumerate(page_caseids):
                if total_processed >= MAX_LOTS:
                    break

                total_processed += 1
                log.info(
                    "[%s/%s] Scraping caseid=%s page=%s row=%s ...",
                    total_processed,
                    MAX_LOTS,
                    caseid,
                    page_num,
                    row_index,
                )

                try:
                    base_case = open_case_by_caseid(page, caseid)
                    case_detail = extract_case_detail(page, base_case)

                    record = build_final_record(case_detail)
                    results.append(record)

                    prop_payload = build_properties_payload(record)

                    sb_result = supabase_upsert_property(prop_payload)
                    supabase_results.append(sb_result)

                    send_to_app(prop_payload)

                    log.info("SUCCESS CASE %s: node=%s", total_processed, prop_payload.get("node"))

                except Exception as e:
                    log.exception("FAILED CASE caseid=%s: %s", caseid, e)
                    failures.append({
                        "caseid": caseid,
                        "page_num": page_num,
                        "row_index": row_index,
                        "error": str(e),
                    })

                finally:
                    if total_processed < MAX_LOTS:
                        ok_back = return_to_results(page)
                        if not ok_back:
                            raise RuntimeError(f"Could not return to results after caseid={caseid}")

                        current_after_back = get_active_page_number(page)
                        if current_after_back != page_num:
                            log.warning(
                                "Returned to wrong page after case %s. Expected=%s got=%s. Re-navigating...",
                                caseid, page_num, current_after_back
                            )
                            ok = go_to_page_number(page, page_num)
                            if not ok:
                                raise RuntimeError(f"Could not restore page {page_num} after caseid={caseid}")

            if total_processed >= MAX_LOTS:
                break

        final_payload = {
            "source": "MiamiDade",
            "mode": "final_operational_v5_direct_page_links_back_nav",
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