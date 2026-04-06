import json
import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import quote

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

log = logging.getLogger("miami")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"
AUCTION_URL = "https://www.miamidade.realforeclose.com/index.cfm"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
MAX_LOTS = int(os.getenv("MAX_LOTS", "1000"))

APP_API_BASE = (os.getenv("APP_API_BASE", "") or "").strip().rstrip("/")
APP_API_TOKEN = (os.getenv("APP_API_TOKEN", "") or "").strip()
SEND_TO_APP = bool(APP_API_BASE and APP_API_TOKEN)

SUPABASE_URL = (os.getenv("SUPABASE_URL", "") or "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or "").strip()
CAN_CHECK_SUPABASE = bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)

PAGINATION_DIAGNOSTIC_MODE = os.getenv("MIAMI_PAGINATION_DIAGNOSTIC_MODE", "false").lower() == "true"


# =========================
# BASIC HELPERS
# =========================
def now_iso() -> str:
    return datetime.utcnow().isoformat()


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


def normalize_sale_date_value(value: str) -> Optional[str]:
    v = clean_text(value or "")
    if not v:
        return None
    if v.lower() in ("not assigned", "null", "none", "n/a", "na"):
        return None
    return v


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
        "applicant_name",
    ]
    return sum(1 for f in fields if payload.get(f) not in (None, "", [], {}))


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
        "opening_bid",
        "deed_status",
        "applicant_name",
    ]

    for f in important_fields:
        old_val = clean_text(existing.get(f) or "")
        new_val = clean_text(payload.get(f) or "")
        if not old_val and new_val:
            return True

    return False


def record_needs_enrichment(existing: dict) -> bool:
    important_fields = [
        "pdf_url",
        "address",
        "city",
        "state_address",
        "zip",
        "opening_bid",
        "deed_status",
        "applicant_name",
        "auction_source_url",
    ]
    return any(not clean_text(existing.get(f) or "") for f in important_fields)


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
# SUPABASE
# =========================
def sb_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }


def supabase_fetch_all_miami_records() -> List[dict]:
    if not CAN_CHECK_SUPABASE:
        return []

    all_rows = []
    offset = 0
    page_size = 1000

    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/properties"
            f"?county=eq.Miami-Dade"
            f"&select=id,node,tax_sale_id,parcel_number,sale_date,address,city,state_address,zip,"
            f"pdf_url,auction_source_url,opening_bid,deed_status,applicant_name,is_active,removed_at"
            f"&offset={offset}"
            f"&limit={page_size}"
        )

        try:
            r = requests.get(url, headers=sb_headers(), timeout=60)
            if r.status_code != 200:
                log.warning("supabase_fetch_all_miami_records failed status=%s body=%s", r.status_code, r.text[:500])
                break

            arr = r.json() or []
            if not arr:
                break

            all_rows.extend(arr)

            if len(arr) < page_size:
                break

            offset += page_size

        except Exception as e:
            log.warning("supabase_fetch_all_miami_records exception: %s", str(e))
            break

    log.info("Loaded %s Miami records from Supabase", len(all_rows))
    return all_rows


def build_supabase_indexes(rows: List[dict]) -> Dict[str, Dict]:
    by_node = {}
    by_tax_sale_parcel = {}

    for row in rows:
        node = clean_text(row.get("node") or "")
        tax_sale_id = clean_text(row.get("tax_sale_id") or "")
        parcel_number = clean_text(row.get("parcel_number") or "")

        if node:
            by_node[node] = row

        if tax_sale_id and parcel_number:
            by_tax_sale_parcel[(tax_sale_id, parcel_number)] = row

    return {
        "by_node": by_node,
        "by_tax_sale_parcel": by_tax_sale_parcel,
    }


def supabase_update_sale_date(record_id: str, sale_date: Optional[str]) -> dict:
    if not CAN_CHECK_SUPABASE or not record_id:
        return {
            "sent": False,
            "status_code": None,
            "record_id": record_id,
            "response_text": "missing config or record id",
        }

    url = f"{SUPABASE_URL}/rest/v1/properties?id=eq.{quote(str(record_id), safe='')}"
    headers = sb_headers()
    headers["Prefer"] = "return=representation"

    payload = {
        "sale_date": sale_date,
        "updated_at": now_iso(),
        "is_active": True,
        "removed_at": None,
    }

    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=20)
        ok = r.status_code in (200, 204)

        if ok:
            log.info(
                "SUPABASE SALE_DATE UPDATE OK id=%s sale_date=%s status=%s",
                record_id,
                sale_date,
                r.status_code,
            )
        else:
            log.warning(
                "SUPABASE SALE_DATE UPDATE FAILED id=%s status=%s body=%s",
                record_id,
                r.status_code,
                r.text[:500],
            )

        return {
            "sent": ok,
            "status_code": r.status_code,
            "record_id": record_id,
            "response_text": r.text[:1000],
        }

    except Exception as e:
        log.warning("SUPABASE SALE_DATE UPDATE EXCEPTION id=%s error=%s", record_id, str(e))
        return {
            "sent": False,
            "status_code": None,
            "record_id": record_id,
            "response_text": str(e),
        }


def supabase_insert_property(payload: dict) -> dict:
    if not CAN_CHECK_SUPABASE:
        return {
            "sent": False,
            "status_code": None,
            "node": payload.get("node"),
            "record_id": None,
            "response_text": "SUPABASE not configured",
        }

    url = f"{SUPABASE_URL}/rest/v1/properties"
    headers = sb_headers()
    headers["Prefer"] = "return=representation"

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        ok = r.status_code in (200, 201)

        record_id = None
        try:
            body = r.json()
            if isinstance(body, list) and body:
                record_id = body[0].get("id")
        except Exception:
            pass

        if ok:
            log.info("SUPABASE INSERT OK node=%s status=%s id=%s", payload.get("node"), r.status_code, record_id)
        else:
            log.error(
                "SUPABASE INSERT FAILED status=%s node=%s body=%s",
                r.status_code,
                payload.get("node"),
                r.text[:600],
            )

        return {
            "sent": ok,
            "status_code": r.status_code,
            "node": payload.get("node"),
            "record_id": record_id,
            "response_text": r.text[:1000],
        }
    except Exception as e:
        log.exception("SUPABASE INSERT EXCEPTION node=%s error=%s", payload.get("node"), str(e))
        return {
            "sent": False,
            "status_code": None,
            "node": payload.get("node"),
            "record_id": None,
            "response_text": str(e),
        }


def supabase_update_property(record_id: str, payload: dict) -> dict:
    if not CAN_CHECK_SUPABASE or not record_id:
        return {
            "sent": False,
            "status_code": None,
            "node": payload.get("node"),
            "record_id": record_id,
            "response_text": "missing config or record id",
        }

    url = f"{SUPABASE_URL}/rest/v1/properties?id=eq.{quote(str(record_id), safe='')}"
    headers = sb_headers()
    headers["Prefer"] = "return=representation"

    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=30)
        ok = r.status_code in (200, 204)

        if ok:
            log.info("SUPABASE PATCH OK id=%s node=%s status=%s", record_id, payload.get("node"), r.status_code)
        else:
            log.error(
                "SUPABASE PATCH FAILED status=%s node=%s body=%s",
                r.status_code,
                payload.get("node"),
                r.text[:600],
            )

        return {
            "sent": ok,
            "status_code": r.status_code,
            "node": payload.get("node"),
            "record_id": record_id,
            "response_text": r.text[:1000],
        }
    except Exception as e:
        log.exception("SUPABASE PATCH EXCEPTION node=%s error=%s", payload.get("node"), str(e))
        return {
            "sent": False,
            "status_code": None,
            "node": payload.get("node"),
            "record_id": record_id,
            "response_text": str(e),
        }


def supabase_save_property(payload: dict, existing: dict | None) -> dict:
    if existing and existing.get("id"):
        return supabase_update_property(existing["id"], payload)
    return supabase_insert_property(payload)


def supabase_list_all_nodes() -> List[str]:
    if not CAN_CHECK_SUPABASE:
        return []

    nodes = []
    offset = 0
    page_size = 1000

    try:
        while True:
            url = (
                f"{SUPABASE_URL}/rest/v1/properties"
                f"?county=eq.Miami-Dade"
                f"&select=node"
                f"&offset={offset}"
                f"&limit={page_size}"
            )

            r = requests.get(url, headers=sb_headers(), timeout=60)

            if r.status_code != 200:
                log.warning(
                    "supabase_list_all_nodes non-200 status=%s body=%s",
                    r.status_code,
                    r.text[:500],
                )
                break

            arr = r.json() or []
            if not arr:
                break

            for item in arr:
                node = str(item.get("node") or "").strip()
                if node:
                    nodes.append(node)

            if len(arr) < page_size:
                break

            offset += page_size

    except Exception as e:
        log.warning("supabase_list_all_nodes failed: %s", str(e))

    return nodes


def supabase_delete_nodes(nodes: List[str]) -> Dict:
    if not CAN_CHECK_SUPABASE:
        return {
            "executed": False,
            "deleted_count": 0,
            "reason": "SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not configured",
        }

    nodes = [str(x).strip() for x in nodes if str(x).strip()]
    if not nodes:
        return {
            "executed": True,
            "deleted_count": 0,
            "reason": "no nodes to delete",
        }

    deleted_count = 0
    errors = []
    batch_size = 100

    for i in range(0, len(nodes), batch_size):
        batch = nodes[i:i + batch_size]

        try:
            in_clause = ",".join(f'"{quote(node, safe="")}"' for node in batch)

            url = (
                f"{SUPABASE_URL}/rest/v1/properties"
                f"?county=eq.Miami-Dade"
                f"&node=in.({in_clause})"
            )

            r = requests.delete(url, headers=sb_headers(), timeout=60)

            if r.status_code in (200, 204):
                deleted_count += len(batch)
                log.info("SUPABASE DELETE batch ok count=%s", len(batch))
            else:
                msg = f"status={r.status_code} body={r.text[:500]}"
                errors.append(msg)
                log.warning("SUPABASE DELETE batch failed: %s", msg)

        except Exception as e:
            msg = str(e)
            errors.append(msg)
            log.warning("SUPABASE DELETE batch exception: %s", msg)

    return {
        "executed": True,
        "deleted_count": deleted_count,
        "requested_delete_count": len(nodes),
        "errors": errors,
    }


def reconcile_supabase_to_site(seen_nodes_this_run: set) -> Dict:
    try:
        existing_nodes = set(supabase_list_all_nodes())
        site_nodes = {str(x).strip() for x in seen_nodes_this_run if str(x).strip()}

        to_delete = sorted(existing_nodes - site_nodes)

        log.info(
            "RECONCILE MIAMI: supabase=%s site=%s delete=%s",
            len(existing_nodes),
            len(site_nodes),
            len(to_delete),
        )

        delete_result = supabase_delete_nodes(to_delete)

        return {
            "executed": True,
            "supabase_nodes_count": len(existing_nodes),
            "site_nodes_count": len(site_nodes),
            "delete_candidates_count": len(to_delete),
            "delete_candidates_sample": to_delete[:50],
            "delete_result": delete_result,
        }

    except Exception as e:
        log.exception("reconcile_supabase_to_site failed: %s", str(e))
        return {
            "executed": False,
            "reason": str(e),
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


def click_search_button(page) -> bool:
    selectors = [
        "button.filters-submit",
        "text=Search",
        "button:has-text('Search')",
    ]

    for sel in selectors:
        try:
            locator = page.locator(sel).first
            if locator.count() == 0:
                continue

            try:
                with page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
                    locator.click(timeout=6000)
                log.info("SEARCH BUTTON clicked with navigation: %s", sel)
                return True
            except Exception:
                pass

            try:
                locator.click(timeout=6000)
                page.wait_for_timeout(3000)
                log.info("SEARCH BUTTON clicked (normal): %s", sel)
                return True
            except Exception:
                pass

            try:
                locator.click(force=True, timeout=6000)
                page.wait_for_timeout(3000)
                log.info("SEARCH BUTTON clicked (force): %s", sel)
                return True
            except Exception:
                pass

            try:
                with page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
                    page.evaluate(
                        """(selector) => {
                            const el = document.querySelector(selector);
                            if (!el) throw new Error('search button not found');
                            el.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
                            el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
                            el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
                            el.click();
                        }""",
                        sel,
                    )
                log.info("SEARCH BUTTON clicked (JS + navigation): %s", sel)
                return True
            except Exception:
                pass

            try:
                page.evaluate(
                    """(selector) => {
                        const el = document.querySelector(selector);
                        if (!el) throw new Error('search button not found');
                        el.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
                        el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
                        el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
                        el.click();
                    }""",
                    sel,
                )
                page.wait_for_timeout(3000)
                log.info("SEARCH BUTTON clicked (JS fallback): %s", sel)
                return True
            except Exception:
                pass

        except Exception as e:
            log.warning("SEARCH BUTTON selector failed %s: %s", sel, str(e))

    log.error("SEARCH BUTTON FAILED")
    return False


def run_search_flow(page):
    log.info("Running Miami ACTIVE-only flow...")

    # ✅ AQUI - Substitua as linhas 787-789 por este código
    reset_selectors = [
        "a.filters-reset",
        "button.filters-reset",
        "[class*='filters-reset']",
        "a[href*='reset']",
        "text=Reset",
    ]
    
    reset_clicked = False
    for selector in reset_selectors:
        if click_safe(page, selector, f"RESET FILTERS ({selector})"):
            reset_clicked = True
            break
    
    if not reset_clicked:
        log.warning("Could not reset filters with standard selectors, taking screenshot for debug...")
        try:
            page.screenshot(path="miami_filters_debug.png")
        except Exception as e:
            log.warning("Could not save screenshot: %s", str(e))
        # Continua mesmo sem conseguir resetar
    
    page.wait_for_timeout(1500)

    # ✅ Resto da função permanece igual (linhas 791+)
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
            const rows = document.querySelectorAll('tr.load-case.table-row.link[data-caseid]').length;

            const pageLinks = Array.from(document.querySelectorAll('a, button, span, div'))
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

                const els = Array.from(document.querySelectorAll('button, a, div, span'));
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
                return 1;
            }
            """
        )
        return int(total or 1)
    except Exception:
        return 1


def parse_total_items(page) -> int:
    try:
        total = page.evaluate(
            """
            () => {
                const body = document.body.innerText || '';

                const patterns = [
                    /Cases List\\s*»\\s*(\\d+)\\s*Cases/i,
                    /(\\d+)\\s*Cases/i
                ];

                for (const rx of patterns) {
                    const m = body.match(rx);
                    if (m) return parseInt(m[1], 10);
                }

                return 0;
            }
            """
        )
        return int(total or 0)
    except Exception as e:
        log.warning("parse_total_items failed: %s", str(e))
        return 0


def get_first_caseid(page) -> str:
    try:
        row = page.locator('tr.load-case.table-row.link[data-caseid]').first
        if row.count() == 0:
            return ""
        return row.get_attribute("data-caseid") or ""
    except Exception:
        return ""


def get_first_row_text(page) -> str:
    try:
        row = page.locator('tr.load-case.table-row.link[data-caseid]').first
        if row.count() == 0:
            return ""
        return clean_text(row.inner_text())
    except Exception:
        return ""


def wait_for_page_change(
    page,
    old_page: int,
    old_first_caseid: str = "",
    old_first_row_text: str = "",
    timeout_ms: int = 22000,
) -> bool:
    waited = 0
    step = 1000

    while waited < timeout_ms:
        try:
            current = get_active_page_number(page)
            rows_count = page.locator('tr.load-case.table-row.link[data-caseid]').count()
            new_first_caseid = get_first_caseid(page)
            new_first_row_text = get_first_row_text(page)

            page_changed = current != old_page and rows_count > 0
            first_case_changed = bool(old_first_caseid and new_first_caseid and new_first_caseid != old_first_caseid)
            first_text_changed = bool(old_first_row_text and new_first_row_text and new_first_row_text != old_first_row_text)

            if page_changed or first_case_changed or first_text_changed:
                log.info(
                    "Miami page changed successfully: old_page=%s new_page=%s old_caseid=%s new_caseid=%s",
                    old_page,
                    current,
                    old_first_caseid,
                    new_first_caseid,
                )
                return True
        except Exception:
            pass

        page.wait_for_timeout(step)
        waited += step

    return False


def open_pager_dropdown(page) -> bool:
    selectors = [
        'text=/^Page\\s+\\d+\\s*$/',
        'text=/^Page\\s+\\d+\\s*\\(results.*\\)$/i',
        'div:has-text("Page 1")',
        'div:has-text("Page 2")',
        'span:has-text("Page 1")',
        'span:has-text("Page 2")',
    ]

    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() == 0:
                continue

            loc.first.click(timeout=4000)
            page.wait_for_timeout(1200)
            log.info("Opened pager dropdown using selector: %s", sel)
            return True
        except Exception:
            continue

    return False


def click_page_option_direct_without_dropdown(page, target_page: int) -> bool:
    current_before = get_active_page_number(page)
    old_first_caseid = get_first_caseid(page)
    old_first_row_text = get_first_row_text(page)

    try:
        result = page.evaluate(
            """
            (targetPage) => {
                function txt(el) {
                    return ((el && el.innerText) || '').replace(/\\s+/g, ' ').trim();
                }

                const selectors = [
                    `a[data-page="${targetPage}"]`,
                    `[data-page="${targetPage}"]`,
                    `a[title*="Page ${targetPage}"]`
                ];

                let target = null;
                for (const sel of selectors) {
                    const el = document.querySelector(sel);
                    if (el) {
                        target = el;
                        break;
                    }
                }

                if (!target) {
                    const all = Array.from(document.querySelectorAll('a, button, div, span, td'));
                    target = all.find(el => {
                        const t = txt(el);
                        return t === `Page ${targetPage}` || t.startsWith(`Page ${targetPage} `);
                    }) || null;
                }

                if (!target) {
                    return { ok: false, reason: 'target page element not found' };
                }

                const r = target.getBoundingClientRect();

                try { target.click(); } catch (e) {}
                try { target.dispatchEvent(new MouseEvent('mousedown', { bubbles: true })); } catch (e) {}
                try { target.dispatchEvent(new MouseEvent('mouseup', { bubbles: true })); } catch (e) {}
                try { target.dispatchEvent(new MouseEvent('click', { bubbles: true })); } catch (e) {}

                return {
                    ok: true,
                    text: txt(target),
                    visible: !!(r.width > 0 && r.height > 0),
                    x: r.left + (r.width / 2),
                    y: r.top + (r.height / 2)
                };
            }
            """,
            int(target_page),
        )

        log.info("Direct hidden/visible page option result for page %s: %s", target_page, result)

        if result and result.get("ok"):
            if wait_for_page_change(
                page,
                old_page=current_before,
                old_first_caseid=old_first_caseid,
                old_first_row_text=old_first_row_text,
                timeout_ms=16000,
            ):
                return True

            try:
                if result.get("visible"):
                    page.mouse.click(result["x"], result["y"])
                    if wait_for_page_change(
                        page,
                        old_page=current_before,
                        old_first_caseid=old_first_caseid,
                        old_first_row_text=old_first_row_text,
                        timeout_ms=16000,
                    ):
                        return True
            except Exception:
                pass

    except Exception as e:
        log.warning("click_page_option_direct_without_dropdown failed for page %s: %s", target_page, str(e))

    return False


def click_page_option_from_dropdown(page, target_page: int) -> bool:
    current_before = get_active_page_number(page)
    old_first_caseid = get_first_caseid(page)
    old_first_row_text = get_first_row_text(page)

    try:
        result = page.evaluate(
            """
            (targetPage) => {
                function txt(el) {
                    return ((el && el.innerText) || '').replace(/\\s+/g, ' ').trim();
                }

                const all = Array.from(document.querySelectorAll('a, button, div, span, td, li'));
                const option = all.find(el => {
                    const t = txt(el);
                    return t === `Page ${targetPage}` || t.startsWith(`Page ${targetPage} (results`);
                });

                if (!option) {
                    return { ok: false, reason: 'dropdown option not found' };
                }

                const r = option.getBoundingClientRect();

                try { option.click(); } catch (e) {}
                try { option.dispatchEvent(new MouseEvent('mousedown', { bubbles: true })); } catch (e) {}
                try { option.dispatchEvent(new MouseEvent('mouseup', { bubbles: true })); } catch (e) {}
                try { option.dispatchEvent(new MouseEvent('click', { bubbles: true })); } catch (e) {}

                return {
                    ok: true,
                    text: txt(option),
                    x: r.left + (r.width / 2),
                    y: r.top + (r.height / 2)
                };
            }
            """,
            int(target_page),
        )

        log.info("Dropdown page option click result for page %s: %s", target_page, result)

        if result and result.get("ok"):
            if wait_for_page_change(
                page,
                old_page=current_before,
                old_first_caseid=old_first_caseid,
                old_first_row_text=old_first_row_text,
                timeout_ms=16000,
            ):
                return True

            try:
                page.mouse.click(result["x"], result["y"])
                if wait_for_page_change(
                    page,
                    old_page=current_before,
                    old_first_caseid=old_first_caseid,
                    old_first_row_text=old_first_row_text,
                    timeout_ms=16000,
                ):
                    return True
            except Exception:
                pass

    except Exception as e:
        log.warning("click_page_option_from_dropdown failed for page %s: %s", target_page, str(e))

    return False


def click_next_page(page) -> bool:
    current_before = get_active_page_number(page)
    old_first_caseid = get_first_caseid(page)
    old_first_row_text = get_first_row_text(page)

    log.info("Current Miami page before NEXT click: %s", current_before)

    try:
        result = page.evaluate(
            """
            () => {
                function isVisible(el) {
                    if (!el) return false;
                    const s = window.getComputedStyle(el);
                    const r = el.getBoundingClientRect();
                    return !!s &&
                        s.display !== 'none' &&
                        s.visibility !== 'hidden' &&
                        r.width > 0 &&
                        r.height > 0;
                }

                function txt(el) {
                    return ((el && el.innerText) || '').replace(/\\s+/g, ' ').trim();
                }

                function cls(el) {
                    return ((el && el.className) || '').toString().toLowerCase();
                }

                const all = Array.from(document.querySelectorAll('a, button, span, div, td, img'));
                const visible = all.filter(isVisible);

                let best = null;

                for (const el of visible) {
                    const t = txt(el);
                    const c = cls(el);
                    let score = 0;

                    if (t === '>' || t === '›' || t === '»') score += 200;
                    if (c.includes('next')) score += 120;
                    if (c.includes('right')) score += 80;
                    if (c.includes('arrow')) score += 80;
                    if (c.includes('chevron')) score += 80;
                    if (el.tagName.toLowerCase() === 'img') score += 40;
                    if (el.querySelector && el.querySelector('img,svg,i')) score += 30;

                    if (score <= 0) continue;

                    const r = el.getBoundingClientRect();
                    const candidate = {
                        score,
                        x: r.left + r.width / 2,
                        y: r.top + r.height / 2,
                        text: t,
                        className: c
                    };

                    if (!best || candidate.score > best.score) {
                        best = candidate;
                    }
                }

                if (!best) {
                    return { ok: false, reason: 'no next candidate found' };
                }

                return { ok: true, ...best };
            }
            """
        )

        log.info("NEXT target detection: %s", result)

        if result and result.get("ok"):
            page.mouse.click(result["x"], result["y"])

            if wait_for_page_change(
                page,
                old_page=current_before,
                old_first_caseid=old_first_caseid,
                old_first_row_text=old_first_row_text,
                timeout_ms=16000,
            ):
                return True
    except Exception as e:
        log.warning("NEXT click failed: %s", str(e))

    log.warning("Could not click NEXT page button")
    return False


def log_pagination_diagnostics(page):
    try:
        data = page.evaluate(
            """
            () => {
                const nodes = Array.from(document.querySelectorAll('[data-page], a, button, div, span, td'))
                    .map((el, idx) => {
                        const r = el.getBoundingClientRect();
                        const style = window.getComputedStyle(el);
                        const text = ((el.innerText || '')).replace(/\\s+/g, ' ').trim();

                        if (
                            !text.includes('Page') &&
                            !el.getAttribute('data-page') &&
                            !((el.getAttribute('href') || '').includes('javascript:void(0)'))
                        ) {
                            return null;
                        }

                        return {
                            idx,
                            tag: el.tagName.toLowerCase(),
                            text,
                            href: el.getAttribute('href') || '',
                            data_page: el.getAttribute('data-page') || '',
                            onclick: el.getAttribute('onclick') || '',
                            class_name: el.className || '',
                            visible: !(style.display === 'none' || style.visibility === 'hidden' || r.width <= 0 || r.height <= 0),
                            x: r.x,
                            y: r.y,
                            w: r.width,
                            h: r.height
                        };
                    })
                    .filter(Boolean);

                return { pager_nodes: nodes.slice(0, 200) };
            }
            """
        )
        log.info("PAGINATION DIAGNOSTICS: %s", json.dumps(data))
    except Exception as e:
        log.warning("Could not collect pagination diagnostics: %s", str(e))


def go_to_page_number(page, page_num: int) -> bool:
    current = get_active_page_number(page)

    if current == page_num:
        return True

    if page_num < current:
        log.warning(
            "Requested page %s but current page is %s. Reload is required to go backwards.",
            page_num,
            current,
        )
        return False

    for attempt in range(1, 4):
        current = get_active_page_number(page)
        if current == page_num:
            return True

        log.info("Trying to navigate from page %s to page %s (attempt %s)", current, page_num, attempt)

        if PAGINATION_DIAGNOSTIC_MODE and attempt == 1:
            log_pagination_diagnostics(page)

        if click_page_option_direct_without_dropdown(page, page_num):
            page.wait_for_timeout(1200)
            return True

        if open_pager_dropdown(page):
            if click_page_option_from_dropdown(page, page_num):
                page.wait_for_timeout(1200)
                return True
        else:
            log.warning("Could not open pager dropdown on attempt %s", attempt)

        current = get_active_page_number(page)
        if page_num == current + 1:
            if click_next_page(page):
                page.wait_for_timeout(1200)
                return True

        page.wait_for_timeout(1000)

    log.warning("Failed navigating from page %s to page %s", get_active_page_number(page), page_num)
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
        "tax_sale_id": record.get("case_number"),
        "parcel_number": record.get("parcel_number"),
        "sale_date": normalize_sale_date_value(record.get("sale_date")),
        "opening_bid": normalize_money(record.get("opening_bid")),
        "deed_status": record.get("case_status"),
        "applicant_name": record.get("applicant_number"),
        "address": address_only,
        "city": city,
        "state_address": state_address,
        "zip": zip_code,
        "address_source_marker": "CASE_SUMMARY_PROPERTY_ADDRESS" if address_only else None,
        "status": "new",
        "notes": None,
        "auction_location": "Miami-Dade RealForeclose",
        "auction_start_time": None,
        "auction_platform": "Miami-Dade RealForeclose",
        "auction_source_url": AUCTION_URL,
        "removed_at": None,
        "is_active": True,
        "updated_at": now_iso(),
    }


def build_index_record(existing_id: Optional[str], payload: dict) -> dict:
    return {
        "id": existing_id,
        "node": payload.get("node"),
        "tax_sale_id": payload.get("tax_sale_id"),
        "parcel_number": payload.get("parcel_number"),
        "sale_date": payload.get("sale_date"),
        "address": payload.get("address"),
        "city": payload.get("city"),
        "state_address": payload.get("state_address"),
        "zip": payload.get("zip"),
        "pdf_url": payload.get("pdf_url"),
        "auction_source_url": payload.get("auction_source_url"),
        "opening_bid": payload.get("opening_bid"),
        "deed_status": payload.get("deed_status"),
        "applicant_name": payload.get("applicant_name"),
        "is_active": True,
        "removed_at": None,
    }


# =========================
# MAIN
# =========================
def run_miami():
    log.info("=== MIAMI FINAL OPERATIONAL V8 + STANDARDIZED SAVE FLOW + SAFE DELETE ===")

    supabase_rows = supabase_fetch_all_miami_records() if CAN_CHECK_SUPABASE else []
    indexes = build_supabase_indexes(supabase_rows)

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

        expected_total_pages = parse_total_pages(page)
        expected_total_items = parse_total_items(page)
        summary = get_results_summary(page)

        log.info("Detected total Miami pages: %s", expected_total_pages)
        log.info("Detected total Miami items: %s", expected_total_items)

        results = []
        failures = []
        supabase_results = []

        total_processed_rows = 0
        processed_pages = 0
        completed_all_pages = False
        can_delete_missing = False
        seen_nodes_this_run = set()

        skipped_fast_same_sale_date = 0
        updated_sale_date_only = 0
        opened_detail_count = 0

        for page_num in range(1, expected_total_pages + 1):
            if total_processed_rows >= MAX_LOTS:
                log.warning("MAX_LOTS limit reached (%s). Safe delete will be blocked.", MAX_LOTS)
                break

            if page_num > 1:
                ok = go_to_page_number(page, page_num)
                if not ok:
                    log.warning("Could not navigate to page %s", page_num)
                    break

            rows = collect_case_rows(page)
            if not rows:
                log.warning("No rows found on page %s", page_num)
                break

            processed_pages += 1
            log.info("Processing all %s rows from page %s before moving forward", len(rows), page_num)

            for row in rows:
                caseid = str(row.get("caseid") or "").strip()
                if caseid:
                    seen_nodes_this_run.add(caseid)

            for row in rows:
                if total_processed_rows >= MAX_LOTS:
                    break

                caseid = str(row.get("caseid") or "").strip()
                row_index = row.get("index")
                total_processed_rows += 1

                log.info(
                    "[row %s/%s] Evaluating caseid=%s page=%s row=%s ...",
                    total_processed_rows,
                    MAX_LOTS,
                    caseid,
                    page_num,
                    row_index,
                )

                try:
                    row_parsed = parse_row_text(row.get("row_text", ""))

                    pre_tax_sale_id = clean_text(row_parsed.get("case_number", ""))
                    pre_parcel_number = clean_text(row_parsed.get("parcel_number", ""))
                    pre_sale_date = normalize_sale_date_value(row_parsed.get("sale_date", ""))

                    existing = indexes["by_node"].get(caseid)
                    if not existing and pre_tax_sale_id and pre_parcel_number:
                        existing = indexes["by_tax_sale_parcel"].get((pre_tax_sale_id, pre_parcel_number))

                    if existing:
                        db_sale_date = normalize_sale_date_value(existing.get("sale_date"))
                        same_identity = (
                            clean_text(existing.get("parcel_number") or "") == pre_parcel_number
                            and (
                                not clean_text(existing.get("tax_sale_id") or "")
                                or clean_text(existing.get("tax_sale_id") or "") == pre_tax_sale_id
                            )
                        )
                        is_inactive = existing.get("is_active") is False or clean_text(existing.get("removed_at") or "") != ""

                        if same_identity and not record_needs_enrichment(existing) and not is_inactive and db_sale_date == pre_sale_date:
                            skipped_fast_same_sale_date += 1
                            log.info(
                                "FAST SKIP node=%s reason=same identity and same sale_date site=%s db=%s",
                                caseid,
                                pre_sale_date,
                                db_sale_date,
                            )
                            continue

                        if same_identity and not record_needs_enrichment(existing) and db_sale_date != pre_sale_date:
                            update_result = supabase_update_sale_date(existing.get("id"), pre_sale_date)
                            supabase_results.append({
                                "node": caseid,
                                "mode": "update_sale_date_only",
                                **update_result,
                            })
                            updated_sale_date_only += 1

                            if update_result.get("sent"):
                                existing["sale_date"] = pre_sale_date
                                existing["is_active"] = True
                                existing["removed_at"] = None

                            log.info(
                                "SALE_DATE ONLY UPDATE node=%s old=%s new=%s",
                                caseid,
                                db_sale_date,
                                pre_sale_date,
                            )
                            continue

                    opened_detail_count += 1

                    if page_num > 1:
                        ok = go_to_page_number(page, page_num)
                        if not ok:
                            raise RuntimeError(f"Could not re-open page {page_num} for caseid={caseid}")

                    base_case = open_case_by_caseid(page, caseid)
                    case_detail = extract_case_detail(page, base_case)

                    record = build_final_record(case_detail)
                    results.append(record)

                    prop_payload = build_properties_payload(record)

                    existing = indexes["by_node"].get(prop_payload.get("node"))
                    if not existing:
                        key = (
                            clean_text(prop_payload.get("tax_sale_id") or ""),
                            clean_text(prop_payload.get("parcel_number") or ""),
                        )
                        if key[0] and key[1]:
                            existing = indexes["by_tax_sale_parcel"].get(key)

                    if existing and not payload_is_better_than_existing(prop_payload, existing):
                        is_inactive = existing.get("is_active") is False or clean_text(existing.get("removed_at") or "") != ""
                        if not is_inactive:
                            log.info("DETAIL READ but payload not better → skip save node=%s", prop_payload.get("node"))
                            open_list_and_apply_filter(page)
                            if page_num > 1:
                                ok = go_to_page_number(page, page_num)
                                if not ok:
                                    raise RuntimeError(f"Could not return to page {page_num} after caseid={caseid}")
                            continue

                    sb_result = supabase_save_property(prop_payload, existing)
                    supabase_results.append({
                        "node": prop_payload.get("node"),
                        "mode": "full_save",
                        **sb_result,
                    })

                    if sb_result.get("sent"):
                        record_id = sb_result.get("record_id") or (existing.get("id") if existing else None)
                        idx_record = build_index_record(record_id, prop_payload)

                        node_key = clean_text(prop_payload.get("node") or "")
                        tax_sale_key = clean_text(prop_payload.get("tax_sale_id") or "")
                        parcel_key = clean_text(prop_payload.get("parcel_number") or "")

                        if node_key:
                            indexes["by_node"][node_key] = idx_record
                        if tax_sale_key and parcel_key:
                            indexes["by_tax_sale_parcel"][(tax_sale_key, parcel_key)] = idx_record

                        send_to_app(prop_payload)

                    log.info("SUCCESS DETAIL OPEN node=%s", prop_payload.get("node"))

                    open_list_and_apply_filter(page)
                    if page_num > 1:
                        ok = go_to_page_number(page, page_num)
                        if not ok:
                            raise RuntimeError(f"Could not return to page {page_num} after caseid={caseid}")

                except Exception as e:
                    log.exception("FAILED CASE caseid=%s: %s", caseid, e)
                    failures.append({
                        "caseid": caseid,
                        "page_num": page_num,
                        "row_index": row_index,
                        "error": str(e),
                    })

                    try:
                        open_list_and_apply_filter(page)
                        if page_num > 1:
                            go_to_page_number(page, page_num)
                    except Exception:
                        pass

        completed_all_pages = (
            expected_total_pages > 0 and processed_pages == expected_total_pages
        )

        can_delete_missing = (
            completed_all_pages
            and expected_total_items > 0
            and len(seen_nodes_this_run) == expected_total_items
            and failures == []
            and total_processed_rows >= expected_total_items
        )

        if can_delete_missing:
            reconcile_result = reconcile_supabase_to_site(seen_nodes_this_run)
        else:
            reconcile_result = {
                "executed": False,
                "reason": (
                    f"safe delete blocked: "
                    f"completed_all_pages={completed_all_pages}, "
                    f"processed_pages={processed_pages}, "
                    f"expected_total_pages={expected_total_pages}, "
                    f"seen_nodes={len(seen_nodes_this_run)}, "
                    f"expected_total_items={expected_total_items}, "
                    f"failures_count={len(failures)}, "
                    f"total_processed_rows={total_processed_rows}, "
                    f"MAX_LOTS={MAX_LOTS}"
                ),
            }

        final_payload = {
            "source": "MiamiDade",
            "mode": "final_operational_v8_standardized_save_flow_safe_delete",
            "expected_total_pages": expected_total_pages,
            "expected_total_items": expected_total_items,
            "processed_pages": processed_pages,
            "seen_nodes_count": len(seen_nodes_this_run),
            "completed_all_pages": completed_all_pages,
            "can_delete_missing": can_delete_missing,
            "rows_evaluated_count": total_processed_rows,
            "records_count": len(results),
            "failures_count": len(failures),
            "fast_skipped_same_sale_date_count": skipped_fast_same_sale_date,
            "sale_date_only_updates_count": updated_sale_date_only,
            "detail_opened_count": opened_detail_count,
            "supabase_results": supabase_results,
            "reconcile_result": reconcile_result,
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
