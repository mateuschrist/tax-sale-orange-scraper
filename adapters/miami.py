import json
import logging
import os
import re
from typing import Dict, List, Optional
from urllib.parse import quote
from datetime import datetime

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
    ]
    return sum(1 for f in fields if payload.get(f) not in (None, "", [], {}))


def wait_long(page, ms=15000):
    page.wait_for_timeout(ms)


def stabilize(page, label="", ms=12000):
    try:
        page.wait_for_load_state("domcontentloaded", timeout=15000)
    except Exception:
        pass

    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass

    wait_long(page, ms)
    log.info("Stabilized: %s", label)


def dump_debug_artifacts(page, prefix: str):
    try:
        page.screenshot(path=f"{prefix}.png", full_page=True)
        log.info("Saved screenshot: %s.png", prefix)
    except Exception as e:
        log.warning("Could not save screenshot %s.png: %s", prefix, str(e))

    try:
        with open(f"{prefix}.html", "w", encoding="utf-8") as f:
            f.write(page.content())
        log.info("Saved html: %s.html", prefix)
    except Exception as e:
        log.warning("Could not save html %s.html: %s", prefix, str(e))


def humanize(page):
    try:
        page.mouse.move(250, 220)
        page.wait_for_timeout(300)
        page.mouse.move(520, 360)
        page.wait_for_timeout(500)
        page.mouse.wheel(0, 350)
        page.wait_for_timeout(700)
        page.mouse.wheel(0, -180)
        page.wait_for_timeout(400)
    except Exception:
        pass


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


def supabase_find_existing_case(node: str):
    if not CAN_CHECK_SUPABASE or not node:
        return None

    url = (
        f"{SUPABASE_URL}/rest/v1/properties"
        f"?county=eq.Miami-Dade"
        f"&node=eq.{quote(str(node), safe='')}"
        f"&select=id,node,tax_sale_id,parcel_number,sale_date,address,city,state_address,zip,pdf_url,auction_source_url,opening_bid,deed_status,applicant_name"
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
        f"&select=id,node,tax_sale_id,parcel_number,sale_date,address,city,state_address,zip,pdf_url,auction_source_url,opening_bid,deed_status,applicant_name"
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


def supabase_fetch_existing_cases_by_nodes(nodes: List[str]) -> Dict[str, dict]:
    if not CAN_CHECK_SUPABASE:
        return {}

    clean_nodes = [str(x).strip() for x in nodes if str(x).strip()]
    if not clean_nodes:
        return {}

    results = {}
    batch_size = 100

    for i in range(0, len(clean_nodes), batch_size):
        batch = clean_nodes[i:i + batch_size]
        try:
            quoted_nodes = ",".join(f'"{quote(node, safe="")}"' for node in batch)
            url = (
                f"{SUPABASE_URL}/rest/v1/properties"
                f"?county=eq.Miami-Dade"
                f"&node=in.({quoted_nodes})"
                f"&select=id,node,tax_sale_id,parcel_number,sale_date,address,city,state_address,zip,pdf_url,auction_source_url,opening_bid,deed_status,applicant_name"
                f"&limit={len(batch)}"
            )

            r = requests.get(url, headers=sb_headers(), timeout=30)
            if r.status_code != 200:
                log.warning(
                    "supabase_fetch_existing_cases_by_nodes non-200 status=%s body=%s",
                    r.status_code,
                    r.text[:500],
                )
                continue

            arr = r.json() or []
            for item in arr:
                node = str(item.get("node") or "").strip()
                if node:
                    results[node] = item

        except Exception as e:
            log.warning("supabase_fetch_existing_cases_by_nodes batch failed: %s", str(e))

    return results


def supabase_update_sale_date(record_id: str, sale_date: Optional[str]) -> dict:
    if not CAN_CHECK_SUPABASE:
        return {"sent": False, "status_code": None, "response_text": "SUPABASE not configured"}
    if not record_id:
        return {"sent": False, "status_code": None, "response_text": "Missing record id"}

    url = f"{SUPABASE_URL}/rest/v1/properties?id=eq.{quote(str(record_id), safe='')}"
    headers = sb_headers()
    headers["Prefer"] = "return=representation"

    payload = {"sale_date": sale_date}

    try:
        r = requests.patch(url, headers=headers, json=payload, timeout=20)
        ok = r.status_code in (200, 204)

        if ok:
            log.info("SUPABASE SALE_DATE UPDATE OK id=%s sale_date=%s status=%s", record_id, sale_date, r.status_code)
        else:
            log.warning("SUPABASE SALE_DATE UPDATE FAILED id=%s status=%s body=%s", record_id, r.status_code, r.text[:500])

        return {"sent": ok, "status_code": r.status_code, "response_text": r.text[:1000]}
    except Exception as e:
        log.warning("SUPABASE SALE_DATE UPDATE EXCEPTION id=%s error=%s", record_id, str(e))
        return {"sent": False, "status_code": None, "response_text": str(e)}


def payload_is_better_than_existing(payload: dict, existing: dict) -> bool:
    new_score = payload_quality_score(payload)
    old_score = payload_quality_score(existing)

    if new_score > old_score:
        return True

    for f in ["address", "city", "state_address", "zip", "pdf_url", "auction_source_url"]:
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
        return {"sent": False, "status_code": None, "node": payload.get("node"), "response_text": "SUPABASE not configured"}

    should_send, reason = should_send_payload(payload)
    log.info("SUPABASE DEDUP node=%s => %s", payload.get("node"), reason)

    if not should_send:
        return {"sent": False, "status_code": 200, "node": payload.get("node"), "response_text": reason}

    url = f"{SUPABASE_URL}/rest/v1/properties"
    headers = sb_headers()
    headers["Prefer"] = "return=representation,resolution=merge-duplicates"

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        ok = r.status_code in (200, 201)

        if ok:
            log.info("SUPABASE UPSERT OK status=%s node=%s", r.status_code, payload.get("node"))
        else:
            log.error("SUPABASE UPSERT FAILED status=%s node=%s body=%s", r.status_code, payload.get("node"), r.text[:600])

        return {"sent": ok, "status_code": r.status_code, "node": payload.get("node"), "response_text": r.text[:1000]}
    except Exception as e:
        log.exception("SUPABASE UPSERT EXCEPTION node=%s error=%s", payload.get("node"), str(e))
        return {"sent": False, "status_code": None, "node": payload.get("node"), "response_text": str(e)}


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
                log.warning("supabase_list_all_nodes non-200 status=%s body=%s", r.status_code, r.text[:500])
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
        return {"executed": False, "deleted_count": 0, "reason": "SUPABASE not configured"}

    nodes = [str(x).strip() for x in nodes if str(x).strip()]
    if not nodes:
        return {"executed": True, "deleted_count": 0, "reason": "no nodes to delete"}

    deleted_count = 0
    errors = []
    batch_size = 100

    for i in range(0, len(nodes), batch_size):
        batch = nodes[i:i + batch_size]

        try:
            in_clause = ",".join(f'"{quote(node, safe="")}"' for node in batch)
            url = f"{SUPABASE_URL}/rest/v1/properties?county=eq.Miami-Dade&node=in.({in_clause})"

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

    return {"executed": True, "deleted_count": deleted_count, "requested_delete_count": len(nodes), "errors": errors}


def reconcile_supabase_to_site(seen_nodes_this_run: set) -> Dict:
    try:
        existing_nodes = set(supabase_list_all_nodes())
        site_nodes = {str(x).strip() for x in seen_nodes_this_run if str(x).strip()}
        to_delete = sorted(existing_nodes - site_nodes)

        log.info("RECONCILE MIAMI: supabase=%s site=%s delete=%s", len(existing_nodes), len(site_nodes), len(to_delete))
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
        return {"executed": False, "reason": str(e)}


# =========================
# UI / FILTER / SEARCH
# =========================
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


def click_safe(page, selector, name, timeout=6000):
    try:
        page.locator(selector).first.click(timeout=timeout)
        log.info("%s clicked (locator): %s", name, selector)
        return True
    except Exception:
        pass

    try:
        page.locator(selector).first.click(force=True, timeout=timeout)
        log.info("%s clicked (force): %s", name, selector)
        return True
    except Exception:
        pass

    try:
        clicked = page.evaluate(
            """(sel) => {
                const el = document.querySelector(sel);
                if (!el) return false;
                el.click();
                return true;
            }""",
            selector,
        )
        if clicked:
            log.info("%s clicked (JS): %s", name, selector)
            return True
    except Exception:
        pass

    log.warning("%s not found/clickable: %s", name, selector)
    return False


def click_by_text_scan(page, texts: List[str], name: str) -> bool:
    for text in texts:
        selectors = [
            f'text="{text}"',
            f'text={text}',
            f'button:has-text("{text}")',
            f'a:has-text("{text}")',
            f'span:has-text("{text}")',
            f'div:has-text("{text}")',
            f'li:has-text("{text}")',
        ]
        for sel in selectors:
            if click_safe(page, sel, f"{name} [{text}]"):
                return True

    return False


def open_dropdown(page):
    log.info("Opening Case Status (robust)...")

    try:
        result = page.evaluate(
            """
            () => {
                const btn = document.querySelector('#filterButtonStatus');
                if (!btn) return { ok: false, reason: 'button not found' };

                const r = btn.getBoundingClientRect();

                btn.scrollIntoView({ block: 'center' });

                return {
                    ok: true,
                    x: r.left + r.width / 2,
                    y: r.top + r.height / 2
                };
            }
            """
        )

        if not result.get("ok"):
            return False

        page.mouse.click(result["x"], result["y"])
        page.wait_for_timeout(10000)

        return True

    except Exception as e:
        log.warning("open_dropdown failed: %s", e)
        return False


def force_select_active(page):
    log.info("Selecting Active (fixed XY from working version)...")

    data = page.evaluate(
        """
        () => {
            const menu = document.querySelector('.dropdown-menu.public, .dropdown-menu');
            if (!menu) return { ok: false };

            const r = menu.getBoundingClientRect();

            return {
                ok: r.width > 0 && r.height > 0,
                left: r.left,
                top: r.top
            };
        }
        """
    )

    if not data.get("ok"):
        raise RuntimeError("Dropdown menu not found")

    x = data["left"] + 90
    y = data["top"] + 80

    page.mouse.click(x, y)
    page.wait_for_timeout(8000)

    state = page.evaluate(
        """
        () => {
            const hidden = document.querySelector('#filterCaseStatus');
            return hidden ? hidden.value : '';
        }
        """
    )

    if not state:
        raise RuntimeError("Active selection failed")

    log.info("ACTIVE selected with hidden=%s", state)
    return state


def get_filter_state(page):
    return page.evaluate(
        """
        () => {
            const hidden = document.querySelector('#filterCaseStatus');
            const label = document.querySelector('#filterCaseStatusLabel');
            const selected = Array.from(document.querySelectorAll('.selected,[aria-selected="true"]'))
                .map(el => ((el.innerText || '').replace(/\\s+/g, ' ').trim()))
                .filter(Boolean)
                .slice(0, 30);

            return {
                label: (label?.innerText || '').trim(),
                hidden_filterCaseStatus: hidden ? hidden.value : '',
                selected_items: selected
            };
        }
        """
    )


def extract_cases(page):
    data = page.evaluate(
        """
        () => {
            function txt(el) {
                return ((el.innerText || el.textContent || '').replace(/\\s+/g, ' ')).trim();
            }

            const selectors = [
                'tr[data-caseid]',
                'tr.load-case',
                'table tbody tr',
                'tbody tr'
            ];

            let rows = [];
            for (const sel of selectors) {
                const found = Array.from(document.querySelectorAll(sel));
                if (found.length > 0) {
                    rows = found;
                    break;
                }
            }

            const parsed = rows.map((row, idx) => {
                const text = txt(row);
                const caseId = row.getAttribute('data-caseid') || '';
                const m = text.match(/\\b(\\d{4}A\\d{5})\\b/);

                return {
                    index: idx,
                    caseid: caseId,
                    text,
                    case_number: m ? m[1] : ''
                };
            });

            return {
                rows_count: parsed.length,
                cases: parsed.map(x => x.case_number).filter(Boolean),
                rows_sample: parsed.slice(0, 20)
            };
        }
        """
    )

    return {
        "count": data["rows_count"],
        "cases": data["cases"],
        "rows_sample": data["rows_sample"],
    }


def click_search_button(page) -> Dict:
    log.info("Clicking Process Search...")

    selectors = [
        "button.filters-submit",
        'button:has-text("Process Search")',
        'text="Process Search"',
    ]

    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue

            loc.click(force=True)
            stabilize(page, "search", 15000)

            body = page.locator("body").inner_text(timeout=5000)
            page_info = extract_cases(page)

            success = (
                page_info["count"] > 0
                or "Found" in body
                or "Cases List" in body
                or "Page 1 of" in body
            )

            return {
                "ok": success,
                "selector": sel,
                "rows": page_info["count"],
                "cases_found": len(page_info["cases"]),
                "body_sample": body[:1500],
            }

        except Exception as e:
            log.warning("click_search %s -> %s", sel, e)

    return {"ok": False, "selector": None, "rows": 0}


def wait_for_case_rows(page, timeout_ms=30000):
    log.info("Waiting for Miami search results...")
    waited = 0
    step = 1000
    while waited < timeout_ms:
        page_info = extract_cases(page)
        if page_info["count"] > 0:
            log.info("Rows after search: %s", page_info["count"])
            return page_info["count"]
        page.wait_for_timeout(step)
        waited += step

    dump_debug_artifacts(page, "miami_no_rows_after_search")
    raise RuntimeError("No case rows found after Miami search")


def run_search_flow(page):
    log.info("Running Miami ACTIVE-only flow...")

    reset_ok = click_safe(page, "a.filters-reset", "RESET FILTERS")
    if reset_ok:
        page.wait_for_timeout(1200)
    else:
        log.warning("RESET FILTERS not found; continuing without reset")

    if not open_dropdown(page):
        raise RuntimeError("Could not open Case Status dropdown")

    active = select_exact_active(page)
    log.info("ACTIVE selection result: %s", active)

    if not active.get("ok"):
        dump_debug_artifacts(page, "miami_active_click_failed")
        raise RuntimeError(f"Could not select exact Active. Result={active}")

    hidden_value = clean_text(active.get("state", {}).get("hidden") or "")
    if hidden_value != "192":
        dump_debug_artifacts(page, "miami_active_not_192")
        raise RuntimeError(f"Active filter did not stick as 192. Result={active}")

    state = get_filter_state(page)
    log.info("SEARCH STATE BEFORE SUBMIT: %s", state)

    search = click_search_button(page)
    log.info("SEARCH CLICK RESULT: %s", search)

    if not search.get("ok"):
        dump_debug_artifacts(page, "miami_search_click_failed")
        raise RuntimeError(f"Could not search successfully. Result={search}")

    wait_for_case_rows(page)


def open_list_and_apply_filter(page):
    page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
    stabilize(page, "initial_load", 15000)
    humanize(page)
    run_search_flow(page)


# =========================
# PAGINATION
# =========================
def get_results_summary(page) -> Dict:
    return page.evaluate(
        """
        () => {
            const bodyText = document.body.innerText || '';
            const rows = document.querySelectorAll('tr[data-caseid], tr.load-case, table tbody tr, tbody tr').length;

            const pageLinks = Array.from(document.querySelectorAll('a, button, span, div'))
                .map(el => (el.innerText || '').replace(/\\s+/g, ' ').trim())
                .filter(x => /^Page\\s+\\d+/i.test(x));

            const m = bodyText.match(/Page\\s+(\\d+)\\s+of\\s+(\\d+)/i) || bodyText.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);

            return {
                rows_on_page: rows,
                page_links: pageLinks,
                current_page_text: m ? `Page ${m[1]} of ${m[2]}` : "",
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
                let m = body.match(/Page\\s+(\\d+)\\s+of\\s+(\\d+)/i);
                if (m) return parseInt(m[1], 10);

                m = body.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);
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
    rows = page.locator('tr[data-caseid]')
    count = rows.count()

    if count == 0:
        rows = page.locator('tr.load-case')
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
                let m = body.match(/Page\\s+(\\d+)\\s+of\\s+(\\d+)/i);
                if (m) return parseInt(m[2], 10);

                m = body.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);
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
                    /Found\\s+(\\d+)\\s+Results/i,
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
        row = page.locator('tr[data-caseid]').first
        if row.count() == 0:
            row = page.locator('tr.load-case').first
            if row.count() == 0:
                return ""
        return row.get_attribute("data-caseid") or ""
    except Exception:
        return ""


def get_first_row_text(page) -> str:
    try:
        row = page.locator('tr[data-caseid]').first
        if row.count() == 0:
            row = page.locator('tr.load-case').first
            if row.count() == 0:
                return ""
        return clean_text(row.inner_text())
    except Exception:
        return ""


def wait_for_page_change(page, old_page: int, old_first_caseid: str = "", old_first_row_text: str = "", timeout_ms: int = 22000) -> bool:
    waited = 0
    step = 1000

    while waited < timeout_ms:
        try:
            current = get_active_page_number(page)
            page_info = extract_cases(page)
            new_first_caseid = get_first_caseid(page)
            new_first_row_text = get_first_row_text(page)

            page_changed = current != old_page and page_info["count"] > 0
            first_case_changed = bool(old_first_caseid and new_first_caseid and new_first_caseid != old_first_caseid)
            first_text_changed = bool(old_first_row_text and new_first_row_text and new_first_row_text != old_first_row_text)

            if page_changed or first_case_changed or first_text_changed:
                log.info("Miami page changed successfully: old_page=%s new_page=%s old_caseid=%s new_caseid=%s",
                         old_page, current, old_first_caseid, new_first_caseid)
                return True
        except Exception:
            pass

        page.wait_for_timeout(step)
        waited += step

    return False


def open_pager_dropdown(page) -> bool:
    selectors = [
        'text=/^Page\\s+\\d+\\s*$/',
        'text=/^Page\\s+\\d+\\s+of\\s+\\d+$/i',
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

                if (!target) return { ok: false, reason: 'target page element not found' };

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
            if wait_for_page_change(page, current_before, old_first_caseid, old_first_row_text, 16000):
                return True

            try:
                if result.get("visible"):
                    page.mouse.click(result["x"], result["y"])
                    if wait_for_page_change(page, current_before, old_first_caseid, old_first_row_text, 16000):
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

                if (!option) return { ok: false, reason: 'dropdown option not found' };

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
            if wait_for_page_change(page, current_before, old_first_caseid, old_first_row_text, 16000):
                return True

            try:
                page.mouse.click(result["x"], result["y"])
                if wait_for_page_change(page, current_before, old_first_caseid, old_first_row_text, 16000):
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

    try:
        result = page.evaluate(
            """
            () => {
                const icon = document.querySelector('i.fa-regular.fa-chevron-right[aria-hidden="true"], i.fa-regular.fa-chevron-right, i.fa-chevron-right');
                if (!icon) return { ok: false, reason: 'icon not found' };

                const parent = icon.closest('a,button,div,span,td,li') || icon;

                try { parent.scrollIntoView({ block: 'center' }); } catch(e) {}
                try { parent.click(); } catch(e) {}
                try { parent.dispatchEvent(new MouseEvent('mousedown', { bubbles:true })); } catch(e) {}
                try { parent.dispatchEvent(new MouseEvent('mouseup', { bubbles:true })); } catch(e) {}
                try { parent.dispatchEvent(new MouseEvent('click', { bubbles:true })); } catch(e) {}

                return {
                    ok: true,
                    tag: parent.tagName.toLowerCase(),
                    class_name: (parent.className || '').toString()
                };
            }
            """
        )

        log.info("NEXT target detection: %s", result)

        if result and result.get("ok"):
            if wait_for_page_change(page, current_before, old_first_caseid, old_first_row_text, 16000):
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

                        if (!text.includes('Page') && !el.getAttribute('data-page') && !((el.getAttribute('href') || '').includes('javascript:void(0)'))) {
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
        log.warning("Requested page %s but current page is %s. Reload is required to go backwards.", page_num, current)
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
    return {
        "status": parts[0] if len(parts) > 0 else "",
        "case_number": parts[1] if len(parts) > 1 else "",
        "date_created": parts[2] if len(parts) > 2 else "",
        "application_number": parts[3] if len(parts) > 3 else "",
        "parcel_number": parts[4] if len(parts) > 4 else "",
        "sale_date": parts[5] if len(parts) > 5 else "",
    }


def open_case_by_caseid(page, caseid: str) -> Dict:
    rows = page.locator(f'tr[data-caseid="{caseid}"]')
    count = rows.count()
    if count == 0:
        rows = page.locator(f'tr.load-case[data-caseid="{caseid}"]')
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

    return {
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
        "sale_date": normalize_sale_date_value(record.get("sale_date")),
        "opening_bid": normalize_money(record.get("opening_bid")),
        "deed_status": record.get("case_status"),
        "applicant_name": record.get("applicant_number"),
        "address": address_only,
        "city": city,
        "state_address": state_address,
        "zip": zip_code,
    }


# =========================
# BROWSER
# =========================
def launch_browser(p):
    try:
        browser = p.chromium.launch(
            channel="chrome",
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        log.info("Launched with channel=chrome")
        return browser
    except Exception as e:
        log.warning("Chrome channel unavailable, falling back to chromium: %s", str(e))
        return p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )


# =========================
# MAIN
# =========================
def run_miami():
    log.info("=== MIAMI FINAL FIXED MODE ===")

    with sync_playwright() as p:
        browser = launch_browser(p)

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

            page_nodes = []
            for row in rows:
                caseid = str(row.get("caseid") or "").strip()
                if caseid:
                    seen_nodes_this_run.add(caseid)
                    page_nodes.append(caseid)

            existing_by_node = supabase_fetch_existing_cases_by_nodes(page_nodes)

            for row in rows:
                if total_processed_rows >= MAX_LOTS:
                    break

                caseid = str(row.get("caseid") or "").strip()
                row_index = row.get("index")
                total_processed_rows += 1

                log.info("[row %s/%s] Evaluating caseid=%s page=%s row=%s ...",
                         total_processed_rows, MAX_LOTS, caseid, page_num, row_index)

                try:
                    row_parsed = parse_row_text(row.get("row_text", ""))

                    pre_tax_sale_id = clean_text(row_parsed.get("case_number", ""))
                    pre_parcel_number = clean_text(row_parsed.get("parcel_number", ""))
                    pre_sale_date = normalize_sale_date_value(row_parsed.get("sale_date", ""))

                    existing = existing_by_node.get(caseid)

                    if existing:
                        existing_tax_sale_id = clean_text(existing.get("tax_sale_id", ""))
                        existing_parcel_number = clean_text(existing.get("parcel_number", ""))
                        existing_sale_date = normalize_sale_date_value(existing.get("sale_date"))

                        same_identity = (
                            existing_parcel_number == pre_parcel_number and
                            (not existing_tax_sale_id or existing_tax_sale_id == pre_tax_sale_id)
                        )

                        if same_identity:
                            if existing_sale_date == pre_sale_date:
                                skipped_fast_same_sale_date += 1
                                log.info("FAST SKIP node=%s reason=same identity and same sale_date site=%s db=%s",
                                         caseid, pre_sale_date, existing_sale_date)
                                continue

                            update_result = supabase_update_sale_date(existing.get("id"), pre_sale_date)
                            supabase_results.append({
                                "sent": update_result.get("sent", False),
                                "status_code": update_result.get("status_code"),
                                "node": caseid,
                                "response_text": f"sale_date_only_update: {update_result.get('response_text', '')}",
                            })
                            updated_sale_date_only += 1

                            log.info("SALE_DATE ONLY UPDATE node=%s old=%s new=%s", caseid, existing_sale_date, pre_sale_date)

                            mini_payload = {
                                "county": "Miami-Dade",
                                "state": "FL",
                                "node": caseid,
                                "tax_sale_id": pre_tax_sale_id,
                                "parcel_number": pre_parcel_number,
                                "sale_date": pre_sale_date,
                                "auction_source_url": AUCTION_URL,
                            }
                            send_to_app(mini_payload)
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

                    sb_result = supabase_upsert_property(prop_payload)
                    supabase_results.append(sb_result)

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

                    dump_debug_artifacts(page, f"miami_failed_case_{caseid}")

                    try:
                        open_list_and_apply_filter(page)
                        if page_num > 1:
                            go_to_page_number(page, page_num)
                    except Exception:
                        pass

        completed_all_pages = (expected_total_pages > 0 and processed_pages == expected_total_pages)

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
                    f"safe delete blocked: completed_all_pages={completed_all_pages}, "
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
            "mode": "final_fixed_mode",
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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    run_miami()