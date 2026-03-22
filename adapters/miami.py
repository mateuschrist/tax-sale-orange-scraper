import os
import re
import json
import time
import traceback
from typing import Any, Dict, List, Optional

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


MIAMI_CASE_LIST_URL = "https://miamidade.realtdm.com/public/cases/list"
MIAMI_AUCTION_URL = "https://www.miamidade.realforeclose.com/index.cfm"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
MAX_LOTS = int(os.getenv("MAX_LOTS", "100"))
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "auctions").strip() or "auctions"


# =========================
# LOG
# =========================
def log(msg: str):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), "| INFO |", msg, flush=True)


def warn(msg: str):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), "| WARNING |", msg, flush=True)


def err(msg: str):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), "| ERROR |", msg, flush=True)


# =========================
# TEXT UTILS
# =========================
def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    return text.strip()


def clean_inline(value: Any) -> str:
    return clean_text(value).replace("\n", ", ").replace(" ,", ",")


def extract_after_label_block(text: str, label: str, next_labels: List[str]) -> str:
    """
    Extrai texto após um label até o próximo label conhecido.
    """
    if not text:
        return ""

    pattern_start = re.escape(label) + r"\s*"
    m = re.search(pattern_start, text, flags=re.IGNORECASE)
    if not m:
        return ""

    start = m.end()
    tail = text[start:]

    next_match_pos = len(tail)
    for nl in next_labels:
        nm = re.search(r"\n\s*" + re.escape(nl) + r"\s*", tail, flags=re.IGNORECASE)
        if nm and nm.start() < next_match_pos:
            next_match_pos = nm.start()

    return clean_text(tail[:next_match_pos])


def body_text(page) -> str:
    try:
        return clean_text(page.locator("body").inner_text(timeout=8000))
    except Exception:
        return ""


# =========================
# SUPABASE
# =========================
def get_supabase_client():
    """
    Tenta usar o pacote oficial. Se não existir, retorna None e usa REST fallback.
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        warn("Supabase env vars ausentes")
        return None

    try:
        from supabase import create_client  # type: ignore
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        log("Supabase official client initialized")
        return client
    except Exception as e:
        warn(f"Supabase official client unavailable, using REST fallback: {e}")
        return None


def send_to_supabase(client, record: Dict[str, Any]) -> Dict[str, Any]:
    external_id = record.get("external_id", "")

    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        return {"sent": False, "reason": "missing env", "external_id": external_id}

    # 1) tenta official client
    if client is not None:
        try:
            res = client.table(SUPABASE_TABLE).upsert(record).execute()
            return {
                "sent": True,
                "mode": "official_client",
                "external_id": external_id,
                "response": str(res),
            }
        except Exception as e:
            warn(f"Official client upsert failed for {external_id}: {e}")

    # 2) fallback REST
    try:
        url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
        headers = {
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=representation",
        }

        # usa external_id como conflito
        params = {"on_conflict": "external_id"}

        resp = requests.post(url, headers=headers, params=params, data=json.dumps([record]), timeout=60)

        if 200 <= resp.status_code < 300:
            return {
                "sent": True,
                "mode": "rest_fallback",
                "external_id": external_id,
                "status_code": resp.status_code,
            }

        return {
            "sent": False,
            "mode": "rest_fallback",
            "external_id": external_id,
            "status_code": resp.status_code,
            "response": resp.text[:2000],
        }
    except Exception as e:
        return {
            "sent": False,
            "mode": "rest_fallback",
            "external_id": external_id,
            "error": str(e),
        }


# =========================
# BROWSER HELPERS
# =========================
def safe_click(page, selector: str, label: str, timeout: int = 8000, force: bool = False) -> bool:
    try:
        page.locator(selector).first.click(timeout=timeout, force=force)
        log(f"{label} clicked")
        return True
    except Exception as e:
        warn(f"{label} click failed on selector [{selector}]: {e}")
        return False


def safe_click_locator(locator, label: str, timeout: int = 8000, force: bool = False) -> bool:
    try:
        locator.click(timeout=timeout, force=force)
        log(f"{label} clicked")
        return True
    except Exception as e:
        warn(f"{label} click failed: {e}")
        return False


def wait_short(page, ms: int = 1500):
    page.wait_for_timeout(ms)


# =========================
# MIAMI FILTER
# =========================
def open_miami_and_apply_active_filter(page):
    log("Running Miami ACTIVE-only flow...")
    page.goto(MIAMI_CASE_LIST_URL, wait_until="domcontentloaded", timeout=90000)
    wait_short(page, 3000)

    # reset
    safe_click(page, "a.filters-reset", "RESET FILTERS", timeout=10000, force=True)
    wait_short(page, 2000)

    # abre menu de status
    # alguns ambientes usam esse id/botão
    if not safe_click(page, "#filterButtonStatus", "FILTER BUTTON", timeout=8000, force=True):
        safe_click(page, "#caseFiltersStatus", "FILTER BUTTON fallback", timeout=8000, force=True)
    wait_short(page, 1500)

    # limpa todos os Active children e seta hidden input = 192
    page.evaluate(
        """
        () => {
            const activeChildren = [...document.querySelectorAll('a.filter-status-nosub.status-sub[data-parentid="2"]')];
            for (const a of activeChildren) {
                a.className = 'filter-status-nosub status-sub';
                const icon = a.querySelector('i');
                if (icon) icon.className = 'icon-circle-blank';
            }

            const only192 = document.querySelector('a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]');
            if (only192) {
                only192.className = 'filter-status-nosub status-sub selected';
                const icon = only192.querySelector('i');
                if (icon) icon.className = 'icon-ok-sign';
            }

            const hidden = document.querySelector('#filterCaseStatus');
            if (hidden) hidden.value = '192';

            const label = document.querySelector('#caseFiltersStatus .filter-option');
            if (label) label.textContent = '1 Selected';
        }
        """
    )
    log("Forced UI + hidden filterCaseStatus=192")
    wait_short(page, 1000)

    state_before = page.evaluate(
        """
        () => {
            const hidden = document.querySelector('#filterCaseStatus');
            const selected192 = document.querySelector('a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]');
            const label = document.querySelector('#caseFiltersStatus .filter-option');
            return {
                label: label ? label.textContent.trim() : '',
                hidden_filterCaseStatus: hidden ? hidden.value : '',
                selected192_class: selected192 ? selected192.className : ''
            };
        }
        """
    )
    log(f"SEARCH STATE BEFORE SUBMIT: {state_before}")

    # submit search
    submitted = (
        safe_click(page, "button.filters-submit", "SEARCH BUTTON", timeout=10000, force=True)
        or safe_click(page, "input.filters-submit", "SEARCH BUTTON", timeout=10000, force=True)
        or safe_click(page, "button[type='submit']", "SEARCH BUTTON", timeout=10000, force=True)
    )

    if not submitted:
        raise RuntimeError("Could not click SEARCH BUTTON")

    log("Waiting for Miami search results...")
    wait_short(page, 6000)

    # aguarda rows aparecerem
    for _ in range(20):
        rows = page.locator("tr.load-case.table-row.link[data-caseid]").count()
        if rows > 0:
            break
        wait_short(page, 1000)

    final_state = page.evaluate(
        """
        () => {
            const hidden = document.querySelector('#filterCaseStatus');
            const selected192 = document.querySelector('a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]');
            const label = document.querySelector('#caseFiltersStatus .filter-option');
            return {
                label: label ? label.textContent.trim() : '',
                hidden_filterCaseStatus: hidden ? hidden.value : '',
                selected192_class: selected192 ? selected192.className : ''
            };
        }
        """
    )
    log(f"SEARCH STATE AFTER SUBMIT: {final_state}")


def try_set_results_per_page(page, target: str = "100") -> Dict[str, Any]:
    """
    Tenta mudar para 100/page. Se o site ignorar, seguimos com 20/page.
    """
    try:
        result = page.evaluate(
            f"""
            () => {{
                const hidden = document.querySelector('#filterCasesPerPage');
                if (hidden) hidden.value = '{target}';

                const select = document.querySelector('select[name="filterCasesPerPage"]');
                if (select) {{
                    select.value = '{target}';
                    select.dispatchEvent(new Event('change', {{ bubbles: true }}));
                }}

                return {{
                    ok: true,
                    hidden_value: hidden ? hidden.value : null,
                    select_value: select ? select.value : null
                }};
            }}
            """
        )
        log(f"Results/page set attempt result: {result}")
        wait_short(page, 1000)

        submitted = (
            safe_click(page, "button.filters-submit", "SEARCH BUTTON AFTER 100/PAGE", timeout=8000, force=True)
            or safe_click(page, "input.filters-submit", "SEARCH BUTTON AFTER 100/PAGE", timeout=8000, force=True)
            or safe_click(page, "button[type='submit']", "SEARCH BUTTON AFTER 100/PAGE", timeout=8000, force=True)
        )

        if submitted:
            log("Waiting for Miami search results after 100/page...")
            wait_short(page, 5000)

        rows = page.locator("tr.load-case.table-row.link[data-caseid]").count()
        hidden_per_page = page.locator("#filterCasesPerPage").input_value() if page.locator("#filterCasesPerPage").count() else ""
        page_links_count = page.locator("a").filter(has_text="Page ").count()

        return {
            "ok": True,
            "target": target,
            "rows_on_page": rows,
            "hidden_per_page": hidden_per_page,
            "page_links_count": page_links_count,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


# =========================
# CASE / PAGE HELPERS
# =========================
def get_case_rows(page):
    return page.locator("tr.load-case.table-row.link[data-caseid]")


def get_page_links(page) -> List[str]:
    try:
        links = page.locator("a").filter(has_text="Page ")
        out = []
        for i in range(links.count()):
            txt = clean_text(links.nth(i).inner_text())
            if txt and txt not in out:
                out.append(txt)
        return out
    except Exception:
        return []


def click_page_link_by_text(page, link_text: str) -> bool:
    try:
        page.locator("a").filter(has_text=link_text).first.click(timeout=10000, force=True)
        log(f"Pagination clicked -> {link_text}")
        wait_short(page, 5000)
        return True
    except Exception as e:
        warn(f"Pagination click failed [{link_text}]: {e}")
        return False


# =========================
# CASE DETAIL EXTRACTION
# =========================
def extract_case_detail(page, row_caseid: str, row_text: str) -> Dict[str, Any]:
    """
    Extrai detalhes do case depois do clique na row.
    """
    txt = body_text(page)

    next_labels_summary = [
        "Homestead",
        "Legal Description",
        "Property Address",
        "Sale Date",
        "App Receive Date",
        "Publish Dates",
        "Opening Bid",
        "Redemption Amount",
        "Case Status",
        "Parcel Number",
        "Case Number",
        "Applicant Number",
        "Tax Collector Number",
    ]

    def pick(label: str, nexts: Optional[List[str]] = None) -> str:
        return extract_after_label_block(txt, label, nexts or next_labels_summary)

    parcel_number = pick("Parcel Number")
    case_number = pick("Case Number")
    applicant_number = pick("Applicant Number")
    tax_collector_number = pick("Tax Collector Number")
    case_status = pick("Case Status")
    redemption_amount = pick("Redemption Amount")
    opening_bid = pick("Opening Bid")

    app_receive_date = pick("App Receive Date")
    sale_date = pick("Sale Date")
    publish_dates = pick("Publish Dates", ["Property Address", "Homestead", "Legal Description"])
    property_address = pick("Property Address", ["Homestead", "Legal Description"])
    homestead = pick("Homestead", ["Legal Description"])
    legal_description = pick("Legal Description", ["Case Events", "Case Notes", "Parcel Number", "Property Information"])

    publish_dates_list = []
    if publish_dates:
        for line in publish_dates.split("\n"):
            line = clean_text(line)
            if line:
                publish_dates_list.append(line)

    parcel_link = {"text": "", "href": ""}
    try:
        if parcel_number:
            link = page.locator(f"a[href*='folio=']").filter(has_text=parcel_number).first
            if link.count():
                parcel_link["text"] = clean_text(link.inner_text())
                parcel_link["href"] = clean_text(link.get_attribute("href"))
        if not parcel_link["href"]:
            link2 = page.locator("a[href*='miamidade.gov/Apps/PA/propertysearch']").first
            if link2.count():
                parcel_link["text"] = clean_text(link2.inner_text())
                parcel_link["href"] = clean_text(link2.get_attribute("href"))
    except Exception:
        pass

    detail = {
        "caseid": row_caseid,
        "row_text": row_text,
        "header": {
            "tax_collector_number": tax_collector_number,
            "applicant_number": applicant_number,
            "case_number": case_number,
            "parcel_number": parcel_number,
            "case_status": case_status,
            "redemption_amount": redemption_amount,
            "opening_bid": opening_bid,
        },
        "case_summary": {
            "app_receive_date": app_receive_date,
            "sale_date": sale_date,
            "publish_dates": publish_dates,
            "publish_dates_list": publish_dates_list,
            "property_address": clean_inline(property_address),
            "homestead": homestead,
            "legal_description": clean_inline(legal_description),
        },
        "parcel_link": parcel_link,
        "has_case_summary": True if (property_address or legal_description or app_receive_date) else False,
    }

    log("CASE DETAIL EXTRACTED: " + json.dumps(detail, indent=2, ensure_ascii=False))
    return detail


# =========================
# PROPERTY APPRAISER EXTRACTION
# =========================
def extract_pa_from_page(pa_page) -> Dict[str, Any]:
    txt = body_text(pa_page)

    # regex helpers
    def grab_block(label: str, next_labels: List[str]) -> str:
        return extract_after_label_block(txt, label, next_labels)

    nexts = [
        "Sub-Division",
        "Property Address",
        "Owner",
        "Mailing Address",
        "PA Primary Zone",
        "Primary Land Use",
        "Beds / Baths /Half",
        "Floors",
        "Living Units",
        "Actual Area",
        "Living Area",
        "Adjusted Area",
        "Lot Size",
        "Year Built",
    ]

    folio = ""
    m = re.search(r"Folio[:\s]*([0-9\-]+)", txt, flags=re.IGNORECASE)
    if m:
        folio = clean_text(m.group(1))

    subdivision = grab_block("Sub-Division", ["Property Address", "Owner", "Mailing Address"])
    property_address = grab_block("Property Address", ["Owner", "Mailing Address", "PA Primary Zone"])
    owner = grab_block("Owner", ["Mailing Address", "PA Primary Zone", "Primary Land Use"])
    mailing_address = grab_block("Mailing Address", ["PA Primary Zone", "Primary Land Use", "Beds / Baths /Half"])
    pa_primary_zone = grab_block("PA Primary Zone", ["Primary Land Use", "Beds / Baths /Half", "Floors"])
    primary_land_use = grab_block("Primary Land Use", ["Beds / Baths /Half", "Floors", "Living Units"])
    beds_baths_half = grab_block("Beds / Baths /Half", ["Floors", "Living Units", "Actual Area"])
    floors = grab_block("Floors", ["Living Units", "Actual Area", "Living Area"])
    living_units = grab_block("Living Units", ["Actual Area", "Living Area", "Adjusted Area"])
    actual_area = grab_block("Actual Area", ["Living Area", "Adjusted Area", "Lot Size"])
    living_area = grab_block("Living Area", ["Adjusted Area", "Lot Size", "Year Built"])
    adjusted_area = grab_block("Adjusted Area", ["Lot Size", "Year Built"])
    lot_size = grab_block("Lot Size", ["Year Built"])
    year_built = grab_block("Year Built", [])

    data = {
        "folio": folio,
        "subdivision": clean_inline(subdivision),
        "property_address": clean_inline(property_address),
        "owner": clean_inline(owner),
        "mailing_address": clean_inline(mailing_address),
        "pa_primary_zone": clean_inline(pa_primary_zone),
        "primary_land_use": clean_inline(primary_land_use),
        "beds_baths_half": clean_inline(beds_baths_half),
        "floors": clean_inline(floors),
        "living_units": clean_inline(living_units),
        "actual_area": clean_inline(actual_area),
        "living_area": clean_inline(living_area),
        "adjusted_area": clean_inline(adjusted_area),
        "lot_size": clean_inline(lot_size),
        "year_built": clean_inline(year_built),
    }

    log("PROPERTY APPRAISER EXTRACTED: " + json.dumps(data, indent=2, ensure_ascii=False))
    return data


def open_property_appraiser_in_new_page(context, href: str) -> Dict[str, Any]:
    if not href:
        return {}

    pa_page = context.new_page()
    try:
        log(f"Opening Property Appraiser URL: {href}")
        pa_page.goto(href, wait_until="domcontentloaded", timeout=90000)
        wait_short(pa_page, 6000)
        return extract_pa_from_page(pa_page)
    finally:
        try:
            pa_page.close()
        except Exception:
            pass


# =========================
# RECORD BUILD
# =========================
def build_record(case_detail: Dict[str, Any], pa_data: Dict[str, Any]) -> Dict[str, Any]:
    header = case_detail.get("header", {})
    summary = case_detail.get("case_summary", {})
    parcel_link = case_detail.get("parcel_link", {})

    sale_date = clean_text(summary.get("sale_date"))
    auction_status = "SOON" if (not sale_date or sale_date.lower() == "not assigned") else "SCHEDULED"

    external_id = f"{header.get('case_number', '')}|{header.get('parcel_number', '')}"

    return {
        "source": "MiamiDade",
        "county": "Miami-Dade",
        "visible_in_app": True,

        "auction_status": auction_status,
        "auction_date": None if auction_status == "SOON" else sale_date,
        "auction_time": None,
        "auction_location": "Miami-Dade RealForeclose",
        "auction_url": MIAMI_AUCTION_URL,

        "external_id": external_id,

        "caseid": case_detail.get("caseid", ""),
        "case_number": header.get("case_number", ""),
        "case_status": header.get("case_status", ""),
        "tax_collector_number": header.get("tax_collector_number", ""),
        "application_number": "",
        "applicant_number": header.get("applicant_number", ""),
        "parcel_number": header.get("parcel_number", ""),

        "sale_date": summary.get("sale_date", ""),
        "app_receive_date": summary.get("app_receive_date", ""),
        "publish_dates": summary.get("publish_dates_list", []),
        "redemption_amount": header.get("redemption_amount", ""),
        "opening_bid": header.get("opening_bid", ""),

        "property_address_case": summary.get("property_address", ""),
        "legal_description": summary.get("legal_description", ""),
        "homestead": summary.get("homestead", ""),

        "folio": pa_data.get("folio", ""),
        "subdivision": pa_data.get("subdivision", ""),
        "property_address_pa": pa_data.get("property_address", ""),
        "owner": pa_data.get("owner", ""),
        "mailing_address": pa_data.get("mailing_address", ""),
        "pa_primary_zone": pa_data.get("pa_primary_zone", ""),
        "primary_land_use": pa_data.get("primary_land_use", ""),
        "beds_baths_half": pa_data.get("beds_baths_half", ""),
        "floors": pa_data.get("floors", ""),
        "living_units": pa_data.get("living_units", ""),
        "actual_area": pa_data.get("actual_area", ""),
        "living_area": pa_data.get("living_area", ""),
        "adjusted_area": pa_data.get("adjusted_area", ""),
        "lot_size": pa_data.get("lot_size", ""),
        "year_built": pa_data.get("year_built", ""),

        "parcel_appraiser_url": parcel_link.get("href", ""),
    }


# =========================
# ROW-BY-ROW SCRAPER
# =========================
def scrape_current_page_rows(context, page, supabase_client, max_remaining: int) -> Dict[str, Any]:
    records: List[Dict[str, Any]] = []
    failures: List[Dict[str, str]] = []
    supabase_results: List[Dict[str, Any]] = []

    rows = get_case_rows(page)
    row_count = rows.count()

    log(f"Collecting case rows from current page: {row_count}")

    limit = min(row_count, max_remaining)

    for i in range(limit):
        try:
            # reacquire locator every time
            rows = get_case_rows(page)
            row = rows.nth(i)

            row_caseid = clean_text(row.get_attribute("data-caseid"))
            row_text = clean_inline(row.inner_text())

            log(f"[{i+1}/{limit}] Scraping row index={i} caseid={row_caseid}")

            row.scroll_into_view_if_needed(timeout=8000)
            wait_short(page, 500)

            # clica no item atual
            safe_click_locator(row, f"CASE ROW {row_caseid}", timeout=10000, force=True)
            log("Waiting for case detail to load...")
            wait_short(page, 5000)

            case_detail = extract_case_detail(page, row_caseid, row_text)

            pa_href = clean_text(case_detail.get("parcel_link", {}).get("href"))
            pa_data = open_property_appraiser_in_new_page(context, pa_href) if pa_href else {}

            record = build_record(case_detail, pa_data)
            records.append(record)

            sb = send_to_supabase(supabase_client, record)
            supabase_results.append(sb)

            log(f"SUCCESS CASE {i+1}: {record.get('external_id', '')}")

            # NÃO volta por case_id
            # NÃO reaplica filtro
            # simplesmente segue para próxima row do grid atual
            wait_short(page, 1000)

        except Exception as e:
            msg = str(e)
            err(f"FAILED ROW {i+1}: {msg}")
            traceback.print_exc()
            failures.append(
                {
                    "index": str(i),
                    "error": msg,
                }
            )

            # tenta continuar sem resetar tudo
            wait_short(page, 1500)

    return {
        "records": records,
        "failures": failures,
        "supabase_results": supabase_results,
    }


# =========================
# MAIN
# =========================
def run_miami():
    log("=== MIAMI FINAL STABLE ROW-BY-ROW + SUPABASE ===")

    supabase_client = get_supabase_client()

    final_payload: Dict[str, Any] = {
        "source": "MiamiDade",
        "mode": "final_stable_row_by_row_supabase",
        "auction_url_default": MIAMI_AUCTION_URL,
        "results_per_page_mode": {},
        "page_summary": {},
        "rows_detected": 0,
        "records_count": 0,
        "failures_count": 0,
        "failures": [],
        "supabase_results": [],
        "records": [],
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        try:
            open_miami_and_apply_active_filter(page)

            # tenta 100/page; se o site não respeitar, seguimos com 20/page
            per_page_result = try_set_results_per_page(page, "100")
            final_payload["results_per_page_mode"] = {
                "mode": "100",
                "details": per_page_result,
            }

            rows_detected = get_case_rows(page).count()
            page_links = get_page_links(page)

            final_payload["rows_detected"] = rows_detected
            final_payload["page_summary"] = {
                "rows_on_page": rows_detected,
                "page_links": page_links,
                "hidden_per_page": page.locator("#filterCasesPerPage").input_value()
                if page.locator("#filterCasesPerPage").count()
                else "",
            }

            total_done = 0

            # processa página atual primeiro
            page_result = scrape_current_page_rows(context, page, supabase_client, MAX_LOTS - total_done)
            final_payload["records"].extend(page_result["records"])
            final_payload["failures"].extend(page_result["failures"])
            final_payload["supabase_results"].extend(page_result["supabase_results"])
            total_done = len(final_payload["records"])

            # paginação
            # importantíssimo: SEM resetar filtro e SEM reabrir por case_id
            if total_done < MAX_LOTS and page_links:
                # começa da página 2, pois a atual já foi processada
                for link_text in page_links[1:]:
                    if total_done >= MAX_LOTS:
                        break

                    ok = click_page_link_by_text(page, link_text)
                    if not ok:
                        final_payload["failures"].append({"page": link_text, "error": "pagination click failed"})
                        continue

                    rows_here = get_case_rows(page).count()
                    log(f"Rows on {link_text}: {rows_here}")

                    page_result = scrape_current_page_rows(context, page, supabase_client, MAX_LOTS - total_done)
                    final_payload["records"].extend(page_result["records"])
                    final_payload["failures"].extend(page_result["failures"])
                    final_payload["supabase_results"].extend(page_result["supabase_results"])
                    total_done = len(final_payload["records"])

            final_payload["records_count"] = len(final_payload["records"])
            final_payload["failures_count"] = len(final_payload["failures"])

            log("===== FINAL PAYLOAD =====")
            log(json.dumps(final_payload, indent=2, ensure_ascii=False))

            print("=== Running MiamiDade ===", flush=True)
            print(json.dumps(final_payload, indent=2, ensure_ascii=False), flush=True)

        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass