"""
MIAMI DEBUG SCRIPT - CORRIGIDO
Objetivo: descobrir exatamente o que funciona no site hoje
"""

import json
import logging
import os
import re
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("miami_debug")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"

HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"

REPORT = {
    "timestamp": datetime.now().isoformat(),
    "steps": [],
    "success_methods": {},
    "page_info": {},
    "dom_inspection": {},
    "samples": {},
}

# =====================
# HELPERS
# =====================
def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def report_step(step, success, method="", details="", data=None):
    REPORT["steps"].append({
        "step": step,
        "success": success,
        "method": method,
        "details": details,
        "time": datetime.now().isoformat(),
        "data": data or {},
    })

    if success and method:
        REPORT["success_methods"].setdefault(step, []).append(method)

    icon = "✅" if success else "❌"
    log.info("%s %s | %s | %s", icon, step, method, details)


def safe_screenshot(page, name):
    try:
        page.screenshot(path=name, full_page=True)
        log.info("Screenshot saved: %s", name)
    except Exception as e:
        log.warning("Could not save screenshot %s: %s", name, str(e))


def human_behavior(page):
    try:
        page.mouse.move(200, 200)
        page.wait_for_timeout(400)
        page.mouse.move(500, 350)
        page.wait_for_timeout(600)
        page.mouse.wheel(0, 500)
        page.wait_for_timeout(800)
        page.mouse.wheel(0, -250)
        page.wait_for_timeout(500)
    except Exception:
        pass


def click_with_methods(page, selector, step_name, screenshot_prefix, timeout=6000):
    methods = [
        ("page.click", lambda: page.click(selector, timeout=timeout)),
        ("locator.click", lambda: page.locator(selector).first.click(timeout=timeout)),
        ("locator.click(force=True)", lambda: page.locator(selector).first.click(force=True, timeout=timeout)),
        ("js click", lambda: page.evaluate(
            """(sel) => {
                const el = document.querySelector(sel);
                if (!el) throw new Error(`not found: ${sel}`);
                el.click();
            }""",
            selector
        )),
    ]

    for method_name, fn in methods:
        try:
            fn()
            page.wait_for_timeout(1200)
            safe_screenshot(page, f"{screenshot_prefix}_{method_name.replace('/', '_').replace(' ', '_')}.png")
            report_step(step_name, True, method_name, f"selector={selector}")
            return True
        except Exception as e:
            report_step(step_name, False, method_name, str(e))

    return False


# =====================
# INSPECTION
# =====================
def inspect_basic_page_info(page):
    try:
        title = page.title()
    except Exception:
        title = ""

    try:
        body_text = page.locator("body").inner_text(timeout=5000)
    except Exception:
        body_text = ""

    REPORT["page_info"] = {
        "url": page.url,
        "title": title,
        "body_sample": body_text[:3000],
    }

    log.info("PAGE URL: %s", page.url)
    log.info("PAGE TITLE: %s", title)
    log.info("BODY SAMPLE: %s", body_text[:500])


def inspect_reset_candidates(page):
    try:
        data = page.evaluate("""
        () => {
            return Array.from(document.querySelectorAll('a,button,span,div'))
            .map(el => ({
                text: (el.innerText || '').trim(),
                class: el.className || '',
                id: el.id || '',
                href: el.getAttribute('href') || '',
                onclick: el.getAttribute('onclick') || ''
            }))
            .filter(x =>
                x.text.toLowerCase().includes('reset') ||
                x.class.toLowerCase().includes('reset') ||
                x.id.toLowerCase().includes('reset')
            )
            .slice(0, 50);
        }
        """)
        REPORT["dom_inspection"]["reset_candidates"] = data
        log.info("RESET CANDIDATES: %s", json.dumps(data, indent=2))
    except Exception as e:
        log.warning("inspect_reset_candidates failed: %s", str(e))


def inspect_filter_candidates(page):
    try:
        data = page.evaluate("""
        () => {
            return Array.from(document.querySelectorAll('a,button,span,div'))
            .map(el => ({
                text: (el.innerText || '').trim(),
                class: el.className || '',
                id: el.id || '',
                data_target: el.getAttribute('data-target') || '',
                onclick: el.getAttribute('onclick') || ''
            }))
            .filter(x =>
                x.text.toLowerCase().includes('status') ||
                x.id.toLowerCase().includes('status') ||
                x.class.toLowerCase().includes('status') ||
                x.data_target.toLowerCase().includes('status')
            )
            .slice(0, 80);
        }
        """)
        REPORT["dom_inspection"]["filter_candidates"] = data
        log.info("FILTER CANDIDATES: %s", json.dumps(data, indent=2))
    except Exception as e:
        log.warning("inspect_filter_candidates failed: %s", str(e))


def inspect_search_candidates(page):
    try:
        data = page.evaluate("""
        () => {
            return Array.from(document.querySelectorAll('button,a,input[type="submit"],input[type="button"]'))
            .map(el => ({
                tag: el.tagName.toLowerCase(),
                text: (el.innerText || el.value || '').trim(),
                class: el.className || '',
                id: el.id || '',
                type: el.getAttribute('type') || ''
            }))
            .filter(x =>
                x.text.toLowerCase().includes('search') ||
                x.class.toLowerCase().includes('search') ||
                x.id.toLowerCase().includes('search')
            )
            .slice(0, 50);
        }
        """)
        REPORT["dom_inspection"]["search_candidates"] = data
        log.info("SEARCH CANDIDATES: %s", json.dumps(data, indent=2))
    except Exception as e:
        log.warning("inspect_search_candidates failed: %s", str(e))


def inspect_pagination(page):
    try:
        data = page.evaluate("""
        () => {
            return Array.from(document.querySelectorAll('[data-page],a,button,span,div,td,li'))
            .map(el => ({
                text: (el.innerText || '').trim(),
                data_page: el.getAttribute('data-page') || '',
                class: el.className || '',
                id: el.id || ''
            }))
            .filter(x => x.text.includes('Page') || x.data_page)
            .slice(0, 100);
        }
        """)
        REPORT["dom_inspection"]["pagination"] = data
        log.info("PAGINATION CANDIDATES: %s", json.dumps(data, indent=2))
    except Exception as e:
        log.warning("inspect_pagination failed: %s", str(e))


# =====================
# TESTS
# =====================
def test_page_load(page):
    try:
        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_load_state("networkidle", timeout=15000)
        page.wait_for_timeout(5000)

        inspect_basic_page_info(page)
        safe_screenshot(page, "debug_01_page_loaded.png")

        report_step("Page Load", True, "goto + networkidle", "Página carregada")
        return True
    except PlaywrightTimeoutError as e:
        safe_screenshot(page, "debug_01_page_timeout.png")
        report_step("Page Load", False, "goto + networkidle", f"Timeout: {str(e)}")
        return False
    except Exception as e:
        safe_screenshot(page, "debug_01_page_error.png")
        report_step("Page Load", False, "goto", str(e))
        return False


def test_reset_filters(page):
    log.info("=" * 80)
    log.info("TEST RESET FILTERS")
    log.info("=" * 80)

    inspect_reset_candidates(page)
    return click_with_methods(page, "a.filters-reset", "Reset Filters", "debug_02_reset")


def test_filter_button(page):
    log.info("=" * 80)
    log.info("TEST FILTER BUTTON")
    log.info("=" * 80)

    inspect_filter_candidates(page)

    selectors = [
        "#filterButtonStatus",
        '[data-target="#filterStatus"]',
        'text=Status',
    ]

    for sel in selectors:
        ok = click_with_methods(page, sel, "Filter Button", "debug_03_filter")
        if ok:
            return True

    return False


def test_clear_statuses(page):
    try:
        page.evaluate("""
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
            }

            const hidden = document.querySelector('#filterCaseStatus');
            if (hidden) hidden.value = '';

            const label = document.querySelector('#filterCaseStatusLabel');
            if (label) label.innerText = 'None Selected';
        }
        """)
        page.wait_for_timeout(700)
        safe_screenshot(page, "debug_04_clear_statuses.png")
        report_step("Clear Statuses", True, "page.evaluate", "Statuses limpos")
        return True
    except Exception as e:
        report_step("Clear Statuses", False, "page.evaluate", str(e))
        return False


def test_select_status_192(page):
    try:
        page.evaluate("""
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
            }

            const hidden = document.querySelector('#filterCaseStatus');
            if (hidden) hidden.value = '192';

            const label = document.querySelector('#filterCaseStatusLabel');
            if (label) label.innerText = '1 Selected';
        }
        """)
        page.wait_for_timeout(1000)

        state = page.evaluate("""
        () => {
            const hidden = document.querySelector('#filterCaseStatus');
            const label = document.querySelector('#filterCaseStatusLabel');
            return {
                hidden_value: hidden ? hidden.value : '',
                label: label ? label.innerText.trim() : ''
            };
        }
        """)

        safe_screenshot(page, "debug_05_select_192.png")
        report_step("Select Status 192", True, "page.evaluate", "Status 192 selecionado", state)
        return True
    except Exception as e:
        report_step("Select Status 192", False, "page.evaluate", str(e))
        return False


def test_click_search(page):
    log.info("=" * 80)
    log.info("TEST SEARCH BUTTON")
    log.info("=" * 80)

    inspect_search_candidates(page)

    selectors = [
        "button.filters-submit",
        "button:has-text('Search')",
        "text=Search",
    ]

    for sel in selectors:
        try:
            locator = page.locator(sel).first
            if locator.count() == 0:
                continue

            try:
                with page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
                    locator.click(timeout=6000)
                page.wait_for_timeout(3000)
                safe_screenshot(page, "debug_06_search_navigation.png")
                report_step("Click Search", True, f"click + navigation | {sel}", "Search executado")
                return True
            except Exception:
                pass

            try:
                locator.click(timeout=6000)
                page.wait_for_timeout(3000)
                safe_screenshot(page, "debug_06_search_click.png")
                report_step("Click Search", True, f"click | {sel}", "Search executado")
                return True
            except Exception:
                pass

            try:
                locator.click(force=True, timeout=6000)
                page.wait_for_timeout(3000)
                safe_screenshot(page, "debug_06_search_force.png")
                report_step("Click Search", True, f"force click | {sel}", "Search executado")
                return True
            except Exception:
                pass

        except Exception as e:
            report_step("Click Search", False, sel, str(e))

    report_step("Click Search", False, "all selectors", "Nenhum método funcionou")
    return False


def test_wait_for_results(page):
    try:
        waited = 0
        while waited < 25000:
            rows = page.locator('tr.load-case.table-row.link[data-caseid]').count()
            if rows > 0:
                samples = []
                for i in range(min(rows, 5)):
                    try:
                        row = page.locator('tr.load-case.table-row.link[data-caseid]').nth(i)
                        samples.append({
                            "caseid": row.get_attribute("data-caseid") or "",
                            "row_text": clean_text(row.inner_text()),
                        })
                    except Exception:
                        pass

                REPORT["samples"]["rows"] = samples
                safe_screenshot(page, "debug_07_results.png")
                report_step("Wait Results", True, "poll rows", f"{rows} rows encontrados", {"rows": rows})
                return True

            page.wait_for_timeout(1000)
            waited += 1000

        report_step("Wait Results", False, "poll rows", "Timeout sem rows")
        return False
    except Exception as e:
        report_step("Wait Results", False, "poll rows", str(e))
        return False


def test_pagination_info(page):
    try:
        inspect_pagination(page)

        data = page.evaluate("""
        () => {
            const body = document.body.innerText || '';
            const m = body.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);
            return {
                current_page: m ? parseInt(m[1], 10) : 1,
                total_pages: m ? parseInt(m[2], 10) : 1
            };
        }
        """)

        REPORT["samples"]["pagination_info"] = data
        report_step("Pagination Info", True, "page.evaluate", f"Page {data['current_page']}/{data['total_pages']}", data)
        return True
    except Exception as e:
        report_step("Pagination Info", False, "page.evaluate", str(e))
        return False


def test_open_case_detail(page):
    try:
        row = page.locator('tr.load-case.table-row.link[data-caseid]').first
        if row.count() == 0:
            report_step("Open Case Detail", False, "locator", "Nenhuma row encontrada")
            return False

        caseid = row.get_attribute("data-caseid") or ""

        try:
            row.click(timeout=6000)
            method = "row.click"
        except Exception:
            try:
                row.click(force=True, timeout=6000)
                method = "row.click(force=True)"
            except Exception:
                page.evaluate("(el) => el.click()", row.element_handle())
                method = "page.evaluate(el.click)"

        page.wait_for_timeout(7000)
        safe_screenshot(page, "debug_09_case_detail.png")
        report_step("Open Case Detail", True, method, f"caseid={caseid}")
        return True
    except Exception as e:
        report_step("Open Case Detail", False, "all methods", str(e))
        return False


def test_parse_case_detail(page):
    try:
        data = page.evaluate("""
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

            return {
                case_number: byLabel('Case Number'),
                parcel_number: byLabel('Parcel Number'),
                case_status: byLabel('Case Status'),
                sale_date: byLabel('Sale Date'),
                property_address: byLabel('Property Address'),
                raw_sample: bodyText.slice(0, 3000)
            };
        }
        """)

        REPORT["samples"]["case_detail"] = data
        safe_screenshot(page, "debug_10_case_parsed.png")
        report_step("Parse Case Detail", True, "page.evaluate", "Detalhe parseado", data)
        return True
    except Exception as e:
        report_step("Parse Case Detail", False, "page.evaluate", str(e))
        return False


# =====================
# MAIN
# =====================
def main():
    log.info("INICIANDO DEBUG MIAMI-DADE")

    with sync_playwright() as p:
        browser = None
        try:
            # tenta Chrome real primeiro, igual ao scraper
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
                log.info("Browser launched with channel=chrome")
            except Exception as e:
                log.warning("Could not launch channel=chrome: %s", str(e))
                browser = p.chromium.launch(
                    headless=HEADLESS,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                    ],
                )
                log.info("Browser launched with default chromium")

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

            ok = test_page_load(page)
            if not ok:
                return

            human_behavior(page)

            reset_ok = test_reset_filters(page)
            if not reset_ok:
                log.warning("RESET FILTERS falhou; continuando sem reset")

            filter_ok = test_filter_button(page)
            if not filter_ok:
                log.warning("FILTER BUTTON falhou; os próximos passos podem não funcionar")

            test_clear_statuses(page)
            test_select_status_192(page)
            test_click_search(page)

            if test_wait_for_results(page):
                test_pagination_info(page)
                if test_open_case_detail(page):
                    test_parse_case_detail(page)

        finally:
            if browser:
                browser.close()

    with open("debug_miami_report.json", "w", encoding="utf-8") as f:
        json.dump(REPORT, f, indent=2, ensure_ascii=False)

    log.info("Relatório salvo em debug_miami_report.json")


if __name__ == "__main__":
    main()