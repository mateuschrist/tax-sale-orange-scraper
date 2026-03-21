import logging
import time
from playwright.sync_api import sync_playwright

log = logging.getLogger("miami")

URL = "https://miamidade.realtdm.com/public/cases/list"


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


def wait_after_search(page):
    log.info("Sleeping 40 seconds to let results load...")
    page.wait_for_timeout(40000)

    try:
        page.wait_for_load_state("domcontentloaded", timeout=10000)
    except Exception:
        pass

    try:
        page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass

    page.wait_for_timeout(3000)


def run_filters(page):
    log.info("Running Miami filters...")

    page.wait_for_timeout(8000)

    if not click_safe(page, "#filterButtonStatus", "FILTER BUTTON"):
        raise RuntimeError("Could not click filter button")

    page.wait_for_timeout(2000)

    if not click_safe(page, "#caseStatus2", "ACTIVE PARENT"):
        raise RuntimeError("Could not click active parent")

    page.wait_for_timeout(1500)

    if not click_safe(page, 'a[data-statusid="192"][data-parentid="2"]', "ACTIVE CHILD"):
        raise RuntimeError("Could not click active child")

    page.wait_for_timeout(1500)

    if not click_safe(page, "button.filters-submit", "SEARCH BUTTON"):
        raise RuntimeError("Could not click search button")

    wait_after_search(page)


def extract_cases(page):
    log.info("Extracting cases...")

    rows = page.locator('tr.load-case.table-row.link[data-caseid]')
    count = rows.count()

    log.info(f"FOUND {count} CASES")

    for i in range(min(count, 10)):
        row = rows.nth(i)

        try:
            caseid = row.get_attribute("data-caseid")
        except Exception:
            caseid = None

        try:
            text = row.inner_text().strip().replace("\n", " ")
        except Exception:
            text = ""

        log.info(f"CASE {i + 1}: caseid={caseid} text={text}")


def run_miami():
    log.info("=== MIAMI V5.1 HUMAN WAIT 40S ===")

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
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="America/New_York",
        )

        page = context.new_page()

        page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(10000)

        run_filters(page)
        extract_cases(page)

        browser.close()