"""
MIAMI DEBUG SCRIPT - FINAL LIMPO
Objetivo: descobrir exatamente o que funciona no site hoje
"""

import json
import logging
import re
from datetime import datetime
from playwright.sync_api import sync_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("miami_debug")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"

REPORT = {
    "timestamp": datetime.now().isoformat(),
    "steps": [],
    "success_methods": {},
}

# =====================
# HELPERS
# =====================
def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()

def report_step(step, success, method="", details=""):
    REPORT["steps"].append({
        "step": step,
        "success": success,
        "method": method,
        "details": details,
        "time": datetime.now().isoformat()
    })

    icon = "✅" if success else "❌"
    log.info(f"{icon} {step} | {method} | {details}")

# =====================
# HUMAN SIMULATION
# =====================
def human_behavior(page):
    page.mouse.move(300, 300)
    page.wait_for_timeout(500)
    page.mouse.move(600, 400)
    page.wait_for_timeout(800)
    page.mouse.wheel(0, 800)
    page.wait_for_timeout(800)

# =====================
# INSPECT ELEMENTS
# =====================
def inspect_reset(page):
    data = page.evaluate("""
    () => {
        return Array.from(document.querySelectorAll('a,button'))
        .map(el => ({
            text: (el.innerText||'').trim(),
            class: el.className || '',
            id: el.id || ''
        }))
        .filter(x => x.text.toLowerCase().includes('reset') 
            || x.class.toLowerCase().includes('reset')
            || x.id.toLowerCase().includes('reset'));
    }
    """)
    log.info("RESET FOUND: %s", json.dumps(data, indent=2))

def inspect_pagination(page):
    data = page.evaluate("""
    () => {
        return Array.from(document.querySelectorAll('[data-page],a,button,span'))
        .map(el => ({
            text: (el.innerText||'').trim(),
            data_page: el.getAttribute('data-page') || '',
            class: el.className || ''
        }))
        .filter(x => x.text.includes('Page') || x.data_page);
    }
    """)
    log.info("PAGINATION: %s", json.dumps(data[:30], indent=2))

# =====================
# TESTS
# =====================
def test_page(page):
    try:
        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)
        report_step("Page Load", True, "goto", "OK")
        return True
    except Exception as e:
        report_step("Page Load", False, "goto", str(e))
        return False

def test_reset(page):
    try:
        page.click("a.filters-reset", timeout=5000)
        report_step("Reset", True, "click", "OK")
        return True
    except Exception:
        report_step("Reset", False, "click", "FAILED")
        return False

def test_filter(page):
    try:
        page.click("#filterButtonStatus", timeout=5000)
        report_step("Filter Button", True, "click", "OK")
        return True
    except Exception as e:
        report_step("Filter Button", False, "click", str(e))
        return False

def force_status(page):
    try:
        page.evaluate("""
        () => {
            const el = document.querySelector('[data-statusid="192"]');
            if (el) el.click();
        }
        """)
        report_step("Select Status 192", True, "js", "OK")
        return True
    except Exception as e:
        report_step("Select Status 192", False, "js", str(e))
        return False

def test_search(page):
    selectors = [
        "button.filters-submit",
        "text=Search"
    ]

    for sel in selectors:
        try:
            page.locator(sel).first.click(timeout=5000)
            page.wait_for_timeout(3000)
            report_step("Search", True, sel, "OK")
            return True
        except:
            continue

    report_step("Search", False, "all", "FAILED")
    return False

def test_results(page):
    try:
        page.wait_for_selector("tr.load-case", timeout=15000)
        rows = page.locator("tr.load-case").count()
        report_step("Results", True, "selector", f"{rows} rows")
        return True
    except Exception as e:
        report_step("Results", False, "selector", str(e))
        return False

def test_open_case(page):
    try:
        row = page.locator("tr.load-case").first
        row.click()
        page.wait_for_timeout(5000)
        report_step("Open Case", True, "click", "OK")
        return True
    except Exception as e:
        report_step("Open Case", False, "click", str(e))
        return False

# =====================
# MAIN
# =====================
def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chrome",
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )

        context = browser.new_context(
            user_agent="Mozilla/5.0",
            viewport={"width": 1366, "height": 900}
        )

        page = context.new_page()

        if not test_page(page):
            return

        human_behavior(page)
        inspect_reset(page)

        reset_ok = test_reset(page)
        if not reset_ok:
            log.warning("RESET FAILED — continuing")

        test_filter(page)
        force_status(page)
        test_search(page)

        if test_results(page):
            inspect_pagination(page)
            test_open_case(page)

        browser.close()

    with open("debug.json", "w") as f:
        json.dump(REPORT, f, indent=2)

    log.info("DONE - check debug.json")


if __name__ == "__main__":
    main()
