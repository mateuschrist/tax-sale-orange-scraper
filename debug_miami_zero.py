import json
import logging
import os
import re
from datetime import datetime

from playwright.sync_api import sync_playwright

URL = "https://miamidade.realtdm.com/public/cases/list"
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
OUT_DIR = os.getenv("MIAMI_DEBUG_DIR", "miami_zero_output")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("miami_zero")


# =========================
# UTILS
# =========================

def ensure_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def save(name, content):
    path = os.path.join(OUT_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        if isinstance(content, (dict, list)):
            json.dump(content, f, indent=2, ensure_ascii=False)
        else:
            f.write(content)
    log.info(f"Saved: {path}")


def screenshot(page, name):
    path = os.path.join(OUT_DIR, name)
    page.screenshot(path=path, full_page=True)
    log.info(f"Saved screenshot: {path}")


def wait_long(page, ms=15000):
    page.wait_for_timeout(ms)


def stabilize(page, label="", ms=12000):
    try:
        page.wait_for_load_state("domcontentloaded", timeout=15000)
    except:
        pass

    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except:
        pass

    wait_long(page, ms)
    log.info(f"Stabilized: {label}")


def current_page_meta(page):
    return {
        "url": page.url,
        "title": page.title(),
        "body_sample": page.locator("body").inner_text()[:5000],
    }


# =========================
# CORE STEPS
# =========================

def open_dropdown(page):
    log.info("Opening Case Status...")

    selectors = [
        "#filterButtonStatus",
        'text="Case Status"',
    ]

    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue

            loc.click(force=True)
            stabilize(page, "open_dropdown", 10000)

            visible = page.evaluate("""
            () => {
                const el = document.querySelector('.dropdown-menu.public');
                if (!el) return false;
                const s = window.getComputedStyle(el);
                const r = el.getBoundingClientRect();
                return s.display !== 'none' && r.height > 0;
            }
            """)

            if visible:
                log.info(f"Dropdown OK via {sel}")
                return True

        except Exception as e:
            log.warning(e)

    return False


def select_active(page):
    log.info("Selecting Active...")

    result = page.evaluate("""
    () => {
        const items = Array.from(document.querySelectorAll('a.dropdown-item.statusgroup1'));

        const active = items.find(el =>
            (el.innerText || '').trim() === 'Active'
        );

        if (!active) return {ok:false};

        const menu = active.closest('.dropdown-menu');
        if (menu) {
            menu.style.display = 'block';
            menu.style.visibility = 'visible';
            menu.classList.add('show');
        }

        active.click();

        return {ok:true};
    }
    """)

    stabilize(page, "select_active", 10000)

    return result.get("ok", False)


def click_search(page):
    log.info("Clicking search...")

    selectors = [
        "button.filters-submit",
        'text="Process Search"',
    ]

    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue

            loc.click(force=True)
            stabilize(page, "search", 15000)

            rows = page.locator('tr.load-case.table-row.link[data-caseid]').count()

            if rows > 0:
                log.info(f"Search OK ({rows} rows)")
                return True

        except Exception as e:
            log.warning(e)

    return False


def extract_cases(page):
    rows = page.locator('tr.load-case.table-row.link[data-caseid]')
    count = rows.count()

    cases = []

    for i in range(count):
        text = re.sub(r"\s+", " ", rows.nth(i).inner_text()).strip()
        parts = text.split(" ")

        if len(parts) > 1:
            cases.append(parts[1])

    return {
        "count": count,
        "cases": cases
    }


def next_page(page):
    log.info("Next page...")

    result = page.evaluate("""
    () => {
        const icon = document.querySelector('i.fa-regular.fa-chevron-right');
        if (!icon) return false;

        const parent = icon.closest('a,button,div,span') || icon;
        parent.click();
        return true;
    }
    """)

    stabilize(page, "next_page", 15000)
    return result


# =========================
# MAIN
# =========================

def main():
    ensure_dir()

    with sync_playwright() as p:

        try:
            browser = p.chromium.launch(channel="chrome", headless=HEADLESS)
            log.info("Using Chrome")
        except:
            browser = p.chromium.launch(headless=HEADLESS)
            log.info("Using Chromium fallback")

        page = browser.new_page()

        # LOAD PAGE
        page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        stabilize(page, "initial_load", 15000)

        screenshot(page, "01_initial.png")
        save("01_initial.json", current_page_meta(page))

        # OPEN FILTER
        if not open_dropdown(page):
            log.error("Failed dropdown")
            return

        screenshot(page, "02_dropdown.png")

        # SELECT ACTIVE
        if not select_active(page):
            log.error("Failed Active")
            return

        screenshot(page, "03_active.png")

        # SEARCH
        if not click_search(page):
            log.error("Search failed")
            return

        screenshot(page, "04_results.png")

        # PAGE 1
        page1 = extract_cases(page)
        save("page1.json", page1)

        # NEXT PAGE
        if next_page(page):
            screenshot(page, "05_page2.png")
            page2 = extract_cases(page)
            save("page2.json", page2)

        browser.close()


if __name__ == "__main__":
    main()