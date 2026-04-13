import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict

from playwright.sync_api import sync_playwright

URL = "https://miamidade.realtdm.com/public/cases/list"
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
OUT_DIR = os.getenv("MIAMI_DEBUG_DIR", "miami_zero_output")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("miami_zero")


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


def current_page_meta(page):
    return {
        "url": page.url,
        "title": page.title(),
        "body_sample": page.locator("body").inner_text()[:5000],
    }


def open_dropdown(page):
    log.info("Opening Case Status dropdown...")

    selectors = [
        "#filterButtonStatus",
        'text="Case Status"',
        ".filter-bar"
    ]

    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue

            loc.click(force=True)
            page.wait_for_timeout(1500)

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
                log.info(f"Dropdown aberto com: {sel}")
                return True

        except Exception as e:
            log.warning(e)

    return False


def select_active(page):
    log.info("Selecting ACTIVE...")

    selectors = [
        'a.dropdown-item.statusgroup1:has-text("Active")',
        'text="Active"'
    ]

    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue

            loc.click(force=True)
            page.wait_for_timeout(1500)

            state = page.evaluate("""
            () => {
                const label = document.querySelector('#filterCaseStatusLabel');
                return label ? label.innerText.trim() : '';
            }
            """)

            if "Active" in state:
                log.info(f"ACTIVE selecionado com {sel}")
                return True

        except Exception as e:
            log.warning(e)

    return False


def click_search(page):
    log.info("Clicking Process Search...")

    selectors = [
        "button.filters-submit",
        'text="Process Search"'
    ]

    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue

            loc.click(force=True)
            page.wait_for_timeout(6000)

            rows = page.locator('tr.load-case.table-row.link[data-caseid]').count()

            if rows > 0:
                log.info(f"Search OK com {sel} ({rows} rows)")
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
    log.info("Trying next page...")

    try:
        result = page.evaluate("""
        () => {
            const icon = document.querySelector('i.fa-regular.fa-chevron-right');
            if (!icon) return false;

            const parent = icon.closest('a,button,div,span') || icon;
            parent.click();
            return true;
        }
        """)

        page.wait_for_timeout(4000)
        return result

    except Exception as e:
        log.warning(e)
        return False


def main():
    ensure_dir()

    report = {
        "started": datetime.utcnow().isoformat()
    }

    with sync_playwright() as p:

        try:
            browser = p.chromium.launch(channel="chrome", headless=HEADLESS)
            log.info("Using Google Chrome")
        except:
            browser = p.chromium.launch(headless=HEADLESS)
            log.info("Using Chromium fallback")

        page = browser.new_page()

        page.goto(URL)
        page.wait_for_timeout(5000)

        screenshot(page, "01_initial.png")
        save("01_initial.json", current_page_meta(page))

        if not open_dropdown(page):
            log.error("Failed to open dropdown")
            return

        screenshot(page, "02_dropdown.png")

        if not select_active(page):
            log.error("Failed to select Active")
            return

        screenshot(page, "03_active.png")

        if not click_search(page):
            log.error("Failed search")
            return

        screenshot(page, "04_results.png")

        page1 = extract_cases(page)
        save("page1.json", page1)

        if next_page(page):
            screenshot(page, "05_page2.png")
            page2 = extract_cases(page)
            save("page2.json", page2)

        browser.close()

    save("report.json", report)


if __name__ == "__main__":
    main()