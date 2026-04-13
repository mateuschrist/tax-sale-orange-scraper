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


def ensure_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def save(name, content):
    path = os.path.join(OUT_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        if isinstance(content, (dict, list)):
            json.dump(content, f, indent=2, ensure_ascii=False)
        else:
            f.write(content)
    log.info("Saved: %s", path)


def screenshot(page, name):
    path = os.path.join(OUT_DIR, name)
    page.screenshot(path=path, full_page=True)
    log.info("Saved screenshot: %s", path)


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


def current_page_meta(page):
    try:
        body = page.locator("body").inner_text(timeout=5000)
    except Exception:
        body = ""

    return {
        "url": page.url,
        "title": page.title(),
        "body_sample": body[:5000],
    }


def get_filter_label(page):
    try:
        return page.locator("#filterCaseStatusLabel").inner_text(timeout=3000).strip()
    except Exception:
        return ""


def open_dropdown(page):
    log.info("Opening Case Status...")

    selectors = [
        "#filterButtonStatus",
        'text="Case Status"',
        'text="Select One or More Statuses..."',
    ]

    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue

            loc.click(force=True)
            stabilize(page, f"open_dropdown_{sel}", 10000)

            visible = page.evaluate(
                """
                () => {
                    const menus = Array.from(document.querySelectorAll('.dropdown-menu.public, .dropdown-menu'));
                    return menus.some(el => {
                        const s = window.getComputedStyle(el);
                        const r = el.getBoundingClientRect();
                        return s.display !== 'none' && s.visibility !== 'hidden' && r.height > 0;
                    });
                }
                """
            )

            if visible:
                log.info("Dropdown OK via %s", sel)
                return True

        except Exception as e:
            log.warning("open_dropdown %s -> %s", sel, e)

    return False


def select_exact_active(page):
    log.info("Selecting exact Active...")

    result = page.evaluate(
        """
        () => {
            function txt(el) {
                return ((el.innerText || el.textContent || '').replace(/\\s+/g, ' ')).trim();
            }

            const menus = Array.from(document.querySelectorAll('.dropdown-menu.public, .dropdown-menu'));
            menus.forEach(menu => {
                menu.style.display = 'block';
                menu.style.visibility = 'visible';
                menu.style.opacity = '1';
                menu.classList.add('show');
            });

            const anchors = Array.from(document.querySelectorAll('a.dropdown-item, a, li, div, span'));

            const exact = anchors.find(el => txt(el) === 'Active');

            if (!exact) {
                return { ok: false, reason: 'exact Active not found' };
            }

            try { exact.scrollIntoView({ block: 'center' }); } catch(e) {}
            try { exact.click(); } catch(e) {}
            try { exact.dispatchEvent(new MouseEvent('mouseover', { bubbles:true })); } catch(e) {}
            try { exact.dispatchEvent(new MouseEvent('mousedown', { bubbles:true })); } catch(e) {}
            try { exact.dispatchEvent(new MouseEvent('mouseup', { bubbles:true })); } catch(e) {}
            try { exact.dispatchEvent(new MouseEvent('click', { bubbles:true })); } catch(e) {}

            return {
                ok: true,
                clicked_text: txt(exact),
                class_name: (exact.className || '').toString()
            };
        }
        """
    )

    stabilize(page, "select_exact_active", 10000)

    label = get_filter_label(page)

    return {
        "ok": result.get("ok", False),
        "result": result,
        "label": label,
    }


def click_search(page):
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

            rows = page.locator('tr.load-case.table-row.link[data-caseid]').count()
            body = page.locator("body").inner_text(timeout=5000)

            if rows > 0:
                log.info("Search OK with %s (%s rows)", sel, rows)
                return {"ok": True, "selector": sel, "rows": rows, "body_sample": body[:1000]}

            return {"ok": False, "selector": sel, "rows": rows, "body_sample": body[:1000]}

        except Exception as e:
            log.warning("click_search %s -> %s", sel, e)

    return {"ok": False, "selector": None, "rows": 0}


def extract_cases(page):
    rows = page.locator('tr.load-case.table-row.link[data-caseid]')
    count = rows.count()

    cases = []
    rows_text = []

    for i in range(count):
        text = re.sub(r"\s+", " ", rows.nth(i).inner_text()).strip()
        rows_text.append(text)

        parts = re.split(r"\s{2,}|\t", text)
        parts = [p.strip() for p in parts if p.strip()]
        if len(parts) >= 2:
            cases.append(parts[1])

    return {
        "count": count,
        "cases": cases,
        "rows_sample": rows_text[:20],
    }


def next_page(page):
    log.info("Trying next page...")

    before = extract_cases(page)

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

    stabilize(page, "next_page", 15000)
    after = extract_cases(page)

    changed = after["cases"] != before["cases"] and after["count"] > 0

    return {
        "ok": bool(result.get("ok")) and changed,
        "click_result": result,
        "before": before,
        "after": after,
    }


def main():
    ensure_dir()

    report = {
        "started": datetime.utcnow().isoformat()
    }

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(channel="chrome", headless=HEADLESS)
            log.info("Using Chrome")
        except Exception:
            browser = p.chromium.launch(headless=HEADLESS)
            log.info("Using Chromium fallback")

        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="America/New_York",
        )

        page = context.new_page()

        page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        stabilize(page, "initial_load", 15000)

        screenshot(page, "01_initial.png")
        save("01_initial.json", current_page_meta(page))

        opened = open_dropdown(page)
        report["dropdown_opened"] = opened
        screenshot(page, "02_dropdown.png")

        if not opened:
            report["error"] = "Failed to open dropdown"
            save("report.json", report)
            browser.close()
            return

        active = select_exact_active(page)
        report["select_exact_active"] = active
        screenshot(page, "03_active.png")
        save("03_active.json", active)

        search = click_search(page)
        report["search"] = search
        screenshot(page, "04_results.png")
        save("04_results.json", current_page_meta(page))

        if search.get("ok"):
            page1 = extract_cases(page)
            report["page1"] = page1
            save("page1.json", page1)

            page2 = next_page(page)
            report["page2_attempt"] = page2
            save("page2_attempt.json", page2)

            if page2.get("ok"):
                screenshot(page, "05_page2.png")
                save("page2.json", page2["after"])

        browser.close()

    report["finished"] = datetime.utcnow().isoformat()
    save("report.json", report)


if __name__ == "__main__":
    main()
