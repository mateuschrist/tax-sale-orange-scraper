import json
import logging
import re
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


def debug_snapshot(page, label):
    try:
        title = page.title()
    except Exception:
        title = ""

    try:
        url = page.url
    except Exception:
        url = ""

    try:
        html = page.content()
    except Exception:
        html = ""

    snippet = re.sub(r"\s+", " ", html[:7000])

    data = page.evaluate(
        """
        () => {
            const rows = Array.from(document.querySelectorAll("tr")).map(el => ({
                text: (el.innerText || "").trim().slice(0, 250),
                id: el.id || "",
                cls: el.className || "",
                role: el.getAttribute("role") || "",
                caseid: el.getAttribute("data-caseid") || ""
            }));

            const links = Array.from(document.querySelectorAll("a")).map(el => ({
                text: (el.innerText || "").trim().slice(0, 200),
                id: el.id || "",
                cls: el.className || "",
                href: el.getAttribute("href") || "",
                statusid: el.getAttribute("data-statusid") || "",
                parentid: el.getAttribute("data-parentid") || ""
            }));

            const tables = Array.from(document.querySelectorAll("table")).map(el => ({
                id: el.id || "",
                cls: el.className || ""
            }));

            const iframes = Array.from(document.querySelectorAll("iframe")).map(el => ({
                id: el.id || "",
                cls: el.className || "",
                src: el.getAttribute("src") || ""
            }));

            const caseRows = Array.from(document.querySelectorAll("[data-caseid]")).map(el => ({
                tag: el.tagName.toLowerCase(),
                text: (el.innerText || "").trim().slice(0, 250),
                id: el.id || "",
                cls: el.className || "",
                caseid: el.getAttribute("data-caseid") || ""
            }));

            return {
                counts: {
                    tables: document.querySelectorAll("table").length,
                    rows: document.querySelectorAll("tr").length,
                    links: document.querySelectorAll("a").length,
                    buttons: document.querySelectorAll("button").length,
                    iframes: document.querySelectorAll("iframe").length,
                    caseid_nodes: document.querySelectorAll("[data-caseid]").length
                },
                exists: {
                    filterButtonStatus: !!document.querySelector("#filterButtonStatus"),
                    caseStatus2: !!document.querySelector("#caseStatus2"),
                    activeChild: !!document.querySelector('a[data-statusid="192"][data-parentid="2"]'),
                    filtersSubmit: !!document.querySelector("button.filters-submit"),
                    propertyAppraiserLink: !!document.querySelector("#propertyAppraiserLink"),
                    caseRowsExact: document.querySelectorAll('tr.load-case.table-row.link[data-caseid]').length
                },
                first_tables: tables.slice(0, 20),
                first_rows: rows.slice(0, 30),
                first_links: links.slice(0, 40),
                first_iframes: iframes.slice(0, 10),
                case_rows: caseRows.slice(0, 30)
            };
        }
        """
    )

    result = {
        "label": label,
        "title": title,
        "url": url,
        "html_snippet": snippet,
        "dom_scan": data,
    }

    log.info("===== %s =====", label)
    log.info("TITLE: %s", title)
    log.info("URL: %s", url)
    log.info("HTML_SNIPPET: %s", snippet[:2000])
    log.info("DOM_COUNTS: %s", data["counts"])
    log.info("DOM_EXISTS: %s", data["exists"])
    log.info("FIRST_TABLES: %s", data["first_tables"])
    log.info("FIRST_ROWS: %s", data["first_rows"])
    log.info("FIRST_LINKS: %s", data["first_links"])
    log.info("FIRST_IFRAMES: %s", data["first_iframes"])
    log.info("CASE_ROWS: %s", data["case_rows"])

    print(json.dumps(result, indent=2, ensure_ascii=False))


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

    log.info("Sleeping 40 seconds to let results load...")
    page.wait_for_timeout(40000)


def run_miami():
    log.info("=== MIAMI POST-SEARCH TEST MODE ===")

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

        debug_snapshot(page, "MIAMI BEFORE SEARCH")

        run_filters(page)

        debug_snapshot(page, "MIAMI AFTER SEARCH")

        browser.close()