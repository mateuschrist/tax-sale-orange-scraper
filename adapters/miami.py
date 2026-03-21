import json
import logging
import re
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

log = logging.getLogger("miami")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"


def wait_network_quiet(page, timeout=15000):
    try:
        page.wait_for_load_state("networkidle", timeout=timeout)
    except PWTimeout:
        pass


def debug_dom_snapshot(page):
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

    snippet = re.sub(r"\s+", " ", html[:5000])

    data = page.evaluate(
        """
        () => {
            const buttons = Array.from(document.querySelectorAll("button")).map(el => ({
                text: (el.innerText || "").trim(),
                id: el.id || "",
                cls: el.className || "",
                name: el.getAttribute("name") || "",
                type: el.getAttribute("type") || ""
            }));

            const links = Array.from(document.querySelectorAll("a")).map(el => ({
                text: (el.innerText || "").trim(),
                id: el.id || "",
                cls: el.className || "",
                href: el.getAttribute("href") || "",
                statusid: el.getAttribute("data-statusid") || "",
                parentid: el.getAttribute("data-parentid") || ""
            }));

            const rows = Array.from(document.querySelectorAll("tr")).map(el => ({
                text: (el.innerText || "").trim().slice(0, 200),
                id: el.id || "",
                cls: el.className || "",
                role: el.getAttribute("role") || "",
                caseid: el.getAttribute("data-caseid") || ""
            }));

            return {
                counts: {
                    buttons: buttons.length,
                    links: links.length,
                    rows: rows.length
                },
                exists: {
                    filterButtonStatus: !!document.querySelector("#filterButtonStatus"),
                    caseStatus2: !!document.querySelector("#caseStatus2"),
                    activeChild: !!document.querySelector('a[data-statusid="192"][data-parentid="2"]'),
                    filtersSubmit: !!document.querySelector("button.filters-submit"),
                    propertyAppraiserLink: !!document.querySelector("#propertyAppraiserLink"),
                    caseRows: document.querySelectorAll("tr.load-case.table-row.link[data-caseid]").length
                },
                first_buttons: buttons.slice(0, 30),
                first_links: links.slice(0, 40),
                first_rows: rows.slice(0, 30)
            };
        }
        """
    )

    result = {
        "title": title,
        "url": url,
        "html_snippet": snippet,
        "dom_scan": data,
    }

    log.info("===== MIAMI TEST SNAPSHOT =====")
    log.info("TITLE: %s", title)
    log.info("URL: %s", url)
    log.info("HTML_SNIPPET: %s", snippet[:1500])
    log.info("DOM_COUNTS: %s", data["counts"])
    log.info("DOM_EXISTS: %s", data["exists"])
    log.info("FIRST_BUTTONS: %s", data["first_buttons"])
    log.info("FIRST_LINKS: %s", data["first_links"])
    log.info("FIRST_ROWS: %s", data["first_rows"])

    print(json.dumps(result, indent=2, ensure_ascii=False))


def run_miami():
    log.info("=== MIAMI TEST ONLY MODE ===")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=30000)
        wait_network_quiet(page, 15000)
        page.wait_for_timeout(8000)

        debug_dom_snapshot(page)

        browser.close()
