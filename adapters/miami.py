import json
import logging
import re
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

log = logging.getLogger("miami-test")

LIST_URL = "https://miamidade.realtdm.com/public/cases/list"


def wait_network_quiet(page, timeout=12000):
    try:
        page.wait_for_load_state("networkidle", timeout=timeout)
    except PWTimeout:
        pass


def run_miami_test():
    log.info("=== MIAMI TEST MODE ===")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=30000)
        wait_network_quiet(page, 12000)
        page.wait_for_timeout(5000)

        title = ""
        html = ""
        url = page.url

        try:
            title = page.title()
        except Exception:
            pass

        try:
            html = page.content()
        except Exception:
            pass

        snippet = re.sub(r"\s+", " ", html[:4000])

        data = page.evaluate("""
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
                    caseRows: document.querySelectorAll("tr.load-case.table-row.link[data-caseid]").length
                },
                first_buttons: buttons.slice(0, 20),
                first_links: links.slice(0, 30),
                first_rows: rows.slice(0, 20)
            };
        }
        """)

        output = {
            "url": url,
            "title": title,
            "html_snippet": snippet,
            "dom_scan": data
        }

        print(json.dumps(output, indent=2, ensure_ascii=False))

        browser.close()
