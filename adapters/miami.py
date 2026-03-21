import json
import logging
import re
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

log = logging.getLogger("miami")

LIST_URL = "https://miamidade.realtdm.com/public/cases/list"


def wait_network_quiet(page, timeout=20000):
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

    snippet = re.sub(r"\s+", " ", html[:6000])

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
                navigator: {
                    userAgent: navigator.userAgent,
                    language: navigator.language,
                    languages: navigator.languages,
                    platform: navigator.platform,
                    webdriver: navigator.webdriver
                },
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

    log.info("===== MIAMI HUMAN TEST SNAPSHOT =====")
    log.info("TITLE: %s", title)
    log.info("URL: %s", url)
    log.info("HTML_SNIPPET: %s", snippet[:1500])
    log.info("NAVIGATOR: %s", data["navigator"])
    log.info("DOM_COUNTS: %s", data["counts"])
    log.info("DOM_EXISTS: %s", data["exists"])
    log.info("FIRST_BUTTONS: %s", data["first_buttons"])
    log.info("FIRST_LINKS: %s", data["first_links"])
    log.info("FIRST_ROWS: %s", data["first_rows"])

    print(json.dumps(result, indent=2, ensure_ascii=False))


def run_miami():
    log.info("=== MIAMI HUMAN TEST MODE ===")

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
            screen={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="America/New_York",
            color_scheme="light",
            device_scale_factor=1,
            has_touch=False,
            is_mobile=False,
        )

        page = context.new_page()

        # Some sites react better when common headers are set explicitly
        page.set_extra_http_headers(
            {
                "Accept-Language": "en-US,en;q=0.9",
                "Upgrade-Insecure-Requests": "1",
                "DNT": "1",
            }
        )

        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=45000)
        wait_network_quiet(page, 20000)
        page.wait_for_timeout(10000)

        debug_dom_snapshot(page)

        browser.close()