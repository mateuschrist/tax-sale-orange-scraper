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

            const selectedActive = Array.from(
                document.querySelectorAll('a.filter-status-nosub.status-sub.selected[data-parentid="2"]')
            ).map(el => ({
                text: (el.innerText || "").trim(),
                statusid: el.getAttribute("data-statusid") || "",
                parentid: el.getAttribute("data-parentid") || ""
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
                    activeChild192: !!document.querySelector('a[data-statusid="192"][data-parentid="2"]'),
                    filtersSubmit: !!document.querySelector("button.filters-submit"),
                    filtersReset: !!document.querySelector("a.filters-reset"),
                    caseRowsExact: document.querySelectorAll('tr.load-case.table-row.link[data-caseid]').length
                },
                status_label: (document.querySelector("#filterCaseStatusLabel")?.innerText || "").trim(),
                selected_active_children: selectedActive,
                first_rows: rows.slice(0, 20),
                first_links: links.slice(0, 40)
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
    log.info("STATUS_LABEL: %s", data["status_label"])
    log.info("SELECTED_ACTIVE_CHILDREN: %s", data["selected_active_children"])
    log.info("FIRST_ROWS: %s", data["first_rows"])
    log.info("FIRST_LINKS: %s", data["first_links"])

    print(json.dumps(result, indent=2, ensure_ascii=False))


def force_exact_active_192(page):
    page.evaluate(
        """
        () => {
            // Reset visual state for all Active children
            document.querySelectorAll('a.filter-status-nosub.status-sub[data-parentid="2"]').forEach(el => {
                el.classList.remove('selected');
                const icon = el.querySelector('i');
                if (icon) {
                    icon.classList.remove('icon-ok-sign');
                    icon.classList.add('icon-circle-blank');
                }
            });

            // Reset parent visual state
            const parent = document.querySelector('#caseStatus2');
            if (parent) {
                parent.classList.remove('selected');
                const icon = parent.querySelector('i');
                if (icon) {
                    icon.classList.remove('icon-ok-sign');
                    icon.classList.add('icon-circle-blank');
                }
            }

            // Select only Active 192
            const target = document.querySelector('a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]');
            if (!target) throw new Error('Active 192 target not found');

            target.classList.add('selected');
            const targetIcon = target.querySelector('i');
            if (targetIcon) {
                targetIcon.classList.remove('icon-circle-blank');
                targetIcon.classList.add('icon-ok-sign');
            }

            // Parent should show selected too
            if (parent) {
                parent.classList.add('selected');
                const parentIcon = parent.querySelector('i');
                if (parentIcon) {
                    parentIcon.classList.remove('icon-circle-blank');
                    parentIcon.classList.add('icon-ok-sign');
                }
            }

            // Update visible label
            const label = document.querySelector('#filterCaseStatusLabel');
            if (label) label.textContent = '1 Selected';

            // Try triggering common events
            ['change', 'input', 'click'].forEach(evtName => {
                try {
                    target.dispatchEvent(new Event(evtName, { bubbles: true }));
                } catch (e) {}
            });

            // Close dropdown if open
            const group = document.querySelector('#caseFiltersStatus');
            if (group) group.classList.remove('open');
        }
        """
    )
    log.info("Forced exact status selection: only 192")


def run_filters(page):
    log.info("Running Miami exact-192 filter flow...")

    page.wait_for_timeout(8000)

    # Reset first
    click_safe(page, "a.filters-reset", "RESET FILTERS")
    page.wait_for_timeout(2000)

    # Open dropdown
    click_safe(page, "#filterButtonStatus", "FILTER BUTTON")
    page.wait_for_timeout(1500)

    # Parent open
    click_safe(page, "#caseStatus2", "ACTIVE PARENT")
    page.wait_for_timeout(1000)

    # Force exact only 192
    force_exact_active_192(page)
    page.wait_for_timeout(2000)

    # Snapshot before search to verify exact selection
    debug_snapshot(page, "MIAMI AFTER EXACT 192 SELECTION")

    # Search
    click_safe(page, "button.filters-submit", "SEARCH BUTTON")
    log.info("Sleeping 40 seconds to let results load...")
    page.wait_for_timeout(40000)


def run_miami():
    log.info("=== MIAMI EXACT 192 TEST MODE ===")

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