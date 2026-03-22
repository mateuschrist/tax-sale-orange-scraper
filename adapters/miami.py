import logging
from playwright.sync_api import sync_playwright

log = logging.getLogger("miami")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"


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


def force_clear_all_active_statuses(page):
    page.evaluate(
        """
        () => {
            const children = document.querySelectorAll('a.filter-status-nosub.status-sub[data-parentid="2"]');

            children.forEach(el => {
                el.classList.remove('selected');

                const icon = el.querySelector('i');
                if (icon) {
                    icon.className = icon.className.replace(/\\bicon-ok-sign\\b/g, '').trim();
                    icon.className = icon.className.replace(/\\bicon-circle-blank\\b/g, '').trim();
                    icon.className = (icon.className + ' icon-circle-blank').trim();
                }
            });

            const parent = document.querySelector('#caseStatus2');
            if (parent) {
                parent.classList.remove('selected');
                const icon = parent.querySelector('i');
                if (icon) {
                    icon.className = icon.className.replace(/\\bicon-ok-sign\\b/g, '').trim();
                    icon.className = icon.className.replace(/\\bicon-circle-blank\\b/g, '').trim();
                    icon.className = (icon.className + ' icon-circle-blank').trim();
                }
            }

            const hidden = document.querySelector('#filterCaseStatus');
            if (hidden) hidden.value = '';

            const label = document.querySelector('#filterCaseStatusLabel');
            if (label) label.innerText = 'None Selected';
        }
        """
    )
    log.info("Cleared UI + hidden filterCaseStatus")


def force_select_only_192(page):
    page.evaluate(
        """
        () => {
            const el = document.querySelector('a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]');
            if (!el) throw new Error("Status 192 not found");

            el.classList.add('selected');

            const icon = el.querySelector('i');
            if (icon) {
                icon.className = icon.className.replace(/\\bicon-circle-blank\\b/g, '').trim();
                icon.className = icon.className.replace(/\\bicon-ok-sign\\b/g, '').trim();
                icon.className = (icon.className + ' icon-ok-sign').trim();
            }

            const parent = document.querySelector('#caseStatus2');
            if (parent) {
                parent.classList.add('selected');
                const picon = parent.querySelector('i');
                if (picon) {
                    picon.className = picon.className.replace(/\\bicon-circle-blank\\b/g, '').trim();
                    picon.className = picon.className.replace(/\\bicon-ok-sign\\b/g, '').trim();
                    picon.className = (picon.className + ' icon-ok-sign').trim();
                }
            }

            const hidden = document.querySelector('#filterCaseStatus');
            if (hidden) hidden.value = '192';

            const label = document.querySelector('#filterCaseStatusLabel');
            if (label) label.innerText = '1 Selected';
        }
        """
    )
    log.info("Forced UI + hidden filterCaseStatus=192")


def dump_state(page, title):
    data = page.evaluate(
        """
        () => {
            const hidden = document.querySelector('#filterCaseStatus');
            const selected192 = document.querySelector('a[data-statusid="192"][data-parentid="2"]');

            return {
                label: (document.querySelector('#filterCaseStatusLabel')?.innerText || '').trim(),
                hidden_filterCaseStatus: hidden ? hidden.value : None,
                selected192: selected192 ? {
                    className: selected192.className || '',
                    outerHTML: selected192.outerHTML
                } : null
            };
        }
        """
    )
    log.info("===== %s =====", title)
    log.info("STATE: %s", data)


def run_miami():
    log.info("=== MIAMI REAL FILTER FIX ===")

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

        def on_request(request):
            if request.method == "POST" and "/public/cases/list" in request.url:
                log.info("===== OUTGOING POST =====")
                log.info("REQ URL: %s", request.url)
                log.info("REQ POST DATA: %s", request.post_data)

        page.on("request", on_request)

        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(8000)

        if not click_safe(page, "a.filters-reset", "RESET FILTERS"):
            raise RuntimeError("Could not reset filters")
        page.wait_for_timeout(2000)

        if not click_safe(page, "#filterButtonStatus", "FILTER BUTTON"):
            raise RuntimeError("Could not open filter button")
        page.wait_for_timeout(1500)

        force_clear_all_active_statuses(page)
        page.wait_for_timeout(800)

        force_select_only_192(page)
        page.wait_for_timeout(1200)

        dump_state(page, "BEFORE SEARCH")

        if not click_safe(page, "button.filters-submit", "SEARCH BUTTON"):
            raise RuntimeError("Could not click search")

        log.info("Sleeping 15 seconds to let search finish...")
        page.wait_for_timeout(15000)

        dump_state(page, "AFTER SEARCH")

        rows = page.locator('tr.load-case.table-row.link[data-caseid]')
        count = rows.count()
        log.info(f"FOUND {count} CASE ROWS")

        for i in range(min(count, 20)):
            row = rows.nth(i)
            try:
                caseid = row.get_attribute("data-caseid")
            except Exception:
                caseid = None
            try:
                text = row.inner_text().strip().replace("\\n", " ")
            except Exception:
                text = ""
            log.info(f"ROW {i+1}: caseid={caseid} text={text}")

        browser.close()