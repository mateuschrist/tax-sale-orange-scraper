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
                    if (!icon.className.includes('icon-circle-blank')) {
                        icon.className = (icon.className + ' icon-circle-blank').trim();
                    }
                }
            });

            const parent = document.querySelector('#caseStatus2');
            if (parent) {
                parent.classList.remove('selected');
                const icon = parent.querySelector('i');
                if (icon) {
                    icon.className = icon.className.replace(/\\bicon-ok-sign\\b/g, '').trim();
                    if (!icon.className.includes('icon-circle-blank')) {
                        icon.className = (icon.className + ' icon-circle-blank').trim();
                    }
                }
            }

            const label = document.querySelector('#filterCaseStatusLabel');
            if (label) label.innerText = 'None Selected';
        }
        """
    )
    log.info("Cleared all Active child statuses via JS")


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
                if (!icon.className.includes('icon-ok-sign')) {
                    icon.className = (icon.className + ' icon-ok-sign').trim();
                }
            }

            const parent = document.querySelector('#caseStatus2');
            if (parent) {
                parent.classList.add('selected');
                const picon = parent.querySelector('i');
                if (picon) {
                    picon.className = picon.className.replace(/\\bicon-circle-blank\\b/g, '').trim();
                    if (!picon.className.includes('icon-ok-sign')) {
                        picon.className = (picon.className + ' icon-ok-sign').trim();
                    }
                }
            }

            const label = document.querySelector('#filterCaseStatusLabel');
            if (label) label.innerText = '1 Selected';
        }
        """
    )
    log.info("Forced only status 192 via JS")


def dump_status_related_state(page, title):
    data = page.evaluate(
        """
        () => {
            const fields = Array.from(document.querySelectorAll('input, select, textarea')).map(el => ({
                tag: el.tagName.toLowerCase(),
                type: el.getAttribute('type') || '',
                name: el.getAttribute('name') || '',
                id: el.id || '',
                value: el.value || '',
                cls: el.className || ''
            }));

            const filteredFields = fields.filter(x =>
                (x.name || '').toLowerCase().includes('status') ||
                (x.id || '').toLowerCase().includes('status') ||
                (x.name || '').toLowerCase().includes('case') ||
                (x.id || '').toLowerCase().includes('case')
            );

            const selected192 = document.querySelector('a[data-statusid="192"][data-parentid="2"]');

            return {
                label: (document.querySelector('#filterCaseStatusLabel')?.innerText || '').trim(),
                selected192: selected192 ? {
                    className: selected192.className || '',
                    outerHTML: selected192.outerHTML
                } : null,
                status_fields: filteredFields
            };
        }
        """
    )

    log.info("===== %s =====", title)
    log.info("STATUS LABEL: %s", data["label"])
    log.info("SELECTED 192: %s", data["selected192"])
    log.info("STATUS FIELDS: %s", data["status_fields"])


def run_miami():
    log.info("=== MIAMI REQUEST DIAGNOSTIC ===")

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
            if "cases" in request.url or "search" in request.url or "list" in request.url:
                log.info("===== OUTGOING REQUEST =====")
                log.info("REQ METHOD: %s", request.method)
                log.info("REQ URL: %s", request.url)
                try:
                    log.info("REQ POST DATA: %s", request.post_data)
                except Exception:
                    pass

        page.on("request", on_request)

        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(8000)

        click_safe(page, "a.filters-reset", "RESET FILTERS")
        page.wait_for_timeout(2000)

        click_safe(page, "#filterButtonStatus", "FILTER BUTTON")
        page.wait_for_timeout(1500)

        force_clear_all_active_statuses(page)
        page.wait_for_timeout(800)

        force_select_only_192(page)
        page.wait_for_timeout(1200)

        dump_status_related_state(page, "BEFORE SEARCH")

        click_safe(page, "button.filters-submit", "SEARCH BUTTON")

        page.wait_for_timeout(15000)

        dump_status_related_state(page, "AFTER SEARCH")

        rows = page.locator('tr.load-case.table-row.link[data-caseid]')
        count = rows.count()
        log.info(f"FOUND {count} CASE ROWS")

        for i in range(min(count, 10)):
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