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


def get_status_debug(page):
    return page.evaluate(
        """
        () => {
            const links = Array.from(document.querySelectorAll('a[data-parentid="2"]')).map(el => ({
                text: (el.innerText || '').trim(),
                statusid: el.getAttribute('data-statusid') || '',
                cls: el.className || '',
                icon: el.querySelector('i') ? el.querySelector('i').className : ''
            }));

            const selectedByClass = links.filter(x => x.cls.includes('selected'));
            const selectedByIcon = links.filter(x => (x.icon || '').includes('icon-ok-sign'));

            return {
                label: (document.querySelector('#filterCaseStatusLabel')?.innerText || '').trim(),
                selected_by_class: selectedByClass,
                selected_by_icon: selectedByIcon,
                all_active_children: links
            };
        }
        """
    )


def log_status_debug(page, title):
    dbg = get_status_debug(page)
    log.info("===== %s =====", title)
    log.info("STATUS LABEL: %s", dbg["label"])
    log.info("SELECTED BY CLASS: %s", dbg["selected_by_class"])
    log.info("SELECTED BY ICON: %s", dbg["selected_by_icon"])


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
            const el = document.querySelector('a[data-statusid="192"][data-parentid="2"]');
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


def run_search(page):
    log.info("Running Miami exact-child-only flow...")

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

    log_status_debug(page, "AFTER FORCING ONLY 192")

    if not click_safe(page, "button.filters-submit", "SEARCH BUTTON"):
        raise RuntimeError("Could not click search")

    log.info("Sleeping 40 seconds to let results load...")
    page.wait_for_timeout(40000)


def extract_rows_debug(page):
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
            text = row.inner_text().strip().replace("\n", " ")
        except Exception:
            text = ""

        log.info(f"ROW {i+1}: caseid={caseid} text={text}")


def run_miami():
    log.info("=== MIAMI FILTER-192 CLEAN TEST ===")

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

        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)

        run_search(page)
        extract_rows_debug(page)

        browser.close()