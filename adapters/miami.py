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


def verify_exact_192(page, title):
    data = page.evaluate(
        """
        () => {
            const exact = document.querySelector('a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]');
            const all192 = Array.from(document.querySelectorAll('[data-statusid="192"]')).map(el => ({
                tag: el.tagName.toLowerCase(),
                text: (el.innerText || '').trim(),
                className: el.className || '',
                dataStatusId: el.getAttribute('data-statusid') || '',
                dataParentId: el.getAttribute('data-parentid') || '',
                outerHTML: el.outerHTML
            }));

            return {
                exact_found: !!exact,
                exact_info: exact ? {
                    text: (exact.innerText || '').trim(),
                    className: exact.className || '',
                    dataStatusId: exact.getAttribute('data-statusid') || '',
                    dataParentId: exact.getAttribute('data-parentid') || '',
                    has_selected_class: exact.classList.contains('selected'),
                    icon_class: exact.querySelector('i') ? exact.querySelector('i').className : '',
                    outerHTML: exact.outerHTML
                } : null,
                all_192_matches: all192
            };
        }
        """
    )

    log.info("===== %s =====", title)
    log.info("EXACT 192 FOUND: %s", data["exact_found"])
    log.info("EXACT 192 INFO: %s", data["exact_info"])
    log.info("ALL 192 MATCHES: %s", data["all_192_matches"])


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

            const label = document.querySelector('#filterCaseStatusLabel');
            if (label) label.innerText = '1 Selected';
        }
        """
    )
    log.info("Forced only status 192 via JS")


def run_miami():
    log.info("=== MIAMI VERIFY EXACT 192 ===")

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

        page.wait_for_timeout(8000)

        click_safe(page, "a.filters-reset", "RESET FILTERS")
        page.wait_for_timeout(2000)

        click_safe(page, "#filterButtonStatus", "FILTER BUTTON")
        page.wait_for_timeout(1500)

        verify_exact_192(page, "BEFORE CLEAR")

        force_clear_all_active_statuses(page)
        page.wait_for_timeout(800)

        verify_exact_192(page, "AFTER CLEAR")

        force_select_only_192(page)
        page.wait_for_timeout(1200)

        verify_exact_192(page, "AFTER FORCE SELECT 192")

        browser.close()