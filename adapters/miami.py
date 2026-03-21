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


def clear_all_active_children_by_click(page):
    # abre o dropdown antes
    children = page.locator('a.filter-status-nosub.status-sub[data-parentid="2"]')
    count = children.count()

    for i in range(count):
        child = children.nth(i)
        try:
            statusid = child.get_attribute("data-statusid")
            cls = child.get_attribute("class") or ""
            icon_cls = ""
            try:
                icon_cls = child.locator("i").first.get_attribute("class") or ""
            except Exception:
                pass

            # se estiver marcado por classe ou por ícone, clica para desmarcar
            if "selected" in cls or "icon-ok-sign" in icon_cls:
                try:
                    child.click(timeout=3000)
                    log.info(f"Unselected child status {statusid}")
                    page.wait_for_timeout(400)
                except Exception:
                    try:
                        child.click(force=True, timeout=3000)
                        log.info(f"Unselected child status {statusid} (force)")
                        page.wait_for_timeout(400)
                    except Exception:
                        log.warning(f"Could not unselect child status {statusid}")
        except Exception:
            continue


def ensure_only_192_selected(page):
    # 1) abrir dropdown
    if not click_safe(page, "#filterButtonStatus", "FILTER BUTTON"):
        raise RuntimeError("Could not open filter dropdown")
    page.wait_for_timeout(1500)

    # 2) NÃO clicar no parent
    # 3) limpar o que estiver marcado no grupo Active
    clear_all_active_children_by_click(page)
    page.wait_for_timeout(1000)

    # 4) clicar somente no 192
    target = 'a[data-statusid="192"][data-parentid="2"]'
    if not click_safe(page, target, "ACTIVE CHILD 192"):
        raise RuntimeError("Could not click status 192")
    page.wait_for_timeout(1500)

    # 5) log de verificação
    log_status_debug(page, "AFTER CLICKING ONLY 192")


def run_search(page):
    log.info("Running Miami exact-child-only flow...")

    page.wait_for_timeout(8000)

    if not click_safe(page, "a.filters-reset", "RESET FILTERS"):
        raise RuntimeError("Could not reset filters")
    page.wait_for_timeout(2000)

    ensure_only_192_selected(page)

    if not click_safe(page, "button.filters-submit", "SEARCH BUTTON"):
        raise RuntimeError("Could not click search")

    log.info("Sleeping 40 seconds to let results load...")
    page.wait_for_timeout(40000)


def extract_rows_debug(page):
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
            text = row.inner_text().strip().replace("\n", " ")
        except Exception:
            text = ""
        log.info(f"ROW {i+1}: caseid={caseid} text={text}")


def run_miami():
    log.info("=== MIAMI FILTER-192 ONLY TEST ===")

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