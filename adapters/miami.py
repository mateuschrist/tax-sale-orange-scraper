import logging
from playwright.sync_api import sync_playwright

log = logging.getLogger("miami")

URL = "https://miamidade.realtdm.com/public/cases/list"


def click_safe(page, selector, name):
    try:
        page.click(selector, timeout=5000)
        log.info(f"{name} clicked (normal)")
        return True
    except:
        try:
            page.evaluate(f'document.querySelector("{selector}").click()')
            log.info(f"{name} clicked (JS fallback)")
            return True
        except:
            log.error(f"{name} FAILED")
            return False


def run_filters(page):
    log.info("Running Miami filters...")

    page.wait_for_timeout(5000)

    # 1. abrir dropdown
    click_safe(page, "#filterButtonStatus", "FILTER BUTTON")

    page.wait_for_timeout(2000)

    # 2. clicar ACTIVE (parent)
    click_safe(page, "#caseStatus2", "ACTIVE PARENT")

    page.wait_for_timeout(1500)

    # 3. clicar ACTIVE (child)
    click_safe(page, 'a[data-statusid="192"][data-parentid="2"]', "ACTIVE CHILD")

    page.wait_for_timeout(1500)

    # 4. garantir estado via JS (anti-bug)
    page.evaluate("""
        document.querySelectorAll('a[data-statusid="192"]').forEach(el=>{
            el.classList.add('selected');
        });
    """)

    # 5. clicar SEARCH
    click_safe(page, "button.filters-submit", "SEARCH BUTTON")

    log.info("Waiting results...")
    page.wait_for_timeout(10000)


def extract_cases(page):
    log.info("Extracting cases...")

    rows = page.query_selector_all('tr.load-case.table-row.link[data-caseid]')

    log.info(f"FOUND {len(rows)} CASES")

    for i, row in enumerate(rows[:5]):
        text = row.inner_text().strip().replace("\n", " ")
        log.info(f"CASE {i+1}: {text}")


def run_miami():
    log.info("=== MIAMI V5 FULL SCRAPER ===")

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
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="America/New_York",
        )

        page = context.new_page()

        page.goto(URL, wait_until="domcontentloaded", timeout=60000)

        page.wait_for_timeout(8000)

        # 🔥 STEP 1 - FILTER
        run_filters(page)

        # 🔥 STEP 2 - EXTRACTION
        extract_cases(page)

        browser.close()