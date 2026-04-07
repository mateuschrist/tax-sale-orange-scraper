import json
import logging
import os
import re
from datetime import datetime
from playwright.sync_api import sync_playwright

log = logging.getLogger("miami_probe")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"

REPORT = {
    "started_at": datetime.utcnow().isoformat(),
    "page": {},
    "steps": [],
    "results": {},
}


def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def dump_artifacts(page, prefix):
    try:
        page.screenshot(path=f"{prefix}.png", full_page=True)
    except Exception:
        pass
    try:
        with open(f"{prefix}.html", "w", encoding="utf-8") as f:
            f.write(page.content())
    except Exception:
        pass


def record(step, ok, detail=None):
    entry = {
        "step": step,
        "ok": ok,
        "detail": detail or {},
        "ts": datetime.utcnow().isoformat(),
    }
    REPORT["steps"].append(entry)
    log.info("%s %s %s", "OK" if ok else "FAIL", step, json.dumps(detail or {}, ensure_ascii=False)[:500])


def launch_browser(p):
    try:
        browser = p.chromium.launch(
            channel="chrome",
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        record("launch_browser", True, {"mode": "chrome"})
        return browser
    except Exception as e:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        record("launch_browser", True, {"mode": "chromium_fallback", "reason": str(e)})
        return browser


def humanize(page):
    try:
        page.mouse.move(250, 220)
        page.wait_for_timeout(300)
        page.mouse.move(520, 360)
        page.wait_for_timeout(500)
        page.mouse.wheel(0, 350)
        page.wait_for_timeout(700)
        page.mouse.wheel(0, -180)
        page.wait_for_timeout(400)
    except Exception:
        pass


def scan_dom(page, prefix):
    data = page.evaluate(
        """
        () => {
            function txt(el) {
                return ((el.innerText || el.textContent || '')).replace(/\\s+/g, ' ').trim();
            }
            function visible(el) {
                const r = el.getBoundingClientRect();
                const s = window.getComputedStyle(el);
                return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
            }

            const clickables = Array.from(document.querySelectorAll('a,button,input[type="button"],input[type="submit"],div,span,li'))
                .map((el, idx) => ({
                    idx,
                    tag: el.tagName.toLowerCase(),
                    text: txt(el),
                    visible: visible(el),
                    id: el.id || '',
                    class_name: (el.className || '').toString(),
                    href: el.getAttribute('href') || '',
                    onclick: el.getAttribute('onclick') || '',
                    data_target: el.getAttribute('data-target') || '',
                    data_toggle: el.getAttribute('data-toggle') || '',
                    data_statusid: el.getAttribute('data-statusid') || '',
                    data_parentid: el.getAttribute('data-parentid') || ''
                }))
                .filter(x => x.text || x.id || x.class_name || x.data_target || x.data_statusid);

            return {
                url: location.href,
                title: document.title,
                body_sample: (document.body.innerText || '').slice(0, 5000),
                clickables: clickables.slice(0, 3000),
                status_candidates: clickables.filter(x =>
                    x.text.toLowerCase().includes('status') ||
                    x.id.toLowerCase().includes('status') ||
                    x.class_name.toLowerCase().includes('status') ||
                    x.data_target.toLowerCase().includes('status')
                ).slice(0, 300),
                active_candidates: clickables.filter(x =>
                    x.text === 'Active' ||
                    x.text.toLowerCase().includes('active')
                ).slice(0, 300),
                search_candidates: clickables.filter(x =>
                    x.text.toLowerCase().includes('search')
                ).slice(0, 300),
                page_candidates: clickables.filter(x =>
                    x.text.includes('Page') || x.data_page
                ).slice(0, 300)
            };
        }
        """
    )
    save_json(f"{prefix}.json", data)
    dump_artifacts(page, prefix)
    return data


def try_click_selector(page, selector, label):
    attempts = []

    try:
        page.locator(selector).first.click(timeout=4000)
        page.wait_for_timeout(1200)
        attempts.append({"method": "locator.click", "ok": True})
        return True, attempts
    except Exception as e:
        attempts.append({"method": "locator.click", "ok": False, "error": str(e)})

    try:
        page.locator(selector).first.click(force=True, timeout=4000)
        page.wait_for_timeout(1200)
        attempts.append({"method": "locator.click(force=True)", "ok": True})
        return True, attempts
    except Exception as e:
        attempts.append({"method": "locator.click(force=True)", "ok": False, "error": str(e)})

    try:
        ok = page.evaluate(
            """
            (sel) => {
                const el = document.querySelector(sel);
                if (!el) return false;
                try { el.click(); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('mousedown', { bubbles:true })); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('mouseup', { bubbles:true })); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('click', { bubbles:true })); } catch(e) {}
                return true;
            }
            """,
            selector,
        )
        if ok:
            page.wait_for_timeout(1200)
            attempts.append({"method": "page.evaluate(js click)", "ok": True})
            return True, attempts
        attempts.append({"method": "page.evaluate(js click)", "ok": False, "error": "selector not found"})
    except Exception as e:
        attempts.append({"method": "page.evaluate(js click)", "ok": False, "error": str(e)})

    return False, attempts


def try_click_text(page, text, label):
    selectors = [
        f'text="{text}"',
        f'text={text}',
        f'button:has-text("{text}")',
        f'a:has-text("{text}")',
        f'span:has-text("{text}")',
        f'div:has-text("{text}")',
        f'li:has-text("{text}")',
    ]

    all_attempts = []
    for sel in selectors:
        ok, attempts = try_click_selector(page, sel, label)
        all_attempts.append({"selector": sel, "attempts": attempts})
        if ok:
            return True, all_attempts
    return False, all_attempts


def current_filter_state(page):
    return page.evaluate(
        """
        () => {
            const hidden = document.querySelector('#filterCaseStatus');
            const label = document.querySelector('#filterCaseStatusLabel');
            const selected = Array.from(document.querySelectorAll('.selected,[aria-selected="true"]'))
                .map(el => ((el.innerText || '').replace(/\\s+/g, ' ').trim()))
                .filter(Boolean)
                .slice(0, 50);

            return {
                hidden_filterCaseStatus: hidden ? hidden.value : '',
                label: label ? label.innerText.trim() : '',
                selected_items: selected
            };
        }
        """
    )


def clear_statuses(page):
    page.evaluate(
        """
        () => {
            const nodes = document.querySelectorAll('[data-statusid]');
            nodes.forEach(el => el.classList.remove('selected'));
            const hidden = document.querySelector('#filterCaseStatus');
            if (hidden) hidden.value = '';
            const label = document.querySelector('#filterCaseStatusLabel');
            if (label) label.innerText = 'None Selected';
        }
        """
    )
    page.wait_for_timeout(600)


def test_open_status(page):
    candidates = [
        "#filterButtonStatus",
        '[data-target="#filterStatus"]',
        '[data-target*="Status"]',
        "#caseStatus2",
    ]

    log.info("=== TEST OPEN STATUS ===")
    details = []

    for sel in candidates:
        ok, attempts = try_click_selector(page, sel, "open_status")
        details.append({"selector": sel, "attempts": attempts, "state": current_filter_state(page)})
        if ok:
            record("open_status", True, {"winner": sel, "details": details[-1]})
            return True, details

    for txt in ["Case Status", "Status"]:
        ok, attempts = try_click_text(page, txt, "open_status_text")
        details.append({"text": txt, "attempts": attempts, "state": current_filter_state(page)})
        if ok:
            record("open_status", True, {"winner_text": txt, "details": details[-1]})
            return True, details

    record("open_status", False, {"details": details})
    return False, details


def test_select_active(page):
    log.info("=== TEST SELECT ACTIVE ===")
    clear_statuses(page)

    # testar por seletor conhecido primeiro
    strategies = []

    known_selectors = [
        '[data-statusid="192"]',
        'a[data-statusid="192"]',
        '[data-statusid="192"][data-parentid="2"]',
        'a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]',
    ]

    for sel in known_selectors:
        clear_statuses(page)
        ok, attempts = try_click_selector(page, sel, "active_known")
        state = current_filter_state(page)
        strategies.append({"type": "selector", "value": sel, "attempts": attempts, "state": state})
        if ok and state.get("hidden_filterCaseStatus") == "192":
            record("select_active", True, {"winner": sel, "state": state})
            return True, strategies

    # depois testar texto exato
    clear_statuses(page)
    ok, attempts = try_click_text(page, "Active", "active_text")
    state = current_filter_state(page)
    strategies.append({"type": "text", "value": "Active", "attempts": attempts, "state": state})
    if ok and state.get("hidden_filterCaseStatus") == "192":
        record("select_active", True, {"winner_text": "Active", "state": state})
        return True, strategies

    # varredura ampla: clicar todos candidatos com texto exact Active
    candidates = page.evaluate(
        """
        () => {
            function txt(el) {
                return ((el.innerText || el.textContent || '')).replace(/\\s+/g, ' ').trim();
            }
            function visible(el) {
                const r = el.getBoundingClientRect();
                const s = window.getComputedStyle(el);
                return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
            }

            return Array.from(document.querySelectorAll('a,li,div,span,button'))
                .filter(visible)
                .map((el, idx) => ({
                    idx,
                    tag: el.tagName.toLowerCase(),
                    text: txt(el),
                    id: el.id || '',
                    class_name: (el.className || '').toString(),
                    data_statusid: el.getAttribute('data-statusid') || '',
                    data_parentid: el.getAttribute('data-parentid') || ''
                }))
                .filter(x => x.text === 'Active')
                .slice(0, 100);
        }
        """
    )

    save_json("miami_active_candidates_probe.json", candidates)

    for i, cand in enumerate(candidates):
        clear_statuses(page)
        clicked = page.evaluate(
            """
            (targetIndex) => {
                function txt(el) {
                    return ((el.innerText || el.textContent || '')).replace(/\\s+/g, ' ').trim();
                }
                function visible(el) {
                    const r = el.getBoundingClientRect();
                    const s = window.getComputedStyle(el);
                    return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
                }

                const matches = Array.from(document.querySelectorAll('a,li,div,span,button'))
                    .filter(visible)
                    .filter(el => txt(el) === 'Active');

                const el = matches[targetIndex];
                if (!el) return false;

                try { el.click(); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('mousedown', { bubbles:true })); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('mouseup', { bubbles:true })); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('click', { bubbles:true })); } catch(e) {}
                return true;
            }
            """,
            i,
        )
        page.wait_for_timeout(900)
        state = current_filter_state(page)
        strategies.append({"type": "candidate_index", "value": i, "candidate": cand, "clicked": clicked, "state": state})

        if clicked and state.get("hidden_filterCaseStatus") == "192":
            record("select_active", True, {"winner_candidate_index": i, "candidate": cand, "state": state})
            return True, strategies

    record("select_active", False, {"strategies": strategies[-20:]})
    return False, strategies


def test_search_submit(page):
    log.info("=== TEST SEARCH SUBMIT ===")
    strategies = []

    selectors = [
        "button:has-text('Process Search')",
        "text=Process Search",
        'input[type="submit"][value*="Process Search"]',
        'input[type="button"][value*="Process Search"]',
        "button.filters-submit",
        "button:has-text('Search')",
        "text=Search",
    ]

    for sel in selectors:
        try:
            locator = page.locator(sel).first
            if locator.count() == 0:
                strategies.append({"selector": sel, "found": False})
                continue

            try:
                locator.click(timeout=5000)
                page.wait_for_timeout(5000)
                rows = page.locator('tr.load-case.table-row.link[data-caseid]').count()
                strategies.append({"selector": sel, "found": True, "rows_after": rows})
                if rows > 0:
                    record("search_submit", True, {"winner": sel, "rows_after": rows})
                    return True, strategies
            except Exception as e:
                strategies.append({"selector": sel, "found": True, "error": str(e)})
        except Exception as e:
            strategies.append({"selector": sel, "error": str(e)})

    record("search_submit", False, {"strategies": strategies})
    return False, strategies


def test_next_page(page):
    log.info("=== TEST NEXT PAGE ===")
    strategies = []

    current_before = page.evaluate(
        """
        () => {
            const body = document.body.innerText || '';
            const m = body.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);
            return m ? parseInt(m[1], 10) : 1;
        }
        """
    )

    selectors = [
        '[data-page="2"]',
        'a[data-page="2"]',
        'text="Page 2"',
        'text=Page 2',
        'div:has-text("Page 2")',
        'span:has-text("Page 2")',
    ]

    for sel in selectors:
        ok, attempts = try_click_selector(page, sel, "next_page")
        page.wait_for_timeout(3000)
        current_after = page.evaluate(
            """
            () => {
                const body = document.body.innerText || '';
                const m = body.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);
                return m ? parseInt(m[1], 10) : 1;
            }
            """
        )
        strategies.append({
            "selector": sel,
            "attempts": attempts,
            "page_before": current_before,
            "page_after": current_after
        })
        if ok and current_after != current_before:
            record("next_page", True, {"winner": sel, "page_after": current_after})
            return True, strategies

    # fallback visual next
    result = page.evaluate(
        """
        () => {
            function isVisible(el) {
                const r = el.getBoundingClientRect();
                const s = window.getComputedStyle(el);
                return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
            }
            function txt(el) {
                return ((el.innerText || '').replace(/\\s+/g, ' ')).trim();
            }
            const els = Array.from(document.querySelectorAll('a,button,span,div,td,img')).filter(isVisible);

            let best = null;
            for (const el of els) {
                const t = txt(el);
                const c = ((el.className || '') + '').toLowerCase();
                let score = 0;
                if (t === '>' || t === '›' || t === '»') score += 200;
                if (c.includes('next')) score += 120;
                if (c.includes('right')) score += 80;
                if (c.includes('arrow')) score += 80;
                if (c.includes('chevron')) score += 80;
                if (score <= 0) continue;

                const r = el.getBoundingClientRect();
                const cand = { x: r.left + r.width/2, y: r.top + r.height/2, text: t, class_name: c, score };
                if (!best || cand.score > best.score) best = cand;
            }
            return best;
        }
        """
    )

    if result:
        page.mouse.click(result["x"], result["y"])
        page.wait_for_timeout(3000)
        current_after = page.evaluate(
            """
            () => {
                const body = document.body.innerText || '';
                const m = body.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);
                return m ? parseInt(m[1], 10) : 1;
            }
            """
        )
        strategies.append({"visual_next": result, "page_before": current_before, "page_after": current_after})
        if current_after != current_before:
            record("next_page", True, {"winner": "visual_next", "page_after": current_after})
            return True, strategies

    record("next_page", False, {"strategies": strategies})
    return False, strategies


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    with sync_playwright() as p:
        browser = launch_browser(p)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/134.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
        )
        page = context.new_page()

        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        page.wait_for_timeout(4000)
        humanize(page)

        REPORT["page"] = {
            "url": page.url,
            "title": page.title(),
        }

        scan_dom(page, "probe_01_initial")

        ok_status, status_details = test_open_status(page)
        REPORT["results"]["open_status"] = status_details
        scan_dom(page, "probe_02_after_open_status")

        if ok_status:
            ok_active, active_details = test_select_active(page)
            REPORT["results"]["select_active"] = active_details
            scan_dom(page, "probe_03_after_select_active")

            if ok_active:
                ok_search, search_details = test_search_submit(page)
                REPORT["results"]["search_submit"] = search_details
                scan_dom(page, "probe_04_after_search")

                if ok_search:
                    ok_next, next_details = test_next_page(page)
                    REPORT["results"]["next_page"] = next_details
                    scan_dom(page, "probe_05_after_next_page")

        browser.close()

    REPORT["finished_at"] = datetime.utcnow().isoformat()
    save_json("miami_probe_report.json", REPORT)
    log.info("Done. Files: miami_probe_report.json + probe_*.json/html/png")


if __name__ == "__main__":
    main()