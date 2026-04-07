import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
OUTPUT_DIR = os.getenv("MIAMI_DEBUG_DIR", "miami_debug_output")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("miami_full_probe")

REPORT: Dict[str, Any] = {
    "started_at": datetime.utcnow().isoformat(),
    "config": {
        "headless": HEADLESS,
        "list_url": LIST_URL,
        "output_dir": OUTPUT_DIR,
    },
    "page": {},
    "steps": [],
    "artifacts": [],
    "results": {},
}


# =========================
# HELPERS
# =========================
def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def out_path(name: str) -> str:
    ensure_output_dir()
    return os.path.join(OUTPUT_DIR, name)


def clean_text(value) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def save_json(name: str, data: Any):
    path = out_path(name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    REPORT["artifacts"].append(path)
    log.info("Saved json: %s", path)


def save_text(name: str, content: str):
    path = out_path(name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    REPORT["artifacts"].append(path)
    log.info("Saved text: %s", path)


def save_screenshot(page, name: str):
    path = out_path(name)
    page.screenshot(path=path, full_page=True)
    REPORT["artifacts"].append(path)
    log.info("Saved screenshot: %s", path)


def save_html(page, name: str):
    path = out_path(name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(page.content())
    REPORT["artifacts"].append(path)
    log.info("Saved html: %s", path)


def dump_state(page, prefix: str):
    try:
        save_screenshot(page, f"{prefix}.png")
    except Exception as e:
        log.warning("Could not save screenshot for %s: %s", prefix, str(e))

    try:
        save_html(page, f"{prefix}.html")
    except Exception as e:
        log.warning("Could not save html for %s: %s", prefix, str(e))

    try:
        scan = scan_dom(page)
        save_json(f"{prefix}.json", scan)
    except Exception as e:
        log.warning("Could not save dom scan for %s: %s", prefix, str(e))


def record_step(step: str, ok: bool, detail: Optional[Dict[str, Any]] = None):
    payload = {
        "step": step,
        "ok": ok,
        "detail": detail or {},
        "timestamp": datetime.utcnow().isoformat(),
    }
    REPORT["steps"].append(payload)
    log.info("%s %s %s", "OK" if ok else "FAIL", step, json.dumps(detail or {}, ensure_ascii=False)[:700])


def humanize(page):
    try:
        page.mouse.move(180, 160)
        page.wait_for_timeout(250)
        page.mouse.move(420, 290)
        page.wait_for_timeout(400)
        page.mouse.move(760, 420)
        page.wait_for_timeout(350)
        page.mouse.wheel(0, 350)
        page.wait_for_timeout(500)
        page.mouse.wheel(0, -180)
        page.wait_for_timeout(300)
    except Exception:
        pass


def current_page_info(page) -> Dict[str, Any]:
    try:
        title = page.title()
    except Exception:
        title = ""

    try:
        url = page.url
    except Exception:
        url = ""

    try:
        body_text = page.locator("body").inner_text(timeout=5000)
    except Exception:
        body_text = ""

    return {
        "url": url,
        "title": title,
        "body_sample": body_text[:5000],
    }


def current_filter_state(page) -> Dict[str, Any]:
    return page.evaluate(
        """
        () => {
            const hidden = document.querySelector('#filterCaseStatus');
            const label = document.querySelector('#filterCaseStatusLabel');

            const selectedItems = Array.from(document.querySelectorAll('.selected,[aria-selected="true"]'))
                .map(el => ((el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim()))
                .filter(Boolean)
                .slice(0, 50);

            return {
                hidden_filterCaseStatus: hidden ? hidden.value : '',
                label: label ? label.innerText.trim() : '',
                selected_items: selectedItems
            };
        }
        """
    )


def scan_dom(page) -> Dict[str, Any]:
    return page.evaluate(
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

            const clickables = Array.from(document.querySelectorAll('a,button,input[type="button"],input[type="submit"],div,span,li,td'))
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
                    data_parentid: el.getAttribute('data-parentid') || '',
                    role: el.getAttribute('role') || '',
                    aria_label: el.getAttribute('aria-label') || ''
                }))
                .filter(x =>
                    x.text || x.id || x.class_name || x.data_target || x.data_statusid || x.aria_label
                );

            const rows = Array.from(document.querySelectorAll('tr.load-case.table-row.link[data-caseid]'))
                .slice(0, 20)
                .map((el, idx) => ({
                    idx,
                    caseid: el.getAttribute('data-caseid') || '',
                    text: txt(el)
                }));

            return {
                title: document.title,
                url: location.href,
                body_sample: (document.body.innerText || '').slice(0, 8000),
                clickables_count: clickables.length,
                rows_count: document.querySelectorAll('tr.load-case.table-row.link[data-caseid]').length,
                clickables: clickables.slice(0, 3000),
                first_rows: rows,
                status_candidates: clickables.filter(x =>
                    x.text.toLowerCase().includes('status') ||
                    x.id.toLowerCase().includes('status') ||
                    x.class_name.toLowerCase().includes('status') ||
                    x.data_target.toLowerCase().includes('status')
                ).slice(0, 300),
                active_candidates: clickables.filter(x =>
                    x.text === 'Active' ||
                    x.text.toLowerCase().includes('active') ||
                    x.data_statusid === '192'
                ).slice(0, 300),
                search_candidates: clickables.filter(x =>
                    x.text.toLowerCase().includes('search')
                ).slice(0, 200),
                pager_candidates: clickables.filter(x =>
                    x.text.includes('Page') ||
                    x.class_name.toLowerCase().includes('next') ||
                    x.class_name.toLowerCase().includes('pager') ||
                    x.class_name.toLowerCase().includes('page')
                ).slice(0, 300)
            };
        }
        """
    )


def try_click_selector(page, selector: str) -> Dict[str, Any]:
    result = {
        "selector": selector,
        "attempts": [],
        "ok": False,
    }

    methods = [
        ("locator.click", lambda: page.locator(selector).first.click(timeout=4000)),
        ("locator.click(force=True)", lambda: page.locator(selector).first.click(force=True, timeout=4000)),
        (
            "evaluate(querySelector.click)",
            lambda: page.evaluate(
                """
                (sel) => {
                    const el = document.querySelector(sel);
                    if (!el) return false;
                    try { el.click(); } catch(e) {}
                    try { el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true })); } catch(e) {}
                    try { el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true })); } catch(e) {}
                    try { el.dispatchEvent(new MouseEvent('click', { bubbles: true })); } catch(e) {}
                    return true;
                }
                """,
                selector,
            ),
        ),
    ]

    for name, fn in methods:
        try:
            value = fn()
            page.wait_for_timeout(900)
            result["attempts"].append({"method": name, "ok": True, "return_value": value})
            result["ok"] = True
            result["winner"] = name
            return result
        except Exception as e:
            result["attempts"].append({"method": name, "ok": False, "error": str(e)})

    return result


def try_click_text(page, text: str) -> Dict[str, Any]:
    selectors = [
        f'text="{text}"',
        f'text={text}',
        f'button:has-text("{text}")',
        f'a:has-text("{text}")',
        f'span:has-text("{text}")',
        f'div:has-text("{text}")',
        f'li:has-text("{text}")',
    ]

    final = {
        "text": text,
        "ok": False,
        "attempts": [],
    }

    for sel in selectors:
        res = try_click_selector(page, sel)
        final["attempts"].append(res)
        if res.get("ok"):
            final["ok"] = True
            final["winner_selector"] = sel
            return final

    return final


def clear_status_filter(page):
    try:
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
    except Exception:
        pass


# =========================
# TESTS
# =========================
def test_page_load(page):
    try:
        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        page.wait_for_timeout(4000)
        humanize(page)

        info = current_page_info(page)
        REPORT["page"] = info
        dump_state(page, "01_page_loaded")
        record_step("page_load", True, info)
        return True
    except Exception as e:
        dump_state(page, "01_page_load_failed")
        record_step("page_load", False, {"error": str(e)})
        return False


def test_open_status(page):
    details = {"strategies": []}

    selector_candidates = [
        "#filterButtonStatus",
        '[data-target="#filterStatus"]',
        '[data-target*="Status"]',
        "#caseStatus2",
    ]

    for sel in selector_candidates:
        res = try_click_selector(page, sel)
        details["strategies"].append({"type": "selector", "value": sel, "result": res, "state": current_filter_state(page)})
        if res.get("ok"):
            dump_state(page, "02_status_opened")
            record_step("open_status", True, {"winner": sel, "state": current_filter_state(page)})
            return True, details

    for txt in ["Case Status", "Status"]:
        res = try_click_text(page, txt)
        details["strategies"].append({"type": "text", "value": txt, "result": res, "state": current_filter_state(page)})
        if res.get("ok"):
            dump_state(page, "02_status_opened")
            record_step("open_status", True, {"winner_text": txt, "state": current_filter_state(page)})
            return True, details

    dump_state(page, "02_status_open_failed")
    record_step("open_status", False, details)
    return False, details


def test_select_active(page):
    details = {"strategies": []}

    strategies = [
        {"kind": "selector", "value": '[data-statusid="192"]'},
        {"kind": "selector", "value": 'a[data-statusid="192"]'},
        {"kind": "selector", "value": '[data-statusid="192"][data-parentid="2"]'},
        {"kind": "selector", "value": 'a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]'},
        {"kind": "text", "value": 'Active'},
    ]

    for strat in strategies:
        clear_status_filter(page)

        if strat["kind"] == "selector":
            res = try_click_selector(page, strat["value"])
        else:
            res = try_click_text(page, strat["value"])

        state = current_filter_state(page)
        item = {
            "strategy": strat,
            "result": res,
            "state": state,
        }
        details["strategies"].append(item)

        if state.get("hidden_filterCaseStatus") == "192":
            dump_state(page, "03_active_selected")
            record_step("select_active", True, item)
            return True, details

    # brute force exato
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
                .filter(x => x.text === 'Active' || x.data_statusid === '192')
                .slice(0, 200);
        }
        """
    )
    save_json("03_active_candidates.json", candidates)

    for i, candidate in enumerate(candidates):
        clear_status_filter(page)

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

                const nodes = Array.from(document.querySelectorAll('a,li,div,span,button'))
                    .filter(visible)
                    .filter(el => {
                        const t = txt(el);
                        return t === 'Active' || el.getAttribute('data-statusid') === '192';
                    });

                const el = nodes[targetIndex];
                if (!el) return false;

                try { el.click(); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('mouseover', { bubbles: true })); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true })); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true })); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('click', { bubbles: true })); } catch(e) {}

                return true;
            }
            """,
            i,
        )
        page.wait_for_timeout(1000)

        state = current_filter_state(page)
        item = {
            "strategy": {"kind": "candidate_index", "value": i, "candidate": candidate},
            "clicked": clicked,
            "state": state,
        }
        details["strategies"].append(item)

        if state.get("hidden_filterCaseStatus") == "192":
            dump_state(page, "03_active_selected")
            record_step("select_active", True, item)
            return True, details

    dump_state(page, "03_active_failed")
    record_step("select_active", False, {"last_strategies": details["strategies"][-20:]})
    return False, details


def test_submit_search(page):
    details = {"strategies": []}

    strategies = [
        {"kind": "selector", "value": 'button:has-text("Process Search")'},
        {"kind": "selector", "value": 'text="Process Search"'},
        {"kind": "selector", "value": 'input[type="submit"][value*="Process Search"]'},
        {"kind": "selector", "value": 'input[type="button"][value*="Process Search"]'},
        {"kind": "selector", "value": "button.filters-submit"},
        {"kind": "selector", "value": 'button:has-text("Search")'},
        {"kind": "selector", "value": 'text="Search"'},
        {"kind": "text", "value": "Process Search"},
        {"kind": "text", "value": "Search"},
    ]

    for strat in strategies:
        if strat["kind"] == "selector":
            res = try_click_selector(page, strat["value"])
        else:
            res = try_click_text(page, strat["value"])

        page.wait_for_timeout(5000)

        rows = page.locator('tr.load-case.table-row.link[data-caseid]').count()
        state = current_filter_state(page)
        item = {
            "strategy": strat,
            "result": res,
            "rows_after": rows,
            "state": state,
        }
        details["strategies"].append(item)

        if rows > 0:
            dump_state(page, "04_search_ok")
            record_step("submit_search", True, item)
            return True, details

    dump_state(page, "04_search_failed")
    record_step("submit_search", False, {"last_strategies": details["strategies"][-20:]})
    return False, details


def test_open_first_detail(page):
    details = {"strategies": []}

    try:
        count = page.locator('tr.load-case.table-row.link[data-caseid]').count()
        if count == 0:
            record_step("open_first_detail", False, {"reason": "no rows"})
            return False, details
    except Exception as e:
        record_step("open_first_detail", False, {"error": str(e)})
        return False, details

    row = page.locator('tr.load-case.table-row.link[data-caseid]').first
    caseid = row.get_attribute("data-caseid") or ""

    methods = [
        ("row.click", lambda: row.click(timeout=4000)),
        ("row.click(force=True)", lambda: row.click(force=True, timeout=4000)),
        ("evaluate(el.click)", lambda: page.evaluate("(el) => el.click()", row.element_handle())),
    ]

    for name, fn in methods:
        try:
            fn()
            page.wait_for_timeout(7000)
            item = {
                "method": name,
                "caseid": caseid,
                "url_after": page.url,
                "body_sample": clean_text(page.locator("body").inner_text(timeout=3000))[:2000],
            }
            details["strategies"].append(item)
            dump_state(page, "05_detail_opened")
            record_step("open_first_detail", True, item)
            return True, details
        except Exception as e:
            details["strategies"].append({"method": name, "caseid": caseid, "error": str(e)})

    dump_state(page, "05_detail_failed")
    record_step("open_first_detail", False, details)
    return False, details


def current_page_num(page) -> int:
    try:
        return page.evaluate(
            """
            () => {
                const body = document.body.innerText || '';
                const m = body.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);
                return m ? parseInt(m[1], 10) : 1;
            }
            """
        )
    except Exception:
        return 1


def test_next_page(page):
    details = {"strategies": []}
    before = current_page_num(page)

    strategies = [
        {"kind": "selector", "value": '[data-page="2"]'},
        {"kind": "selector", "value": 'a[data-page="2"]'},
        {"kind": "selector", "value": 'text="Page 2"'},
        {"kind": "selector", "value": 'text=Page 2'},
        {"kind": "selector", "value": 'div:has-text("Page 2")'},
        {"kind": "selector", "value": 'span:has-text("Page 2")'},
    ]

    for strat in strategies:
        res = try_click_selector(page, strat["value"])
        page.wait_for_timeout(3000)
        after = current_page_num(page)

        item = {
            "strategy": strat,
            "result": res,
            "page_before": before,
            "page_after": after,
        }
        details["strategies"].append(item)

        if after != before:
            dump_state(page, "06_next_page_ok")
            record_step("next_page", True, item)
            return True, details

    # visual next
    visual = page.evaluate(
        """
        () => {
            function txt(el) {
                return ((el.innerText || '').replace(/\\s+/g, ' ')).trim();
            }
            function visible(el) {
                const r = el.getBoundingClientRect();
                const s = window.getComputedStyle(el);
                return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
            }
            function cls(el) {
                return ((el.className || '') + '').toLowerCase();
            }

            const nodes = Array.from(document.querySelectorAll('a,button,span,div,td,img')).filter(visible);

            let best = null;
            for (const el of nodes) {
                const t = txt(el);
                const c = cls(el);
                let score = 0;

                if (t === '>' || t === '›' || t === '»') score += 200;
                if (c.includes('next')) score += 120;
                if (c.includes('right')) score += 80;
                if (c.includes('arrow')) score += 80;
                if (c.includes('chevron')) score += 80;
                if (score <= 0) continue;

                const r = el.getBoundingClientRect();
                const cand = {
                    score,
                    text: t,
                    class_name: c,
                    x: r.left + r.width / 2,
                    y: r.top + r.height / 2
                };

                if (!best || cand.score > best.score) best = cand;
            }
            return best;
        }
        """
    )

    if visual:
        page.mouse.click(visual["x"], visual["y"])
        page.wait_for_timeout(3000)
        after = current_page_num(page)
        item = {
            "strategy": {"kind": "visual", "value": visual},
            "page_before": before,
            "page_after": after,
        }
        details["strategies"].append(item)

        if after != before:
            dump_state(page, "06_next_page_ok")
            record_step("next_page", True, item)
            return True, details

    dump_state(page, "06_next_page_failed")
    record_step("next_page", False, {"last_strategies": details["strategies"][-20:]})
    return False, details


# =========================
# MAIN
# =========================
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
        record_step("launch_browser", True, {"mode": "chrome", "headless": HEADLESS})
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
        record_step("launch_browser", True, {"mode": "chromium_fallback", "headless": HEADLESS, "reason": str(e)})
        return browser


def main():
    ensure_output_dir()

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

        if not test_page_load(page):
            browser.close()
            REPORT["finished_at"] = datetime.utcnow().isoformat()
            save_json("miami_full_probe_report.json", REPORT)
            return

        ok_status, status_details = test_open_status(page)
        REPORT["results"]["open_status"] = status_details

        ok_active = False
        ok_search = False

        if ok_status:
            ok_active, active_details = test_select_active(page)
            REPORT["results"]["select_active"] = active_details

            if ok_active:
                ok_search, search_details = test_submit_search(page)
                REPORT["results"]["submit_search"] = search_details

                if ok_search:
                    ok_detail, detail_details = test_open_first_detail(page)
                    REPORT["results"]["open_first_detail"] = detail_details

                    # voltar para tentar paginação só se detalhe não tiver mudado tudo
                    try:
                        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
                        try:
                            page.wait_for_load_state("networkidle", timeout=15000)
                        except Exception:
                            pass
                        page.wait_for_timeout(3000)
                        humanize(page)

                        # repetir fluxo mínimo até rows
                        test_open_status(page)
                        test_select_active(page)
                        test_submit_search(page)

                        ok_next, next_details = test_next_page(page)
                        REPORT["results"]["next_page"] = next_details
                    except Exception as e:
                        REPORT["results"]["next_page"] = {"error": str(e)}
                        record_step("next_page", False, {"error": str(e)})

        browser.close()

    REPORT["finished_at"] = datetime.utcnow().isoformat()
    save_json("miami_full_probe_report.json", REPORT)
    log.info("Finalizado. Relatório principal: %s", out_path("miami_full_probe_report.json"))


if __name__ == "__main__":
    main()