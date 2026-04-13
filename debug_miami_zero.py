import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright

URL = "https://miamidade.realtdm.com/public/cases/list"
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
OUT_DIR = os.getenv("MIAMI_DEBUG_DIR", "miami_zero_output")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("miami_zero")


REPORT: Dict[str, Any] = {
    "started_at": datetime.utcnow().isoformat(),
    "config": {
        "url": URL,
        "headless": HEADLESS,
        "out_dir": OUT_DIR,
    },
    "steps": [],
    "results": {},
    "artifacts": [],
}


def ensure_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def out_path(name: str) -> str:
    ensure_dir()
    return os.path.join(OUT_DIR, name)


def save_json(name: str, data: Any) -> None:
    path = out_path(name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    REPORT["artifacts"].append(path)
    log.info("Saved json: %s", path)


def save_html(page, name: str) -> None:
    path = out_path(name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(page.content())
    REPORT["artifacts"].append(path)
    log.info("Saved html: %s", path)


def save_png(page, name: str) -> None:
    path = out_path(name)
    page.screenshot(path=path, full_page=True)
    REPORT["artifacts"].append(path)
    log.info("Saved screenshot: %s", path)


def snapshot(page, prefix: str) -> None:
    try:
        save_png(page, f"{prefix}.png")
    except Exception as e:
        log.warning("screenshot failed for %s: %s", prefix, e)

    try:
        save_html(page, f"{prefix}.html")
    except Exception as e:
        log.warning("html save failed for %s: %s", prefix, e)

    try:
        save_json(f"{prefix}.json", scan_dom(page))
    except Exception as e:
        log.warning("scan save failed for %s: %s", prefix, e)


def record(step: str, ok: bool, detail: Optional[Dict[str, Any]] = None) -> None:
    item = {
        "step": step,
        "ok": ok,
        "detail": detail or {},
        "ts": datetime.utcnow().isoformat(),
    }
    REPORT["steps"].append(item)
    log.info("%s %s %s", "OK" if ok else "FAIL", step, json.dumps(detail or {}, ensure_ascii=False)[:1000])


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()

def current_page_meta(page) -> Dict[str, Any]:
    try:
        title = page.title()
    except Exception:
        title = ""

    try:
        url = page.url
    except Exception:
        url = ""

    body_text = ""
    try:
        body_text = page.locator("body").inner_text(timeout=4000)
    except Exception:
        pass

    return {
        "url": url,
        "title": title,
        "body_sample": clean_text(body_text)[:8000],
    }


def wait(page, ms: int) -> None:
    try:
        page.wait_for_timeout(ms)
    except Exception:
        pass


def humanize(page) -> None:
    try:
        page.mouse.move(180, 140)
        wait(page, 250)
        page.mouse.move(430, 260)
        wait(page, 300)
        page.mouse.move(760, 380)
        wait(page, 250)
        page.mouse.wheel(0, 250)
        wait(page, 400)
        page.mouse.wheel(0, -120)
        wait(page, 300)
    except Exception:
        pass


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

            const clickables = Array.from(document.querySelectorAll(
                'a,button,input[type="button"],input[type="submit"],div,span,li,td,label,i'
            )).map((el, idx) => ({
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
                aria_label: el.getAttribute('aria-label') || '',
                title: el.getAttribute('title') || '',
            })).filter(x =>
                x.text || x.id || x.class_name || x.data_statusid || x.aria_label || x.title || x.data_target
            );

            const rows = Array.from(document.querySelectorAll('tr.load-case.table-row.link[data-caseid]'))
                .map((el, idx) => ({
                    idx,
                    caseid: el.getAttribute('data-caseid') || '',
                    text: txt(el)
                }))
                .slice(0, 100);

            return {
                url: location.href,
                title: document.title,
                body_sample: (document.body.innerText || '').slice(0, 12000),
                clickables_count: clickables.length,
                rows_count: document.querySelectorAll('tr.load-case.table-row.link[data-caseid]').length,
                clickables: clickables.slice(0, 5000),
                rows: rows,
                case_status_candidates: clickables.filter(x =>
                    x.text.toLowerCase().includes('case status') ||
                    x.id.toLowerCase().includes('status') ||
                    x.class_name.toLowerCase().includes('status')
                ).slice(0, 300),
                active_candidates: clickables.filter(x =>
                    x.text === 'Active' ||
                    x.text.toLowerCase().includes('active') ||
                    x.data_statusid === '192'
                ).slice(0, 500),
                process_search_candidates: clickables.filter(x =>
                    x.text.toLowerCase().includes('process search') ||
                    x.text.toLowerCase() === 'search'
                ).slice(0, 300),
                pager_candidates: clickables.filter(x =>
                    x.text.includes('Page') ||
                    x.class_name.toLowerCase().includes('chevron') ||
                    x.class_name.toLowerCase().includes('next') ||
                    x.aria_label.toLowerCase().includes('next')
                ).slice(0, 300)
            };
        }
        """
    )


def filter_state(page) -> Dict[str, Any]:
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


def try_click_selector(page, selector: str, name: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "name": name,
        "selector": selector,
        "ok": False,
        "attempts": [],
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
                    if (!el) return { found: false };
                    try { el.click(); } catch(e) {}
                    try { el.dispatchEvent(new MouseEvent('mouseover', { bubbles:true })); } catch(e) {}
                    try { el.dispatchEvent(new MouseEvent('mousedown', { bubbles:true })); } catch(e) {}
                    try { el.dispatchEvent(new MouseEvent('mouseup', { bubbles:true })); } catch(e) {}
                    try { el.dispatchEvent(new MouseEvent('click', { bubbles:true })); } catch(e) {}
                    return { found: true };
                }
                """,
                selector,
            ),
        ),
    ]

    for method_name, fn in methods:
        try:
            value = fn()
            wait(page, 900)

            really_ok = True
            if method_name == "evaluate(querySelector.click)":
                really_ok = bool(isinstance(value, dict) and value.get("found") is True)

            attempt = {
                "method": method_name,
                "ok": really_ok,
                "return_value": value,
                "filter_state": filter_state(page),
            }
            result["attempts"].append(attempt)

            if really_ok:
                result["ok"] = True
                result["winner"] = method_name
                return result

        except Exception as e:
            result["attempts"].append({
                "method": method_name,
                "ok": False,
                "error": str(e),
                "filter_state": filter_state(page),
            })

    return result


def try_click_text(page, text: str, name: str) -> Dict[str, Any]:
    selectors = [
        f'text="{text}"',
        f'text={text}',
        f'button:has-text("{text}")',
        f'a:has-text("{text}")',
        f'div:has-text("{text}")',
        f'span:has-text("{text}")',
        f'li:has-text("{text}")',
        f'label:has-text("{text}")',
    ]

    all_attempts: List[Dict[str, Any]] = []
    for sel in selectors:
        res = try_click_selector(page, sel, name)
        res["used_selector"] = sel
        all_attempts.append(res)
        if res.get("ok"):
            return {
                "name": name,
                "text": text,
                "ok": True,
                "winner_selector": sel,
                "attempts": all_attempts,
            }

    return {
        "name": name,
        "text": text,
        "ok": False,
        "attempts": all_attempts,
    }


def page_numbers_from_rows(page) -> Dict[str, Any]:
    rows = page.locator('tr.load-case.table-row.link[data-caseid]')
    count = rows.count()

    case_numbers: List[str] = []
    raw_rows: List[Dict[str, str]] = []

    for i in range(count):
        row = rows.nth(i)
        text = clean_text(row.inner_text())
        raw_rows.append({"index": str(i), "text": text})

        # pega só o CASE # visível na linha
        # ex: "Active 2024-12345 ..."
        parts = re.split(r"\s{2,}|\t", text)
        parts = [clean_text(x) for x in parts if clean_text(x)]
        if len(parts) >= 2:
            case_numbers.append(parts[1])

    return {
        "rows_count": count,
        "case_numbers": case_numbers,
        "rows_sample": raw_rows[:20],
    }


def open_case_status(page) -> Dict[str, Any]:
    before = scan_dom(page)
    save_json("01_before_case_status.json", before)

    strategies: List[Dict[str, Any]] = []

    candidates = [
        ("selector", "#filterButtonStatus"),
        ("selector", '[data-target="#filterStatus"]'),
        ("selector", '[data-target*="Status"]'),
        ("text", "Case Status"),
        ("text", "Status"),
    ]

    for kind, value in candidates:
        if kind == "selector":
            res = try_click_selector(page, value, "open_case_status")
        else:
            res = try_click_text(page, value, "open_case_status")

        after = scan_dom(page)
        item = {
            "kind": kind,
            "value": value,
            "result": res,
            "before_clickables": before.get("clickables_count"),
            "after_clickables": after.get("clickables_count"),
            "before_status_candidates": len(before.get("case_status_candidates", [])),
            "after_status_candidates": len(after.get("case_status_candidates", [])),
            "filter_state": filter_state(page),
        }
        strategies.append(item)

        opened = (
            res.get("ok")
            and (
                after.get("clickables_count", 0) > before.get("clickables_count", 0)
                or len(after.get("active_candidates", [])) > len(before.get("active_candidates", []))
                or "Active" in after.get("body_sample", "")
            )
        )

        if opened:
            snapshot(page, "02_case_status_opened")
            return {"ok": True, "winner": item, "attempts": strategies}

    snapshot(page, "02_case_status_failed")
    return {"ok": False, "attempts": strategies}


def clear_status(page) -> None:
    try:
        page.evaluate(
            """
            () => {
                const nodes = document.querySelectorAll('[data-statusid], .selected, [aria-selected="true"]');
                nodes.forEach(el => el.classList.remove('selected'));

                const hidden = document.querySelector('#filterCaseStatus');
                if (hidden) hidden.value = '';

                const label = document.querySelector('#filterCaseStatusLabel');
                if (label) label.innerText = '';
            }
            """
        )
        wait(page, 500)
    except Exception:
        pass


def select_active_192_only(page) -> Dict[str, Any]:
    attempts: List[Dict[str, Any]] = []

    fixed = [
        ("selector", '[data-statusid="192"]'),
        ("selector", 'a[data-statusid="192"]'),
        ("selector", '[data-statusid="192"][data-parentid="2"]'),
        ("selector", 'a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]'),
        ("text", "Active"),
    ]

    for kind, value in fixed:
        clear_status(page)

        if kind == "selector":
            res = try_click_selector(page, value, "select_active_192")
        else:
            res = try_click_text(page, value, "select_active_192")

        state = filter_state(page)
        item = {
            "kind": kind,
            "value": value,
            "result": res,
            "filter_state": state,
        }
        attempts.append(item)

        if state.get("hidden_filterCaseStatus") == "192":
            snapshot(page, "03_active_192_success")
            return {"ok": True, "winner": item, "attempts": attempts}

    # varredura total dos candidatos
    candidates = scan_dom(page).get("active_candidates", [])
    save_json("03_active_candidates_full.json", candidates)

    for idx, candidate in enumerate(candidates[:150]):
        clear_status(page)

        click_result = page.evaluate(
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

                const nodes = Array.from(document.querySelectorAll('a,button,div,span,li,label'))
                    .filter(visible)
                    .filter(el => {
                        const t = txt(el);
                        return t === 'Active' || t.toLowerCase().includes('active') || el.getAttribute('data-statusid') === '192';
                    });

                const el = nodes[targetIndex];
                if (!el) return { clicked: false, reason: 'not found' };

                try { el.click(); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('mouseover', { bubbles:true })); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('mousedown', { bubbles:true })); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('mouseup', { bubbles:true })); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('click', { bubbles:true })); } catch(e) {}

                const inner = el.querySelector ? el.querySelector('a') : null;
                if (inner) {
                    try { inner.click(); } catch(e) {}
                    try { inner.dispatchEvent(new MouseEvent('mousedown', { bubbles:true })); } catch(e) {}
                    try { inner.dispatchEvent(new MouseEvent('mouseup', { bubbles:true })); } catch(e) {}
                    try { inner.dispatchEvent(new MouseEvent('click', { bubbles:true })); } catch(e) {}
                }

                return { clicked: true };
            }
            """,
            idx,
        )
        wait(page, 900)

        state = filter_state(page)
        item = {
            "kind": "candidate_scan",
            "index": idx,
            "candidate": candidate,
            "click_result": click_result,
            "filter_state": state,
        }
        attempts.append(item)

        if state.get("hidden_filterCaseStatus") == "192":
            snapshot(page, "03_active_192_success")
            return {"ok": True, "winner": item, "attempts": attempts}

    snapshot(page, "03_active_192_failed")
    return {"ok": False, "attempts": attempts}


def submit_search(page) -> Dict[str, Any]:
    attempts: List[Dict[str, Any]] = []

    strategies = [
        ("selector", 'button:has-text("Process Search")'),
        ("selector", 'text="Process Search"'),
        ("selector", 'input[type="submit"][value*="Process Search"]'),
        ("selector", 'input[type="button"][value*="Process Search"]'),
        ("selector", "button.filters-submit"),
        ("selector", 'button:has-text("Search")'),
        ("selector", 'text="Search"'),
        ("text", "Process Search"),
        ("text", "Search"),
    ]

    for kind, value in strategies:
        if kind == "selector":
            res = try_click_selector(page, value, "submit_search")
        else:
            res = try_click_text(page, value, "submit_search")

        wait(page, 5000)
        rows_info = page_numbers_from_rows(page)

        item = {
            "kind": kind,
            "value": value,
            "result": res,
            "rows_count": rows_info["rows_count"],
            "case_numbers": rows_info["case_numbers"],
            "filter_state": filter_state(page),
        }
        attempts.append(item)

        if rows_info["rows_count"] > 0:
            snapshot(page, "04_search_success")
            return {"ok": True, "winner": item, "attempts": attempts, "rows": rows_info}

    snapshot(page, "04_search_failed")
    return {"ok": False, "attempts": attempts}


def next_page(page) -> Dict[str, Any]:
    before = page_numbers_from_rows(page)
    attempts: List[Dict[str, Any]] = []

    strategies = [
        ("selector", 'i.fa-regular.fa-chevron-right'),
        ("selector", 'text="Page 2"'),
        ("selector", "text=Page 2"),
        ("selector", '[data-page="2"]'),
        ("selector", 'a[data-page="2"]'),
        ("selector", '[aria-label*="Next"]'),
        ("selector", '[class*="next"]'),
    ]

    for kind, value in strategies:
        res = try_click_selector(page, value, "next_page")
        wait(page, 3500)
        after = page_numbers_from_rows(page)

        item = {
            "kind": kind,
            "value": value,
            "result": res,
            "before_rows": before["rows_count"],
            "after_rows": after["rows_count"],
            "before_case_numbers": before["case_numbers"][:10],
            "after_case_numbers": after["case_numbers"][:10],
        }
        attempts.append(item)

        changed = (
            after["rows_count"] > 0 and after["case_numbers"] != before["case_numbers"]
        )
        if changed:
            snapshot(page, "05_next_page_success")
            return {"ok": True, "winner": item, "attempts": attempts, "page_2": after}

    # fallback: clicar no pai do ícone
    try:
        clicked = page.evaluate(
            """
            () => {
                const icon = document.querySelector('i.fa-regular.fa-chevron-right[aria-hidden="true"]');
                if (!icon) return { found: false };

                const parent = icon.closest('a,button,span,div,td,li');
                const target = parent || icon;

                try { target.click(); } catch(e) {}
                try { target.dispatchEvent(new MouseEvent('mousedown', { bubbles:true })); } catch(e) {}
                try { target.dispatchEvent(new MouseEvent('mouseup', { bubbles:true })); } catch(e) {}
                try { target.dispatchEvent(new MouseEvent('click', { bubbles:true })); } catch(e) {}

                return { found: true, used_parent: !!parent };
            }
            """
        )
        wait(page, 3500)
        after = page_numbers_from_rows(page)

        item = {
            "kind": "js_parent_of_icon",
            "value": 'i.fa-regular.fa-chevron-right[aria-hidden="true"]',
            "result": clicked,
            "before_rows": before["rows_count"],
            "after_rows": after["rows_count"],
            "before_case_numbers": before["case_numbers"][:10],
            "after_case_numbers": after["case_numbers"][:10],
        }
        attempts.append(item)

        changed = after["rows_count"] > 0 and after["case_numbers"] != before["case_numbers"]
        if changed:
            snapshot(page, "05_next_page_success")
            return {"ok": True, "winner": item, "attempts": attempts, "page_2": after}
    except Exception as e:
        attempts.append({"kind": "js_parent_of_icon", "error": str(e)})

    snapshot(page, "05_next_page_failed")
    return {"ok": False, "attempts": attempts}


def launch_browser(p):
    # tenta Google Chrome instalado pelo workflow
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
        record("launch_browser", True, {"mode": "google_chrome", "headless": HEADLESS})
        return browser
    except Exception as e:
        # fallback seguro
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        record("launch_browser", True, {"mode": "playwright_chromium_fallback", "headless": HEADLESS, "reason": str(e)})
        return browser


def main():
    ensure_dir()

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

        page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        wait(page, 4000)
        humanize(page)

        snapshot(page, "00_initial")
        REPORT["results"]["initial_scan"] = scan_dom(page)
        record("page_load", True, current_page_meta(page))

        open_status_result = open_case_status(page)
        REPORT["results"]["open_case_status"] = open_status_result
        record("open_case_status", open_status_result["ok"], open_status_result.get("winner"))

        if open_status_result["ok"]:
            active_result = select_active_192_only(page)
            REPORT["results"]["select_active_192_only"] = active_result
            record("select_active_192_only", active_result["ok"], active_result.get("winner"))

            if active_result["ok"]:
                search_result = submit_search(page)
                REPORT["results"]["submit_search"] = search_result
                record("submit_search", search_result["ok"], search_result.get("winner"))

                if search_result["ok"]:
                    page_1 = search_result["rows"]
                    REPORT["results"]["page_1_list"] = page_1

                    next_result = next_page(page)
                    REPORT["results"]["next_page"] = next_result
                    record("next_page", next_result["ok"], next_result.get("winner"))

                    if next_result["ok"]:
                        REPORT["results"]["page_2_list"] = next_result["page_2"]

        browser.close()

    REPORT["finished_at"] = datetime.utcnow().isoformat()
    save_json("miami_zero_report.json", REPORT)
    log.info("Done: %s", out_path("miami_zero_report.json"))


if __name__ == "__main__":
    main()
