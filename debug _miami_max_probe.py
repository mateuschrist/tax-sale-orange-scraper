import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
OUTPUT_DIR = os.getenv("MIAMI_DEBUG_DIR", "miami_debug_max_output")

WAIT_SHORT = 800
WAIT_MED = 1800
WAIT_LONG = 5000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("miami_max_probe")

REPORT: Dict[str, Any] = {
    "started_at": datetime.utcnow().isoformat(),
    "config": {
        "headless": HEADLESS,
        "base_url": BASE_URL,
        "list_url": LIST_URL,
        "output_dir": OUTPUT_DIR,
    },
    "steps": [],
    "artifacts": [],
    "network": {
        "requests": [],
        "responses": [],
        "failures": [],
    },
    "results": {},
}


# =========================================================
# FILE HELPERS
# =========================================================
def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def out_path(name: str) -> str:
    ensure_output_dir()
    return os.path.join(OUTPUT_DIR, name)


def save_json(name: str, data: Any):
    path = out_path(name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    REPORT["artifacts"].append(path)
    log.info("Saved json: %s", path)


def save_html(page, name: str):
    path = out_path(name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(page.content())
    REPORT["artifacts"].append(path)
    log.info("Saved html: %s", path)


def save_screenshot(page, name: str):
    path = out_path(name)
    page.screenshot(path=path, full_page=True)
    REPORT["artifacts"].append(path)
    log.info("Saved screenshot: %s", path)


def record_step(step: str, ok: bool, detail: Optional[Dict[str, Any]] = None):
    payload = {
        "step": step,
        "ok": ok,
        "detail": detail or {},
        "timestamp": datetime.utcnow().isoformat(),
    }
    REPORT["steps"].append(payload)
    log.info("%s %s %s", "OK" if ok else "FAIL", step, json.dumps(detail or {}, ensure_ascii=False)[:1200])


# =========================================================
# BASIC HELPERS
# =========================================================
def clean_text(value) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def safe_wait(page, ms: int):
    try:
        page.wait_for_timeout(ms)
    except Exception:
        pass


def humanize(page):
    try:
        page.mouse.move(160, 140)
        safe_wait(page, 250)
        page.mouse.move(380, 260)
        safe_wait(page, 350)
        page.mouse.move(700, 360)
        safe_wait(page, 300)
        page.mouse.wheel(0, 300)
        safe_wait(page, 500)
        page.mouse.wheel(0, -150)
        safe_wait(page, 350)
    except Exception:
        pass


# =========================================================
# NETWORK
# =========================================================
def attach_network_logging(page):
    def on_request(request):
        try:
            REPORT["network"]["requests"].append({
                "ts": datetime.utcnow().isoformat(),
                "method": request.method,
                "url": request.url,
                "resource_type": request.resource_type,
            })
        except Exception:
            pass

    def on_response(response):
        try:
            REPORT["network"]["responses"].append({
                "ts": datetime.utcnow().isoformat(),
                "status": response.status,
                "url": response.url,
            })
        except Exception:
            pass

    def on_request_failed(request):
        try:
            REPORT["network"]["failures"].append({
                "ts": datetime.utcnow().isoformat(),
                "url": request.url,
                "method": request.method,
                "resource_type": request.resource_type,
                "failure": request.failure,
            })
        except Exception:
            pass

    page.on("request", on_request)
    page.on("response", on_response)
    page.on("requestfailed", on_request_failed)


# =========================================================
# PAGE STATE / SCAN
# =========================================================
def current_filter_state(page) -> Dict[str, Any]:
    return page.evaluate(
        """
        () => {
            const hidden = document.querySelector('#filterCaseStatus');
            const label = document.querySelector('#filterCaseStatusLabel');

            const selectedItems = Array.from(document.querySelectorAll('.selected,[aria-selected="true"]'))
                .map(el => ((el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim()))
                .filter(Boolean)
                .slice(0, 100);

            return {
                hidden_filterCaseStatus: hidden ? hidden.value : '',
                label: label ? label.innerText.trim() : '',
                selected_items: selectedItems
            };
        }
        """
    )


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

            const frames = Array.from(document.querySelectorAll('iframe,frame')).map((el, idx) => ({
                idx,
                id: el.id || '',
                name: el.getAttribute('name') || '',
                src: el.getAttribute('src') || '',
                class_name: (el.className || '').toString()
            }));

            const clickables = Array.from(
                document.querySelectorAll('a,button,input[type="button"],input[type="submit"],div,span,li,td,label')
            ).map((el, idx) => ({
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
                aria_label: el.getAttribute('aria-label') || '',
                title: el.getAttribute('title') || '',
            })).filter(x =>
                x.text || x.id || x.class_name || x.data_target || x.data_statusid || x.aria_label || x.title
            );

            const rows = Array.from(document.querySelectorAll('tr.load-case.table-row.link[data-caseid]'))
                .slice(0, 30)
                .map((el, idx) => ({
                    idx,
                    caseid: el.getAttribute('data-caseid') || '',
                    text: txt(el)
                }));

            return {
                url: location.href,
                title: document.title,
                body_sample: (document.body.innerText || '').slice(0, 12000),
                html_length: document.documentElement.outerHTML.length,
                frames,
                frames_count: frames.length,
                clickables_count: clickables.length,
                rows_count: document.querySelectorAll('tr.load-case.table-row.link[data-caseid]').length,
                first_rows: rows,
                status_candidates: clickables.filter(x =>
                    x.text.toLowerCase().includes('status') ||
                    x.id.toLowerCase().includes('status') ||
                    x.class_name.toLowerCase().includes('status') ||
                    x.data_target.toLowerCase().includes('status')
                ).slice(0, 500),
                active_candidates: clickables.filter(x =>
                    x.text === 'Active' ||
                    x.text.toLowerCase().includes('active') ||
                    x.data_statusid === '192'
                ).slice(0, 500),
                search_candidates: clickables.filter(x =>
                    x.text.toLowerCase().includes('search')
                ).slice(0, 400),
                pager_candidates: clickables.filter(x =>
                    x.text.includes('Page') ||
                    x.class_name.toLowerCase().includes('next') ||
                    x.class_name.toLowerCase().includes('pager') ||
                    x.class_name.toLowerCase().includes('page')
                ).slice(0, 500),
                clickables: clickables.slice(0, 5000)
            };
        }
        """
    )


def snapshot(page, prefix: str):
    try:
        save_screenshot(page, f"{prefix}.png")
    except Exception as e:
        log.warning("snapshot screenshot failed %s: %s", prefix, str(e))

    try:
        save_html(page, f"{prefix}.html")
    except Exception as e:
        log.warning("snapshot html failed %s: %s", prefix, str(e))

    try:
        save_json(f"{prefix}.json", scan_dom(page))
    except Exception as e:
        log.warning("snapshot json failed %s: %s", prefix, str(e))


def dom_diff(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    before_set = {
        (x.get("tag", ""), x.get("text", ""), x.get("id", ""), x.get("class_name", ""), x.get("data_statusid", ""))
        for x in before.get("clickables", [])
    }
    after_set = {
        (x.get("tag", ""), x.get("text", ""), x.get("id", ""), x.get("class_name", ""), x.get("data_statusid", ""))
        for x in after.get("clickables", [])
    }

    added = after_set - before_set
    removed = before_set - after_set

    return {
        "before_clickables_count": before.get("clickables_count"),
        "after_clickables_count": after.get("clickables_count"),
        "before_rows_count": before.get("rows_count"),
        "after_rows_count": after.get("rows_count"),
        "before_frames_count": before.get("frames_count"),
        "after_frames_count": after.get("frames_count"),
        "added_samples": list(added)[:150],
        "removed_samples": list(removed)[:150],
    }


# =========================================================
# CLICK ENGINE
# =========================================================
def try_click_selector(page, selector: str, label: str = "") -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "label": label,
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
            safe_wait(page, WAIT_SHORT)

            really_ok = True
            if method_name == "evaluate(querySelector.click)":
                really_ok = bool(isinstance(value, dict) and value.get("found") is True)

            attempt = {
                "method": method_name,
                "ok": really_ok,
                "return_value": value,
                "state": current_filter_state(page),
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
                "state": current_filter_state(page),
            })

    return result


def try_click_text(page, text: str, label: str = "") -> Dict[str, Any]:
    selectors = [
        f'text="{text}"',
        f'text={text}',
        f'button:has-text("{text}")',
        f'a:has-text("{text}")',
        f'span:has-text("{text}")',
        f'div:has-text("{text}")',
        f'li:has-text("{text}")',
        f'label:has-text("{text}")',
    ]

    result: Dict[str, Any] = {
        "label": label,
        "text": text,
        "ok": False,
        "attempts": [],
    }

    for sel in selectors:
        clicked = try_click_selector(page, sel, label=label)
        clicked["used_selector"] = sel
        result["attempts"].append(clicked)
        if clicked.get("ok") is True:
            result["ok"] = True
            result["winner_selector"] = sel
            return result

    return result


def clear_status_filter(page):
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

                const icons = document.querySelectorAll('i');
                icons.forEach(icon => {
                    if (icon.className && icon.className.includes('icon-ok-sign')) {
                        icon.className = icon.className.replace(/\\bicon-ok-sign\\b/g, 'icon-circle-blank').trim();
                    }
                });
            }
            """
        )
        safe_wait(page, 700)
    except Exception:
        pass


# =========================================================
# ASSERTIONS / DETECTORS
# =========================================================
def looks_like_status_panel_opened(before: Dict[str, Any], after: Dict[str, Any], state: Dict[str, Any]) -> bool:
    diff = dom_diff(before, after)

    if after.get("clickables_count", 0) > before.get("clickables_count", 0):
        return True

    if after.get("frames_count", 0) > before.get("frames_count", 0):
        return True

    after_status = after.get("status_candidates", [])
    before_status = before.get("status_candidates", [])
    if len(after_status) > len(before_status):
        return True

    if state.get("label") or state.get("selected_items"):
        return True

    body = after.get("body_sample", "")
    if "Active" in body or "Select One or More Statuses" in body:
        return True

    return False


# =========================================================
# TEST PHASES
# =========================================================
def test_page_load(page) -> bool:
    try:
        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        safe_wait(page, 3500)
        humanize(page)

        meta = current_page_meta(page)
        snapshot(page, "01_page_loaded")
        record_step("page_load", True, meta)
        REPORT["results"]["page_meta"] = meta
        return True
    except Exception as e:
        snapshot(page, "01_page_load_failed")
        record_step("page_load", False, {"error": str(e)})
        return False


def test_open_status(page) -> Dict[str, Any]:
    before = scan_dom(page)
    save_json("02_before_open_status.json", before)

    strategies: List[Dict[str, Any]] = []

    selector_candidates = [
        "#filterButtonStatus",
        '[data-target="#filterStatus"]',
        '[data-target*="Status"]',
        "#caseStatus2",
    ]

    for sel in selector_candidates:
        res = try_click_selector(page, sel, "open_status")
        after = scan_dom(page)
        state = current_filter_state(page)
        item = {
            "kind": "selector",
            "value": sel,
            "result": res,
            "diff": dom_diff(before, after),
            "state": state,
            "really_opened": looks_like_status_panel_opened(before, after, state),
        }
        strategies.append(item)
        if res.get("ok") and item["really_opened"]:
            snapshot(page, "02_status_opened")
            record_step("open_status", True, item)
            return {"ok": True, "strategies": strategies}

    for text in ["Case Status", "Status"]:
        res = try_click_text(page, text, "open_status_text")
        after = scan_dom(page)
        state = current_filter_state(page)
        item = {
            "kind": "text",
            "value": text,
            "result": res,
            "diff": dom_diff(before, after),
            "state": state,
            "really_opened": looks_like_status_panel_opened(before, after, state),
        }
        strategies.append(item)
        if res.get("ok") and item["really_opened"]:
            snapshot(page, "02_status_opened")
            record_step("open_status", True, item)
            return {"ok": True, "strategies": strategies}

    snapshot(page, "02_status_open_failed")
    record_step("open_status", False, {"last": strategies[-10:]})
    return {"ok": False, "strategies": strategies}


def test_select_active(page) -> Dict[str, Any]:
    strategies: List[Dict[str, Any]] = []

    fixed_strategies = [
        ("selector", '[data-statusid="192"]'),
        ("selector", 'a[data-statusid="192"]'),
        ("selector", '[data-statusid="192"][data-parentid="2"]'),
        ("selector", 'a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]'),
        ("text", "Active"),
    ]

    for kind, value in fixed_strategies:
        clear_status_filter(page)

        if kind == "selector":
            res = try_click_selector(page, value, "select_active")
        else:
            res = try_click_text(page, value, "select_active")

        state = current_filter_state(page)
        item = {
            "kind": kind,
            "value": value,
            "result": res,
            "state": state,
        }
        strategies.append(item)

        if state.get("hidden_filterCaseStatus") == "192":
            snapshot(page, "03_active_selected")
            record_step("select_active", True, item)
            return {"ok": True, "strategies": strategies}

    candidates = scan_dom(page).get("active_candidates", [])
    save_json("03_active_candidates.json", candidates)

    for idx, candidate in enumerate(candidates[:150]):
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

                const nodes = Array.from(document.querySelectorAll('a,button,div,span,li,label'))
                    .filter(visible)
                    .filter(el => {
                        const t = txt(el);
                        return t === 'Active' || t.toLowerCase().includes('active') || el.getAttribute('data-statusid') === '192';
                    });

                const el = nodes[targetIndex];
                if (!el) return { ok: false, reason: 'candidate not found' };

                try { el.click(); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('mouseover', { bubbles:true })); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('mousedown', { bubbles:true })); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('mouseup', { bubbles:true })); } catch(e) {}
                try { el.dispatchEvent(new MouseEvent('click', { bubbles:true })); } catch(e) {}

                const innerA = el.querySelector ? el.querySelector('a') : null;
                if (innerA) {
                    try { innerA.click(); } catch(e) {}
                    try { innerA.dispatchEvent(new MouseEvent('mousedown', { bubbles:true })); } catch(e) {}
                    try { innerA.dispatchEvent(new MouseEvent('mouseup', { bubbles:true })); } catch(e) {}
                    try { innerA.dispatchEvent(new MouseEvent('click', { bubbles:true })); } catch(e) {}
                }

                return { ok: true };
            }
            """,
            idx,
        )
        safe_wait(page, 900)

        state = current_filter_state(page)
        item = {
            "kind": "candidate_scan",
            "index": idx,
            "candidate": candidate,
            "clicked": clicked,
            "state": state,
        }
        strategies.append(item)

        if state.get("hidden_filterCaseStatus") == "192":
            snapshot(page, "03_active_selected")
            record_step("select_active", True, item)
            return {"ok": True, "strategies": strategies}

    snapshot(page, "03_active_failed")
    record_step("select_active", False, {"last_strategies": strategies[-25:]})
    return {"ok": False, "strategies": strategies}


def count_rows(page) -> int:
    try:
        return page.locator('tr.load-case.table-row.link[data-caseid]').count()
    except Exception:
        return 0


def test_submit_search(page) -> Dict[str, Any]:
    strategies: List[Dict[str, Any]] = []

    strategy_list = [
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

    for kind, value in strategy_list:
        if kind == "selector":
            res = try_click_selector(page, value, "submit_search")
        else:
            res = try_click_text(page, value, "submit_search")

        safe_wait(page, 5500)

        rows = count_rows(page)
        item = {
            "kind": kind,
            "value": value,
            "result": res,
            "rows_after": rows,
            "state": current_filter_state(page),
        }
        strategies.append(item)

        if rows > 0:
            snapshot(page, "04_search_ok")
            record_step("submit_search", True, item)
            return {"ok": True, "strategies": strategies}

    snapshot(page, "04_search_failed")
    record_step("submit_search", False, {"last_strategies": strategies[-20:]})
    return {"ok": False, "strategies": strategies}


def test_open_first_detail(page) -> Dict[str, Any]:
    strategies: List[Dict[str, Any]] = []

    rows_count = count_rows(page)
    if rows_count == 0:
        record_step("open_first_detail", False, {"reason": "no rows"})
        return {"ok": False, "strategies": strategies}

    row = page.locator('tr.load-case.table-row.link[data-caseid]').first
    caseid = ""
    try:
        caseid = row.get_attribute("data-caseid") or ""
    except Exception:
        pass

    methods = [
        ("row.click", lambda: row.click(timeout=4000)),
        ("row.click(force=True)", lambda: row.click(force=True, timeout=4000)),
        ("element_handle.click", lambda: row.element_handle().click(timeout=4000)),
        ("evaluate(el.click)", lambda: page.evaluate("(el) => el.click()", row.element_handle())),
    ]

    for name, fn in methods:
        try:
            fn()
            safe_wait(page, 7000)

            item = {
                "method": name,
                "caseid": caseid,
                "url_after": page.url,
                "body_sample": current_page_meta(page).get("body_sample", "")[:2500],
            }
            strategies.append(item)

            snapshot(page, "05_detail_opened")
            record_step("open_first_detail", True, item)
            return {"ok": True, "strategies": strategies}
        except Exception as e:
            strategies.append({
                "method": name,
                "caseid": caseid,
                "error": str(e),
            })

    snapshot(page, "05_detail_failed")
    record_step("open_first_detail", False, {"last_strategies": strategies[-10:]})
    return {"ok": False, "strategies": strategies}


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


def test_next_page(page) -> Dict[str, Any]:
    strategies: List[Dict[str, Any]] = []
    before = current_page_num(page)

    strategy_list = [
        ("selector", '[data-page="2"]'),
        ("selector", 'a[data-page="2"]'),
        ("selector", 'text="Page 2"'),
        ("selector", "text=Page 2"),
        ("selector", 'div:has-text("Page 2")'),
        ("selector", 'span:has-text("Page 2")'),
    ]

    for kind, value in strategy_list:
        res = try_click_selector(page, value, "next_page")
        safe_wait(page, 3200)
        after = current_page_num(page)

        item = {
            "kind": kind,
            "value": value,
            "result": res,
            "page_before": before,
            "page_after": after,
        }
        strategies.append(item)

        if after != before:
            snapshot(page, "06_next_page_ok")
            record_step("next_page", True, item)
            return {"ok": True, "strategies": strategies}

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
        try:
            page.mouse.click(visual["x"], visual["y"])
            safe_wait(page, 3000)
            after = current_page_num(page)

            item = {
                "kind": "visual",
                "value": visual,
                "page_before": before,
                "page_after": after,
            }
            strategies.append(item)

            if after != before:
                snapshot(page, "06_next_page_ok")
                record_step("next_page", True, item)
                return {"ok": True, "strategies": strategies}
        except Exception as e:
            strategies.append({
                "kind": "visual",
                "value": visual,
                "error": str(e),
            })

    snapshot(page, "06_next_page_failed")
    record_step("next_page", False, {"last_strategies": strategies[-20:]})
    return {"ok": False, "strategies": strategies}


def rerun_minimal_flow_to_rows(page):
    page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    safe_wait(page, 3000)
    humanize(page)

    test_open_status(page)
    test_select_active(page)
    test_submit_search(page)


def launch_browser(p):
    browser = p.chromium.launch(
        headless=HEADLESS,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    )
    record_step("launch_browser", True, {"mode": "playwright_chromium", "headless": HEADLESS})
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
        attach_network_logging(page)

        if not test_page_load(page):
            browser.close()
            REPORT["finished_at"] = datetime.utcnow().isoformat()
            save_json("miami_max_probe_report.json", REPORT)
            return

        open_status_result = test_open_status(page)
        REPORT["results"]["open_status"] = open_status_result

        if open_status_result.get("ok"):
            select_active_result = test_select_active(page)
            REPORT["results"]["select_active"] = select_active_result

            if select_active_result.get("ok"):
                submit_search_result = test_submit_search(page)
                REPORT["results"]["submit_search"] = submit_search_result

                if submit_search_result.get("ok"):
                    open_detail_result = test_open_first_detail(page)
                    REPORT["results"]["open_first_detail"] = open_detail_result

                    try:
                        rerun_minimal_flow_to_rows(page)
                        next_page_result = test_next_page(page)
                        REPORT["results"]["next_page"] = next_page_result
                    except Exception as e:
                        REPORT["results"]["next_page"] = {"ok": False, "error": str(e)}
                        record_step("next_page", False, {"error": str(e)})

        browser.close()

    REPORT["finished_at"] = datetime.utcnow().isoformat()
    save_json("miami_max_probe_report.json", REPORT)
    log.info("Relatório final: %s", out_path("miami_max_probe_report.json"))


if __name__ == "__main__":
    main()