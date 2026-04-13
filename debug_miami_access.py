import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright

BASE_URL = "https://miamidade.realtdm.com"
HOME_URL = BASE_URL
LIST_URL = f"{BASE_URL}/public/cases/list"

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
OUT_DIR = os.getenv("MIAMI_DEBUG_DIR", "miami_access_output")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("miami_access")


REPORT: Dict[str, Any] = {
    "started_at": datetime.utcnow().isoformat(),
    "config": {
        "headless": HEADLESS,
        "home_url": HOME_URL,
        "list_url": LIST_URL,
        "out_dir": OUT_DIR,
    },
    "steps": [],
    "network": {
        "requests": [],
        "responses": [],
        "failures": [],
    },
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
        save_json(f"{prefix}.json", page_state(page))
    except Exception as e:
        log.warning("json save failed for %s: %s", prefix, e)


def record(step: str, ok: bool, detail: Optional[Dict[str, Any]] = None) -> None:
    item = {
        "step": step,
        "ok": ok,
        "detail": detail or {},
        "ts": datetime.utcnow().isoformat(),
    }
    REPORT["steps"].append(item)
    log.info("%s %s %s", "OK" if ok else "FAIL", step, json.dumps(detail or {}, ensure_ascii=False)[:1200])


def wait(page, ms: int) -> None:
    try:
        page.wait_for_timeout(ms)
    except Exception:
        pass


def stabilize(page, label: str, ms: int = 15000) -> None:
    try:
        page.wait_for_load_state("domcontentloaded", timeout=20000)
    except Exception:
        pass

    try:
        page.wait_for_load_state("networkidle", timeout=20000)
    except Exception:
        pass

    wait(page, ms)
    log.info("Stabilized: %s (%sms)", label, ms)


def humanize(page) -> None:
    try:
        page.mouse.move(120, 140)
        wait(page, 300)
        page.mouse.move(340, 220)
        wait(page, 400)
        page.mouse.move(680, 320)
        wait(page, 350)
        page.mouse.wheel(0, 220)
        wait(page, 700)
        page.mouse.wheel(0, -100)
        wait(page, 500)
    except Exception:
        pass


def attach_network(page) -> None:
    def on_request(request):
        try:
            REPORT["network"]["requests"].append({
                "ts": datetime.utcnow().isoformat(),
                "method": request.method,
                "url": request.url,
                "resource_type": request.resource_type,
                "headers": dict(request.headers),
            })
        except Exception:
            pass

    def on_response(response):
        try:
            REPORT["network"]["responses"].append({
                "ts": datetime.utcnow().isoformat(),
                "url": response.url,
                "status": response.status,
                "ok": response.ok,
                "headers": dict(response.headers),
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


def body_text(page) -> str:
    try:
        return page.locator("body").inner_text(timeout=5000)
    except Exception:
        return ""


def page_state(page) -> Dict[str, Any]:
    text = body_text(page)
    title = ""
    url = ""
    try:
        title = page.title()
    except Exception:
        pass
    try:
        url = page.url
    except Exception:
        pass

    return {
        "url": url,
        "title": title,
        "body_sample": text[:8000],
        "is_403": "403 forbidden" in text.lower() or "403 forbidden" in title.lower(),
    }


def goto_and_capture(page, url: str, label: str, wait_ms: int = 15000) -> Dict[str, Any]:
    main_status = None
    main_url = url

    def capture_main_response(resp):
        nonlocal main_status, main_url
        try:
            if resp.url.startswith(url):
                main_status = resp.status
                main_url = resp.url
        except Exception:
            pass

    page.on("response", capture_main_response)

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
    except Exception as e:
        record(f"{label}_goto_exception", False, {"url": url, "error": str(e)})

    stabilize(page, label, wait_ms)
    humanize(page)
    snapshot(page, label)

    state = page_state(page)
    result = {
        "requested_url": url,
        "final_url": state.get("url"),
        "main_response_status": main_status,
        "title": state.get("title"),
        "body_sample": state.get("body_sample", "")[:1200],
        "is_403": state.get("is_403"),
    }

    record(label, not result["is_403"], result)
    return result


def build_context(browser):
    ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    )

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Ch-Ua": '"Google Chrome";v="135", "Chromium";v="135", "Not.A/Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }

    context = browser.new_context(
        user_agent=ua,
        extra_http_headers=headers,
        viewport={"width": 1366, "height": 768},
        screen={"width": 1366, "height": 768},
        locale="en-US",
        timezone_id="America/New_York",
        color_scheme="light",
        device_scale_factor=1,
        has_touch=False,
        is_mobile=False,
    )

    context.add_init_script(
        """
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
        Object.defineProperty(navigator, 'language', { get: () => 'en-US' });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        """
    )
    return context, ua, headers


def launch_browser(p):
    try:
        browser = p.chromium.launch(
            channel="chrome",
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-features=IsolateOrigins,site-per-process",
                "--start-maximized",
            ],
        )
        record("launch_browser", True, {"mode": "google_chrome", "headless": HEADLESS})
        return browser
    except Exception as e:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        record("launch_browser", True, {
            "mode": "playwright_chromium_fallback",
            "headless": HEADLESS,
            "reason": str(e),
        })
        return browser


def main():
    ensure_dir()

    with sync_playwright() as p:
        browser = launch_browser(p)
        context, ua, headers = build_context(browser)
        page = context.new_page()
        attach_network(page)

        REPORT["results"]["browser_context"] = {
            "user_agent": ua,
            "extra_http_headers": headers,
            "viewport": {"width": 1366, "height": 768},
            "locale": "en-US",
            "timezone_id": "America/New_York",
        }

        # tentativa 1: home primeiro, depois list
        home_result = goto_and_capture(page, HOME_URL, "01_home", 15000)
        REPORT["results"]["home_first"] = home_result

        list_result = goto_and_capture(page, LIST_URL, "02_list_after_home", 15000)
        REPORT["results"]["list_after_home"] = list_result

        # tentativa 2: nova aba, indo direto para list
        page2 = context.new_page()
        attach_network(page2)
        direct_result = goto_and_capture(page2, LIST_URL, "03_list_direct", 15000)
        REPORT["results"]["list_direct"] = direct_result

        # tentativa 3: home -> esperar -> list novamente
        page3 = context.new_page()
        attach_network(page3)
        goto_and_capture(page3, HOME_URL, "04_home_again", 15000)
        wait(page3, 10000)
        again_result = goto_and_capture(page3, LIST_URL, "05_list_after_extra_wait", 15000)
        REPORT["results"]["list_after_extra_wait"] = again_result

        context.close()
        browser.close()

    REPORT["finished_at"] = datetime.utcnow().isoformat()
    save_json("miami_access_report.json", REPORT)
    log.info("Done: %s", out_path("miami_access_report.json"))


if __name__ == "__main__":
    main()