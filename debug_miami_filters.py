"""
Script para debugar os seletores de filtros da página Miami-Dade
"""
import logging
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("debug")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"

def debug_filters():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chrome",
            headless=False,  # Mostrar o navegador
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
            viewport={"width": 1366, "height": 900},
        )

        page = context.new_page()
        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(6000)

        # Tirar screenshot da página inicial
        page.screenshot(path="debug_page_initial.png")
        log.info("✅ Screenshot inicial salvo: debug_page_initial.png")

        # Procurar por qualquer elemento que contenha "reset"
        log.info("\n🔍 Procurando elementos com 'reset'...")
        reset_elements = page.query_selector_all('[class*="reset"], [id*="reset"], [title*="reset"]')
        log.info(f"Encontrados {len(reset_elements)} elementos com 'reset'")
        for i, el in enumerate(reset_elements):
            tag = el.evaluate("el => el.tagName")
            class_name = el.get_attribute("class") or ""
            id_attr = el.get_attribute("id") or ""
            text = el.inner_text() or ""
            log.info(f"  [{i}] <{tag}> class='{class_name}' id='{id_attr}' text='{text}'")

        # Procurar por qualquer link ou botão na seção de filtros
        log.info("\n🔍 Procurando elementos na seção de filtros...")
        filter_elements = page.query_selector_all('a, button')
        log.info(f"Total de links/botões encontrados: {len(filter_elements)}")
        
        # Mostrar apenas os primeiros 20 que podem ser relevantes
        relevant = []
        for el in filter_elements:
            text = (el.inner_text() or "").strip()
            class_name = el.get_attribute("class") or ""
            if any(keyword in text.lower() + class_name.lower() 
                   for keyword in ["reset", "filter", "clear", "search", "submit"]):
                relevant.append({
                    "tag": el.evaluate("el => el.tagName"),
                    "class": class_name,
                    "text": text,
                    "selector": el.evaluate("el => el.getAttribute('class') ? '.' + el.getAttribute('class').split(' ')[0] : el.tagName")
                })
        
        log.info(f"\n✅ Elementos relevantes encontrados: {len(relevant)}")
        for i, el in enumerate(relevant):
            log.info(f"  [{i}] <{el['tag']}> class='{el['class']}' text='{el['text']}'")

        # Salvar HTML da página para análise
        html_content = page.content()
        with open("debug_page_html.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        log.info("\n✅ HTML da página salvo: debug_page_html.html")

        # Procurar especificamente por "filters-reset"
        log.info("\n🔍 Procurando especificamente por 'filters-reset'...")
        try:
            element = page.query_selector("a.filters-reset")
            if element:
                log.info("✅ Encontrado: a.filters-reset")
            else:
                log.warning("❌ NÃO encontrado: a.filters-reset")
        except Exception as e:
            log.warning(f"❌ Erro ao procurar: {e}")

        # Procurar por variações do seletor
        selectors_to_try = [
            "a.filters-reset",
            "button.filters-reset",
            "[class*='filters-reset']",
            "a[href*='reset']",
            "button[class*='reset']",
            "a[class*='reset']",
            ".filters-reset",
            "#reset-filters",
        ]
        
        log.info("\n🔍 Testando variações de seletores...")
        for selector in selectors_to_try:
            try:
                element = page.query_selector(selector)
                status = "✅ ENCONTRADO" if element else "❌ não encontrado"
                log.info(f"  {status}: {selector}")
            except Exception as e:
                log.warning(f"  ❌ Erro: {selector} - {e}")

        browser.close()
        log.info("\n✅ Debug concluído!")

if __name__ == "__main__":
    debug_filters()
