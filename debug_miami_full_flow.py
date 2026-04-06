"""
Debug script para testar TODOS os passos de navegação do Miami-Dade
Testa múltiplas estratégias para cada ação e retorna um relatório completo
"""
import json
import logging
import time
from datetime import datetime
from playwright.sync_api import sync_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("miami_debug")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"

REPORT = {
    "timestamp": datetime.now().isoformat(),
    "steps": [],
    "success_methods": {},
    "failed_steps": [],
}

def report_step(step_name, success, method="", details="", screenshot_name=""):
    """Registra um passo no relatório"""
    entry = {
        "step": step_name,
        "success": success,
        "method": method,
        "details": details,
        "screenshot": screenshot_name,
        "timestamp": datetime.now().isoformat(),
    }
    REPORT["steps"].append(entry)
    
    if success and method:
        if step_name not in REPORT["success_methods"]:
            REPORT["success_methods"][step_name] = []
        REPORT["success_methods"][step_name].append(method)
    
    status = "✅" if success else "❌"
    log.info(f"{status} {step_name}: {method or 'N/A'} | {details}")

def test_page_load(page):
    """Testa carregamento da página"""
    log.info("\n" + "="*80)
    log.info("STEP 1: Carregando página do Miami-Dade")
    log.info("="*80)
    
    try:
        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(6000)
        page.screenshot(path="debug_01_page_loaded.png")
        
        # Verificar se página carregou
        body_text = page.content()
        if "miamidade" in body_text.lower():
            report_step("Page Load", True, "goto + domcontentloaded", 
                       "Página carregada com sucesso", "debug_01_page_loaded.png")
            return True
    except Exception as e:
        report_step("Page Load", False, "goto", f"Erro: {str(e)}")
        return False
    
    return False

def test_reset_filters(page):
    """Testa todas as maneiras de clicar no botão RESET FILTERS"""
    log.info("\n" + "="*80)
    log.info("STEP 2: Testando click em RESET FILTERS")
    log.info("="*80)
    
    # Método 1: Locator direto
    try:
        loc = page.locator("a.filters-reset").first
        if loc.count() > 0:
            loc.click(timeout=5000)
            page.wait_for_timeout(1500)
            page.screenshot(path="debug_02_reset_method1.png")
            report_step("Reset Filters", True, "Locator: a.filters-reset", 
                       "Click normal bem-sucedido")
            return True
    except Exception as e:
        report_step("Reset Filters", False, "Locator: a.filters-reset", str(e))

    # Método 2: Force click
    try:
        loc = page.locator("a.filters-reset").first
        if loc.count() > 0:
            loc.click(force=True, timeout=5000)
            page.wait_for_timeout(1500)
            page.screenshot(path="debug_02_reset_method2.png")
            report_step("Reset Filters", True, "Locator: a.filters-reset (force)", 
                       "Force click bem-sucedido")
            return True
    except Exception as e:
        report_step("Reset Filters", False, "Locator: a.filters-reset (force)", str(e))

    # Método 3: JavaScript direto
    try:
        result = page.evaluate(
            """() => {
                const el = document.querySelector('a.filters-reset');
                if (!el) return false;
                el.click();
                return true;
            }"""
        )
        if result:
            page.wait_for_timeout(1500)
            page.screenshot(path="debug_02_reset_method3.png")
            report_step("Reset Filters", True, "JavaScript: document.querySelector + click", 
                       "JS direto bem-sucedido")
            return True
    except Exception as e:
        report_step("Reset Filters", False, "JavaScript: querySelector + click", str(e))

    # Método 4: Mouse events via JS
    try:
        page.evaluate(
            """() => {
                const el = document.querySelector('a.filters-reset');
                if (!el) return false;
                el.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
                el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
                el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
                el.dispatchEvent(new MouseEvent('click', { bubbles: true }));
                return true;
            }"""
        )
        page.wait_for_timeout(1500)
        page.screenshot(path="debug_02_reset_method4.png")
        report_step("Reset Filters", True, "JavaScript: Synthetic mouse events", 
                   "Mouse events bem-sucedidos")
        return True
    except Exception as e:
        report_step("Reset Filters", False, "JavaScript: Synthetic mouse events", str(e))

    # Método 5: Procurar por variações do seletor
    selectors = [
        "button.filters-reset",
        ".filters-reset",
        "[class*='filters-reset']",
        "a[class*='reset']",
    ]
    
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0:
                loc.click(timeout=5000)
                page.wait_for_timeout(1500)
                page.screenshot(path=f"debug_02_reset_method5_{sel.replace('[', '').replace(']', '')}.png")
                report_step("Reset Filters", True, f"Locator: {sel}", 
                           f"Seletor alternativo encontrado")
                return True
        except Exception:
            continue
    
    report_step("Reset Filters", False, "Todos os métodos", "Nenhum método funcionou")
    return False

def test_filter_button(page):
    """Testa todas as maneiras de abrir o filtro de STATUS"""
    log.info("\n" + "="*80)
    log.info("STEP 3: Testando click em FILTER BUTTON (Status)")
    log.info("="*80)
    
    # Método 1: Locator direto
    try:
        loc = page.locator("#filterButtonStatus").first
        if loc.count() > 0:
            loc.click(timeout=5000)
            page.wait_for_timeout(1000)
            page.screenshot(path="debug_03_filter_method1.png")
            report_step("Filter Button", True, "Locator: #filterButtonStatus", 
                       "Click normal bem-sucedido")
            return True
    except Exception as e:
        report_step("Filter Button", False, "Locator: #filterButtonStatus", str(e))

    # Método 2: Force click
    try:
        loc = page.locator("#filterButtonStatus").first
        if loc.count() > 0:
            loc.click(force=True, timeout=5000)
            page.wait_for_timeout(1000)
            page.screenshot(path="debug_03_filter_method2.png")
            report_step("Filter Button", True, "Locator: #filterButtonStatus (force)", 
                       "Force click bem-sucedido")
            return True
    except Exception as e:
        report_step("Filter Button", False, "Locator: #filterButtonStatus (force)", str(e))

    # Método 3: JavaScript
    try:
        result = page.evaluate(
            """() => {
                const el = document.querySelector('#filterButtonStatus');
                if (!el) return false;
                el.click();
                return true;
            }"""
        )
        if result:
            page.wait_for_timeout(1000)
            page.screenshot(path="debug_03_filter_method3.png")
            report_step("Filter Button", True, "JavaScript: querySelector + click", 
                       "JS direto bem-sucedido")
            return True
    except Exception as e:
        report_step("Filter Button", False, "JavaScript: querySelector + click", str(e))

    # Método 4: Mouse events
    try:
        page.evaluate(
            """() => {
                const el = document.querySelector('#filterButtonStatus');
                if (!el) return false;
                el.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
                el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
                el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
                el.dispatchEvent(new MouseEvent('click', { bubbles: true }));
                return true;
            }"""
        )
        page.wait_for_timeout(1000)
        page.screenshot(path="debug_03_filter_method4.png")
        report_step("Filter Button", True, "JavaScript: Synthetic mouse events", 
                   "Mouse events bem-sucedidos")
        return True
    except Exception as e:
        report_step("Filter Button", False, "JavaScript: Synthetic mouse events", str(e))

    report_step("Filter Button", False, "Todos os métodos", "Nenhum método funcionou")
    return False

def test_search_button(page):
    """Testa todas as maneiras de clicar no botão SEARCH"""
    log.info("\n" + "="*80)
    log.info("STEP 4: Testando click em SEARCH BUTTON")
    log.info("="*80)
    
    selectors = [
        ("button.filters-submit", "button.filters-submit"),
        ("text=Search", "text=Search"),
        ("button:has-text('Search')", "button:has-text"),
    ]
    
    for sel_name, sel in selectors:
        # Método 1: Click normal
        try:
            loc = page.locator(sel).first
            if loc.count() > 0:
                loc.click(timeout=6000)
                page.wait_for_timeout(3000)
                page.screenshot(path=f"debug_04_search_method1_{sel_name}.png")
                report_step("Search Button", True, f"Locator: {sel_name}", 
                           "Click normal bem-sucedido")
                return True
        except Exception:
            pass

        # Método 2: Force click
        try:
            loc = page.locator(sel).first
            if loc.count() > 0:
                loc.click(force=True, timeout=6000)
                page.wait_for_timeout(3000)
                page.screenshot(path=f"debug_04_search_method2_{sel_name}.png")
                report_step("Search Button", True, f"Locator: {sel_name} (force)", 
                           "Force click bem-sucedido")
                return True
        except Exception:
            pass

        # Método 3: JavaScript
        try:
            result = page.evaluate(
                f"""() => {{
                    const el = document.querySelector('{sel if sel.startswith('.') or sel.startswith('#') else 'button'}');
                    if (!el) return false;
                    el.click();
                    return true;
                }}"""
            )
            if result:
                page.wait_for_timeout(3000)
                page.screenshot(path=f"debug_04_search_method3_{sel_name}.png")
                report_step("Search Button", True, f"JavaScript: {sel_name}", 
                           "JS direto bem-sucedido")
                return True
        except Exception:
            pass

    report_step("Search Button", False, "Todos os métodos", "Nenhum método funcionou")
    return False

def test_wait_for_results(page):
    """Testa se consegue detectar resultados"""
    log.info("\n" + "="*80)
    log.info("STEP 5: Aguardando resultados de busca")
    log.info("="*80)
    
    try:
        waited = 0
        max_wait = 30000
        step = 1000
        
        while waited < max_wait:
            rows = page.locator('tr.load-case.table-row.link[data-caseid]').count()
            if rows > 0:
                log.info(f"✅ Encontrados {rows} casos!")
                page.screenshot(path="debug_05_results_found.png")
                report_step("Wait for Results", True, "Locator: tr.load-case", 
                           f"{rows} casos encontrados")
                return True
            
            page.wait_for_timeout(step)
            waited += step
        
        report_step("Wait for Results", False, "Polling", "Timeout esperando resultados")
    except Exception as e:
        report_step("Wait for Results", False, "Polling", str(e))
    
    return False

def test_pagination(page):
    """Testa navegação entre páginas"""
    log.info("\n" + "="*80)
    log.info("STEP 6: Testando navegação de páginas")
    log.info("="*80)
    
    # Detectar página atual
    try:
        current_page = page.evaluate(
            """() => {
                const body = document.body.innerText || '';
                const m = body.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);
                if (m) return { current: parseInt(m[1], 10), total: parseInt(m[2], 10) };
                return null;
            }"""
        )
        
        if current_page:
            log.info(f"Página atual: {current_page['current']} de {current_page['total']}")
            report_step("Detect Pagination", True, "JavaScript: regex body text", 
                       f"Página {current_page['current']}/{current_page['total']}")
            
            # Se não estamos na última página, testa ir para próxima
            if current_page['current'] < current_page['total']:
                next_page = current_page['current'] + 1
                
                # Método 1: Clicar no link direto a[data-page="X"]
                try:
                    loc = page.locator(f'a[data-page="{next_page}"]').first
                    if loc.count() > 0:
                        loc.click(timeout=5000)
                        page.wait_for_timeout(3000)
                        page.screenshot(path=f"debug_06_pagination_method1.png")
                        report_step("Navigate to Next Page", True, 
                                   f"Locator: a[data-page='{next_page}']", 
                                   "Click direto no link bem-sucedido")
                        return True
                except Exception:
                    pass

                # Método 2: Force click
                try:
                    loc = page.locator(f'a[data-page="{next_page}"]').first
                    if loc.count() > 0:
                        loc.click(force=True, timeout=5000)
                        page.wait_for_timeout(3000)
                        page.screenshot(path=f"debug_06_pagination_method2.png")
                        report_step("Navigate to Next Page", True, 
                                   f"Locator: a[data-page='{next_page}'] (force)", 
                                   "Force click bem-sucedido")
                        return True
                except Exception:
                    pass

                # Método 3: JavaScript
                try:
                    page.evaluate(
                        f"""() => {{
                            const el = document.querySelector('a[data-page="{next_page}"]');
                            if (!el) throw new Error('Page link not found');
                            el.click();
                        }}"""
                    )
                    page.wait_for_timeout(3000)
                    page.screenshot(path=f"debug_06_pagination_method3.png")
                    report_step("Navigate to Next Page", True, 
                               "JavaScript: querySelector + click", 
                               "JS direto bem-sucedido")
                    return True
                except Exception:
                    pass

                report_step("Navigate to Next Page", False, "Todos os métodos", 
                           "Não conseguiu navegar para próxima página")
            else:
                report_step("Navigate to Next Page", True, "N/A", 
                           "Já está na última página")
                return True
        else:
            report_step("Detect Pagination", False, "JavaScript", 
                       "Não conseguiu detectar paginação")
    except Exception as e:
        report_step("Detect Pagination", False, "JavaScript", str(e))
    
    return False

def test_case_detail(page):
    """Testa abertura de detalhe de caso"""
    log.info("\n" + "="*80)
    log.info("STEP 7: Testando abertura de detalhe de caso")
    log.info("="*80)
    
    try:
        # Pegar primeiro caso
        rows = page.locator('tr.load-case.table-row.link[data-caseid]')
        if rows.count() == 0:
            report_step("Open Case Detail", False, "Locator", "Nenhum caso encontrado")
            return False
        
        first_row = rows.first
        caseid = first_row.get_attribute("data-caseid")
        log.info(f"Clicando no caso: {caseid}")
        
        # Método 1: Click normal
        try:
            first_row.click(timeout=6000)
            page.wait_for_timeout(7000)
            page.screenshot(path="debug_07_case_detail_method1.png")
            report_step("Open Case Detail", True, "Click normal no row", 
                       f"Caso {caseid} aberto")
            return True
        except Exception:
            pass

        # Método 2: Force click
        try:
            first_row.click(force=True, timeout=6000)
            page.wait_for_timeout(7000)
            page.screenshot(path="debug_07_case_detail_method2.png")
            report_step("Open Case Detail", True, "Force click no row", 
                       f"Caso {caseid} aberto")
            return True
        except Exception:
            pass

        # Método 3: JavaScript
        try:
            page.evaluate(
                f"""() => {{
                    const row = document.querySelector('tr.load-case.table-row.link[data-caseid="{caseid}"]');
                    if (!row) throw new Error('Row not found');
                    row.click();
                }}"""
            )
            page.wait_for_timeout(7000)
            page.screenshot(path="debug_07_case_detail_method3.png")
            report_step("Open Case Detail", True, "JavaScript: querySelector + click", 
                       f"Caso {caseid} aberto via JS")
            return True
        except Exception:
            pass

        report_step("Open Case Detail", False, "Todos os métodos", 
                   f"Não conseguiu abrir caso {caseid}")
    except Exception as e:
        report_step("Open Case Detail", False, "Geral", str(e))
    
    return False

def main():
    log.info("🚀 INICIANDO DEBUG COMPLETO DO MIAMI-DADE")
    log.info("="*80)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chrome",
            headless=False,
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
            locale="en-US",
            timezone_id="America/New_York",
        )

        page = context.new_page()

        try:
            # Executar todos os testes
            if not test_page_load(page):
                log.error("Falha no carregamento da página!")
                return
            
            test_reset_filters(page)
            test_filter_button(page)
            test_search_button(page)
            test_wait_for_results(page)
            test_pagination(page)
            test_case_detail(page)
            
        except Exception as e:
            log.exception(f"Erro geral: {e}")
        finally:
            browser.close()
    
    # Salvar relatório
    log.info("\n" + "="*80)
    log.info("📋 SALVANDO RELATÓRIO")
    log.info("="*80)
    
    with open("debug_miami_report.json", "w", encoding="utf-8") as f:
        json.dump(REPORT, f, indent=2, ensure_ascii=False)
    
    log.info("✅ Relatório salvo em: debug_miami_report.json")
    log.info("\n📊 RESUMO:")
    log.info(f"Total de passos testados: {len(REPORT['steps'])}")
    log.info(f"Métodos bem-sucedidos encontrados: {len(REPORT['success_methods'])}")
    for step, methods in REPORT['success_methods'].items():
        log.info(f"  ✅ {step}: {', '.join(methods)}")
    
    if REPORT['failed_steps']:
        log.warning(f"\n⚠️ Passos que falharam:")
        for step in REPORT['failed_steps']:
            log.warning(f"  ❌ {step}")

if __name__ == "__main__":
    main()
