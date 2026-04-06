"""
Debug script COMPLETO do Miami-Dade
Extrai TODAS as etapas do scraper original + múltiplas estratégias
Objetivo: Descobrir EXATAMENTE qual método funciona para cada ação
"""
import json
import logging
import re
import time
from datetime import datetime
from playwright.sync_api import sync_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("miami_debug_full")

BASE_URL = "https://miamidade.realtdm.com"
LIST_URL = f"{BASE_URL}/public/cases/list"

REPORT = {
    "timestamp": datetime.now().isoformat(),
    "steps": [],
    "success_methods": {},
    "collected_data": {
        "first_page_cases": [],
        "case_details": [],
        "pagination_info": {},
    },
    "failed_steps": [],
}

# =====================
# HELPERS DO SCRAPER
# =====================
def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()

def clean_multiline(value):
    if value is None:
        return ""
    lines = [re.sub(r"\s+", " ", x).strip(" ,") for x in str(value).splitlines()]
    lines = [x for x in lines if x]
    return ", ".join(lines)

def money_from_text(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"\$[\d,]+(?:\.\d{2})?", text)
    return m.group(0) if m else ""

def parse_row_text(row_text: str):
    """Parse da linha de caso conforme scraper original"""
    parts = re.split(r"\s{2,}|\t", row_text)
    parts = [clean_text(x) for x in parts if clean_text(x)]
    return {
        "status": parts[0] if len(parts) > 0 else "",
        "case_number": parts[1] if len(parts) > 1 else "",
        "date_created": parts[2] if len(parts) > 2 else "",
        "application_number": parts[3] if len(parts) > 3 else "",
        "parcel_number": parts[4] if len(parts) > 4 else "",
        "sale_date": parts[5] if len(parts) > 5 else "",
    }

# =====================
# REPORT SYSTEM
# =====================
def report_step(step_name, success, method="", details="", screenshot_name="", data=None):
    """Registra um passo no relatório"""
    entry = {
        "step": step_name,
        "success": success,
        "method": method,
        "details": details,
        "screenshot": screenshot_name,
        "timestamp": datetime.now().isoformat(),
        "data": data or {}
    }
    REPORT["steps"].append(entry)
    
    if success and method:
        if step_name not in REPORT["success_methods"]:
            REPORT["success_methods"][step_name] = []
        REPORT["success_methods"][step_name].append(method)
    
    status = "✅" if success else "❌"
    log.info(f"{status} {step_name}: {method or 'N/A'} | {details}")

# =====================
# STEP 1: PAGE LOAD
# =====================
def test_page_load(page):
    """Carrega a página"""
    log.info("\n" + "="*80)
    log.info("STEP 1: Carregando página do Miami-Dade")
    log.info("="*80)
    
    try:
        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(6000)
        page.screenshot(path="debug_01_page_loaded.png")
        
        body_text = page.content()
        if "miamidade" in body_text.lower():
            report_step("Page Load", True, "goto + domcontentloaded", 
                       "Página carregada com sucesso", "debug_01_page_loaded.png")
            return True
    except Exception as e:
        report_step("Page Load", False, "goto", f"Erro: {str(e)}")
    
    return False

# =====================
# STEP 2: RESET FILTERS
# =====================
def test_reset_filters(page):
    """Testa RESET FILTERS com todas as estratégias do scraper"""
    log.info("\n" + "="*80)
    log.info("STEP 2: Reset Filters (a.filters-reset)")
    log.info("="*80)
    
    # Método 1: Click normal
    try:
        page.click("a.filters-reset", timeout=6000)
        page.wait_for_timeout(1500)
        page.screenshot(path="debug_02_reset_m1.png")
        report_step("Reset Filters", True, "page.click(selector)", 
                   "Click normal bem-sucedido", "debug_02_reset_m1.png")
        return True
    except Exception as e:
        report_step("Reset Filters", False, "page.click(selector)", str(e))

    # Método 2: Locator force
    try:
        page.locator("a.filters-reset").first.click(force=True, timeout=6000)
        page.wait_for_timeout(1500)
        page.screenshot(path="debug_02_reset_m2.png")
        report_step("Reset Filters", True, "locator.first.click(force=True)", 
                   "Force click bem-sucedido", "debug_02_reset_m2.png")
        return True
    except Exception as e:
        report_step("Reset Filters", False, "locator.first.click(force=True)", str(e))

    # Método 3: JS fallback
    try:
        page.evaluate(
            """(sel) => {
                const el = document.querySelector(sel);
                if (!el) throw new Error(`not found: ${sel}`);
                el.click();
            }""",
            "a.filters-reset",
        )
        page.wait_for_timeout(1500)
        page.screenshot(path="debug_02_reset_m3.png")
        report_step("Reset Filters", True, "page.evaluate(js)", 
                   "JS fallback bem-sucedido", "debug_02_reset_m3.png")
        return True
    except Exception as e:
        report_step("Reset Filters", False, "page.evaluate(js)", str(e))
    
    return False

# =====================
# STEP 3: OPEN FILTER BUTTON
# =====================
def test_filter_button(page):
    """Abre o botão de filtro STATUS"""
    log.info("\n" + "="*80)
    log.info("STEP 3: Open Filter Button (#filterButtonStatus)")
    log.info("="*80)
    
    # Método 1: Click normal
    try:
        page.click("#filterButtonStatus", timeout=6000)
        page.wait_for_timeout(1000)
        page.screenshot(path="debug_03_filter_m1.png")
        report_step("Filter Button", True, "page.click(selector)", 
                   "Click normal bem-sucedido", "debug_03_filter_m1.png")
        return True
    except Exception as e:
        report_step("Filter Button", False, "page.click(selector)", str(e))

    # Método 2: Locator force
    try:
        page.locator("#filterButtonStatus").first.click(force=True, timeout=6000)
        page.wait_for_timeout(1000)
        page.screenshot(path="debug_03_filter_m2.png")
        report_step("Filter Button", True, "locator.first.click(force=True)", 
                   "Force click bem-sucedido", "debug_03_filter_m2.png")
        return True
    except Exception as e:
        report_step("Filter Button", False, "locator.first.click(force=True)", str(e))

    # Método 3: JS
    try:
        page.evaluate(
            """(sel) => {
                const el = document.querySelector(sel);
                if (!el) throw new Error(`not found: ${sel}`);
                el.click();
            }""",
            "#filterButtonStatus",
        )
        page.wait_for_timeout(1000)
        page.screenshot(path="debug_03_filter_m3.png")
        report_step("Filter Button", True, "page.evaluate(js)", 
                   "JS bem-sucedido", "debug_03_filter_m3.png")
        return True
    except Exception as e:
        report_step("Filter Button", False, "page.evaluate(js)", str(e))
    
    return False

# =====================
# STEP 4: CLEAR ALL STATUSES
# =====================
def test_clear_statuses(page):
    """Limpa todos os status selecionados (conforme scraper)"""
    log.info("\n" + "="*80)
    log.info("STEP 4: Clear All Active Statuses")
    log.info("="*80)
    
    try:
        page.evaluate(
            """
            () => {
                const children = document.querySelectorAll('a.filter-status-nosub.status-sub[data-parentid="2"]');
                children.forEach(el => {
                    el.classList.remove('selected');
                    const icon = el.querySelector('i');
                    if (icon) {
                        icon.className = icon.className.replace(/\\bicon-ok-sign\\b/g, '').trim();
                        icon.className = icon.className.replace(/\\bicon-circle-blank\\b/g, '').trim();
                        icon.className = (icon.className + ' icon-circle-blank').trim();
                    }
                });

                const parent = document.querySelector('#caseStatus2');
                if (parent) {
                    parent.classList.remove('selected');
                    const picon = parent.querySelector('i');
                    if (picon) {
                        picon.className = picon.className.replace(/\\bicon-ok-sign\\b/g, '').trim();
                        picon.className = picon.className.replace(/\\bicon-circle-blank\\b/g, '').trim();
                        picon.className = (picon.className + ' icon-circle-blank').trim();
                    }
                }

                const hidden = document.querySelector('#filterCaseStatus');
                if (hidden) hidden.value = '';

                const label = document.querySelector('#filterCaseStatusLabel');
                if (label) label.innerText = 'None Selected';
            }
            """
        )
        page.wait_for_timeout(400)
        page.screenshot(path="debug_04_clear_statuses.png")
        report_step("Clear All Statuses", True, "page.evaluate(js)", 
                   "Statuses limpados", "debug_04_clear_statuses.png")
        return True
    except Exception as e:
        report_step("Clear All Statuses", False, "page.evaluate(js)", str(e))
    
    return False

# =====================
# STEP 5: SELECT STATUS 192
# =====================
def test_select_status_192(page):
    """Seleciona apenas status 192 (ACTIVE)"""
    log.info("\n" + "="*80)
    log.info("STEP 5: Select Only Status 192 (ACTIVE)")
    log.info("="*80)
    
    try:
        page.evaluate(
            """
            () => {
                const el = document.querySelector('a.filter-status-nosub.status-sub[data-statusid="192"][data-parentid="2"]');
                if (!el) throw new Error("Status 192 not found");

                el.classList.add('selected');

                const icon = el.querySelector('i');
                if (icon) {
                    icon.className = icon.className.replace(/\\bicon-circle-blank\\b/g, '').trim();
                    icon.className = icon.className.replace(/\\bicon-ok-sign\\b/g, '').trim();
                    icon.className = (icon.className + ' icon-ok-sign').trim();
                }

                const parent = document.querySelector('#caseStatus2');
                if (parent) {
                    parent.classList.add('selected');
                    const picon = parent.querySelector('i');
                    if (picon) {
                        picon.className = picon.className.replace(/\\bicon-circle-blank\\b/g, '').trim();
                        picon.className = picon.className.replace(/\\bicon-ok-sign\\b/g, '').trim();
                        picon.className = (picon.className + ' icon-ok-sign').trim();
                    }
                }

                const hidden = document.querySelector('#filterCaseStatus');
                if (hidden) hidden.value = '192';

                const label = document.querySelector('#filterCaseStatusLabel');
                if (label) label.innerText = '1 Selected';
            }
            """
        )
        page.wait_for_timeout(800)
        page.screenshot(path="debug_05_select_192.png")
        report_step("Select Status 192", True, "page.evaluate(js)", 
                   "Status 192 selecionado", "debug_05_select_192.png")
        return True
    except Exception as e:
        report_step("Select Status 192", False, "page.evaluate(js)", str(e))
    
    return False

# =====================
# STEP 6: CLICK SEARCH
# =====================
def test_click_search(page):
    """Clica no botão SEARCH com múltiplas estratégias"""
    log.info("\n" + "="*80)
    log.info("STEP 6: Click Search Button")
    log.info("="*80)
    
    selectors = [
        "button.filters-submit",
        "text=Search",
        "button:has-text('Search')",
    ]

    for sel in selectors:
        # Método 1: Click normal com navigation
        try:
            locator = page.locator(sel).first
            if locator.count() == 0:
                continue

            with page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
                locator.click(timeout=6000)
            page.wait_for_timeout(3000)
            page.screenshot(path=f"debug_06_search_m1_{sel[:20]}.png")
            report_step("Click Search", True, f"locator.click() with navigation | {sel}", 
                       "Search com navigation bem-sucedido")
            return True
        except Exception:
            pass

        # Método 2: Click normal sem navigation
        try:
            locator = page.locator(sel).first
            if locator.count() == 0:
                continue
            
            locator.click(timeout=6000)
            page.wait_for_timeout(3000)
            page.screenshot(path=f"debug_06_search_m2_{sel[:20]}.png")
            report_step("Click Search", True, f"locator.click() | {sel}", 
                       "Search normal bem-sucedido")
            return True
        except Exception:
            pass

        # Método 3: Force click
        try:
            locator = page.locator(sel).first
            if locator.count() == 0:
                continue
            
            locator.click(force=True, timeout=6000)
            page.wait_for_timeout(3000)
            page.screenshot(path=f"debug_06_search_m3_{sel[:20]}.png")
            report_step("Click Search", True, f"locator.click(force=True) | {sel}", 
                       "Search force bem-sucedido")
            return True
        except Exception:
            pass

        # Método 4: JavaScript direto
        try:
            page.evaluate(
                f"""(selector) => {{
                    const el = document.querySelector(selector);
                    if (!el) throw new Error('search button not found');
                    el.dispatchEvent(new MouseEvent('mouseover', {{ bubbles: true }}));
                    el.dispatchEvent(new MouseEvent('mousedown', {{ bubbles: true }}));
                    el.dispatchEvent(new MouseEvent('mouseup', {{ bubbles: true }}));
                    el.click();
                }}""",
                sel,
            )
            page.wait_for_timeout(3000)
            page.screenshot(path=f"debug_06_search_m4_{sel[:20]}.png")
            report_step("Click Search", True, f"page.evaluate(js) | {sel}", 
                       "Search JS bem-sucedido")
            return True
        except Exception:
            pass

    report_step("Click Search", False, "Todos os métodos", "Nenhum método funcionou")
    return False

# =====================
# STEP 7: WAIT FOR RESULTS
# =====================
def test_wait_for_results(page):
    """Aguarda e coleta resultados"""
    log.info("\n" + "="*80)
    log.info("STEP 7: Wait for Case Rows (Results)")
    log.info("="*80)
    
    try:
        waited = 0
        while waited < 25000:
            rows = page.locator('tr.load-case.table-row.link[data-caseid]').count()
            if rows > 0:
                log.info(f"✅ Encontrados {rows} casos!")
                page.screenshot(path="debug_07_results.png")
                
                # Coletar dados da primeira página
                collected = []
                for i in range(min(rows, 5)):  # Pega os primeiros 5 casos
                    try:
                        row = page.locator('tr.load-case.table-row.link[data-caseid]').nth(i)
                        caseid = row.get_attribute("data-caseid")
                        row_text = clean_text(row.inner_text())
                        parsed = parse_row_text(row_text)
                        collected.append({
                            "caseid": caseid,
                            "row_text": row_text,
                            "parsed": parsed
                        })
                    except Exception as e:
                        log.warning(f"Erro ao coletar caso {i}: {e}")
                
                REPORT["collected_data"]["first_page_cases"] = collected
                
                report_step("Wait for Results", True, "Locator: tr.load-case", 
                           f"{rows} casos encontrados", "debug_07_results.png",
                           {"total_rows": rows, "collected_samples": len(collected)})
                return True
            
            page.wait_for_timeout(1000)
            waited += 1000
        
        report_step("Wait for Results", False, "Polling", "Timeout esperando resultados")
    except Exception as e:
        report_step("Wait for Results", False, "Polling", str(e))
    
    return False

# =====================
# STEP 8: PAGINATION INFO
# =====================
def test_pagination_info(page):
    """Coleta informações de paginação"""
    log.info("\n" + "="*80)
    log.info("STEP 8: Detect Pagination Info")
    log.info("="*80)
    
    try:
        total_pages = page.evaluate(
            """
            () => {
                const body = document.body.innerText || '';
                const m = body.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);
                if (m) return parseInt(m[2], 10);

                let maxPage = 1;
                document.querySelectorAll('a[data-page]').forEach(a => {
                    const p = parseInt(a.getAttribute('data-page') || '', 10);
                    if (!isNaN(p) && p > maxPage) maxPage = p;
                });
                return maxPage;
            }
            """
        )
        
        current_page = page.evaluate(
            """
            () => {
                const body = document.body.innerText || '';
                const m = body.match(/Page\\s+(\\d+)\\s*\\/\\s*(\\d+)/i);
                if (m) return parseInt(m[1], 10);
                return 1;
            }
            """
        )
        
        REPORT["collected_data"]["pagination_info"] = {
            "current_page": current_page,
            "total_pages": total_pages
        }
        
        report_step("Pagination Info", True, "page.evaluate(js)", 
                   f"Página {current_page}/{total_pages}", data={
                       "current_page": current_page,
                       "total_pages": total_pages
                   })
        return True
    except Exception as e:
        report_step("Pagination Info", False, "page.evaluate(js)", str(e))
    
    return False

# =====================
# STEP 9: OPEN CASE DETAIL
# =====================
def test_open_case_detail(page):
    """Abre detalhe do primeiro caso"""
    log.info("\n" + "="*80)
    log.info("STEP 9: Open Case Detail")
    log.info("="*80)
    
    try:
        rows = page.locator('tr.load-case.table-row.link[data-caseid]')
        if rows.count() == 0:
            report_step("Open Case Detail", False, "Locator", "Nenhum caso encontrado")
            return False
        
        row = rows.first
        caseid = row.get_attribute("data-caseid")
        row_text = clean_text(row.inner_text())
        
        # Método 1: Click normal
        try:
            row.click(timeout=6000)
            page.wait_for_timeout(7000)
            page.screenshot(path="debug_09_case_detail_m1.png")
            report_step("Open Case Detail", True, "row.click()", 
                       f"Caso {caseid} aberto", "debug_09_case_detail_m1.png")
            return True
        except Exception:
            pass

        # Método 2: Force click
        try:
            row.click(force=True, timeout=6000)
            page.wait_for_timeout(7000)
            page.screenshot(path="debug_09_case_detail_m2.png")
            report_step("Open Case Detail", True, "row.click(force=True)", 
                       f"Caso {caseid} aberto", "debug_09_case_detail_m2.png")
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
            page.screenshot(path="debug_09_case_detail_m3.png")
            report_step("Open Case Detail", True, "element_handle.evaluate() + click", 
                       f"Caso {caseid} aberto via JS")
            return True
        except Exception:
            pass

        report_step("Open Case Detail", False, "Todos os métodos", f"Não conseguiu abrir {caseid}")
    except Exception as e:
        report_step("Open Case Detail", False, "Geral", str(e))
    
    return False

# =====================
# STEP 10: PARSE CASE DETAIL
# =====================
def test_parse_case_detail(page):
    """Parse os dados do detalhe do caso"""
    log.info("\n" + "="*80)
    log.info("STEP 10: Parse Case Header & Summary")
    log.info("="*80)
    
    try:
        # Parse header
        header_data = page.evaluate(
            """
            () => {
                const bodyText = document.body.innerText || '';

                function byLabel(label) {
                    const nodes = Array.from(document.querySelectorAll('body *'));
                    for (const node of nodes) {
                        const txt = (node.innerText || '').trim();
                        if (txt === label) {
                            const parent = node.parentElement;
                            if (parent) {
                                const parentText = (parent.innerText || '').trim();
                                if (parentText && parentText !== label) {
                                    return parentText.replace(label, '').trim();
                                }
                            }
                            const next = node.nextElementSibling;
                            if (next) return (next.innerText || '').trim();
                        }
                    }
                    return '';
                }

                const parcelLink = document.querySelector('#propertyAppraiserLink');

                return {
                    case_number: byLabel('Case Number'),
                    parcel_number: byLabel('Parcel Number'),
                    case_status: byLabel('Case Status'),
                    raw_body: bodyText.slice(0, 5000),
                };
            }
            """
        )
        
        # Parse summary
        summary_data = page.evaluate(
            """
            () => {
                function byLabel(label) {
                    const nodes = Array.from(document.querySelectorAll('body *'));
                    for (const node of nodes) {
                        const txt = (node.innerText || '').trim();
                        if (txt === label) {
                            const parent = node.parentElement;
                            if (parent) {
                                const parentText = (parent.innerText || '').trim();
                                if (parentText && parentText !== label) {
                                    return parentText.replace(label, '').trim();
                                }
                            }
                            const next = node.nextElementSibling;
                            if (next) return (next.innerText || '').trim();
                        }
                    }
                    return '';
                }

                return {
                    app_receive_date: byLabel('App Receive Date'),
                    sale_date: byLabel('Sale Date'),
                    publish_dates: byLabel('Publish Date(s)'),
                    property_address: byLabel('Property Address'),
                    homestead: byLabel('Homestead'),
                    legal_description: byLabel('Legal Description')
                };
            }
            """
        )
        
        case_data = {
            "header": header_data,
            "summary": summary_data,
        }
        
        REPORT["collected_data"]["case_details"] = case_data
        
        report_step("Parse Case Detail", True, "page.evaluate(js)", 
                   f"Caso parseado: {header_data.get('case_number', 'N/A')}", data=case_data)
        page.screenshot(path="debug_10_case_parsed.png")
        return True
    except Exception as e:
        report_step("Parse Case Detail", False, "page.evaluate(js)", str(e))
    
    return False

# =====================
# MAIN
# =====================
def main():
    log.info("🚀 INICIANDO DEBUG COMPLETO DO MIAMI-DADE (COM TODOS OS DADOS)")
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
            # Executar todos os testes em sequência
            if not test_page_load(page):
                log.error("❌ Falha no carregamento da página!")
                return
            
            test_reset_filters(page)
            test_filter_button(page)
            test_clear_statuses(page)
            test_select_status_192(page)
            test_click_search(page)
            test_wait_for_results(page)
            test_pagination_info(page)
            test_open_case_detail(page)
            test_parse_case_detail(page)
            
        except Exception as e:
            log.exception(f"❌ Erro geral: {e}")
        finally:
            browser.close()
    
    # =====================
    # SALVAR RELATÓRIO
    # =====================
    log.info("\n" + "="*80)
    log.info("📋 SALVANDO RELATÓRIO COMPLETO")
    log.info("="*80)
    
    with open("debug_miami_report.json", "w", encoding="utf-8") as f:
        json.dump(REPORT, f, indent=2, ensure_ascii=False)
    
    log.info("✅ Relatório salvo em: debug_miami_report.json")
    
    # =====================
    # RESUMO FINAL
    # =====================
    log.info("\n" + "="*80)
    log.info("📊 RESUMO FINAL")
    log.info("="*80)
    
    log.info(f"Total de passos testados: {len(REPORT['steps'])}")
    log.info(f"\n✅ Métodos bem-sucedidos encontrados:")
    for step, methods in REPORT['success_methods'].items():
        log.info(f"   {step}:")
        for method in methods:
            log.info(f"      • {method}")
    
    log.info(f"\n📊 Dados coletados:")
    log.info(f"   • Primeiros 5 casos: {len(REPORT['collected_data']['first_page_cases'])}")
    log.info(f"   • Paginação: {REPORT['collected_data']['pagination_info']}")
    log.info(f"   • Detalhe do caso: {'Sim' if REPORT['collected_data']['case_details'] else 'Não'}")
    
    log.info("\n🎯 PRÓXIMO PASSO: Abra o arquivo 'debug_miami_report.json' para análise detalhada")

if __name__ == "__main__":
    main()
