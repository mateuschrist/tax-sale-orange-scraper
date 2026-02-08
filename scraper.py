import asyncio
from datetime import datetime

from playwright.async_api import async_playwright
import requests

ENDPOINT = "https://qeboakaofiqgvbyykvwi.supabase.co/functions/v1/import-properties"
LOGIN_URL = "https://or.occompt.com/recorder/web/login.jsp"
SEARCH_URL = "https://or.occompt.com/recorder/tdsmweb/applicationSearch.jsp"


def clean_money(v):
    if not v:
        return 0.0
    return float(v.replace("$", "").replace(",", "").strip())


def parse_date(v):
    if not v:
        return None
    for fmt in ("%b %d, %Y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(v.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


async def scrape_with_playwright():
    print("🔍 Iniciando Playwright...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 1) Login / aceite de termos
        print("🌐 Acessando página inicial...")
        await page.goto(LOGIN_URL, wait_until="networkidle")

        if await page.locator("input[value='I Acknowledge']").count() > 0:
            print("🟢 Clicando em 'I Acknowledge'...")
            await page.click("input[value='I Acknowledge']")
            await page.wait_for_load_state("networkidle")

        if await page.locator("button:has-text('Tax Deed Sales')").count() > 0:
            print("🟢 Clicando em 'Tax Deed Sales'...")
            await page.click("button:has-text('Tax Deed Sales')")
            await page.wait_for_load_state("networkidle")

        # 2) Página de busca + filtro Active Sale
        print("🌐 Acessando página de busca...")
        await page.goto(SEARCH_URL, wait_until="networkidle")

        print("🟢 Selecionando 'Active Sale'...")
        await page.select_option("select[name='DeedStatusID']", value="AS")

        print("🔎 Clicando em Search...")
        await page.click("input[value='Search']")
        await page.wait_for_load_state("networkidle")

        # 3) Printable Version
        printable = page.locator("text=Printable Version")
        if await printable.count() == 0:
            print("⚠️ 'Printable Version' não encontrado.")
            await browser.close()
            return []

        print("🖨️ Clicando em Printable Version...")
        await printable.first.click()
        await page.wait_for_load_state("networkidle")

        url_final = page.url
        print(f"📄 URL final (printable): {url_final}")

        # 4) Capturar todos os links "Tax Sale ..."
        links = page.locator("#searchResultsTable a:has-text('Tax Sale')")
        link_count = await links.count()
        print(f"🔗 Links de Tax Sale encontrados: {link_count}")

        if link_count == 0:
            print("⚠️ Nenhum link de Tax Sale encontrado.")
            await browser.close()
            return []

        # Pegar todos os hrefs primeiro (para evitar problemas de navegação)
        hrefs = await links.evaluate_all("els => els.map(e => e.href)")

        data = []

        # 5) Visitar cada página de detalhe
        for idx, href in enumerate(hrefs):
            print(f"➡️ ({idx+1}/{len(hrefs)}) Acessando detalhe: {href}")
            await page.goto(href, wait_until="networkidle")

            # Campos típicos da página de detalhe
            def get_text_after(label):
                return page.locator(f"text={label}").locator("xpath=following-sibling::*[1]")

            async def safe_inner_text(locator):
                if await locator.count() == 0:
                    return None
                return (await locator.first.inner_text()).strip()

            sale_date = await safe_inner_text(get_text_after("Sale Date:"))
            applicant = await safe_inner_text(get_text_after("Applicant Name:"))
            status = await safe_inner_text(get_text_after("Status:"))
            parcel = await safe_inner_text(get_text_after("Parcel:"))
            min_bid = await safe_inner_text(get_text_after("Min Bid:"))
            high_bid = await safe_inner_text(get_text_after("High Bid:"))

            row_data = {
                "address": parcel,
                "city": "Orlando",
                "county": "Orange",
                "state": "FL",
                "amount_due": clean_money(min_bid),
                "sale_type": "tax_deed",
                "auction_date": parse_date(sale_date),
                "official_link": href,
                "notes": f"Applicant: {applicant} | Status: {status} | High Bid: {high_bid}",
            }

            data.append(row_data)

        print(f"🗂️ Total de propriedades extraídas (detalhes): {len(data)}")

        await browser.close()
        return data


def send_to_supabase(data):
    if not data:
        print("⚠️ Nenhuma propriedade para enviar.")
        return

    print("🚀 Enviando dados para a Edge Function...")
    resp = requests.post(
        ENDPOINT,
        json=data,
        headers={"Content-Type": "application/json"},
        timeout=60,
    )

    print("📨 Resposta da Edge Function:")
    print(resp.status_code, resp.text)

    if resp.status_code != 200:
        print("❌ Erro ao enviar para Supabase.")
    else:
        print("✅ Dados enviados com sucesso!")


def run():
    data = asyncio.run(scrape_with_playwright())
    if not data:
        print("⚠️ Nenhuma propriedade encontrada. Encerrando.")
        return
    send_to_supabase(data)


if __name__ == "__main__":
    run()
