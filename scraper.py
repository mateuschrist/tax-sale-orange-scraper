import asyncio
from playwright.async_api import async_playwright

LOGIN_URL = "https://or.occompt.com/recorder/web/login.jsp"
SEARCH_URL = "https://or.occompt.com/recorder/tdsmweb/applicationSearch.jsp"


async def scrape_html_text():
    print("🔍 Iniciando Playwright...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 1) Login / aceite
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

        # 2) Página de busca
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
            return None

        print("🖨️ Clicando em Printable Version...")
        await printable.first.click()
        await page.wait_for_load_state("networkidle")

        # 4) Capturar links de Tax Sale
        links = page.locator("#searchResultsTable a:has-text('Tax Sale')")
        link_count = await links.count()
        print(f"🔗 Links de Tax Sale encontrados: {link_count}")

        if link_count == 0:
            await browser.close()
            return None

        # Pegar o primeiro link
        first_link = await links.first.get_attribute("href")

        # Normalizar URL
        BASE = "https://or.occompt.com/recorder/"
        if first_link.startswith("http"):
            full_link = first_link
        else:
            cleaned = first_link.lstrip("./")
            full_link = BASE + cleaned

        print(f"➡️ Acessando primeiro Tax Sale: {full_link}")
        await page.goto(full_link, wait_until="networkidle")

        # 5) Extrair TODO o texto visível da página
        print("📄 Extraindo texto da página HTML...")

        html_text = await page.inner_text("body")

        await browser.close()
        return html_text


def run():
    text = asyncio.run(scrape_html_text())

    if not text:
        print("⚠️ Nenhum texto encontrado.")
        return

    print("\n==================== HTML RAW TEXT ====================\n")
    print(text)
    print("\n==================== FIM ======================\n")


if __name__ == "__main__":
    run()
