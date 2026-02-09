import asyncio
import io
import pdfplumber
import requests
from playwright.async_api import async_playwright

LOGIN_URL = "https://or.occompt.com/recorder/web/login.jsp"
SEARCH_URL = "https://or.occompt.com/recorder/tdsmweb/applicationSearch.jsp"


async def scrape_pdf_raw_text():
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

        print(f"➡️ Acessando primeiro Tax Sale: {first_link}")
        await page.goto(first_link, wait_until="networkidle")

        # 5) Achar link do PDF "View Property Information"
        pdf_locator = page.locator("a:has-text('View Property Information')")
        if await pdf_locator.count() == 0:
            print("⚠️ Sem 'View Property Information'.")
            await browser.close()
            return None

        pdf_link = await pdf_locator.first.get_attribute("href")

        if not pdf_link.startswith("http"):
            pdf_link = "https://or.occompt.com/recorder/eagleweb/" + pdf_link.lstrip("/")

        print(f"📄 Baixando PDF: {pdf_link}")
        pdf_bytes = requests.get(pdf_link).content

        # 6) Ler PDF cru
        print("📄 Lendo PDF...")

        pages_text = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                pages_text.append(text)

        await browser.close()
        return pages_text


def run():
    pages = asyncio.run(scrape_pdf_raw_text())

    if not pages:
        print("⚠️ Nenhum PDF lido.")
        return

    print("\n==================== PDF RAW TEXT ====================\n")
    for i, page in enumerate(pages):
        print(f"\n----- PAGE {i+1} -----\n")
        print(page)
    print("\n==================== FIM DO PDF ======================\n")


if __name__ == "__main__":
    run()

