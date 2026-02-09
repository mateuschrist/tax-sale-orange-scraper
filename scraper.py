import asyncio
import re
from playwright.async_api import async_playwright

LOGIN_URL = "https://or.occompt.com/recorder/web/login.jsp"
SEARCH_URL = "https://or.occompt.com/recorder/tdsmweb/applicationSearch.jsp"
BASE = "https://or.occompt.com/recorder/"


# -----------------------------
# Helpers
# -----------------------------
def extract(pattern, text):
    m = re.search(pattern, text, re.IGNORECASE)
    return m.group(1).strip() if m else None


def clean_money(v):
    if not v:
        return None
    return float(v.replace("$", "").replace(",", "").strip())


def has_address_block(text):
    return "ADDRESS ON RECORD ON CURRENT TAX ROLL" in text.upper()


def parse_property_block(text):
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    address = None
    city = None
    state = "FL"
    zip_code = None

    idx = None
    for i, l in enumerate(lines):
        if "ADDRESS ON RECORD ON CURRENT TAX ROLL" in l.upper():
            idx = i
            break

    if idx is not None:
        for j in range(idx + 1, idx + 6):
            if j >= len(lines):
                break
            l = lines[j]

            if re.match(r"^\d+\s+.+", l):
                address = l

            m = re.search(r"([A-Za-z\s]+),\s*FL\s*(\d{5})", l)
            if m:
                city = m.group(1).strip().upper()
                zip_code = m.group(2).strip()

    return {
        "address": address,
        "city": city,
        "state": state,
        "zip": zip_code,
    }


def parse_tax_sale(text):
    return {
        "parcel_number": extract(r"Parcel Number\s+([0-9\-]+)", text),
        "sale_date": extract(r"Sale Date\s+([0-9/]+)", text),
        "opening_bid": clean_money(extract(r"Opening Bid Amount\$?([0-9\.,]+)", text)),
        "application_number": extract(r"Tax Deed Application Number\s+([0-9\-]+)", text),
        "deed_status": extract(r"Deed Status\s+([A-Za-z ]+)", text),
        "homestead": extract(r"Homestead\?\s*([A-Za-z]+)", text),
    }


# -----------------------------
# SCRAPER PRINCIPAL
# -----------------------------
async def scrape_properties(limit=3):
    print("🔍 Iniciando Playwright...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # LOGIN
        print("🌐 Acessando página inicial...")
        await page.goto(LOGIN_URL, wait_until="networkidle")

        if await page.locator("input[value='I Acknowledge']").count() > 0:
            print("🟢 Clicando em 'I Acknowledge'...")
            await page.click("input[value='I Acknowledge']")
            await page.wait_for_load_state("networkidle")

        print("🟢 Clicando em 'Tax Deed Sales'...")
        await page.click("button:has-text('Tax Deed Sales')")
        await page.wait_for_load_state("networkidle")

        # BUSCA
        print("🌐 Acessando página de busca...")
        await page.goto(SEARCH_URL, wait_until="networkidle")

        print("🟢 Selecionando 'Active Sale'...")
        await page.select_option("select[name='DeedStatusID']", value="AS")

        print("🔎 Clicando em Search...")
        await page.click("input[value='Search']")
        await page.wait_for_load_state("networkidle")

        # PRINTABLE VERSION
        print("🖨️ Clicando em Printable Version...")
        await page.locator("text=Printable Version").first.click()
        await page.wait_for_load_state("networkidle")

        results = []

        for idx in range(limit):
            print(f"\n================ PROPRIEDADE {idx+1}/{limit} ================")

            links = page.locator("#searchResultsTable a:has-text('Tax Sale')")
            print("🔗 Links na lista:", await links.count())

            print("➡️ Clicando no link do Tax Sale...")
            await links.nth(idx).click()
            await page.wait_for_load_state("networkidle")

            # Ler dados do lote
            tax_text = await page.inner_text("body")
            tax_data = parse_tax_sale(tax_text)

            # PEGAR LINK CORRETO
            print("➡️ Clicando no link 'View Property Information'")
            all_links = await page.locator("a:has-text('View Property Information')").all()

            href = None
            for link in all_links:
                url = await link.get_attribute("href")
                if url and "/eagleweb/" in url:
                    href = url
                    break

            if not href:
                print("⚠️ Nenhum link válido encontrado.")
                prop_data = {"address": None, "city": None, "state": "FL", "zip": None}
            else:
                href_full = href if href.startswith("http") else BASE + href.lstrip("./")
                await page.goto(href_full, wait_until="networkidle")

                print("⏳ Aguardando 10 segundos...")
                await page.wait_for_timeout(10000)

                html_text = await page.inner_text("body")

                if has_address_block(html_text):
                    prop_data = parse_property_block(html_text)
                else:
                    print("⚠️ Bloco não encontrado.")
                    prop_data = {"address": None, "city": None, "state": "FL", "zip": None}

            # VOLTAR
            print("↩️ Voltando para Tax Sale...")
            await page.go_back(wait_until="networkidle")

            print("↩️ Voltando para Printable Version...")
            await page.go_back(wait_until="networkidle")

            final = {**tax_data, **prop_data}
            results.append(final)

        await browser.close()
        return results


def run():
    properties = asyncio.run(scrape_properties(limit=3))

    print("\n\n================ RESULTADOS FINAIS ================")
    for i, prop in enumerate(properties, start=1):
        print(f"\n--- PROPRIEDADE {i} ---")
        for k, v in prop.items():
            print(f"{k}: {v}")


if __name__ == "__main__":
    run()
