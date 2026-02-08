import asyncio
import json
from datetime import datetime

from playwright.async_api import async_playwright
import requests

ENDPOINT = "https://qeboakaofiqgvbyykvwi.supabase.co/functions/v1/import-properties"
SEARCH_URL = "https://or.occompt.com/recorder/tdsmweb/applicationSearch.jsp"


def clean_money(v):
    if not v:
        return 0.0
    return float(v.replace("$", "").replace(",", "").strip())


def parse_date(v):
    if not v:
        return None
    try:
        dt = datetime.strptime(v.strip(), "%b %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


async def scrape_with_playwright():
    print("🔍 Iniciando Playwright...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        print("🌐 Acessando página inicial...")
        await page.goto("https://or.occompt.com/recorder/web/login.jsp", wait_until="networkidle")

        # 1️⃣ Clicar no botão "I Acknowledge"
        if await page.locator("input[value='I Acknowledge']").count() > 0:
            print("🟢 Clicando em 'I Acknowledge'...")
            await page.click("input[value='I Acknowledge']")
            await page.wait_for_load_state("networkidle")

        # 2️⃣ Clicar no botão "Tax Deed Sales" (se aparecer)
        if await page.locator("button:has-text('Tax Deed Sales')").count() > 0:
            print("🟢 Clicando em 'Tax Deed Sales'...")
            await page.click("button:has-text('Tax Deed Sales')")
            await page.wait_for_load_state("networkidle")

        # 3️⃣ Agora estamos na página de busca
        print("🌐 Acessando página de busca...")
        await page.goto(SEARCH_URL, wait_until="networkidle")

        # 4️⃣ Clicar em "Search" (sem filtros)
        print("🔎 Clicando em Search...")
        await page.click("input[value='Search']")
        await page.wait_for_load_state("networkidle")

        # 5️⃣ Clicar em "Printable Version"
        printable = page.locator("text=Printable Version")
        if await printable.count() > 0:
            print("🖨️ Clicando em Printable Version...")
            await printable.first.click()
            await page.wait_for_load_state("networkidle")

        url_final = page.url
        print(f"📄 URL final: {url_final}")

        rows = page.locator("#searchResultsTable tbody tr")
        row_count = await rows.count()
        print(f"📄 Linhas encontradas: {row_count}")

        data = []

        for i in range(row_count):
            row = rows.nth(i)
            cols = row.locator("td")
            if await cols.count() < 2:
                continue

            desc_text = (await cols.nth(0).inner_text()).strip()
            parts = desc_text.split()
            tax_sale_id = parts[-1] if parts else None

            details_text = (await cols.nth(1).inner_text()).splitlines()
            details_text = [t.strip() for t in details_text if t.strip()]

            sale_date = applicant = status = parcel = min_bid = high_bid = None

            for line in details_text:
                if line.startswith("Sale Date:"):
                    sale_date = line.replace("Sale Date:", "").strip()
                elif line.startswith("Applicant Name:"):
                    applicant = line.replace("Applicant Name:", "").strip()
                elif line.startswith("Status:"):
                    status = line.replace("Status:", "").strip()
                elif line.startswith("Parcel:"):
                    parcel = line.replace("Parcel:", "").strip()
                elif line.startswith("Min Bid:"):
                    min_bid = line.replace("Min Bid:", "").strip()
                elif line.startswith("High Bid:"):
                    high_bid = line.replace("High Bid:", "").strip()

            row_data = {
                "address": parcel,
                "city": "Orlando",
                "county": "Orange",
                "state": "FL",
                "amount_due": clean_money(min_bid),
                "sale_type": "tax_deed",
                "auction_date": parse_date(sale_date),
                "official_link": url_final,
                "notes": f"Tax Sale: {tax_sale_id} | Applicant: {applicant} | Status: {status} | High Bid: {high_bid}",
            }

            data.append(row_data)

        print(f"🗂️ Total de propriedades extraídas: {len(data)}")

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
