import asyncio
import io
import re
from datetime import datetime

import pdfplumber
import requests
from playwright.async_api import async_playwright

ENDPOINT = "https://qeboakaofiqgvbyykvwi.supabase.co/functions/v1/import-properties"
LOGIN_URL = "https://or.occompt.com/recorder/web/login.jsp"
SEARCH_URL = "https://or.occompt.com/recorder/tdsmweb/applicationSearch.jsp"


# -----------------------------
# Helpers
# -----------------------------
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


def extract_situs_and_legal_from_pdf(pdf_bytes):
    """Extrai o Situs Address e Legal Description do PDF."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception:
        return None, None

    # Normalizar
    text = text.replace("\r", "").strip()

    # -----------------------------
    # 1) Extrair SITUS ADDRESS
    # -----------------------------
    situs_regex = r"(Situs Address|Situs)[:\s]*\n(.+?)\n(.+?)\n"
    match = re.search(situs_regex, text, re.IGNORECASE)

    if not match:
        return None, None

    line1 = match.group(2).strip()
    line2 = match.group(3).strip()

    # Extrair cidade, estado, zip
    city_state_zip = re.search(r"([A-Z\s]+),\s*(FL)\s*(\d{5})", line2)
    if not city_state_zip:
        return None, None

    city = city_state_zip.group(1).strip()
    state = city_state_zip.group(2).strip()
    zip_code = city_state_zip.group(3).strip()

    # -----------------------------
    # 2) Extrair LEGAL DESCRIPTION
    # -----------------------------
    legal_regex = r"(Legal Description|Legal)[:\s]*\n([\s\S]*?)(?=\n[A-Z][a-zA-Z ]+?:|\Z)"
    legal_match = re.search(legal_regex, text, re.IGNORECASE)

    legal_description = None
    if legal_match:
        legal_description = legal_match.group(2).strip()

    return {
        "address": line1,
        "city": city,
        "state": state,
        "zip": zip_code,
    }, legal_description


# -----------------------------
# Scraper principal
# -----------------------------
async def scrape_with_playwright():
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
            return []

        print("🖨️ Clicando em Printable Version...")
        await printable.first.click()
        await page.wait_for_load_state("networkidle")

        # 4) Capturar links de Tax Sale
        links = page.locator("#searchResultsTable a:has-text('Tax Sale')")
        link_count = await links.count()
        print(f"🔗 Links de Tax Sale encontrados: {link_count}")

        if link_count == 0:
            await browser.close()
            return []

        hrefs = await links.evaluate_all("els => els.map(e => e.href)")

        data = []

        # 5) Visitar cada página de detalhe
        for idx, href in enumerate(hrefs):
            print(f"\n➡️ ({idx+1}/{len(hrefs)}) Acessando detalhe: {href}")
            await page.goto(href, wait_until="networkidle")

            # Extrair dados básicos
            def get_text_after(label):
                return page.locator(f"text={label}").locator("xpath=following-sibling::*[1]")

            async def safe_text(locator):
                if await locator.count() == 0:
                    return None
                return (await locator.first.inner_text()).strip()

            sale_date = await safe_text(get_text_after("Sale Date:"))
            applicant = await safe_text(get_text_after("Applicant Name:"))
            status = await safe_text(get_text_after("Status:"))
            parcel = await safe_text(get_text_after("Parcel:"))
            min_bid = await safe_text(get_text_after("Min Bid:"))
            high_bid = await safe_text(get_text_after("High Bid:"))

            # 6) Achar link do PDF "Other Documents"
            pdf_link = None
            pdf_locator = page.locator("a:has-text('Other Documents')")
            if await pdf_locator.count() > 0:
                pdf_link = await pdf_locator.first.get_attribute("href")

            if not pdf_link:
                print("⚠️ Sem PDF de Other Documents. Ignorando propriedade.")
                continue

            if not pdf_link.startswith("http"):
                pdf_link = "https://or.occompt.com/recorder/eagleweb/" + pdf_link.lstrip("/")

            print(f"📄 Baixando PDF: {pdf_link}")
            pdf_bytes = requests.get(pdf_link).content

            situs, legal_description = extract_situs_and_legal_from_pdf(pdf_bytes)

            if not situs:
                print("⚠️ Sem Situs Address no PDF. Ignorando propriedade.")
                continue

            # Montar registro final
            row_data = {
                "address": situs["address"],
                "city": situs["city"],
                "state": situs["state"],
                "zip": situs["zip"],
                "county": "Orange",
                "amount_due": clean_money(min_bid),
                "sale_type": "tax_deed",
                "auction_date": parse_date(sale_date),
                "official_link": href,
                "notes": legal_description or "",
            }

            data.append(row_data)
            print("✅ Propriedade adicionada.")

        print(f"\n🗂️ Total final de propriedades válidas: {len(data)}")

        await browser.close()
        return data


# -----------------------------
# Envio para Supabase
# -----------------------------
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


# -----------------------------
# Execução
# -----------------------------
def run():
    data = asyncio.run(scrape_with_playwright())
    if not data:
        print("⚠️ Nenhuma propriedade encontrada.")
        return
    send_to_supabase(data)


if __name__ == "__main__":
    run()
