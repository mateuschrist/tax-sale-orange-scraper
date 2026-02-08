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


# -----------------------------
# PARSER ULTRA TOLERANTE
# -----------------------------
def extract_from_property_info_pdf(pdf_bytes):
    """Extrai endereço e legal description de forma ultra tolerante."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception:
        return None, None

    text = text.replace("\r", "").strip()
    lines = [l.strip() for l in text.split("\n")]

    # -----------------------------
    # 1) Achar linha "ADDRESS ON RECORD ON CURRENT TAX ROLL:"
    # -----------------------------
    idx = None
    for i, line in enumerate(lines):
        if "ADDRESS ON RECORD ON CURRENT TAX ROLL:" in line.upper():
            idx = i
            break

    if idx is None:
        return None, None

    # -----------------------------
    # 2) Coletar próximas 5 linhas (ultra tolerante)
    # -----------------------------
    candidates = []
    for j in range(idx + 1, min(idx + 6, len(lines))):
        if lines[j].strip():
            candidates.append(lines[j].strip())

    # Procurar linha da rua (tem número + texto)
    street = None
    for c in candidates:
        if re.match(r"^\d+\s+.+", c):
            street = c
            break

    # Procurar linha da cidade (tem , FL + ZIP)
    city_line = None
    for c in candidates:
        if re.search(r",\s*FL\s*\d{5}", c):
            city_line = c
            break

    if not street or not city_line:
        return None, None

    # Extrair cidade, estado, zip
    m = re.search(r"([A-Za-z\s]+),\s*(FL)\s*(\d{5})", city_line)
    if not m:
        return None, None

    city = m.group(1).strip().upper()
    state = m.group(2).strip()
    zip_code = m.group(3).strip()

    situs = {
        "address": street,
        "city": city,
        "state": state,
        "zip": zip_code,
    }

    # -----------------------------
    # 3) Extrair LEGAL DESCRIPTION (ultra tolerante)
    # -----------------------------
    legal_description = None
    legal_idx = None

    for i, line in enumerate(lines):
        if "LEGAL DESCRIPTION" in line.upper():
            legal_idx = i
            break

    if legal_idx is not None:
        collected = []
        for k in range(legal_idx + 1, len(lines)):
            l = lines[k]

            # Novo bloco em CAPS com dois pontos → parar
            if re.match(r"^[A-Z0-9 \(\)\/]+:\s*$", l):
                break

            if l.strip():
                collected.append(l)

        if collected:
            legal_description = "\n".join(collected).strip()

    return situs, legal_description


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
            min_bid = await safe_text(get_text_after("Min Bid:"))

            # 6) Achar link do PDF "View Property Information"
            pdf_locator = page.locator("a:has-text('View Property Information')")
            if await pdf_locator.count() == 0:
                print("⚠️ Sem 'View Property Information'. Ignorando propriedade.")
                continue

            pdf_link = await pdf_locator.first.get_attribute("href")

            if not pdf_link.startswith("http"):
                pdf_link = "https://or.occompt.com/recorder/eagleweb/" + pdf_link.lstrip("/")

            print(f"📄 Baixando PDF: {pdf_link}")
            pdf_bytes = requests.get(pdf_link).content

            situs, legal_description = extract_from_property_info_pdf(pdf_bytes)

            if not situs:
                print("⚠️ Sem endereço no PDF. Ignorando propriedade.")
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
