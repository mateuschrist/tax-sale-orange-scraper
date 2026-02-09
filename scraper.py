import asyncio
import re
from playwright.async_api import async_playwright

LOGIN_URL = "https://or.occompt.com/recorder/web/login.jsp"
SEARCH_URL = "https://or.occompt.com/recorder/tdsmweb/applicationSearch.jsp"
BASE = "https://or.occompt.com/recorder/"


# -----------------------------
# Helpers
# -----------------------------
def extract_first_match(pattern, text, flags=re.IGNORECASE):
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None


def clean_money(v):
    if not v:
        return None
    return float(v.replace("$", "").replace(",", "").strip())


def parse_date_us(v):
    return v


# -----------------------------
# PARSER DO HTML PRINCIPAL (TAX SALE)
# -----------------------------
def parse_main_html(text: str):
    data = {}

    data["application_number"] = extract_first_match(
        r"Tax Deed Application Number\s+([0-9\-]+)", text
    )
    data["deed_status"] = extract_first_match(
        r"Deed Status\s+([A-Za-z ]+)", text
    )
    data["parcel_number"] = extract_first_match(
        r"Parcel Number\s+([0-9\-]+)", text
    )
    data["sale_date_raw"] = extract_first_match(
        r"Sale Date\s+([0-9/]+)", text
    )
    data["opening_bid_raw"] = extract_first_match(
        r"Opening Bid Amount\$?([0-9\.,]+)", text
    )
    data["homestead"] = extract_first_match(
        r"Homestead\?\s*([A-Za-z]+)", text
    )
    data["appraised_value_raw"] = extract_first_match(
        r"Property Appraised Value\$?([0-9\.,]+)", text
    )

    return data


# -----------------------------
# PARSER DO BLOCO DE ADDRESS / OWNER / LEGAL
# -----------------------------
def parse_property_block(text: str):
    data = {}

    lines = [l.strip() for l in text.splitlines() if l.strip()]

    address = None
    city = None
    state = "FL"
    zip_code = None
    owner = None
    legal_description = None

    # 1) ADDRESS ON RECORD ON CURRENT TAX ROLL
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

    # 2) OWNER
    for l in lines:
        if "OWNER" in l.upper():
            m = re.search(r"Owner[:\-]?\s*(.+)", l, re.IGNORECASE)
            if m:
                owner = m.group(1).strip()
            break

    # 3) LEGAL DESCRIPTION
    legal_idx = None
    for i, l in enumerate(lines):
        if "LEGAL DESCRIPTION" in l.upper():
            legal_idx = i
            break

    if legal_idx is not None:
        collected = []
        for k in range(legal_idx + 1, len(lines)):
            l = lines[k]
            if re.match(r"^[A-Z0-9 \(\)\/]+:\s*$", l):
                break
            collected.append(l)
        if collected:
            legal_description = "\n".join(collected).strip()

    data["address"] = address
    data["city"] = city
    data["state"] = state
    data["zip"] = zip_code
    data["owner"] = owner
    data["legal_description"] = legal_description

    return data


def has_address_block(text: str) -> bool:
    return "ADDRESS ON RECORD ON CURRENT TAX ROLL" in text.upper()


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

        if await page.locator("button:has-text('Tax Deed Sales')").count() > 0:
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
        printable = page.locator("text=Printable Version")
        print("🖨️ Clicando em Printable Version...")
        await printable.first.click()
        await page.wait_for_load_state("networkidle")

        results = []

        for idx in range(limit):
            print(f"\n================ PROPRIEDADE {idx+1}/{limit} ================")

            links = page.locator("#searchResultsTable a:has-text('Tax Sale')")
            link_count = await links.count()
            print("🔗 Links na lista:", link_count)

            if idx >= link_count:
                print("⚠️ Índice maior que quantidade de links. Parando.")
                break

            # 1) Clicar no lote
            print("➡️ Clicando no link do Tax Sale...")
            await links.nth(idx).click()
            await page.wait_for_load_state("networkidle")

            # 2) HTML principal (Tax Sale)
            main_text = await page.inner_text("body")
            main_data = parse_main_html(main_text)

            # 3) Pegar SOMENTE o link correto de View Property Information
            print("🔍 Buscando link correto de 'View Property Information'...")

            all_links = await page.locator("a:has-text('View Property Information')").all()

            href = None
            for link in all_links:
                url = await link.get_attribute("href")
                if url and "/eagleweb/" in url:
                    href = url
                    break

            if not href:
                print("⚠️ Nenhum link válido encontrado (com /eagleweb/).")
                prop_data = {
                    "address": None,
                    "city": None,
                    "state": "FL",
                    "zip": None,
                    "owner": None,
                    "legal_description": None,
                }
            else:
                href_full = href if href.startswith("http") else BASE + href.lstrip("./")
                print(f"📄 Indo direto para Property Information: {href_full}")
                await page.goto(href_full, wait_until="networkidle")

                # AGUARDAR 10 SEGUNDOS
                print("⏳ Aguardando 10 segundos para carregar Property Information...")
                await page.wait_for_timeout(10000)

                # PRIMEIRA TENTATIVA: HTML DIRETO
                html_text = await page.inner_text("body")
                print("[HTML] Tamanho do texto:", len(html_text))

                if has_address_block(html_text):
                    print("[HTML] Bloco de ADDRESS encontrado.")
                    prop_data = parse_property_block(html_text)
                else:
                    print("[HTML] Bloco não encontrado. Tentando iframe...")

                    # FALLBACK: IFRAME
                    prop_data = None
                    for frame in page.frames:
                        if frame == page.main_frame:
                            continue
                        try:
                            frame_text = await frame.inner_text("body")
                            print("[IFRAME] Tamanho do texto:", len(frame_text))
                            if has_address_block(frame_text):
                                print("[IFRAME] Bloco de ADDRESS encontrado.")
                                prop_data = parse_property_block(frame_text)
                                break
                        except:
                            continue

                    if prop_data is None:
                        print("[IFRAME] Nenhum bloco encontrado. Usando valores vazios.")
                        prop_data = {
                            "address": None,
                            "city": None,
                            "state": "FL",
                            "zip": None,
                            "owner": None,
                            "legal_description": None,
                        }

            # 6) Voltar 1x (Tax Sale)
            print("↩️ Voltando para página do Tax Sale...")
            await page.go_back(wait_until="networkidle")

            # 7) Voltar 1x (lista)
            print("↩️ Voltando para lista (Printable Version)...")
            await page.go_back(wait_until="networkidle")

            # 8) Montar objeto final
            final = {
                "parcel_number": main_data.get("parcel_number"),
                "sale_date": parse_date_us(main_data.get("sale_date_raw")),
                "opening_bid": clean_money(main_data.get("opening_bid_raw")),
                "application_number": main_data.get("application_number"),
                "deed_status": main_data.get("deed_status"),
                "homestead": main_data.get("homestead"),
                "appraised_value": clean_money(main_data.get("appraised_value_raw")),
                "address": prop_data.get("address"),
                "city": prop_data.get("city"),
                "state": prop_data.get("state"),
                "zip": prop_data.get("zip"),
                "owner": prop_data.get("owner"),
                "legal_description": prop_data.get("legal_description"),
            }

            results.append(final)

        await browser.close()
        return results


# -----------------------------
# EXECUÇÃO
# -----------------------------
def run():
    properties = asyncio.run(scrape_properties(limit=3))

    for i, prop in enumerate(properties, start=1):
        print(f"\n==================== PROPRIEDADE {i} ====================")
        for k, v in prop.items():
            print(f"{k}: {repr(v)}")
        print("=======================================================")


if __name__ == "__main__":
    run()
