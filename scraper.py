import asyncio
import re
from playwright.async_api import async_playwright

LOGIN_URL = "https://or.occompt.com/recorder/web/login.jsp"
SEARCH_URL = "https://or.occompt.com/recorder/tdsmweb/applicationSearch.jsp"


# -----------------------------
# Helpers de parsing (texto)
# -----------------------------
def extract_first_match(pattern, text, flags=re.IGNORECASE):
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None


def parse_main_html(text: str):
    """
    Parser do HTML principal (Tax Sale).
    Trabalha em cima do texto cru (inner_text("body")).
    """
    data = {}

    # Tax Deed Application Number
    data["application_number"] = extract_first_match(
        r"Tax Deed Application Number\s+([0-9\-]+)", text
    )

    # Deed Status
    data["deed_status"] = extract_first_match(
        r"Deed Status\s+([A-Za-z ]+)", text
    )

    # Parcel Number
    data["parcel_number"] = extract_first_match(
        r"Parcel Number\s+([0-9\-]+)", text
    )

    # Sale Date
    data["sale_date_raw"] = extract_first_match(
        r"Sale Date\s+([0-9/]+)", text
    )

    # Opening Bid Amount
    data["opening_bid_raw"] = extract_first_match(
        r"Opening Bid Amount\$?([0-9\.,]+)", text
    )

    # Homestead? (pode ser "Homestead?" seguido de algo)
    data["homestead"] = extract_first_match(
        r"Homestead\?\s*([A-Za-z]+)", text
    )

    # Property Appraised Value
    data["appraised_value_raw"] = extract_first_match(
        r"Property Appraised Value\$?([0-9\.,]+)", text
    )

    return data


def parse_property_info_html(text: str):
    """
    Parser da página 'View Property Information'.
    Aqui vamos buscar:
    - Address
    - City, State, ZIP
    - Owner
    - Legal Description
    etc.
    Como ainda não vimos o HTML, vamos fazer um parser bem genérico.
    Depois refinamos com base no output real.
    """
    data = {}

    # Tentativa genérica de pegar endereço (linha com número + rua)
    # Ex: "123 MAIN ST"
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    address = None
    city = None
    state = "FL"
    zip_code = None
    owner = None
    legal_description = None

    # Heurística para endereço: primeira linha com "^\d+ "
    for l in lines:
        if re.match(r"^\d+\s+.+", l):
            address = l
            break

    # Heurística para cidade/estado/zip: linha com ", FL 328xx"
    for l in lines:
        m = re.search(r"([A-Za-z\s]+),\s*FL\s*(\d{5})", l)
        if m:
            city = m.group(1).strip().upper()
            zip_code = m.group(2).strip()
            break

    # Owner: linha que contenha "Owner" seguido de nome (bem genérico)
    for l in lines:
        if "OWNER" in l.upper():
            # Ex: "Owner: JOHN DOE"
            m = re.search(r"Owner[:\-]?\s*(.+)", l, re.IGNORECASE)
            if m:
                owner = m.group(1).strip()
            break

    # Legal Description: bloco após "LEGAL DESCRIPTION"
    legal_idx = None
    for i, l in enumerate(lines):
        if "LEGAL DESCRIPTION" in l.upper():
            legal_idx = i
            break

    if legal_idx is not None:
        collected = []
        for k in range(legal_idx + 1, len(lines)):
            l = lines[k]
            # Se aparecer um novo título em CAPS com dois pontos, paramos
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


def clean_money(v):
    if not v:
        return None
    return float(v.replace("$", "").replace(",", "").strip())


def parse_date_us(v):
    # Mantemos como string dd/mm/aaaa por enquanto, ou adaptamos depois
    return v


# -----------------------------
# Scraper principal
# -----------------------------
async def scrape_properties(limit=3):
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

        # Limitar ao número pedido
        count = min(limit, link_count)

        BASE = "https://or.occompt.com/recorder/"
        results = []

        for idx in range(count):
            print(f"\n================ PROPRIEDADE {idx+1}/{count} ================")

            link_handle = links.nth(idx)
            href = await link_handle.get_attribute("href")

            if href.startswith("http"):
                full_link = href
            else:
                cleaned = href.lstrip("./")
                full_link = BASE + cleaned

            print(f"➡️ Acessando Tax Sale: {full_link}")
            await page.goto(full_link, wait_until="networkidle")

            # 4.1) HTML principal
            main_text = await page.inner_text("body")
            main_data = parse_main_html(main_text)

            # 4.2) Abrir "View Property Information"
            prop_info_link = page.locator("a:has-text('View Property Information')")
            if await prop_info_link.count() == 0:
                print("⚠️ 'View Property Information' não encontrado.")
                prop_data = {}
            else:
                print("📄 Acessando 'View Property Information'...")
                await prop_info_link.first.click()
                await page.wait_for_load_state("networkidle")

                prop_text = await page.inner_text("body")
                prop_data = parse_property_info_html(prop_text)

                # Voltar para a página do Tax Sale (caso precise)
                await page.go_back(wait_until="networkidle")

            # 4.3) Montar dicionário final
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
                "official_link": full_link,
            }

            results.append(final)

        await browser.close()
        return results


def run():
    properties = asyncio.run(scrape_properties(limit=3))

    if not properties:
        print("⚠️ Nenhuma propriedade processada.")
        return

    for i, prop in enumerate(properties, start=1):
        print(f"\n==================== PROPRIEDADE {i} ====================")
        for k, v in prop.items():
            print(f"{k}: {repr(v)}")
        print("=======================================================")


if __name__ == "__main__":
    run()
