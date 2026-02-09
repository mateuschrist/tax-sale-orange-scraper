import asyncio
import re
from playwright.async_api import async_playwright

LOGIN_URL = "https://or.occompt.com/recorder/web/login.jsp"
SEARCH_URL = "https://or.occompt.com/recorder/tdsmweb/applicationSearch.jsp"


def extract_first_match(pattern, text, flags=re.IGNORECASE):
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None


def clean_money(v):
    if not v:
        return None
    return float(v.replace("$", "").replace(",", "").strip())


def parse_date_us(v):
    return v


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


def parse_property_info_html(text: str):
    data = {}

    lines = [l.strip() for l in text.splitlines() if l.strip()]

    address = None
    city = None
    state = "FL"
    zip_code = None
    owner = None
    legal_description = None

    for l in lines:
        if re.match(r"^\d+\s+.+", l):
            address = l
            break

    for l in lines:
        m = re.search(r"([A-Za-z\s]+),\s*FL\s*(\d{5})", l)
        if m:
            city = m.group(1).strip().upper()
            zip_code = m.group(2).strip()
            break

    for l in lines:
        if "OWNER" in l.upper():
            m = re.search(r"Owner[:\-]?\s*(.+)", l, re.IGNORECASE)
            if m:
                owner = m.group(1).strip()
            break

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


async def scrape_properties(limit=3):
    print("🔍 Iniciando Playwright...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

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

        print("🌐 Acessando página de busca...")
        await page.goto(SEARCH_URL, wait_until="networkidle")

        print("🟢 Selecionando 'Active Sale'...")
        await page.select_option("select[name='DeedStatusID']", value="AS")

        print("🔎 Clicando em Search...")
        await page.click("input[value='Search']")
        await page.wait_for_load_state("networkidle")

        printable = page.locator("text=Printable Version")
        print("🖨️ Clicando em Printable Version...")
        await printable.first.click()
        await page.wait_for_load_state("networkidle")

        results = []

        for idx in range(limit):
            print(f"\n================ PROPRIEDADE {idx+1}/{limit} ================")

            links = page.locator("#searchResultsTable a:has-text('Tax Sale')")
            link_count = await links.count()
            print(f"🔗 Links de Tax Sale na lista: {link_count}")

            if idx >= link_count:
                print("⚠️ Índice maior que quantidade de links. Parando.")
                break

            print("➡️ Clicando no link do Tax Sale na lista...")
            await links.nth(idx).click()
            await page.wait_for_load_state("networkidle")

            main_text = await page.inner_text("body")
            main_data = parse_main_html(main_text)

            prop_info_link = page.locator("a:has-text('View Property Information')")
            if await prop_info_link.count() == 0:
                print("⚠️ 'View Property Information' não encontrado.")
                prop_data = {
                    "address": None,
                    "city": None,
                    "state": "FL",
                    "zip": None,
                    "owner": None,
                    "legal_description": None,
                }
            else:
                print("📄 Clicando em 'View Property Information'...")
                await prop_info_link.first.click()
                await page.wait_for_load_state("networkidle")

                prop_text = await page.inner_text("body")

                print("\n----- DEBUG: PRIMEIRAS LINHAS DA PÁGINA DE PROPERTY INFO -----")
                print("\n".join(prop_text.splitlines()[:40]))
                print("--------------------------------------------------------------\n")

                prop_data = parse_property_info_html(prop_text)

                print("↩️ Voltando para página do Tax Sale...")
                await page.go_back(wait_until="networkidle")

            print("↩️ Voltando para lista (Printable Version)...")
            await page.go_back(wait_until="networkidle")

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
                "official_link": None,  # se quiser depois, podemos montar via href
            }

            results.append(final)

        await browser.close()
        return results


def run():
    properties = asyncio.run(scrape_properties(limit=3))

    for i, prop in enumerate(properties, start=1):
        print(f"\n==================== PROPRIEDADE {i} ====================")
        for k, v in prop.items():
            print(f"{k}: {repr(v)}")
        print("=======================================================")


if __name__ == "__main__":
    run()
