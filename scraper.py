import requests
from bs4 import BeautifulSoup
import pandas as pd

# 🔗 Endpoint da sua Edge Function
ENDPOINT = "https://qeboakaofiqgvbyykvwi.supabase.co/functions/v1/import-properties"

# 🔗 Página de resultados (versão para imprimir)
RESULTS_URL = "https://or.occompt.com/recorder/tdsmweb/applicationSearchResults.jsp?searchId=2&printing=true"


# ---------------------------------------------------------
# 1) SCRAPER — Extrai dados da versão para imprimir
# ---------------------------------------------------------
def scrape_orange_county():
    print("🔍 Baixando página de resultados...")

    # Sessão persistente + User-Agent (PARTE 1 + PARTE 2)
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    })

    resp = session.get(RESULTS_URL)

    # DEBUG (PARTE 3)
    print("\n--- DEBUG ---")
    print("URL final:", resp.url)
    print("Status:", resp.status_code)
    print("HTML (primeiros 1000 chars):")
    print(resp.text[:1000])
    print("--- FIM DEBUG ---\n")

    soup = BeautifulSoup(resp.text, "html.parser")

    rows = soup.select("#searchResultsTable tbody tr")
    print(f"📄 Linhas encontradas: {len(rows)}")

    data = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        # --- COLUNA 1: Tax Sale ID ---
        desc_text = cols[0].get_text(" ", strip=True)
        parts = desc_text.split()
        tax_sale_id = parts[-1] if parts else None

        # --- COLUNA 2: Detalhes ---
        details = cols[1]

        def extract(label):
            node = details.find(string=label)
            if not node:
                return None
            b = node.find_next("b")
            return b.get_text(strip=True) if b else None

        sale_date = extract("Sale Date:")
        applicant = extract("Applicant Name:")
        status = extract("Status:")
        parcel = extract("Parcel:")
        min_bid = extract("Min Bid:")
        high_bid = extract("High Bid:")

        def clean_money(v):
            if not v:
                return 0.0
            return float(v.replace("$", "").replace(",", ""))

        row_data = {
            "address": parcel,
            "city": "Orlando",
            "county": "Orange",
            "state": "FL",
            "amount_due": clean_money(min_bid),
            "sale_type": "tax_deed",
            "auction_date": pd.to_datetime(sale_date, errors="coerce").strftime("%Y-%m-%d"),
            "official_link": RESULTS_URL,
            "notes": f"Tax Sale: {tax_sale_id} | Applicant: {applicant} | Status: {status} | High Bid: {high_bid}",
        }

        data.append(row_data)

    print(f"🗂️ Total de propriedades extraídas: {len(data)}")
    return data


# ---------------------------------------------------------
# 2) ENVIO PARA A EDGE FUNCTION
# ---------------------------------------------------------
def send_to_supabase(data):
    print("🚀 Enviando dados para a Edge Function...")

    resp = requests.post(
        ENDPOINT,
        json=data,
        headers={"Content-Type": "application/json"}
    )

    print("📨 Resposta da Edge Function:")
    print(resp.text)

    if resp.status_code != 200:
        print("❌ Erro ao enviar para Supabase:", resp.status_code)
    else:
        print("✅ Dados enviados com sucesso!")


# ---------------------------------------------------------
# 3) EXECUÇÃO PRINCIPAL
# ---------------------------------------------------------
def run():
    data = scrape_orange_county()

    if not data:
        print("⚠️ Nenhuma propriedade encontrada. Encerrando.")
        return

    send_to_supabase(data)


if __name__ == "__main__":
    run()
