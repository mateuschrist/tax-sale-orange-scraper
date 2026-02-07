import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from supabase import create_client

# 🔐 Variáveis de ambiente (configure no GitHub Actions)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# 🔗 Versão para imprimir — já filtrada em Active Sale (DeedStatusID in 'AS')
RESULTS_URL = "https://or.occompt.com/recorder/tdsmweb/applicationSearchResults.jsp?searchId=2&printing=true"


def scrape_orange_county():
    resp = requests.get(RESULTS_URL)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Cada linha de resultado
    rows = soup.select("#searchResultsTable tbody tr")

    data = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        # --- COLUNA 1: Tax Sale + número ---
        desc_text = cols[0].get_text(" ", strip=True)
        # Ex: "Tax Sale 2023-17830"
        parts = desc_text.split()
        tax_sale_id = parts[-1] if parts else None

        # --- COLUNA 2: Detalhes (Sale Date, Applicant, Status, Parcel, Min Bid, High Bid) ---
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
            "address": parcel,  # aqui usamos o Parcel como "address"
            "city": "Orange County",
            "county": "Orange",
            "state": "FL",
            "amount_due": clean_money(min_bid),
            "sale_type": "tax_deed",
            "auction_date": pd.to_datetime(sale_date, errors="coerce"),
            "official_link": RESULTS_URL,
            "notes": f"Tax Sale: {tax_sale_id} | Applicant: {applicant} | Status: {status} | High Bid: {high_bid}",
        }

        data.append(row_data)

    return data


def upsert_property(row):
    # Evita duplicar: chave = address + auction_date
    existing = (
        supabase.table("properties")
        .select("*")
        .eq("address", row["address"])
        .eq("auction_date", row["auction_date"])
        .execute()
    )

    if existing.data:
        prop_id = existing.data[0]["id"]
        supabase.table("properties").update(row).eq("id", prop_id).execute()
    else:
        supabase.table("properties").insert(row).execute()


def run():
    properties = scrape_orange_county()
    for row in properties:
        # ignora linhas sem parcel ou sem data
        if not row["address"] or pd.isna(row["auction_date"]):
            continue
        upsert_property(row)


if __name__ == "__main__":
    run()
