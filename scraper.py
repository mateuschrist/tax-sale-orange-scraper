import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# COLE AQUI SUA URL DE RESULTADOS
RESULTS_URL = "https://or.occompt.com/recorder/tdsmweb/applicationSearchResults.jsp?searchId=2"

def scrape_orange_county():
    response = requests.get(RESULTS_URL)
    soup = BeautifulSoup(response.text, "html.parser")

    rows = soup.select("table#searchResultsTable tbody tr")

    data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        # coluna 1: Tax Sale + número
        desc = cols[0].get_text(" ", strip=True)

        # coluna 2: tabela interna com detalhes
        details = cols[1]

        sale_date = details.find(text="Sale Date:")
        sale_date = sale_date.find_next("b").text.strip() if sale_date else None

        applicant = details.find(text="Applicant Name:")
        applicant = applicant.find_next("b").text.strip() if applicant else None

        status = details.find(text="Status:")
        status = status.find_next("b").text.strip() if status else None

        parcel = desc.split()[-1]  # último item é o número do tax sale

        data.append({
            "address": parcel,
            "city": "Orange County",
            "county": "Orange",
            "state": "FL",
            "amount_due": 0,
            "sale_type": "tax_deed",
            "auction_date": pd.to_datetime(sale_date, errors="coerce"),
            "official_link": RESULTS_URL,
            "notes": f"Applicant: {applicant} | Status: {status}"
        })

    return data

def upsert_property(row):
    existing = (
        supabase.table("properties")
        .select("*")
        .eq("address", row["address"])
        .eq("auction_date", row["auction_date"])
        .execute()
    )

    if existing.data:
        supabase.table("properties").update(row).eq("id", existing.data[0]["id"]).execute()
    else:
        supabase.table("properties").insert(row).execute()

def run():
    data = scrape_orange_county()
    for row in data:
        upsert_property(row)

if __name__ == "__main__":
    run()
