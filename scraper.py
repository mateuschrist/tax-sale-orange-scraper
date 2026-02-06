import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def scrape_orange_county():
    url = "https://www.octaxdeeds.com/TaxDeedSales"  # exemplo, pode mudar depois se o site for outro
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table")
    df = pd.read_html(str(table))[0]

    df = df.rename(columns={
        "Address": "address",
        "City": "city",
        "County": "county",
        "State": "state",
        "Opening Bid": "amount_due",
        "Sale Date": "auction_date",
    })

    df["sale_type"] = "tax_deed"

    links = []
    for row in table.find_all("tr")[1:]:
        a = row.find("a")
        links.append(a["href"] if a else None)

    df["official_link"] = links

    df["amount_due"] = (
        df["amount_due"]
        .astype(str)
        .str.replace("$", "")
        .str.replace(",", "")
        .astype(float)
    )

    df["auction_date"] = pd.to_datetime(df["auction_date"], errors="coerce")

    df["notes"] = None

    return df

def upsert_property(row):
    existing = (
        supabase.table("properties")
        .select("*")
        .eq("address", row["address"])
        .eq("auction_date", row["auction_date"])
        .execute()
    )

    if existing.data:
        supabase.table("properties").update({
            "city": row["city"],
            "county": row["county"],
            "state": row["state"],
            "amount_due": row["amount_due"],
            "sale_type": row["sale_type"],
            "official_link": row["official_link"],
            "notes": row["notes"],
        }).eq("id", existing.data[0]["id"]).execute()
    else:
        supabase.table("properties").insert(row).execute()

def run():
    df = scrape_orange_county()
    for row in df.to_dict(orient="records"):
        upsert_property(row)

if __name__ == "__main__":
    run()
