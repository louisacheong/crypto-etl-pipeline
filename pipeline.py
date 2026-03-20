import requests
import pandas as pd
from google.cloud import bigquery

# -----------------------
# 1. EXTRACT
# -----------------------
def extract_data():
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=eur"
    response = requests.get(url)
    data = response.json()
    return pd.DataFrame(data)

# -----------------------
# 2. TRANSFORM
# -----------------------
def transform_data(df):
    df = df[[
        'id', 'symbol', 'name',
        'current_price', 'market_cap',
        'market_cap_rank',
        'total_volume',
        'price_change_percentage_24h',
        'last_updated'
    ]].copy()

    df['last_updated'] = pd.to_datetime(df['last_updated'], utc=True)
    df['date'] = df['last_updated'].dt.date
    df['hour'] = df['last_updated'].dt.hour
    df['price_change_ratio'] = df['price_change_percentage_24h'] / 100

    return df

# -----------------------
# 3. LOAD TO BIGQUERY
# -----------------------
def load_to_bq(df):
    client = bigquery.Client()

    table_id = "159851688843.crypto_dataset.crypto_data"

    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print("Loaded data into BigQuery")

# -----------------------
# MAIN PIPELINE
# -----------------------
def run_pipeline():
    print("Starting pipeline...")

    df = extract_data()
    df = transform_data(df)
    load_to_bq(df)

    print("Pipeline completed successfully")

if __name__ == "__main__":
    run_pipeline()
