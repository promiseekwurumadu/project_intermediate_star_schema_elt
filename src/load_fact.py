import pandas as pd
from sqlalchemy import text

from db import get_engine
from extract_clean import extract_and_clean


def load_fact(chunk_size: int = 50000) -> None:
    engine = get_engine()
    df = extract_and_clean()

    # Build date_key (yyyymmdd) from invoicedate
    df["date_key"] = pd.to_datetime(df["invoicedate"]).dt.strftime("%Y%m%d").astype(int)

    # Pull dimension mappings
    dim_customer = pd.read_sql("SELECT customer_key, customer_id FROM analytics.dim_customer", engine)
    dim_product = pd.read_sql("SELECT product_key, stockcode, description FROM analytics.dim_product", engine)
    dim_country = pd.read_sql("SELECT country_key, country FROM analytics.dim_country", engine)

    # Merge to get foreign keys
    df = df.merge(dim_customer, left_on="customerid", right_on="customer_id", how="left")
    df = df.merge(dim_country, left_on="country", right_on="country", how="left")
    df = df.merge(dim_product, on=["stockcode", "description"], how="left")

    # Sanity check: keys must exist
    missing = df[["customer_key", "country_key", "product_key"]].isna().sum()
    if missing.any():
        raise ValueError(f"Missing dimension keys found:\n{missing}")

    fact = df[[
        "invoiceno",
        "customer_key",
        "product_key",
        "country_key",
        "date_key",
        "quantity",
        "unitprice",
        "revenue",
    ]].copy()

    # Load in chunks using pandas -> SQLAlchemy
    # (Intermediate, practical approach; fast enough for this dataset)
    loaded = 0
    for start in range(0, len(fact), chunk_size):
        chunk = fact.iloc[start:start + chunk_size]
        chunk.to_sql(
            "fact_sales",
            con=engine,
            schema="analytics",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
        loaded += len(chunk)
        print(f"Loaded {loaded}/{len(fact)} rows into analytics.fact_sales...")

    print("Fact load complete.")


if __name__ == "__main__":
    load_fact()

