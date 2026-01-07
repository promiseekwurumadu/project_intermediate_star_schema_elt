import pandas as pd
from sqlalchemy import text

from db import get_engine
from extract_clean import extract_and_clean


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    # Use date only (no time) for the date dimension
    d = pd.to_datetime(df["invoicedate"]).dt.date
    dim = pd.DataFrame({"date_value": d}).drop_duplicates()

    dim["date_key"] = pd.to_datetime(dim["date_value"]).dt.strftime("%Y%m%d").astype(int)
    dim["year"] = pd.to_datetime(dim["date_value"]).dt.year
    dim["month"] = pd.to_datetime(dim["date_value"]).dt.month
    dim["month_name"] = pd.to_datetime(dim["date_value"]).dt.strftime("%B")
    dim["day"] = pd.to_datetime(dim["date_value"]).dt.day
    # Monday=1 ... Sunday=7 (business-friendly)
    dim["day_of_week"] = pd.to_datetime(dim["date_value"]).dt.dayofweek + 1

    return dim[["date_key", "date_value", "year", "month", "month_name", "day", "day_of_week"]]


def load_dimensions() -> None:
    engine = get_engine()
    df = extract_and_clean()

    dim_customer = df[["customerid"]].drop_duplicates().rename(columns={"customerid": "customer_id"})
    dim_product = (
        df[["stockcode", "description"]]
        .drop_duplicates()
        .fillna({"description": ""})
    )
    dim_country = df[["country"]].drop_duplicates()
    dim_date = build_dim_date(df)

    with engine.begin() as conn:
        # Customers
        conn.execute(
            text("""
                INSERT INTO analytics.dim_customer (customer_id)
                SELECT customer_id FROM (SELECT DISTINCT :customer_id AS customer_id) s
                ON CONFLICT (customer_id) DO NOTHING
            """),
            [{"customer_id": v} for v in dim_customer["customer_id"].tolist()],
        )

        # Products
        conn.execute(
            text("""
                INSERT INTO analytics.dim_product (stockcode, description)
                SELECT stockcode, description
                FROM (SELECT :stockcode AS stockcode, :description AS description) s
                ON CONFLICT (stockcode, description) DO NOTHING
            """),
            [{"stockcode": r.stockcode, "description": r.description} for r in dim_product.itertuples(index=False)],
        )

        # Countries
        conn.execute(
            text("""
                INSERT INTO analytics.dim_country (country)
                SELECT country FROM (SELECT :country AS country) s
                ON CONFLICT (country) DO NOTHING
            """),
            [{"country": v} for v in dim_country["country"].tolist()],
        )

        # Dates
        conn.execute(
            text("""
                INSERT INTO analytics.dim_date (date_key, date_value, year, month, month_name, day, day_of_week)
                VALUES (:date_key, :date_value, :year, :month, :month_name, :day, :day_of_week)
                ON CONFLICT (date_key) DO NOTHING
            """),
            dim_date.to_dict(orient="records"),
        )

    print("Dimensions loaded:")
    print(f"  dim_customer: {len(dim_customer)} unique customers")
    print(f"  dim_product : {len(dim_product)} unique products")
    print(f"  dim_country : {len(dim_country)} unique countries")
    print(f"  dim_date    : {len(dim_date)} unique dates")


if __name__ == "__main__":
    load_dimensions()

