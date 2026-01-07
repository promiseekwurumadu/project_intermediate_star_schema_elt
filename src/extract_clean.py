import pandas as pd

RAW_XLSX_PATH = "data/Online_Retail.xlsx"


def extract_and_clean() -> pd.DataFrame:
    df = pd.read_excel(RAW_XLSX_PATH)

    df = df.dropna(subset=["CustomerID"]).copy()
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)].copy()

    df["Revenue"] = df["Quantity"] * df["UnitPrice"]
    df["CustomerID"] = df["CustomerID"].astype(int).astype(str)

    # standardise column names for the warehouse
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    df["stockcode"] = df["stockcode"].astype(str).str.strip()
    df["description"] = df["description"].fillna("").astype(str).str.strip()
    df["country"] = df["country"].astype(str).str.strip()


    return df


if __name__ == "__main__":
    df = extract_and_clean()
    print(df.head())
    print("Rows:", len(df))
