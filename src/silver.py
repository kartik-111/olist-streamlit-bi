import pandas as pd

from src.config import BRONZE_DIR, SILVER_DIR
from src.data_io import read_csv, write_parquet

def _to_datetime(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:

    df = df.copy()

    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce") 
    return df

def build_orders_silver() -> None:
    df = read_csv(BRONZE_DIR / "olist_orders_dataset.csv")

    df = _to_datetime(
        df,
        [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )

    df = df.drop_duplicates(subset=["order_id"])
    write_parquet(df, SILVER_DIR / "orders.parquet")

def build_order_items_silver() -> None:
    df = read_csv(BRONZE_DIR / "olist_order_items_dataset.csv")

    df = _to_datetime(
        df,
        [
            "shipping_limit_date",
        ],
    )

    # Basic Sanity Check
    for c in ["price", "freight_value"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.drop_duplicates(subset=["order_id", "order_item_id"])
    write_parquet(df, SILVER_DIR / "order_items.parquet")

def build_customers_silver() -> None:
    df = read_csv(BRONZE_DIR / "olist_customers_dataset.csv")
    df = df.drop_duplicates(subset=["customer_id"])
    write_parquet(df, SILVER_DIR / "customers.parquet")

def build_customers_silver() -> None:
    df = read_csv(BRONZE_DIR / "olist_customers_dataset.csv")
    df = df.drop_duplicates(subset=["customer_id"])
    write_parquet(df, SILVER_DIR / "customers.parquet")

def build_sellers_silver() -> None:
    df = read_csv(BRONZE_DIR / "olist_sellers_dataset.csv")
    df = df.drop_duplicates(subset=["seller_id"])
    write_parquet(df, SILVER_DIR / "sellers.parquet")

def build_products_silver() -> None:
    products = read_csv(BRONZE_DIR / "olist_products_dataset.csv")
    translation = read_csv(BRONZE_DIR / "product_category_name_translation.csv")

    products = products.merge(translation, 
        how="left",
        on="product_category_name",
    )

    products =products.rename(columns={"product_category_name_english": "product_category"})
    products = products.drop_duplicates(subset=["product_id"])

    write_parquet(products, SILVER_DIR / "products.parquet")

def build_reviews_silver() -> None:
    df = read_csv(BRONZE_DIR / "olist_order_reviews_dataset.csv")

    df = _to_datetime(
        df,
        [
            "review_creation_date",
            "review_answer_timestamp",
        ],
    )

    df = df.drop_duplicates(subset=["review_id"])
    write_parquet(df, SILVER_DIR / "reviews.parquet")

def build_payments_silver() -> None:
    df = read_csv(BRONZE_DIR / "olist_order_payments_dataset.csv")

    # Basic Sanity Check
    for c in [ "payment_value"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.drop_duplicates(subset=["order_id", "payment_type"])
    write_parquet(df, SILVER_DIR / "payments.parquet")

def build_all_silver() -> None: 
    build_orders_silver()
    build_order_items_silver()
    build_customers_silver()
    build_sellers_silver()
    build_products_silver()
    build_reviews_silver()
    build_payments_silver()

if __name__ == "__main__":
    build_all_silver()