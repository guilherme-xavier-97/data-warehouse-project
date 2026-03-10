import numpy as np
import pandas as pd
from sqlalchemy import text

from database.postgres_connection import database_connection


def run_crm_sales_details():
    table_name = "crm_sales_details"
    db_connection = database_connection()

    df = pd.read_sql(f"SELECT * FROM bronze.{table_name}", db_connection)

    # 1 - Remove whitespaces spaces
    text_cols = df.select_dtypes(include=["str", "object"]).columns
    for col in text_cols:
        df[col] = df[col].str.strip()

    # 2 - Cast and format date columns
    df["sls_order_dt"] = pd.to_datetime(
        df["sls_order_dt"], format="%Y%m%d", errors="coerce"
    )
    df["sls_ship_dt"] = pd.to_datetime(
        df["sls_ship_dt"], format="%Y%m%d", errors="coerce"
    )
    df["sls_due_dt"] = pd.to_datetime(
        df["sls_due_dt"], format="%Y%m%d", errors="coerce"
    )

    # 3 - Validate quantity, sales and price. The rules is: None of then can be negative and sales = price * quantity
    # Remove negative values
    df["sls_sales"] = df["sls_sales"].abs()
    df["sls_quantity"] = df["sls_quantity"].abs()
    df["sls_price"] = df["sls_price"].abs()

    # Filter only the wrong values and use .loc to update them.
    # Is important separate the sales to the calculatation, because if price is null calculation will be wrong (Ex: 10 * null = null)

    # Process sales transformations
    mask_invalid_sale = (df["sls_sales"].isna()) | (df["sls_sales"] <= 0)
    mask_wrong_calculation = (
        df["sls_sales"] != df["sls_quantity"] * df["sls_price"]
    ) & (df["sls_price"] > 0)

    df.loc[mask_invalid_sale | mask_wrong_calculation, "sls_sales"] = (
        df["sls_quantity"] * df["sls_price"]
    )

    # Process price transformations, after fix sales.
    mask_invalid_price = (df["sls_price"].isna()) | (df["sls_price"] <= 0)
    df.loc[mask_invalid_price, "sls_price"] = df["sls_sales"] / df[
        "sls_quantity"
    ].replace(0, np.nan)

    # 6 - TRUNCATE AND INSERT DATA
    with db_connection.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE silver.{table_name}"))

        df.to_sql(
            name=f"{table_name}",
            con=conn,
            schema="silver",
            if_exists="append",
            index=False,
        )

    print(f"Silver {table_name} Load finished! {len(df)} records processed.")


if __name__ == "__main__":
    run_crm_sales_details()
