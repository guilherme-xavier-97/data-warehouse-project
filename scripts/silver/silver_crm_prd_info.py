import pandas as pd
from sqlalchemy import text
from database.postgres_connection import database_connection

def run_crm_prd_info():
    table_name = 'crm_prd_info'
    db_connection = database_connection()

    df = pd.read_sql(f"SELECT * FROM bronze.{table_name}", db_connection) 

    # 1 - Remove whitespaces spaces
    text_cols = df.select_dtypes(include=['str', 'object']).columns
    for col in text_cols:
        df[col] = df[col].str.strip()

    # 2 - Standard Product Line 
    product_line_map = {'M': 'Mountain', 'R': 'Road', 'S': 'Other Sales', 'T': 'Touring'}
    df['prd_line'] = df['prd_line'].map(product_line_map).fillna('N/A')

    # 3 - Extract new columns: category id and product key
    df['cat_id'] = df['prd_key'].str[0:5].str.replace('-','_', regex=False)
    df['prd_key'] = df['prd_key'].str[6:]

    # 4 - Change nulls
    df['prd_cost'] = df['prd_cost'].fillna(0)

    # 5 - Calculate dates dinamically
    df = df.sort_values(['prd_key', 'prd_start_dt'])
    df['prd_end_dt'] = df.groupby('prd_key')['prd_start_dt'].shift(-1) - pd.Timedelta(days=1)

    # 6 - TRUNCATE AND INSERT DATA
    with db_connection.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE silver.{table_name}"))
        
        df.to_sql(
            name=f'{table_name}',
            con=conn,
            schema='silver',
            if_exists='append',
            index=False
        )
    
    print(f"Silver {table_name} Load finished! {len(df)} records processed.")


if __name__ == "__main__":
    run_crm_prd_info()


