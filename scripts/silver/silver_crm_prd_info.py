import pandas as pd
from sqlalchemy import text
from database.postgres_connection import database_connection

def run_crm_prd_info():
    table_name = 'crm_prd_info'
    db_connection = database_connection()

    df = pd.read_sql(f"SELECT * FROM bronze.{table_name}", db_connection) 

    # 1 - Remove nulls
    df = df.dropna(subset=['cst_id'])

    # 2 - Remove whitespaces spaces
    text_cols = df.select_dtypes(include=['str', 'object']).columns
    for col in text_cols:
        df[col] = df[col].str.strip()

    # 3 - Standard Gender and Married names
    gender_map = {'M': 'Male', 'F': 'Female'}
    marital_status_map = {'S': 'Single', 'M': 'Married'}

    df['cst_gndr'] = df['cst_gndr'].map(gender_map).fillna('N/A')
    df['cst_marital_status'] = df['cst_marital_status'].map(marital_status_map).fillna('N/A')

    # 4 - Remove duplicates
    df['cst_create_date'] = pd.to_datetime(df['cst_create_date'])
    df = df.sort_values(by=['cst_id', 'cst_create_date'], ascending=[True, False])
    df = df.drop_duplicates(subset='cst_id', keep='first')
    

    # 5 - TRUNCATE AND INSERT DATA
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


