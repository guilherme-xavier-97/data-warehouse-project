import pandas as pd
from sqlalchemy import text
from database.postgres_connection import database_connection

def run_crm_cust_info():
    db_connection = database_connection()

    df = pd.read_sql("SELECT * FROM bronze.crm_cust_info", db_connection) 

    # 1 - Remove whitespaces spaces
    text_cols = df.select_dtypes(include=['str', 'object']).columns
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()

    # 2 - Standard Gender and Married names
    gender_map = {'M': 'Male', 'F': 'Female'}
    marital_status_map = {'S': 'Single', 'M': 'Married'}

    df['cst_gndr'] = df['cst_gndr'].map(gender_map).fillna('N/A')
    df['cst_marital_status'] = df['cst_marital_status'].map(marital_status_map).fillna('N/A')

    # 3 - Remove duplicates
    df['cst_create_date'] = pd.to_datetime(df['cst_create_date'])
    df = df.sort_values(by=['cst_id', 'cst_create_date'], ascending=[True, False])
    df_no_duplicates = df.drop_duplicates(subset='cst_id', keep='first')

    # 4 - Load
    df_no_duplicates.to_sql(
        name='crm_cust_info',
        con=db_connection,
        schema='silver',
        if_exists='replace',
        index=False
    )
    
    print(f"Silver CRM_CUST_INFO Load finished! {len(df_no_duplicates)} records processed.")


if __name__ == "__main__":
    run_crm_cust_info()


