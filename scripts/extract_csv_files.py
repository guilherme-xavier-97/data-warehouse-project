import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# Database Connection URL
db_url = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}"
)

# Create engine
engine = create_engine(db_url)

# 1. Extract data from CSV files
data_map = {
    'crm_cust_info': pd.read_csv('../datasets/source_crm/cust_info.csv'),
    'crm_prd_info': pd.read_csv('../datasets/source_crm/prd_info.csv'),
    'crm_sales_details': pd.read_csv('../datasets/source_crm/sales_details.csv'),
    'erp_cust_az12': pd.read_csv('../datasets/source_erp/CUST_AZ12.csv'),
    'erp_loc_a101': pd.read_csv('../datasets/source_erp/LOC_A101.csv'),
    'erp_px_cat_g1v2': pd.read_csv('../datasets/source_erp/PX_CAT_G1V2.csv')
}

# 2. Load extracted CSV files to database
with engine.connect() as conn:
    for table_name, df in data_map.items():
        print(f"Cleaning the table: {table_name}...")

        #Set all the column names to lowercase
        df.columns = [col.lower().strip() for col in df.columns]
        
        # Its important use TRUNCATE directaly intead of 'if_exists' replace atribute in the to_sql function because I did the DDL script
        # in the database. If I use 'replace', all the table structure, mean, the type os the columns will be lost.
        conn.execute(text(f"TRUNCATE TABLE bronze.{table_name}"))
        conn.commit()
        
        # Load extrated data to database
        df.to_sql(
            name=table_name,
            con=engine,
            schema='bronze',
            if_exists='append',
            index=False,
            chunksize=10000,
            method='multi'
        )

print("Bronze load fineshed!")



