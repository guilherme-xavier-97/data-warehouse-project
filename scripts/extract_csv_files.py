import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

# Standard URL: postgresql://user:password@host:port/db_name
db_url = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}"
)

# Create engine
engine = create_engine(db_url)

#Extract data from CSV files
df_crm_cust_info = pd.read_csv('../datasets/source_crm/cust_info.csv')
df_crm_prd_info = pd.read_csv('../datasets/source_crm/prd_info.csv')
df_crm_sales_details = pd.read_csv('../datasets/source_crm/sales_details.csv')
df_erp_cust_AZ12 = pd.read_csv('../datasets/source_erp/CUST_AZ12.csv')
df_erp_loc_A101 = pd.read_csv('../datasets/source_erp/LOC_A101.csv')
df_erp_px_cat_G1V2 = pd.read_csv('../datasets/source_erp/PX_CAT_G1V2.csv')


#Load extracted CSV files to database
df_crm_cust_info.to_sql('crm_cust_info', con=engine, schema='bronze', if_exists='replace', index=False)
df_crm_prd_info.to_sql('crm_prd_info', con=engine, schema='bronze', if_exists='replace', index=False)
df_crm_sales_details.to_sql('crm_sales_details', con=engine, schema='bronze', if_exists='replace', index=False)
df_erp_cust_AZ12.to_sql('erp_cust_az12', con=engine, schema='bronze', if_exists='replace', index=False)
df_erp_loc_A101.to_sql('erp_loc_a101', con=engine, schema='bronze', if_exists='replace', index=False)
df_erp_px_cat_G1V2.to_sql('erp_px_cat_g1v2', con=engine, schema='bronze', if_exists='replace', index=False)



