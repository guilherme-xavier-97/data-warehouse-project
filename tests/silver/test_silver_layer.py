import pytest
import pandas as pd
from sqlalchemy import text
from database.postgres_connection import database_connection

@pytest.fixture
def db_connection():
    engine = database_connection()
    yield engine
    engine.dispose()



def test_crm_cust_info_no_duplicates_or_nulls(db_connection):
    query = """
        SELECT 
            cst_id,
        COUNT(*) 
        FROM silver.crm_cust_info
        GROUP BY cst_id
        HAVING COUNT(*) > 1 OR cst_id IS NULL;
    """
    df = pd.read_sql_query(text(query), db_connection)

    assert df.empty, f"Test fail! Records were found."

def test_crm_cust_info_no_whitespaces(db_connection):
    query = """
        SELECT 
            cst_key 
        FROM silver.crm_cust_info
        WHERE cst_key != TRIM(cst_key);
    """
    df = pd.read_sql_query(text(query), db_connection)

    assert df.empty, f"Test fail! Records were found."