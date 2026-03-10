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

def test_crm_prd_info_no_duplicates_or_nulls(db_connection):
    query = """
        SELECT 
            prd_id,
            COUNT(*) 
        FROM silver.crm_prd_info
        GROUP BY prd_id
        HAVING COUNT(*) > 1 OR prd_id IS NULL;
    """
    df = pd.read_sql_query(text(query), db_connection)

    assert df.empty, f"Test fail! Records were found."

def test_crm_prd_info_no_whitespaces(db_connection):
    query = """
        SELECT 
            prd_nm 
        FROM silver.crm_prd_info
        WHERE prd_nm != TRIM(prd_nm);
    """
    df = pd.read_sql_query(text(query), db_connection)

    assert df.empty, f"Test fail! Records were found."

def test_crm_prd_info_null_or_negative_values_on_cost(db_connection):
    query = """
        SELECT 
            prd_cost 
        FROM silver.crm_prd_info
        WHERE prd_cost < 0 OR prd_cost IS NULL;
    """
    df = pd.read_sql_query(text(query), db_connection)

    assert df.empty, f"Test fail! Records were found."

def test_crm_prd_info_date_integrity(db_connection):
    query = """
        SELECT 
            * 
        FROM silver.crm_prd_info
        WHERE prd_end_dt < prd_start_dt;
    """
    df = pd.read_sql_query(text(query), db_connection)

    assert df.empty, f"Test fail! Records were found."