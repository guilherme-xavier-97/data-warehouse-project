import pandas as pd
import pytest
from sqlalchemy import text

from database.postgres_connection import database_connection


@pytest.fixture
def db_connection():
    engine = database_connection()
    yield engine
    engine.dispose()


# ======================= crm_cust_info tests =========================
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

    assert df.empty, "Test fail! Records were found."


def test_crm_cust_info_no_whitespaces(db_connection):
    query = """
        SELECT 
            cst_key 
        FROM silver.crm_cust_info
        WHERE cst_key != TRIM(cst_key);
    """
    df = pd.read_sql_query(text(query), db_connection)

    assert df.empty, "Test fail! Records were found."


# ======================= crm_prd_info tests =========================
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

    assert df.empty, "Test fail! Records were found."


def test_crm_prd_info_no_whitespaces(db_connection):
    query = """
        SELECT 
            prd_nm 
        FROM silver.crm_prd_info
        WHERE prd_nm != TRIM(prd_nm);
    """
    df = pd.read_sql_query(text(query), db_connection)

    assert df.empty, "Test fail! Records were found."


def test_crm_prd_info_null_or_negative_values_on_cost(db_connection):
    query = """
        SELECT 
            prd_cost 
        FROM silver.crm_prd_info
        WHERE prd_cost < 0 OR prd_cost IS NULL;
    """
    df = pd.read_sql_query(text(query), db_connection)

    assert df.empty, "Test fail! Records were found."


def test_crm_prd_info_date_integrity(db_connection):
    query = """
        SELECT 
            * 
        FROM silver.crm_prd_info
        WHERE prd_end_dt < prd_start_dt;
    """
    df = pd.read_sql_query(text(query), db_connection)

    assert df.empty, "Test fail! Records were found."


# ======================= crm_sales_details tests =========================


def test_crm_sales_details_no_invalid_dates(db_connection):
    query = """
        SELECT
            NULLIF(sls_due_dt, 0) AS sls_due_dt
        FROM bronze.crm_sales_details
        WHERE sls_due_dt <= 0
            OR LENGTH(CAST (sls_due_dt as VARCHAR)) != 8
            OR sls_due_dt > 20500101
            OR sls_due_dt < 19000101;
    """
    df = pd.read_sql_query(text(query), db_connection)

    assert df.empty, "Test fail! Records were found."


def test_crm_sales_details_date_order(db_connection):
    query = """
        SELECT 
            * 
        FROM silver.crm_sales_details
        WHERE sls_order_dt > sls_ship_dt 
        OR sls_order_dt > sls_due_dt;
    """
    df = pd.read_sql_query(text(query), db_connection)

    assert df.empty, "Test fail! Records were found."


def test_crm_sales_details_data_consistency(db_connection):
    query = """
        SELECT DISTINCT 
            sls_sales,
            sls_quantity,
            sls_price 
        FROM silver.crm_sales_details
        WHERE sls_sales != sls_quantity * sls_price
        OR sls_sales IS NULL 
        OR sls_quantity IS NULL 
        OR sls_price IS NULL
        OR sls_sales <= 0 
        OR sls_quantity <= 0 
        OR sls_price <= 0
        ORDER BY sls_sales, sls_quantity, sls_price;
    """
    df = pd.read_sql_query(text(query), db_connection)

    assert df.empty, "Test fail! Records were found."
