/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `Import/Export Data` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/




CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
    batch_start_time TIMESTAMPTZ;
    batch_end_time TIMESTAMPTZ;
BEGIN
    batch_start_time := CURRENT_TIMESTAMP;

    RAISE NOTICE '============================';
    RAISE NOTICE 'LOADING SILVER LAYER';
    RAISE NOTICE '============================';

    ------------------------------------------------
    -- CRM TABLES
    ------------------------------------------------
    
   Raise Notice '-----------------------';
   Raise Notice 'Loading CRM Tables';
   Raise Notice '-----------------------';
   
    -------------------------------
    -- SILVER.CRM_CUST_INFO
    -------------------------------
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Loading silver.crm_cust_info';

    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY cst_id
                   ORDER BY cst_create_date DESC
               ) rn
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE rn = 1;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'crm_cust_info loaded in % sec',
        EXTRACT(EPOCH FROM (end_time - start_time));

    -------------------------------
    -- SILVER.CRM_PRD_INFO
    -------------------------------
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Loading silver.crm_prd_info';

    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key FROM 7) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0),
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        TO_DATE(prd_start_dt::TEXT, 'YYYYMMDD') AS prd_start_dt,
        CASE
            WHEN LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) IS NOT NULL
            THEN TO_DATE(
                    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)::TEXT,
                    'YYYYMMDD'
                 ) - INTERVAL '1 day'
            ELSE NULL
        END AS prd_end_dt
    FROM bronze.crm_prd_info
    WHERE prd_start_dt IS NOT NULL
      AND prd_start_dt::TEXT ~ '^[0-9]{8}$';
 end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'crm_prd_info loaded in % sec',
        EXTRACT(EPOCH FROM (end_time - start_time));



    -------------------------------
    -- SILVER.CRM_SALES_DETAILS
    -------------------------------
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Loading silver.crm_sales_details';

    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,

        CASE WHEN sls_order_dt::TEXT ~ '^[0-9]{8}$'
             THEN TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD') END,

        CASE WHEN sls_ship_dt::TEXT ~ '^[0-9]{8}$'
             THEN TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD') END,

        CASE WHEN sls_due_dt::TEXT ~ '^[0-9]{8}$'
             THEN TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD') END,

        sls_quantity * ABS(COALESCE(sls_price, 0)) AS sls_sales,
        sls_quantity,
        ABS(COALESCE(sls_price, 0)) AS sls_price
    FROM bronze.crm_sales_details;
	 end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'crm_sales_details loaded in % sec',
        EXTRACT(EPOCH FROM (end_time - start_time));

    -----------------------
    -- ERP TABLES
    -----------------------


   Raise Notice '-----------------------';
   Raise Notice ' Loading ERP Tables';
   Raise Notice '-----------------------';

    -- SILVER.ERP_CUST_AZ12
	
	start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Loading silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4) ELSE cid END,
        CASE WHEN bdate > CURRENT_DATE THEN NULL ELSE bdate END,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;
	end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'erp_cust_az12 loaded in % sec',
        EXTRACT(EPOCH FROM (end_time - start_time));


    -- SILVER.ERP_LOC_A101
	
	start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Loading silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', ''),
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
            WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;
	end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'erp_loc_a101 loaded in % sec',
        EXTRACT(EPOCH FROM (end_time - start_time));
	
    -- SILVER.ERP_PX_CAT_G1V2
	
	start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Loading silver.erp_px_cat_g1v2';
	
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;
	end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'erp_px_cat_g1v2 loaded in % sec',
        EXTRACT(EPOCH FROM (end_time - start_time));

    batch_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Silver Layer completed in % sec',
        EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'ERROR DURING SILVER LOAD';
        RAISE NOTICE '%', SQLERRM;
        RAISE;
END;
$$;
