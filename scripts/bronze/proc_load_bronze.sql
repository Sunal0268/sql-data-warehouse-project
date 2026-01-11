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

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    start_time := clock_timestamp();

    RAISE NOTICE '=========================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE 'Start time: %', start_time;
    RAISE NOTICE '=========================';

    RAISE NOTICE 'Truncating: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

	RAISE NOTICE 'Truncating: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE 'Truncating: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE 'Truncating: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE 'Truncating: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE 'Truncating: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    end_time := clock_timestamp();
    RAISE NOTICE 'End time: %', end_time;
    RAISE NOTICE 'Duration (seconds): %',
        EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE 'Bronze layer ready for load';
END;
$$;
CALL bronze.load_bronze();
