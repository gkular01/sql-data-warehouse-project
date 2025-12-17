/*
==========================================================================

Stored Procedure: Load Bronze Layer (Source -> Bronze)

==========================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files to bronze tables.

Parameters:
    None.

    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

==========================================================================
*/


create or alter procedure bronze.load_bronze as
begin
    DECLARE @start_time datetime,@end_time datetime,@batch_end_time datetime,@batch_start_time datetime;

    begin try
        set @batch_start_time = GETDATE();
        Print '==========================================================' ;
        print 'Loading Bronze Layer';
        Print '==========================================================' ;

        print '-----------------------------------------------------------';
        print 'Loading CRM Tables Layer';
        print '-----------------------------------------------------------'

        set @start_time=GETDATE();
        print'>>Truncating table:bronze.crm_cust_info';
        Truncate table bronze.crm_cust_info;

    
        print'>>Inserting data into:bronze.crm_cust_info';
        bulk insert bronze.crm_cust_info
        from 'C:\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        with (
            firstrow=2,
            fieldterminator = ',',
            tablock

        );
         set @end_time=GETDATE();
         print'>> Load Duration:'+CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) +'seconds';
         print '-----------------------------------------------------------'
    

        set @start_time=GETDATE();
        print'>>Truncating table:bronze.crm_prd_info';
        Truncate table bronze.crm_prd_info;

        print'>>Inserting data into:bronze.crm_prd_info';
        bulk insert bronze.crm_prd_info
        from 'C:\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        with (
            firstrow=2,
            fieldterminator = ',',
            tablock

        );
        set @end_time=GETDATE();
        print'>> Load Duration:'+CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) +'seconds';
        print '-----------------------------------------------------------'

        set @start_time=GETDATE();
        print'>>Truncating table:bronze.crm_sales_details';
        Truncate table bronze.crm_sales_details;

        print'>>Inserting data into:bronze.crm_sales_details';
        bulk insert bronze.crm_sales_details
        from 'C:\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        with (
            firstrow=2,
            fieldterminator = ',',
            tablock

        );
        set @end_time=GETDATE();
         print'>> Load Duration:'+CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) +'seconds';
        print '-----------------------------------------------------------'

        print '-----------------------------------------------------------';
        print 'Loading ERP Tables Layer';
        print '-----------------------------------------------------------';

        set @start_time=GETDATE();
        print'>>Truncating table:bronze.erp_CUST_AZ12';
        Truncate table bronze.erp_CUST_AZ12;

        print'>>Inserting data into:bronze.erp_CUST_AZ12';
        bulk insert bronze.erp_CUST_AZ12
        from 'C:\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        with (
            firstrow=2,
            fieldterminator = ',',
            tablock

        );
        set @end_time=GETDATE();
        print'>> Load Duration:'+CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) +'seconds';
        print '-----------------------------------------------------------'

        set @start_time=GETDATE();
        print'>>Truncating table:bronze.erp_LOC_A101';
        Truncate table bronze.erp_LOC_A101;

        print'>>Inserting data into:bronze.erp_LOC_A101';
        bulk insert bronze.erp_LOC_A101
        from 'C:\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        with (
            firstrow=2,
            fieldterminator = ',',
            tablock

        );
        set @end_time=GETDATE();
        print'>> Load Duration:'+CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) +'seconds';
        print '-----------------------------------------------------------'

        set @start_time=GETDATE();
        print'>>Truncating table:bronze.erp_PX_CAT_G1V2';
        Truncate table bronze.erp_PX_CAT_G1V2;

        print'>>Inserting data into:bronze.erp_PX_CAT_G1V2';
        bulk insert bronze.erp_PX_CAT_G1V2
        from 'C:\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        with (
            firstrow=2,
            fieldterminator = ',',
            tablock
    
        );
        set @end_time=GETDATE();
        print'>> Load Duration:'+CAST(DATEDIFF(second, @start_time, @end_time) as nvarchar) +'seconds';
        print '-----------------------------------------------------------'

        set @batch_end_time=GETDATE();
        print '===================================================================';
        print 'Loading Bronze layer is completed';
        print'>> Total Load Duration:'+CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as nvarchar) +'seconds';
        print '===================================================================';
    end try

   
    begin catch
        print '===================================================================';
        print 'Error occurred during loading bronze layer';
        print 'Error Message' +ERROR_MESSAGE();
        print 'Error Message' + CAST(ERROR_MESSAGE() AS NVARCHAR);
        print 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
        print '===================================================================';
    end catch
end
