/* Bronze Layer ETL Procedure

The bronze.load_bronze stored procedure automates the ingestion of raw CRM and ERP data into the bronze layer using BULK INSERT.

It follows a full refresh approach by truncating existing tables and reloading data from CSV files. The procedure processes multiple datasets, logs execution time for each table, and tracks the total batch duration.

Error handling is implemented using TRY...CATCH to capture and report failures during execution.

🔑 Key Points
	•	Loads data from CSV files into staging tables
	•	Uses TRUNCATE + BULK INSERT for efficient processing
	•	Separates CRM and ERP data loading*/


create or alter procedure bronze.load_bronze as 
begin
DECLARE @start_time datetime,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME ;
 begin TRY

        set @batch_start_time= GETDATE();
        print'======================================';
        print'Loading Bronze Layer ';
        print'======================================';

        print'--------------------------------------';
        print'Loading CRM Tables ';
        print'--------------------------------------';

        set @start_time =GETDATE();
        print'>TRUNCATING Table: bronze.crm_cust_info';
        truncate TABLE bronze.crm_cust_info;

        print'Inserting Date into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time =GETDATE();
        print'>>Load Duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
        print'>>-----------------------'

        set @start_time =GETDATE();
        print'>TRUNCATING Table: bronze.crm_prd_info';
        truncate TABLE bronze.crm_prd_info;

        print'Inserting Date into: bronze.crm_prd_info';
        bulk insert bronze.crm_prd_info
        from '/prd_info.csv'
        with(
            firstrow=2,
            fieldterminator=',',
            TABLOCK
        );
        set @end_time =GETDATE();
        print'>>Load Duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
        print'>>-----------------------'

        set @start_time =GETDATE();
        print'>TRUNCATING Table: bronze.crm_sales_details';
        truncate TABLE bronze.crm_sales_details;

        print'Inserting Date into: bronze.crm_sales_details';
        bulk insert bronze.crm_sales_details
        from'/sales_details.csv'
        with(
            FIRSTROW = 2,
            fieldterminator=',',
            TABLOCK
        );
        set @end_time =GETDATE();
        print'>>Load Duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
        print'>>-----------------------'

            print'--------------------------------------';
            print'Loading ERP Tables ';
            print'--------------------------------------';


        set @start_time =GETDATE();
        print'>TRUNCATING Table: bronze.erp_CUST_AZ12';
        truncate TABLE bronze.erp_CUST_AZ12;

        print'Inserting Date into: bronze.erp_CUST_AZ12';
        bulk insert bronze.erp_CUST_AZ12
        from '/CUST_AZ12.csv'
        with(
        FIRSTROW = 2,
            fieldterminator=',',
            TABLOCK
        );
        set @end_time =GETDATE();
        print'>>Load Duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
        print'>>-----------------------'

        set @start_time =GETDATE();
        print'>TRUNCATING Table: bronze.erp_LOC_A101';
        truncate TABLE bronze.erp_LOC_A101;

        print'Inserting Date into: bronze.erp_LOC_A101';
        bulk insert bronze.erp_LOC_A101
        from '/LOC_A101.csv'
        with(
            firstrow=2,
            fieldterminator=',',
            TABLOCK
        );
        set @end_time =GETDATE();
        print'>>Load Duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
        print'>>-----------------------'

        set @start_time =GETDATE();
        print'>TRUNCATING Table: bronze.erp_PX_CAT_G1V2';
        truncate TABLE bronze.erp_PX_CAT_G1V2;

        print'Inserting Date into: bronze.erp_PX_CAT_G1V2';
        bulk insert bronze.erp_PX_CAT_G1V2
        from'/PX_CAT_G1V2.csv'
        with(
            firstrow=2,
            fieldterminator=',',
            TABLOCK
        );
        set @end_time =GETDATE();
        print'>>Load Duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+ 'seconds';
        print'>>-----------------------'
         set @batch_end_time= GETDATE();
         print'----------------------------------------------------';
         print'loading bronze layer is completed ';
         print'      - total load duration:'+ cast(DATEDIFF(second,@batch_start_time,@batch_end_time) as NVARCHAR)+ 'seconds';
         print'----------------------------------------------------';
    end TRY
    BEGIN catch 
      print'======================================';
      print'ERROR OCCURED DURING LOADING BRONZE LAYER'
      print'Error message'+ERROR_MESSAGE();
      print'Error message'+CAST(ERROR_NUMBER() as NVARCHAR);
      print'Error message'+CAST(ERROR_STATE() as NVARCHAR);
      print'======================================';
    end CATCH
    END




