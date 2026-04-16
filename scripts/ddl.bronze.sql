/*🧱 Bronze Layer – Data Warehouse Schema

This script initializes the bronze layer of the data warehouse by creating raw staging tables for CRM and ERP data sources. The bronze layer stores data in its original format with minimal transformation, serving as the foundation for downstream processing.

📌 Key Features
	•	Drops existing tables if they exist to ensure a clean setup
	•	Creates structured tables for both CRM and ERP systems
	•	Uses appropriate data types (INT, NVARCHAR, DATE, DATETIME) for raw ingestion

📂 Tables Created

🔹 CRM Tables
	•	crm_cust_info: Customer details (ID, name, gender, marital status, creation date)
	•	crm_prd_info: Product information (ID, name, cost, product line, lifecycle dates)
	•	crm_sales_details: Sales transactions (order details, product, customer, pricing, quantities)

🔹 ERP Tables
	•	erp_CUST_AZ12: Customer demographic data (ID, birthdate, gender)
	•	erp_LOC_A101: Customer location data (country)
	•	erp_PX_CAT_G1V2: Product category hierarchy (category, subcategory, maintenance)

🧠 Purpose

This layer is designed for:
	•	Raw data ingestion from multiple source systems
	•	Preserving original data for traceability and auditing
	•	Acting as the input layer for transformation into silver and gold layers


=================================================================================================
*/

if OBJECT_ID('bronze.crm_cust_info','U') is not NULL
    drop table bronze.crm_cust_info;
create table bronze.crm_cust_info(
   cst_id int ,
   cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50) ,
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50) ,
    cst_create_date DATE
)
if OBJECT_ID('bronze.crm_prd_info','U') is not NULL
    drop table bronze.crm_prd_info;
create table bronze.crm_prd_info(
    prd_id int ,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost int ,
    prd_line NVARCHAR(50),
    prd_start_dt datetime ,
    prd_end_dt datetime
)
if OBJECT_ID('bronze.crm_sales_details','U') is not NULL
    drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
    sls_ord_num NVARCHAR(50), 
    sls_prd_key NVARCHAR(50), 
    sls_cust_id int,
    sls_order_dt int, 
    sls_ship_dt int,
    sls_due_dt int ,
    sls_sales int ,
    sls_quantity int,
    sls_price int
)
if OBJECT_ID('bronze.erp_CUST_AZ12','U') is not NULL
    drop table bronze.erp_CUST_AZ12;
create table bronze.erp_CUST_AZ12(
    CID NVARCHAR(50),
    BDATE date,
    GEN NVARCHAR(50) 
)
if OBJECT_ID('bronze.erp_LOC_A101','U') is not NULL
    drop table bronze.erp_LOC_A101;
create table bronze.erp_LOC_A101(
    CID NVARCHAR(50),
    CNTRY NVARCHAR(50)
)
if OBJECT_ID('bronze.erp_LOC_A101','U') is not NULL
    drop table bronze.erp_PX_CAT_G1V2;
create table bronze.erp_PX_CAT_G1V2(
    ID NVARCHAR(50),
    CAT NVARCHAR(50),
    SUBCAT NVARCHAR(50),
    MAINTENANCE NVARCHAR(50)
);

