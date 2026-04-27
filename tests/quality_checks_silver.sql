/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

-- This script is written to clean and transform the bronze.crm_cust_info table and transfer it to the silver layer
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING  COUNT(*)>1 OR cst_id IS NULL

-- şimdi tabloya baktık ve cst_id de tekrar eden ya da null olan değerler olduğunu gördük. 
-- Bu sebeple null olmayan ama tekrar eden değerleri 
-- create date göre sıralayıp en son oluşturulanı getiren bir tablo oluşturduk
SELECT 
*
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL 
	) t WHERE flag_last = 1

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--Aynısını lastname için yapalım
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)



--Aynısını gender için yapalım
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

--Gerekli dönüşümleri yaparak tek sorgu şeklinde birleştirmeye devam ediyoruz
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_material_status,
cst_gndr,
cst_create_date
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL 
	) t WHERE flag_last = 1

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info


-- gndr sütunu için gerekli değişiklikleri yaparak devam ediyoruz
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_material_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL 
	) t WHERE flag_last = 1


	SELECT DISTINCT cst_material_status
	FROM bronze.crm_cust_info

-- material_status sütunu için gerekli değişiklikleri yaparak devam ediyoruz
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_marital_status,              -- Normalize marital status values to red<ble format
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gndr,                          -- Normalize gender values to readable format
cst_create_date
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL 
	) t WHERE flag_last = 1            -- Select the most recent record per customer


-- Şimdi düzenlemesi tamamlanan bronze.crm_cust_info tablosunu silver katmana yükleyelim
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gndr,
	cst_create_date)

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL 
	) t WHERE flag_last = 1


--Şimdi en baştan aynı sorguları silver katman için yapalım ve verileri düzgün temizlemiş miyiz bakalım
-- cst_id için hiçbir duplicate ya da NULL kayıt kalmamış
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING  COUNT(*)>1 OR cst_id IS NULL

-- cst_fistname de boşluklu değer var mı bakalım
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- cst_lastname de boşluklu değer var mı bakalım
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


-- cst_gndr sütununda değerler düzgün mü bakalım
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

-- şimdi tüm silver cust infoya göz atalım
SELECT * FROM silver.crm_cust_info;
-- This script is written to clean and transform the bronze.crm_prd_info table and transfer it to the silver layer
-- Check for NULLS or Duplicates in Primary Key
-- Expectation: No Result
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL

-- prd_key i iki farklı sütuna ayıralım ve - _ ile değiştirelim
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-','_') AS cat_id,   --Extract category ID (Derived Columns)
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,         --Extract product key
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_date
FROM bronze.crm_prd_info

-- Ürün adında boşluklar var mı kontrol edelim
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs OR Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- prd_cost sütunundaki NULL değerleri 0 ile değiştirelim
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost,        --Handling missing value
prd_line,
prd_start_dt,
prd_end_date
FROM bronze.crm_prd_info

--prd_line sütununa bakalım
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

--prd_line sütununda kısaltmaları düzletelim ve Null değerlere n/a atayalım
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
CASE UPPER(TRIM(prd_line))      -- Data Normalization
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,    -- Map product line codes to descriptive values 
prd_start_dt,
prd_end_date
FROM bronze.crm_prd_info

SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_date< prd_start_dt

SELECT * FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_date,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

-- start ve end date sütunlarını düzenliyoruz.
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,      --Data Type Transfromation
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt   --Data Type Transformation
FROM bronze.crm_prd_info

--Şimdi oluşturmuş olduğumuz bu yeni tabloyu silver layer katmanına ekliyoruz.
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
REPLACE(SUBSTRING(prd_key,1,5), '-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,                                 
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt  --Calculate end date as one day before the next start date DATA ENRICHMENT
FROM bronze.crm_prd_info

SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt< prd_start_dt

SELECT*FROM silver.crm_prd_info

-- This script is written to clean and transform the bronze.crm_sales_details table and transfer it to the silver layer
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id from silver.crm_cust_info)

--check for invalid dates
SELECT NULLIF(sls_order_dt,0) sls_order_dt 
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0;

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL   --sayı değeri 0 a eşitse ya da 8 e eşit değilse(8 den fazla ya da az basamak varsa tarihe dönüştürülemez)
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)            --Yerine NULL koy ve kalanların veri tipini DATE e dönüştür.
END AS sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details

SELECT NULLIF(sls_ship_dt,0) sls_ship_dt 
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8;

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL   --sayı değeri 0 a eşitse ya da 8 e eşit değilse(8 den fazla ya da az basamak varsa tarihe dönüştürülemez)
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)            --Yerine NULL koy ve kalanların veri tipini DATE e dönüştür.
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details

SELECT NULLIF(sls_due_dt,0) sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8;

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL   --sayı değeri 0 a eşitse ya da 8 e eşit değilse(8 den fazla ya da az basamak varsa tarihe dönüştürülemez)
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)            --Yerine NULL koy ve kalanların veri tipini DATE e dönüştür.
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details

--check for invalid date orders
SELECT 
* 
FROM bronze.crm_sales_details
WHERE sls_order_dt>sls_ship_dt OR sls_order_dt> sls_due_dt OR sls_ship_dt>sls_due_dt

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	   THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,                              --Recalculate sales if original value is misssing or incorrect
CASE WHEN sls_price IS NULL OR sls_price <= 0
	  THEN sls_sales /NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price                               --Derive price if original value is invalid 
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity,sls_price

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL   --sayı değeri 0 a eşitse ya da 8 e eşit değilse(8 den fazla ya da az basamak varsa tarihe dönüştürülemez)
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)            --Yerine NULL koy ve kalanların veri tipini DATE e dönüştür.
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR  sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details

--ELDE ETTİĞİMİZ  BU YENİ TABLOYU SİLVER KATMANDAKİ SALES_DETAİLS TABLOSUNA YÜKLEYELİM
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
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL   
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)            
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR  sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details

--şimdi aynı sorguları silver katmandaki tablonun doğruluğunu kontrol etmek için silver için çalıştıralım
SELECT NULLIF(sls_order_dt,0) sls_order_dt 
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0;

SELECT NULLIF(sls_ship_dt,0) sls_ship_dt 
FROM silver.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8;

SELECT NULLIF(sls_due_dt,0) sls_due_dt 
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8;

SELECT 
* 
FROM silver.crm_sales_details
WHERE sls_order_dt>sls_ship_dt OR sls_order_dt> sls_due_dt OR sls_ship_dt>sls_due_dt

SELECT DISTINCT
sls_sales ,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity,sls_price

SELECT * FROM silver.crm_sales_details

SELECT* FROM [silver].[crm_cust_info];
SELECT * FROM bronze.erp_cust_az12;

SELECT * FROM bronze.erp_cust_az12
WHERE cid like 'AW%'


--cust_info daki cst_id ile uysun diye baştali NAS leri kaldırdık.
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12

--bdate sütununun kalitesine bakalım
SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate> GETDATE()  --Identify out of range dates

SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL          --bdate gelecekten büyük ise NULL ile değiştirdik
	ELSE bdate
END AS bdate,
gen
FROM bronze.erp_cust_az12

-- Data Standardization & Consistency
SELECT DISTINCT gen
FROM bronze.erp_cust_az12

SELECT  DISTINCT
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL          --bdate gelecekten büyük ise NULL ile değiştirdik
	ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

--VERİLERİN SON HALİNİ SİLVER KATMANA YÜKLEYELİM
INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen
)
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))   --REMOVE 'NAS' PREFİX IF PRESENT
	ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL          --bdate gelecekten büyük ise NULL ile değiştirdik
	ELSE bdate
END AS bdate,                                  -- Set future birthdates to NULL
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     ELSE 'n/a'
END AS gen                                     -- Normalize gender values and handle unknown cases
FROM bronze.erp_cust_az12

-- silver katmandaki verilerimizin kalitesini kontrol edelim
--NAS ile başlayan veri kalmamış
SELECT * FROM silver.erp_cust_az12
WHERE cid like 'NAS%'

--Bdate tarihi de düzgün
SELECT * FROM silver.erp_cust_az12
WHERE bdate > GETDATE();

--GEN SÜTUNUNU KONTROL EDELİM
SELECT DISTINCT gen
FROM silver.erp_cust_az12


--crm.cust_info tablosundaki cst_key ile bağlayabilmek için şunu kaldırıp  - birleştirdik
SELECT 
REPLACE (cid,'-','') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101

--Data Standardization & Consistency
SELECT DISTINCT cntry 
FROM bronze.erp_loc_a101

--Şimdi elde ettiğimiz son tabloyu silver katmana ekleyelim
INSERT INTO silver.erp_loc_a101 (
cid,
cntry
)
SELECT 
REPLACE (cid,'-','') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry                 --Normalize and Handle missing or blank country codes
FROM bronze.erp_loc_a101

--Silver katmandaki verilerin kalitesine bakalım
SELECT DISTINCT cntry 
FROM silver.erp_loc_a101

SELECT *
FROM bronze.erp_px_cat_g1v2

--Check for unwanted spaces
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE TRIM(cat) != cat OR TRIM(subcat) != subcat OR TRIM(maintenance) != maintenance

--Data Standardization & Consistency
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2

INSERT INTO silver.erp_px_cat_g1v2 (
id,
cat,
subcat,
maintenance
)
SELECT 
id,
cat,
subcat,
maintenance 
FROM bronze.erp_px_cat_g1v2

