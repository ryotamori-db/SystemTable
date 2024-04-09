-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # コスト分析ダッシュボード用クエリー集
-- MAGIC このノートブックは、コスト分析ダッシュボード用のクエリーをまとめたものです。  
-- MAGIC **dndemosでダッシュボードをインストールできない方向けのノートブックになります**  
-- MAGIC
-- MAGIC 適宜以下クエリーをSQLエディタで編集&保存してください。  
-- MAGIC **・編集イメージ**  
-- MAGIC <img src="https://github.com/ryotamori-db/SystemTable/blob/main/images/saveimages1.png?raw=true" width="1000px">  
-- MAGIC
-- MAGIC **・保存イメージ**  
-- MAGIC <img src="https://github.com/ryotamori-db/SystemTable/blob/main/images/saveimages2.png?raw=true" width="1000px">  

-- COMMAND ----------

-- System Tables - date ranges
SELECT CONCAT(start_date, ' | ', label) FROM (
  SELECT cast(current_date as date) as start_date , label, o FROM 
( SELECT DATE_TRUNC('YEAR', CURRENT_DATE()) as current_date, 'Current Year' as label, 1 as o
  UNION
  SELECT DATE_TRUNC('MONTH', CURRENT_DATE()) as current_date, 'Current Month' as label, 2 as o
  UNION
  SELECT add_months(DATE_TRUNC('MONTH', CURRENT_DATE()), -6)  as current_date, 'Last 6 Months' as label, 3 as o
  UNION
  SELECT add_months(DATE_TRUNC('MONTH', CURRENT_DATE()), -12)  as current_date, 'Last 12 Months' as label, 4 as o
  )
order by o );

-- COMMAND ----------

-- System Tables - Distinct SKU
-- param query - the import needs the  query to be run first even empty.
CREATE SCHEMA IF NOT EXISTS `main`.`billing_forecast`;
CREATE TABLE IF NOT EXISTS `main`.`billing_forecast`.billing_forecast (sku STRING, workspace_id STRING);
INSERT INTO `main`.`billing_forecast`.billing_forecast (sku, workspace_id) SELECT 'ALL', 'ALL'
  WHERE NOT EXISTS (SELECT * FROM  `main`.`billing_forecast`.billing_forecast);
  
select distinct(sku) from `main`.`billing_forecast`.billing_forecast order by sku 

-- COMMAND ----------

-- System Table - Distinct workspace id
-- param query - the import needs the  query to be run first even empty.
CREATE SCHEMA IF NOT EXISTS `main`.`billing_forecast`;
CREATE TABLE IF NOT EXISTS `main`.`billing_forecast`.billing_forecast (sku STRING, workspace_id STRING);
INSERT INTO `main`.`billing_forecast`.billing_forecast (sku, workspace_id) SELECT 'ALL', 'ALL'
  WHERE NOT EXISTS (SELECT * FROM  `main`.`billing_forecast`.billing_forecast);


select distinct(workspace_id) from `main`.`billing_forecast`.billing_forecast order by workspace_id DESC;

-- COMMAND ----------

-- System Tables - DBUs forecast monthly 
select date_trunc('MONTH', date) as month, sum(list_cost)
from `main`.`billing_forecast`.detailed_billing_forecast where sku='{{sku}}' and workspace_id='{{workspace_id}}'
and date_trunc('MONTH', date) in (add_months(date_trunc('MONTH', now()),1), date_trunc('MONTH', now()))
GROUP BY 1
ORDER By 1 desc

-- COMMAND ----------

-- System Tables - DBUs monthly
select date_trunc('MONTH', date), sum(list_cost)
from `main`.`billing_forecast`.detailed_billing_forecast 
WHERE past_list_cost is not null AND sku='{{sku}}' and workspace_id='{{workspace_id}}'
GROUP BY 1
ORDER By 1 desc

-- COMMAND ----------

-- System Tables - forecast DBUs
select * from `main`.`billing_forecast`.detailed_billing_forecast 
  where sku='{{sku}}' and workspace_id='{{ workspace_id }}' and date > SUBSTRING_INDEX('{{start_date}}', ' |', 1)

-- COMMAND ----------

-- System Tables - next 6mo forecast
select sum(list_cost)
from `main`.`billing_forecast`.detailed_billing_forecast 
WHERE  sku='{{sku}}' and workspace_id='{{workspace_id}}' and date >= now() and date < add_months(now(), 3)

-- COMMAND ----------

-- System Tables - SKU repartition
WITH forecast as (
  select date, sku, list_cost from `main`.`billing_forecast`.detailed_billing_forecast 
      where workspace_id='{{ workspace_id }}' and sku !='ALL' and date > SUBSTRING_INDEX('{{start_date}}', ' |', 1) )
SELECT * FROM forecast
UNION
  select NOW() as date, '_NOW' as sku, sum(list_cost) * 1.5 as list_cost from forecast where date=CURRENT_DATE()
order by sku ASC

-- COMMAND ----------

-- System Tables - spend from date
select sum(list_cost)
from `main`.`billing_forecast`.detailed_billing_forecast 
where sku='{{sku}}' and workspace_id='{{workspace_id}}' and date <= now() and date >= SUBSTRING_INDEX('{{start_date}}', ' |', 1)

-- COMMAND ----------

-- System Tables - Table Count
SELECT  count(DISTINCT table_catalog) AS catalog_count,
        count(DISTINCT concat(table_catalog, table_schema)) AS schema_count,
        count(DISTINCT concat(table_catalog, table_schema, table_name)) AS table_count
FROM
    system.information_schema.tables
WHERE
    table_type != 'VIEW'
    AND table_schema NOT IN ('information_schema')



-- COMMAND ----------

-- System Tables - Table Creation
-- Dashboard Name: DBSQL Governance Dashboard
-- Report Name: Table Types by Created Mont
-- Query Name: dbsql-table-types
SELECT
    table_catalog,
    table_schema,
    table_type,
    DATE_TRUNC('month', created) AS month_created,
    COUNT(*) AS table_count
FROM
    system.information_schema.tables
WHERE
    table_schema NOT IN ('information_schema')
GROUP BY
    table_catalog,
    table_schema,
    DATE_TRUNC('month', created),
    table_type
ORDER BY
    month_created ASC,
    table_count DESC

-- COMMAND ----------

-- System Tables - Table format
-- Dashboard Name: DBSQL Governance Dashboard
-- Report Name: Tables by Format
-- Query Name: dbsql-table-formats
SELECT
    table_catalog AS cat_name,
    table_schema AS sch_name,
    data_source_format AS table_format,
    COUNT(*) AS table_count
FROM
    system.information_schema.tables
WHERE
    table_type != 'VIEW'
    AND table_schema NOT IN ('information_schema')
GROUP BY
    cat_name,
    sch_name,
    table_format 
ORDER BY
    table_count DESC

-- COMMAND ----------

-- System Tables - Total consumption
WITH 
-- This makes sure we don't have gap (days without consumption) by creating 1 dataframe containing 1 row per day and then joining it to the actual billing.
numbers AS (
  SELECT ROW_NUMBER() OVER (PARTITION BY 0 ORDER BY ds)  AS num
  FROM `main`.`billing_forecast`.billing_forecast
  LIMIT abs(datediff(SUBSTRING_INDEX('{{start_date}}', ' |', 1), add_months(CURRENT_DATE(), 3)))
),
skus AS (
  SELECT distinct(sku) as sku FROM `main`.`billing_forecast`.billing_forecast where sku != 'ALL'
),
days as (select date_add(add_months(CURRENT_DATE(), 3), -num) as date, sku FROM numbers cross join skus),
f as (SELECT * FROM `main`.`billing_forecast`.detailed_billing_forecast  where workspace_id='{{ workspace_id }}' ),
cum as (
  SELECT *, SUM(list_cost) OVER (PARTITION BY sku ORDER BY date) AS cum_list_cost FROM (
    select days.sku, days.date, coalesce(f.list_cost, 0) as list_cost FROM days left join f on f.date = days.date and f.sku=days.sku )
    where sku != 'ALL' )
SELECT * from cum
-- Add a vertical bar on current date to highlight forecast
UNION
  SELECT '_NOW' as sku, NOW() as date, sum(cum.cum_list_cost)*1.4 as list_cost, sum(cum.cum_list_cost)*1.4 as cum_list_cost 
    from cum where date=CURRENT_DATE() 
order by sku asc 
;




-- COMMAND ----------

-- System Tables - Training date check
select datediff(current_date(), training_date) as days_since_last_training, training_date, 0 as target from (SELECT max(training_date) as training_date from `main`.`billing_forecast`.billing_forecast)

-- COMMAND ----------

-- System Tables - Volume creation
-- Dashboard Name: DBSQL Governance Dashboard
-- Report Name: Volume Types by Created Mont
-- Query Name: dbsql-volume-types
SELECT
    volume_catalog,
    volume_schema,
    volume_type,
    DATE_TRUNC('month', created) AS month_created,
    COUNT(*) AS volume_count
FROM
    system.information_schema.volumes
WHERE
    volume_schema NOT IN ('information_schema')
GROUP BY
    volume_catalog,
    volume_schema,
    DATE_TRUNC('month', created),
    volume_type
ORDER BY
    month_created ASC,
    volume_count DESC
