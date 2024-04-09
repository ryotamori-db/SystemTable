-- Databricks notebook source
-- MAGIC %md
-- MAGIC # billing.usageテーブルを用いた請求額の確認
-- MAGIC ```system.billing.usage``` テーブルを用いる事で、Databricksの全てのワークスペースにおける使用量を追跡する事ができます。  
-- MAGIC また、```system.billing.list_prices``` テーブルと組み合わせることで、コストを確認する事も可能です。  
-- MAGIC
-- MAGIC ```system.billing.usage``` テーブルには以下の情報が含まれます：  
-- MAGIC
-- MAGIC - ```account_id```
-- MAGIC : DatabricksのアカウントIDまたはAzureサブスクリプションID  
-- MAGIC - ```workspace_id```
-- MAGIC : 使用量が関連付けられたワークスペースID  
-- MAGIC - ```record_id```
-- MAGIC : レコードのユニークID  
-- MAGIC - ```sku_name```
-- MAGIC : SKU名（クラスター名など）  
-- MAGIC - ```cloud```
-- MAGIC : 利用が関連付けられたクラウド名  
-- MAGIC - ```usage_start_time```
-- MAGIC : 利用開始時間  
-- MAGIC - ```usage_end_time```
-- MAGIC : 利用終了時間  
-- MAGIC - ```usage_date```
-- MAGIC : 利用日  
-- MAGIC - ```custom_tags```
-- MAGIC : 利用に関連付けられているタグのメタデータ  
-- MAGIC - ```usage_unit```
-- MAGIC : 利用の単位（DBU）  
-- MAGIC - ```usage_quantity```
-- MAGIC : 消費された単位数  
-- MAGIC - ```usage_metadata```
-- MAGIC : 使用量に関するその他の関連情報  
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = 'main'
-- MAGIC schema = 'billing_forecast'
-- MAGIC
-- MAGIC dbutils.widgets.text('catalog', catalog)
-- MAGIC dbutils.widgets.text('schema', schema)

-- COMMAND ----------

-- MAGIC %run ./_resources/00-setup $reset_all_data=reset_all_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # billing.usageテーブルの中身を見てみる
-- MAGIC 以下セルを実行してみましょう。  
-- MAGIC workspace_idごとに、sku_name（クラスター）ごとの利用開始・終了時間、DBUの消費量（usage_quantity）が分かります。  

-- COMMAND ----------

select 
  * 
from 
  system.billing.usage 
  limit 20
;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # billing.list_pricesテーブルから各クラスターの価格を取得する
-- MAGIC 以下セルを実行してみましょう。  
-- MAGIC sku_name（クラスター名など）・CloudごとのDBUコスト（pricing_default）が分かります。 

-- COMMAND ----------

SELECT 
  sku_name, cloud, currency_code, usage_unit, pricing.default AS pricing_default
FROM
  system.billing.list_prices
  limit 20
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # billing.usageテーブルとbilling.list_pricesテーブルを用いてコストを確認する
-- MAGIC 以下セルを実行してみましょう。  
-- MAGIC usageテーブルの各行ごとに使用量（list_cost）が分かります。

-- COMMAND ----------

select
  u.account_id,
  u.workspace_id,
  u.sku_name,
  u.cloud,
  u.usage_start_time,
  u.usage_end_time,
  u.usage_date,
  date_format(u.usage_date, 'yyyy-MM') as YearMonth,
  u.usage_unit,
  u.usage_quantity,
  lp.pricing.default as list_price,
  lp.pricing.default * u.usage_quantity as list_cost,
  u.usage_metadata.*
from
  system.billing.usage u 
  inner join system.billing.list_prices lp on u.cloud = lp.cloud and
    u.sku_name = lp.sku_name and
    u.usage_start_time >= lp.price_start_time and
    (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)

where
  usage_metadata.job_id is not Null
