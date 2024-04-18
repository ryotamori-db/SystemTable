-- Databricks notebook source
-- MAGIC %md
-- MAGIC # サンプルクエリー集  
-- MAGIC ```system.marketplace.listing_access_events``` テーブル、```system.marketplace.listing_funnel_events``` テーブルを用いて、マーケットプレイスイベントを分析してみましょう。  
-- MAGIC
-- MAGIC **マーケットプレイス**  
-- MAGIC https://docs.databricks.com/ja/administration-guide/system-tables/marketplace.html#example-queries  
-- MAGIC
-- MAGIC - 過去10日間の消費者の要求
-- MAGIC - 要求数別の上位リスト
-- MAGIC - 上位の要求者を表示する
-- MAGIC - リスティングに対して各消費者アクションが実行された回数を表示する

-- COMMAND ----------

-- 過去10日間の消費者の要求
SELECT event_date, provider_name, listing_name, listing_id, consumer_delta_sharing_recipient_name, consumer_cloud, consumer_region, consumer_name, consumer_email, consumer_company
FROM system.marketplace.listing_access_events
WHERE event_type = 'REQUEST_DATA'
AND event_date >= date_add(current_date(), -10)


-- COMMAND ----------

-- 要求数別の上位リスト
SELECT listing_name, consumer_cloud, count(*) as requestCount
FROM system.marketplace.listing_access_events
GROUP BY listing_name, consumer_cloud
ORDER BY requestCount DESC


-- COMMAND ----------

-- 上位の要求者を表示する
SELECT consumer_name, consumer_email, count(*) as requestCount
FROM system.marketplace.listing_access_events
GROUP BY consumer_name, consumer_email
ORDER BY requestCount DESC


-- COMMAND ----------

-- リスティングに対して各消費者アクションが実行された回数を表示する
SELECT event_type, COUNT(*) as occurrences
FROM system.marketplace.listing_funnel_events
WHERE listing_name = `{{listing_name}}`
GROUP BY event_type

