-- Databricks notebook source
-- MAGIC %md
-- MAGIC # サンプルクエリー集  
-- MAGIC ```system.billing.list_prices``` テーブルを用いて、価格表を分析してみましょう。  
-- MAGIC これらのクエリーを機能させるには、中括弧{{}}内の値を編集する必要があります。  
-- MAGIC
-- MAGIC **価格表**  
-- MAGIC https://docs.databricks.com/ja/administration-guide/system-tables/pricing.html#sample-queries  
-- MAGIC
-- MAGIC - 特定の日付に対する特定のSKUの定価を検索する
-- MAGIC - 前月に特定のカスタムタグを使用したすべての合計費用を表示する
-- MAGIC - 月間で変更された価格を表示する
-- MAGIC - 前暦月の使用に対するアドオンコストを見積もる

-- COMMAND ----------

-- 特定の日付に対する特定のSKUの定価を検索する
-- テーブルには SKU 価格が変更された時刻のレコードのみが含まれているため、日付以前の最新の価格変更を検索する必要があります。
SELECT sku_name, price_start_time, pricing.default
FROM system.billing.list_prices
WHERE sku_name = 'STANDARD_ALL_PURPOSE_COMPUTE'
AND price_start_time <= "2023-01-01"
ORDER BY price_start_time DESC
LIMIT 1

-- COMMAND ----------

-- 前月に特定のカスタムタグを使用したすべての合計費用を表示する
-- カスタム タグのキーと値を必ず置き換えてください。
SELECT SUM(usage.usage_quantity * list_prices.pricing.default) as `Total Dollar Cost`
FROM system.billing.usage
JOIN system.billing.list_prices
ON list_prices.sku_name = usage.sku_name
WHERE usage.custom_tags.{{ tag_key }} = {{ tag_value }}
AND usage.usage_end_time >= list_prices.price_start_time
AND (list_prices.price_end_time IS NULL OR usage.usage_end_time < list_prices.price_end_time)
AND usage.usage_date BETWEEN "2023-05-01" AND "2023-05-31"

-- COMMAND ----------

-- 月間で変更された価格を表示する
SELECT sku_name, price_start_time, pricing.default
FROM system.billing.list_prices
WHERE price_start_time BETWEEN "2023-05-01" AND "2023-07-01"

-- COMMAND ----------

-- 前暦月の使用に対するアドオンコストを見積もる
-- このクエリは、期間内のすべての使用量に単純な割合を適用します。 これは、一部のアドオンのエンタイトルメントの管理方法により、実際の収益化とは若干異なる場合があることに注意してください。 アドオン レートをアカウントのレートに置き換えます。
SELECT SUM(usage.usage_quantity * list_prices.pricing.default) * {{ add_on_rate }} as `Total Add-On Dollar Cost`
FROM system.billing.usage
JOIN system.billing.list_prices ON list_prices.sku_name = usage.sku_name
  WHERE usage.usage_end_time >= list_prices.price_start_time
  AND (list_prices.price_end_time IS NULL OR usage.usage_end_time < list_prices.price_end_time)
  AND usage.usage_date BETWEEN "2024-02-01" AND "2024-02-29"
