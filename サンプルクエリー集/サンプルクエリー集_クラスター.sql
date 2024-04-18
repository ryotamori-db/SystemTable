-- Databricks notebook source
-- MAGIC %md
-- MAGIC # サンプルクエリー集  
-- MAGIC ```system.compute.clusters``` テーブル、```system.compute.node_types``` テーブルを用いて、クラスターを分析してみましょう。  
-- MAGIC
-- MAGIC **クラスター**  
-- MAGIC https://docs.databricks.com/ja/administration-guide/system-tables/compute.html#sample-queries  
-- MAGIC
-- MAGIC - クラスターレコードを最新の請求レコードと結合する
-- MAGIC - クラスター所有者ごとのクラスターコストを算出する

-- COMMAND ----------

-- クラスターレコードを最新の請求レコードと結合する
-- このクエリーは、時間の経過に伴う支出を理解するのに役立ちます。
-- usage_start_timeを最新の請求期間に更新すると、請求レコードに対する最新の更新が取得され、クラスターデータに結合されます。
-- 各レコードは、その特定の実行中にクラスター所有者に関連付けられます。そのため、クラスターの所有者が変更された場合、コストはクラスターが使用された時期に基づいて正しい所有者にロールアップされます。
SELECT
  u.record_id,
  c.cluster_id,
  c.owned_by,
  c.change_time,
  u.usage_start_time,
  u.usage_quantity
FROM
  system.billing.usage u
  JOIN system.compute.clusters c
  JOIN (SELECT u.record_id, c.cluster_id, max(c.change_time) change_time
    FROM system.billing.usage u
    JOIN system.compute.clusters c
    WHERE
      u.usage_metadata.cluster_id is not null
      and u.usage_start_time >= '2023-01-01'
      and u.usage_metadata.cluster_id = c.cluster_id
      and date_trunc('HOUR', c.change_time) <= date_trunc('HOUR', u.usage_start_time)
    GROUP BY all) config
WHERE
  u.usage_metadata.cluster_id is not null
  and u.usage_start_time >= '2023-01-01'
  and u.usage_metadata.cluster_id = c.cluster_id
  and u.record_id = config.record_id
  and c.cluster_id = config.cluster_id
  and c.change_time = config.change_time
ORDER BY cluster_id, usage_start_time desc;


-- COMMAND ----------

-- クラスター所有者ごとのクラスターコストを算出する
-- コンピュート コストの削減を検討している場合は、このクエリーを使用して、アカウント内のどのクラスター所有者が最も多くのDBUを使用しているかを確認できます。
SELECT
  u.record_id record_id,
  c.cluster_id cluster_id,
  max_by(c.owned_by, c.change_time) owned_by,
  max(c.change_time) change_time,
  any_value(u.usage_start_time) usage_start_time,
  any_value(u.usage_quantity) usage_quantity
FROM
  system.billing.usage u
  JOIN system.compute.clusters c
WHERE
  u.usage_metadata.cluster_id is not null
  and u.usage_start_time >= '2023-01-01'
  and u.usage_metadata.cluster_id = c.cluster_id
  and c.change_time <= u.usage_start_time
GROUP BY 1, 2
ORDER BY cluster_id, usage_start_time desc;

