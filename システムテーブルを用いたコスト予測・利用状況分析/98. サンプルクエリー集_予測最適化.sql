-- Databricks notebook source
-- MAGIC %md
-- MAGIC # サンプルクエリー集  
-- MAGIC ```system.storage.predictive_optimization_operations_history``` テーブルを用いて、予測最適化の操作履歴を分析してみましょう。  
-- MAGIC これらのクエリーを機能させるには、中括弧{{}}内の値を編集する必要があります。  
-- MAGIC
-- MAGIC **予測最適化**  
-- MAGIC https://docs.databricks.com/ja/administration-guide/system-tables/predictive-optimization.html#example-queries  
-- MAGIC
-- MAGIC - 過去30日間に予測的最適化が使用されたDBUの数はいくつですか?
-- MAGIC - 過去30日間に予測的最適化に最も多く費やされたのはどのテーブルですか?
-- MAGIC - 予測的最適化が最も多くの操作を実行しているのはどのテーブルですか?
-- MAGIC - 特定のカタログについて、合計何バイトが圧縮されましたか?
-- MAGIC - バキューム処理されたバイト数が最も多かったテーブルはどれですか?
-- MAGIC - 予測的最適化によって実行される操作の成功率はどれくらいですか?

-- COMMAND ----------

-- 過去30日間に予測的最適化が使用されたDBUの数はいくつですか?
SELECT SUM(usage_quantity)
FROM system.storage.predictive_optimization_operations_history
WHERE
     usage_unit = "ESTIMATED_DBU"
     AND  timestampdiff(day, start_time, Now()) < 30


-- COMMAND ----------

-- 過去30日間に予測的最適化に最も多く費やされたのはどのテーブルですか?
SELECT
     metastore_name,
     catalog_name,
     schema_name,
     table_name,
     SUM(usage_quantity) as totalDbus
FROM system.storage.predictive_optimization_operations_history
WHERE
    usage_unit = "ESTIMATED_DBU"
    AND timestampdiff(day, start_time, Now()) < 30
GROUP BY ALL
ORDER BY totalDbus DESC


-- COMMAND ----------

-- 予測的最適化が最も多くの操作を実行しているのはどのテーブルですか?
SELECT
     metastore_name,
     catalog_name,
     schema_name,
     table_name,
     operation_type,
     COUNT(DISTINCT operation_id) as operations
FROM system.storage.predictive_optimization_operations_history
GROUP BY ALL
ORDER BY operations DESC


-- COMMAND ----------

-- 特定のカタログについて、合計何バイトが圧縮されましたか?
SELECT
     schema_name,
     table_name,
     SUM(operation_metrics["amount_of_data_compacted_bytes"]) as bytesCompacted
FROM system.storage.predictive_optimization_operations_history
WHERE
    metastore_name = {{metastore_name}}
    AND catalog_name = {{catalog_name}}
    AND operation_type = "COMPACTION"
GROUP BY ALL
ORDER BY bytesCompacted DESC


-- COMMAND ----------

-- バキューム処理されたバイト数が最も多かったテーブルはどれですか?
SELECT
     metastore_name,
     catalog_name,
     schema_name,
     table_name,
     SUM(operation_metrics["amount_of_data_deleted_bytes"]) as bytesVacuumed
FROM system.storage.predictive_optimization_operations_history
WHERE operation_type = "VACUUM"
GROUP BY ALL
ORDER BY bytesVacuumed DESC


-- COMMAND ----------

-- 予測的最適化によって実行される操作の成功率はどれくらいですか?
WITH operation_counts AS (
     SELECT
           COUNT(DISTINCT (CASE WHEN operation_status = "SUCCESSFUL" THEN operation_id END)) as successes,
           COUNT(DISTINCT operation_id) as total_operations
    FROM system.storage.predictive_optimization_operations_history
 )
SELECT successes / total_operations as success_rate
FROM operation_counts

