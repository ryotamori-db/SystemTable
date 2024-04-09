-- Databricks notebook source
-- MAGIC %md
-- MAGIC # サンプルクエリー集  
-- MAGIC ```system.compute.warehouse_events``` テーブルを用いて、ウェアハウスイベントを分析してみましょう。  
-- MAGIC
-- MAGIC **ウェアハウスイベント**  
-- MAGIC https://docs.databricks.com/ja/administration-guide/system-tables/warehouse-events.html#sample-queries  
-- MAGIC
-- MAGIC - どのウェアハウスがどのくらいの期間アクティブに稼働しているか?
-- MAGIC - 予想よりも長くアップスケールされているウェアハウスを特定する
-- MAGIC - 初めて起動するウェアハウス
-- MAGIC - 請求料金の調査
-- MAGIC - 過去30日間に使用されていないウェアハウスはどれですか?
-- MAGIC - 月間稼働率が最も長いウェアハウス
-- MAGIC - 1ヶ月にアップスケールに最も時間を費やしたウェアハウス
-- MAGIC - 日付別ウェアハウス稼働時間

-- COMMAND ----------

-- どのウェアハウスがどのくらいの期間アクティブに稼働しているか?
-- このクエリは、現在アクティブなウェアハウスとその実行時間 (時間単位) を識別します。
USE CATALOG `system`;

SELECT
we.warehouse_id,
we.event_time,
TIMESTAMPDIFF(MINUTE, we.event_time, CURRENT_TIMESTAMP()) / 60.0 AS running_hours,
we.cluster_count
FROM
compute.warehouse_events we
WHERE
we.event_type = 'RUNNING'
AND NOT EXISTS (
SELECT 1
FROM compute.warehouse_events we2
WHERE we2.warehouse_id = we.warehouse_id
AND we2.event_time > we.event_time
)

-- COMMAND ----------

-- 予想よりも長くアップスケールされているウェアハウスを特定する
-- このクエリは、現在アクティブなウェアハウスとその実行時間 (時間単位) を識別します。
use catalog `system`;

SELECT
   we.warehouse_id,
   we.event_time,
   TIMESTAMPDIFF(MINUTE, we.event_time, CURRENT_TIMESTAMP()) / 60.0 AS upscaled_hours,
   we.cluster_count
FROM
   compute.warehouse_events we
WHERE
   we.event_type = 'SCALED_UP'
   AND we.cluster_count >= 2
   AND NOT EXISTS (
       SELECT 1
       FROM compute.warehouse_events we2
       WHERE we2.warehouse_id = we.warehouse_id
       AND (
           (we2.event_type = 'SCALED_DOWN') OR
           (we2.event_type = 'SCALED_UP' AND we2.cluster_count < 2)
       )
       AND we2.event_time > we.event_time
   )


-- COMMAND ----------

-- 初めて起動するウェアハウス
-- このクエリは、初めて開始される新規ウェアハウスについて通知します。
use catalog `system`;

SELECT
   we.warehouse_id,
   we.event_time,
   we.cluster_count
FROM
   compute.warehouse_events we
WHERE
   (we.event_type = 'STARTING' OR we.event_type = 'RUNNING')
   AND NOT EXISTS (
       SELECT 1
       FROM compute.warehouse_events we2
       WHERE we2.warehouse_id = we.warehouse_id
       AND we2.event_time < we.event_time
   )


-- COMMAND ----------

-- 請求料金の調査
-- ウェアハウスが請求料金を生成するために何をしていたかを具体的に理解したい場合は、このクエリで、ウェアハウスがスケールアップまたはスケールダウン、または開始と停止を行った正確な日時を知ることができます。
use catalog `system`;

SELECT
    we.warehouse_id AS warehouse_id,
    we.event_type AS event,
    we.event_time AS event_time,
    we.cluster_count AS cluster_count
FROM
    compute.warehouse_events AS we
WHERE
    we.event_type IN (
        'STARTING', 'RUNNING', 'STOPPING', 'STOPPED',
        'SCALING_UP', 'SCALED_UP', 'SCALING_DOWN', 'SCALED_DOWN'
    )
    AND MONTH(we.event_time) = 7
    AND YEAR(we.event_time) = YEAR(CURRENT_DATE())
    AND we.warehouse_id = '19c9d68652189278'
ORDER BY
    event_time DESC


-- COMMAND ----------

-- 過去30日間に使用されていないウェアハウスはどれですか?
-- このクエリは、未使用のリソースを特定するのに役立ち、コスト最適化の機会を提供します。
use catalog `system`;

SELECT
    we.warehouse_id,
    we.event_time,
    we.event_type,
    we.cluster_count
FROM
    compute.warehouse_events AS we
WHERE
    we.warehouse_id IN (
        SELECT DISTINCT
            warehouse_id
        FROM
            compute.warehouse_events
        WHERE
            MONTH(event_time) = 6
            AND YEAR(event_time) = YEAR(CURRENT_DATE())
    )
    AND we.warehouse_id NOT IN (
        SELECT DISTINCT
            warehouse_id
        FROM
            compute.warehouse_events
        WHERE
            MONTH(event_time) = 7
            AND YEAR(event_time) = YEAR(CURRENT_DATE())
    )
ORDER BY
    event_time DESC


-- COMMAND ----------

-- 月間稼働率が最も長いウェアハウス
-- このクエリは、特定の月に最も使用されたウェアハウスを示します。 このクエリでは、例として July を使用します。
use catalog `system`;

SELECT
   warehouse_id,
   SUM(TIMESTAMPDIFF(MINUTE, start_time, end_time)) / 60.0 AS uptime_hours
FROM (
   SELECT
      starting.warehouse_id,
      starting.event_time AS start_time,
      (
         SELECT
            MIN(stopping.event_time)
         FROM
            compute.warehouse_events AS stopping
         WHERE
            stopping.warehouse_id = starting.warehouse_id
            AND stopping.event_type = 'STOPPED'
            AND stopping.event_time > starting.event_time
      ) AS end_time
   FROM
      compute.warehouse_events AS starting
   WHERE
      starting.event_type = 'STARTING'
      AND MONTH(starting.event_time) = 7
      AND YEAR(starting.event_time) = YEAR(CURRENT_DATE())
) AS warehouse_uptime
WHERE
   end_time IS NOT NULL
GROUP BY
   warehouse_id
ORDER BY
   uptime_hours DESC


-- COMMAND ----------

-- 1ヶ月にアップスケールに最も時間を費やしたウェアハウス
-- このクエリは、1 か月間にアップスケーリングされた状態でかなりの時間を費やしたウェアハウスについて通知します。 このクエリでは、例として July を使用します。
use catalog `system`;

SELECT
   warehouse_id,
   SUM(TIMESTAMPDIFF(MINUTE, upscaled_time, downscaled_time)) / 60.0 AS upscaled_hours
FROM (
   SELECT
      upscaled.warehouse_id,
      upscaled.event_time AS upscaled_time,
      (
         SELECT
            MIN(downscaled.event_time)
         FROM
            compute.warehouse_events AS downscaled
         WHERE
            downscaled.warehouse_id = upscaled.warehouse_id
            AND (downscaled.event_type = 'SCALED_DOWN' OR downscaled.event_type = 'STOPPED')
            AND downscaled.event_time > upscaled.event_time
      ) AS downscaled_time
   FROM
      compute.warehouse_events AS upscaled
   WHERE
      upscaled.event_type = 'SCALED_UP'
      AND upscaled.cluster_count >= 2
      AND MONTH(upscaled.event_time) = 7
      AND YEAR(upscaled.event_time) = YEAR(CURRENT_DATE())
) AS warehouse_upscaled
WHERE
   downscaled_time IS NOT NULL
GROUP BY
   warehouse_id
ORDER BY
   upscaled_hours DESC limit 0;


-- COMMAND ----------

-- 日付別ウェアハウス稼働時間
-- このクエリは、ウェアハウスが実行されていた合計分数を日付別に整理して通知します。 このクエリは、予測のベースとして使用できます。
use catalog `system`;

SELECT
   warehouse_id,
   SUM(TIMESTAMPDIFF(MINUTE, upscaled_time, downscaled_time)) / 60.0 AS upscaled_hours
FROM (
   SELECT
      upscaled.warehouse_id,
      upscaled.event_time AS upscaled_time,
      (
         SELECT
            MIN(downscaled.event_time)
         FROM
            compute.warehouse_events AS downscaled
         WHERE
            downscaled.warehouse_id = upscaled.warehouse_id
            AND (downscaled.event_type = 'SCALED_DOWN' OR downscaled.event_type = 'STOPPED')
            AND downscaled.event_time > upscaled.event_time
      ) AS downscaled_time
   FROM
      compute.warehouse_events AS upscaled
   WHERE
      upscaled.event_type = 'SCALED_UP'
      AND upscaled.cluster_count >= 2
      AND MONTH(upscaled.event_time) = 7
      AND YEAR(upscaled.event_time) = YEAR(CURRENT_DATE())
) AS warehouse_upscaled
WHERE
   downscaled_time IS NOT NULL
GROUP BY
   warehouse_id
ORDER BY
   upscaled_hours DESC limit 0;

