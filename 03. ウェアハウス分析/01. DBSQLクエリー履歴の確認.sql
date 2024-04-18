-- Databricks notebook source
-- MAGIC %md
-- MAGIC # システムテーブルによるクエリ履歴分析
-- MAGIC
-- MAGIC ## Business Questions: 
-- MAGIC

-- COMMAND ----------

-- クラスタのコア数がないと正確なスキュー比は得られないが、あるタイプのグループ内の全クエリの平均比と比較することはできる。
-- 期間内のすべてのクエリについて合計した全タスク時間 (これは IDLE 時間を含まない)、クエリごとの全タスク時間の割合を取得する。
-- ウェアハウスで分けるには？ステートメントタイプ？
-- クエリのハッシュ化、クエリtakのプルなど、タスク時間を利用するために、より意味のある次元でグループ化する必要がある。

CREATE OR REPLACE TEMPORARY VIEW query_logs AS (
SELECT
statement_id,
executed_by,
statement_text,
warehouse_id,
execution_status,
f.statement_type,
total_duration_ms AS QueryRuntime,
total_task_duration_ms AS CPUTotalExecutionTime,
execution_duration_ms AS ExecutionQueryTime,
compilation_duration_ms AS CompilationQueryTime,
waiting_at_capacity_duration_ms AS QueueQueryTime,
waiting_for_compute_duration_ms AS StartUpQueryTime,
result_fetch_duration_ms AS ResultFetchTime,
start_time,
end_time,
update_time,
COALESCE(total_task_duration_ms / execution_duration_ms, NULL) AS TotalCPUTime_To_Execution_Time_Ratio,
COALESCE(waiting_at_capacity_duration_ms / total_duration_ms, 0) AS ProportionQueueTime,
AVG(COALESCE(total_task_duration_ms / execution_duration_ms, NULL)) OVER (PARTITION BY f.statement_type) AS AvgTaskSkewRatio,
AVG(total_duration_ms)  OVER (PARTITION BY f.statement_type) AS AvgQueryRuntime,
AVG(waiting_at_capacity_duration_ms) OVER (PARTITION BY f.statement_type) AS AvgQueueTime,
AVG(COALESCE(waiting_at_capacity_duration_ms / total_duration_ms, 0)) OVER (PARTITION BY f.statement_type) AS AvgProportionTimeQueueing,
SUM(total_task_duration_ms) OVER () AS TotalUsedTaskTimeInPeriod,
total_task_duration_ms / TotalUsedTaskTimeInPeriod AS ProportionOfTaskTimeUsedByQuery 
-- これをチャージバックに使用することができる（分母は使用されたタスクの時間のみで、アイデアの時間は含まれないことが分かっている限り）。
FROM system.query.history f
WHERE warehouse_id = '${warehouse_id}'
AND start_time >= dateadd(DAY, -${lookback_days}::int, now())
)

-- COMMAND ----------

-- DBTITLE 1,Create Automated Parser for Query Tagging
CREATE OR REPLACE FUNCTION parse_query_tag(input_sql STRING, pattern STRING DEFAULT '--QUERY_TAG:(.*?)(\r\n|\n|$)', no_tag_default STRING DEFAULT 'UNTAGGED')
RETURNS STRING
DETERMINISTIC
READS SQL DATA
COMMENT 'Extract tag from a query based on regex template'
RETURN    
  WITH tagged_query AS (
      SELECT regexp_replace(regexp_extract(input_sql, pattern, 0), '--QUERY_TAG:', '') AS raw_tagged
      )
    SELECT CASE WHEN length(raw_tagged) >= 1 THEN raw_tagged ELSE no_tag_default END AS clean_tag FROM tagged_query


    regexp_replace(regexp_extract(input_sql, '--QUERY_TAG:(.*?)(\r\n|\n|$)', 0), '--QUERY_TAG:', '') AS raw_tagged
;

-- COMMAND ----------

-- DBTITLE 1,Query Runtime Allocation - Greedy Query Trends
-- 手動でクラスタサイズと価格設定を定義すれば、クエリ・チャージバックの計算を開始できる。

-- パレート分析 - タスク時間のY%に寄与する上位X個のクエリを見つける。
WITH statement_aggs AS (
SELECT
statement_text,
SUM(CPUTotalExecutionTime/1000) AS TotalTaskTimeForStatement,
MAX(TotalUsedTaskTimeInPeriod/1000) AS TotalTaskTimeInPeriod,
SUM(CPUTotalExecutionTime/1000)/ MAX(TotalUsedTaskTimeInPeriod/1000) AS StatementTaskTimeProportion
FROM query_logs
WHERE CPUTotalExecutionTime > 0
GROUP BY statement_text
ORDER BY TotalTaskTimeForStatement DESC

), 

cumulative_sum AS (
SELECT 
*,
SUM(StatementTaskTimeProportion) OVER (ORDER BY StatementTaskTimeProportion DESC) AS CumulativeProportionOfTime
FROM statement_aggs
)

SELECT 
*,
parse_query_tag(statement_text) AS query_tag,
CASE WHEN CumulativeProportionOfTime::float <= '${pareto_point}'::float THEN statement_text ELSE 'Remaining Queries' END AS QueryPareto
FROM cumulative_sum

-- ウェアハウスで使われていない時間の割合は？
-- 各クエリに使用された割合は？
