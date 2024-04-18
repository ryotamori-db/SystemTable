-- Databricks notebook source
-- MAGIC %md
-- MAGIC # サンプルクエリー集  
-- MAGIC ```system.billing.usage``` テーブルを用いて、コストを分析してみましょう。  
-- MAGIC これらのクエリーを機能させるには、中括弧{{}}内の値を編集する必要があります。  
-- MAGIC
-- MAGIC **コスト**  
-- MAGIC https://docs.databricks.com/ja/administration-guide/system-tables/billing.html#sample-queries  
-- MAGIC
-- MAGIC - DBU消費の日々の傾向は何ですか?
-- MAGIC - 今月中に使用された各SKUのDBUの数はいくつですか?
-- MAGIC - 6月1日にワークスペースで使用された各SKUの量
-- MAGIC - どのジョブが最も多くのDBUを消費しましたか?
-- MAGIC - 特定のタグを持つリソースに起因する使用量はどれくらいですか?
-- MAGIC - 使用量が増加しているSKUを表示する
-- MAGIC - All Purposeコンピュート(フォトン)の利用動向は?
-- MAGIC - マテリアライズドビューまたはストリーミングテーブルのDBU消費量はどれくらいですか？
-- MAGIC - **All PurposeコンピュートからJobコンピュートへの移行状況を監視する**
-- MAGIC - **モデル推論の使用量**

-- COMMAND ----------

-- DBU消費の日々の傾向は何ですか?
SELECT usage_date as `Date`, sum(usage_quantity) as `DBUs Consumed`
  FROM system.billing.usage
WHERE sku_name = "STANDARD_ALL_PURPOSE_COMPUTE"
GROUP BY usage_date
ORDER BY usage_date ASC

-- COMMAND ----------

-- 今月中に使用された各SKUのDBUの数はいくつですか?
SELECT sku_name, usage_date, sum(usage_quantity) as `DBUs`
    FROM system.billing.usage
WHERE
    month(usage_date) = month(NOW())
    AND year(usage_date) = year(NOW())
GROUP BY sku_name, usage_date

-- COMMAND ----------

-- 6月1日にワークスペースで使用された各SKUの量
SELECT sku_name, sum(usage_quantity) as `DBUs consumed`
FROM system.billing.usage
WHERE workspace_id = 1234567890123456
AND usage_date = "2023-06-01"
GROUP BY sku_name

-- COMMAND ----------

-- どのジョブが最も多くのDBUを消費しましたか?
SELECT usage_metadata.job_id as `Job ID`, sum(usage_quantity) as `DBUs`
FROM system.billing.usage
WHERE usage_metadata.job_id IS NOT NULL
GROUP BY `Job ID`
ORDER BY `DBUs` DESC

-- COMMAND ----------

-- 特定のタグを持つリソースに起因する使用量はどれくらいですか?
-- この例では、カスタム タグ別にコストを分類する方法を示します。 クエリー内のカスタムタグのキーと値を必ず置き換えてください。
SELECT sku_name, usage_unit, SUM(usage_quantity) as `DBUs consumed`
FROM system.billing.usage
WHERE custom_tags.{{key}} = "{{value}}"
GROUP BY 1, 2

-- COMMAND ----------

-- 使用量が増加しているSKUを表示する
SELECT after.sku_name, before_dbus, after_dbus, ((after_dbus - before_dbus)/before_dbus * 100) AS growth_rate
FROM
(SELECT sku_name, sum(usage_quantity) as before_dbus
    FROM system.billing.usage
WHERE usage_date BETWEEN "2023-04-01" and "2023-04-30"
GROUP BY sku_name) as before
JOIN
(SELECT sku_name, sum(usage_quantity) as after_dbus
    FROM system.billing.usage
WHERE usage_date BETWEEN "2023-05-01" and "2023-05-30"
GROUP BY sku_name) as after
where before.sku_name = after.sku_name
SORT by growth_rate DESC

-- COMMAND ----------

-- All Purposeコンピュート(フォトン)の利用動向は?
SELECT sku_name, usage_date, sum(usage_quantity) as `DBUs consumed`
    FROM system.billing.usage
WHERE year(usage_date) = year(CURRENT_DATE)
AND sku_name = "ENTERPRISE_ALL_PURPOSE_COMPUTE_(PHOTON)"
AND usage_date > "2023-04-15"
GROUP BY sku_name, usage_date

-- COMMAND ----------

-- マテリアライズドビューまたはストリーミングテーブルのDBU消費量はどれくらいですか？
SELECT
  sku_name,
  usage_date,
  SUM(usage_quantity) AS `DBUs`
FROM
  system.billing.usage
WHERE
  usage_metadata.dlt_pipeline_id = "113739b7-3f45-4a88-b6d9-e97051e773b9"
  AND usage_start_time > "2023-05-30"
GROUP BY
  ALL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # All PurposeコンピュートからJobコンピュートへの移行状況を監視する
-- MAGIC 本来All Puroposeコンピュートは開発プロセス等でアドホックに分析する際に使用するクラスターです。  
-- MAGIC 本番プロセスでは、Jobコンピュートを使用する事がベストプラクティスと考えられています。  
-- MAGIC
-- MAGIC そこで、All Purposeコンピュートでどれだけのジョブが作成されるかを監視し、変更が発生したらユーザーに警告を出すような仕組みを作りましょう。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC with created_jobs as (
-- MAGIC   select
-- MAGIC     workspace_id,
-- MAGIC     event_time as created_time,
-- MAGIC     user_identity.email as creator,
-- MAGIC     request_id,
-- MAGIC     event_id,
-- MAGIC     get_json_object(response.result, '$.job_id') as job_id,
-- MAGIC     request_params.name as job_name,
-- MAGIC     request_params.job_type,
-- MAGIC     request_params.schedule
-- MAGIC   from
-- MAGIC     system.access.audit
-- MAGIC   where
-- MAGIC     action_name = 'create'
-- MAGIC     and service_name = 'jobs'
-- MAGIC     and response.status_code = 200
-- MAGIC ),
-- MAGIC deleted_jobs as (
-- MAGIC   select
-- MAGIC     request_params.job_id,
-- MAGIC     workspace_id
-- MAGIC   from
-- MAGIC     system.access.audit
-- MAGIC   where
-- MAGIC     action_name = 'delete'
-- MAGIC     and service_name = 'jobs'
-- MAGIC     and response.status_code = 200
-- MAGIC )
-- MAGIC select
-- MAGIC   a.workspace_id,
-- MAGIC   a.sku_name,
-- MAGIC   a.cloud,
-- MAGIC   a.usage_date,
-- MAGIC   date_format(usage_date, 'yyyy-MM') as YearMonth,
-- MAGIC   a.usage_unit,
-- MAGIC   d.pricing.default as list_price,
-- MAGIC   sum(a.usage_quantity) total_dbus,
-- MAGIC   sum(a.usage_quantity) * d.pricing.default as list_cost,
-- MAGIC   a.usage_metadata.*,
-- MAGIC   case
-- MAGIC     when b.job_id is not null then TRUE
-- MAGIC     else FALSE
-- MAGIC   end as job_created_flag,
-- MAGIC   case
-- MAGIC     when c.job_id is not null then TRUE
-- MAGIC     else FALSE
-- MAGIC   end as job_deleted_flag
-- MAGIC from
-- MAGIC   system.billing.usage a
-- MAGIC   left join created_jobs b on a.workspace_id = b.workspace_id
-- MAGIC   and a.usage_metadata.job_id = b.job_id
-- MAGIC   left join deleted_jobs c on a.workspace_id = c.workspace_id
-- MAGIC   and a.usage_metadata.job_id = c.job_id
-- MAGIC   left join system.billing.list_prices d on a.cloud = d.cloud and
-- MAGIC     a.sku_name = d.sku_name and
-- MAGIC     a.usage_start_time >= d.price_start_time and
-- MAGIC     (a.usage_end_time <= d.price_end_time or d.price_end_time is null)
-- MAGIC where
-- MAGIC   usage_metadata.job_id is not Null
-- MAGIC   and contains(a.sku_name, 'ALL_PURPOSE')
-- MAGIC group by
-- MAGIC   all

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # モデル推論の使用量
-- MAGIC Databricksは、可用性が高くコスト効率の高いREST APIのためのサーバーレスモデルエンドポイントをホストし、デプロイする能力を持っています。  
-- MAGIC エンドポイントは、ゼロまでスケールダウンすることができ、エンドユーザーにレスポンスを提供するために素早く立ち上がり、エクスペリエンスとコストを最適化します。  
-- MAGIC 私たちがデプロイしたモデルの数と、それらのモデルの使用率から目を離さないようにしましよう。

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
  u.custom_tags.Team, -- parse out custom tags if available
  u.usage_metadata.*
from
  system.billing.usage u 
  inner join system.billing.list_prices lp on u.cloud = lp.cloud and
    u.sku_name = lp.sku_name and
    u.usage_start_time >= lp.price_start_time 
where
  contains(u.sku_name, 'INFERENCE')
