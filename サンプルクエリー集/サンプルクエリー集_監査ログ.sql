-- Databricks notebook source
-- MAGIC %md
-- MAGIC # サンプルクエリー集  
-- MAGIC ```system.access.audit``` テーブルを用いて、監査ログを分析してみましょう。  
-- MAGIC これらのクエリーを機能させるには、中括弧{{}}内の値を編集する必要があります。  
-- MAGIC
-- MAGIC **監査ログ**  
-- MAGIC https://docs.databricks.com/ja/administration-guide/system-tables/audit-logs.html#sample-queries  
-- MAGIC
-- MAGIC - 誰がこのテーブルにアクセスしましたか?  
-- MAGIC - 直近1日以内にどのユーザーがテーブルにアクセスしましたか？  
-- MAGIC - ユーザーはどのテーブルにアクセスしましたか?  
-- MAGIC - すべての権限の変更を表示する  
-- MAGIC - 最近実行したノートブックコマンドを表示する    

-- COMMAND ----------

-- 誰がこのテーブルにアクセスしましたか?
-- このクエリーでは、 information_schemaを使用します。

SELECT DISTINCT(grantee) AS `ACCESSIBLE BY`
FROM system.information_schema.table_privileges
WHERE table_schema = '{{schema_name}}' AND table_name = '{{table_name}}'
  UNION
    SELECT table_owner
    FROM system.information_schema.tables
    WHERE table_schema = '{{schema_name}}' AND table_name = '{{table}}'
  UNION
    SELECT DISTINCT(grantee)
    FROM system.information_schema.schema_privileges
    WHERE schema_name = '{{schema_name}}'



-- COMMAND ----------

-- 直近1日以内にどのユーザーがテーブルにアクセスしたか？

SELECT
  user_identity.email as `User`,
  IFNULL(request_params.full_name_arg,
    request_params.name)
    AS `Table`,
    action_name AS `Type of Access`,
    event_time AS `Time of Access`
FROM system.access.audit
WHERE (request_params.full_name_arg = '{{catalog.schema.table}}'
  OR (request_params.name = '{{table_name}}'
  AND request_params.schema_name = '{{schema_name}}'))
  AND action_name
    IN ('createTable','getTable','deleteTable')
  AND event_date > now() - interval '1 day'
ORDER BY event_date DESC


-- COMMAND ----------

-- ユーザーはどのテーブルにアクセスしましたか?
-- 日付範囲でフィルター処理するには、クエリーの下部にある日付句のコメントアウトを解除します。

SELECT
        action_name as `EVENT`,
        event_time as `WHEN`,
        IFNULL(request_params.full_name_arg, 'Non-specific') AS `TABLE ACCESSED`,
        IFNULL(request_params.commandText,'GET table') AS `QUERY TEXT`
FROM system.access.audit
WHERE user_identity.email = '{{User}}'
        AND action_name IN ('createTable',
'commandSubmit','getTable','deleteTable')
        -- AND datediff(now(), event_date) < 1
        -- ORDER BY event_date DESC 

-- COMMAND ----------

-- セキュリティ保護可能なすべてのオブジェクトのアクセス許可の変更を表示する
-- このクエリは、アカウントで発生したすべての権限変更のイベントを返します。 クエリは、変更を行ったユーザー、セキュリティ保護可能なオブジェクトの種類と名前、および行われた特定の変更を返します。

SELECT event_time, user_identity.email, request_params.securable_type, request_params.securable_full_name, request_params.changes
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name = 'updatePermissions'
ORDER BY 1 DESC

-- COMMAND ----------

-- 最近実行したノートブックコマンドを表示する
-- このクエリーは、最後に実行されたノートブックコマンドと、コマンドを実行したユーザーを返します。
-- 注
-- runCommandアクションは、詳細な監査ログが有効になっている場合にのみ出力されます。詳細監査ログを有効にするには、「 詳細監査ログを有効にする」を参照してください。

SELECT event_time, user_identity.email, request_params.commandText
FROM system.access.audit
WHERE action_name = `runCommand`
ORDER BY event_time DESC
LIMIT 100
