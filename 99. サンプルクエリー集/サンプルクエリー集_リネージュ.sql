-- Databricks notebook source
-- MAGIC %md
-- MAGIC # サンプルクエリー集  
-- MAGIC ```system.access.table_lineage``` テーブル、```system.access.column_lineage``` テーブルを用いて、リネージュを分析してみましょう。  
-- MAGIC
-- MAGIC **リネージュ**  
-- MAGIC https://docs.databricks.com/ja/administration-guide/system-tables/lineage.html#lineage-system-table-example 
-- MAGIC
-- MAGIC Databricksは、Unity Catalogの全アイテムのリネージュを追跡する事が可能です。  
-- MAGIC これには、すべての項目の**データがどこから来ているのか**と**誰がそれを使っているのか**の情報が含まれています：  
-- MAGIC - テーブル
-- MAGIC - クエリ
-- MAGIC - ダッシュボード
-- MAGIC - Job
-- MAGIC - ML/AI
-- MAGIC - ...
-- MAGIC   
-- MAGIC 詳細は、以下をご確認ください:  
-- MAGIC <a href="https://app.getreprise.com/launch/MnqjQDX" target="_blank">discover Unity Catalog from the UI</a>  

-- COMMAND ----------

SHOW TABLES IN system.access

-- COMMAND ----------

SELECT * FROM system.access.table_lineage

-- COMMAND ----------

-- テーブルにアクセスするすべてのエンティティ（ワークフロー、ノートブック、DLT、DBSQLなど）をレビューする
SELECT DISTINCT(entity_type) FROM system.access.table_lineage

-- COMMAND ----------

-- カラムレベルのリネージュ情報
SELECT * FROM system.access.column_lineage

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # リネージを見つけるためにクラウドストレージパスを手動で取得したくない場合は、次の関数を使用して、テーブル名を使用してリネージデータを取得できます。 
-- MAGIC # カラムレベルのリネージが必要な場合は、関数内の system.access.table_lineage を system.access.column_lineage に置き換えることもできます。
-- MAGIC def getLineageForTable(table_name):
-- MAGIC   table_path = spark.sql(f"describe detail {table_name}").select("location").head()[0]
-- MAGIC
-- MAGIC   df = spark.read.table("system.access.table_lineage")
-- MAGIC   return df.where(
-- MAGIC     (df.source_table_full_name == table_name)
-- MAGIC     | (df.target_table_full_name == table_name)
-- MAGIC     | (df.source_path == table_path)
-- MAGIC     | (df.target_path == table_path)
-- MAGIC   )
-- MAGIC
-- MAGIC # 次のコマンドを使用して関数を呼び出し、外部テーブルのリネージ レコードを表示します。
-- MAGIC display(getLineageForTable("table_name"))
