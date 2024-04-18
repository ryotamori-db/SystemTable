# Databricks notebook source
# MAGIC %md
# MAGIC # システムテーブルを有効化する
# MAGIC システムテーブルはUnity Catalogによって管理されるため、システムテーブルを有効にするには、アカウント内に少なくとも1つのUnity Catalogが管理するワークスペースが必要です。  
# MAGIC システムテーブルは、**アカウント管理者**が有効にする必要があります。  
# MAGIC アカウントでシステムテーブルを有効にするには、Databricks CLIを使用するか、ノートブックでUnity Catalog APIを呼び出します。  
# MAGIC <br>
# MAGIC 詳細は各クラウドのドキュメントを参照してください。  
# MAGIC ([AWS](https://docs.databricks.com/administration-guide/system-tables/index.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/system-tables/))
# MAGIC
# MAGIC **このノートブックを実行すると、システムテーブルが有効化され、システムカタログが表示されます。**

# COMMAND ----------

# MAGIC %md
# MAGIC ### メタストアIDの入力として使用するウィジェットの作成
# MAGIC メタストアIDはDatabricksワークスペースの`Data`タブにあるメタストア詳細アイコンをクリックすることで確認できます。

# COMMAND ----------

metastore_id = spark.sql("SELECT current_metastore() as metastore_id").collect()[0]["metastore_id"]
metastore_id = metastore_id[metastore_id.rfind(':')+1:]
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("metastore_id", metastore_id, "Metastore ID")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 認証の設定
# MAGIC すでにDatabricksノートブックを使用しているため、トークンや環境変数を扱う必要がなく、dbutilsを使用して必要なものをインポートすることができます。

# COMMAND ----------

import requests
from time import sleep
metastore_id = dbutils.widgets.get("metastore_id")
host = "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
headers = {"Authorization": "Bearer "+dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}

# COMMAND ----------

# MAGIC %md
# MAGIC 有効化する必要があるスキーマのリストを確認するために、どのスキーマが利用可能かをチェックします。

# COMMAND ----------

import json
r = requests.get(f"{host}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas", headers=headers).json()
print(json.dumps(r, indent=1))

# COMMAND ----------

schemas_to_enable = []
already_enabled = []
others = []
for schema in r['schemas']:
    if schema['state'].lower() == "available":
        schemas_to_enable.append(schema["schema"])
    elif schema['state'].lower() == "enablecompleted":
        already_enabled.append(schema["schema"])
    else:
        others.append(schema["schema"])
print(f"Schemas that will be enabled: {schemas_to_enable}")
print(f"Schemas that are already enabled: {already_enabled}")
print(f"Unavailable schemas: {others}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### スキーマの有効化

# COMMAND ----------

for schema in schemas_to_enable:
    host = "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    headers = {"Authorization": "Bearer "+dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}
    r = requests.put(f"{host}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas/{schema}", headers=headers)
    if r.status_code == 200:
        print(f"Schema {schema} enabled successfully")
    else:
        print(f"""Error enabling the schema `{schema}`: {r.json()["error_code"]} | Description: {r.json()["message"]}""")
    sleep(1)
