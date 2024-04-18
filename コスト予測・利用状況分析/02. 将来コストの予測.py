# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # システムテーブルを用いた将来コストの予測
# MAGIC このノートブックでは、現在の使用量を分析し、SKUとワークスペースに基づいて将来のコストを予測する**Prophet**モデルを構築します。  
# MAGIC 各ワークスペースは異なるパターン／傾向を持つ可能性があるため、各SKUとワークスペースに複数のモデルを特化させ、並行してトレーニングする仕組みになっています。
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/system_tables/uc-system-tables-flow.png?raw=true">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Note: 予測精度の向上のため、予測データは毎日更新してください
# MAGIC 更新しないと予測が直ちに期限切れとなり、潜在的な消費量の変化や警告のトリガーが反映されなくなる恐れがあります。  
# MAGIC ダッシュボードにはトラッカーを追加し、前回の予測からの経過日数を表示するようにしています。  
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/system_tables/uc-system-job.png?raw=true" style="float: left; margin: 20px" width="550px">
# MAGIC   
# MAGIC     
# MAGIC   
# MAGIC 更新するためには「スケジュール」をクリックした後、以下設定することで実現できます。
# MAGIC - 「スケジュール」の「間隔」を「毎日」  
# MAGIC - 「クラスター」を定義  
# MAGIC - 「アラート」は、通知するべきユーザーのメールアドレスを入力  

# COMMAND ----------

catalog = 'main'
schema = 'billing_forecast'

dbutils.widgets.text('catalog', catalog)
dbutils.widgets.text('schema', schema)

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=reset_all_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前処理
# MAGIC SKUは製品ラインごと、リージョンごとに細かく分類されています。  
# MAGIC ここではコスト管理分析ダッシュボードを簡素化するために、 `STANDARD_ALL_PURPOSE_COMPUTE` と `PREMIUM_ALL_PURPOSE_COMPUTE`をグループ化し、`ALL_PURPOSE` として統合します。

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from system.billing.usage u
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)

# COMMAND ----------

# DBTITLE 1,Group by main SKU category
from pyspark.sql.functions import col, when, sum, current_date, lit
data_to_predict = spark.sql("""
  select u.workspace_id, 
  u.usage_date as ds, 
  u.sku_name as sku, 
  cast(u.usage_quantity as double) as dbus, 
  cast(lp.pricing.default*usage_quantity as double) as cost_at_list_price 
  
  from system.billing.usage u 
      inner join system.billing.list_prices lp on u.cloud = lp.cloud and
        u.sku_name = lp.sku_name and
        u.usage_start_time >= lp.price_start_time and
        (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
  where u.usage_unit = 'DBU'
""")

# SKUをメインファミリーにグループ分けし、予測
data_to_predict = data_to_predict.withColumn("sku",
                     when(col("sku").contains("ALL_PURPOSE"), "ALL_PURPOSE")
                    .when(col("sku").contains("JOBS"), "JOBS")
                    .when(col("sku").contains("DLT"), "DLT")
                    .when(col("sku").contains("SQL"), "SQL")
                    .when(col("sku").contains("INFERENCE"), "MODEL_INFERENCE")
                    .otherwise("OTHER"))
                  
# 消費量を1日単位で合計する（1日あたり1値、1sku＋ワークスペースあたり）
data_to_predict_daily = data_to_predict.groupBy(col("ds"), col('sku'), col('workspace_id')).agg(sum('dbus').alias("dbus"), sum('cost_at_list_price').alias("cost_at_list_price"))

display(data_to_predict_daily)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Prophetを使った請求データの予測
# MAGIC 一旦日次レベルで集計されると、請求情報は時系列として予測することができます。  
# MAGIC このデモでは、単純なProphetモデルを使い、次の四半期まで時系列を拡張します。

# COMMAND ----------

from prophet import Prophet

#今後3カ月間の日数予測
forecast_frequency='d'
forecast_periods=31*3

interval_width=0.8
include_history=True

def generate_forecast(history_pd, display_graph = True):
    # 欠損値を除去
    history_pd = history_pd.dropna()
    if history_pd.shape[0] > 10:
        # モデルの訓練と設定
        model = Prophet(interval_width=interval_width )
        model.add_country_holidays(country_name='US')

        model.fit(history_pd)

        # 予測の作成
        future_pd = model.make_future_dataframe(periods=forecast_periods, freq=forecast_frequency, include_history=include_history)
        forecast_pd = model.predict(future_pd)

        if display_graph:
           model.plot(forecast_pd)
        # 履歴データセットにyの値を戻す
        f_pd = forecast_pd[['ds', 'yhat', 'yhat_upper', 'yhat_lower']].set_index('ds')
        # 履歴と予測を結合
        results_pd = f_pd.join(history_pd[['ds','y','dbus']].set_index('ds'), how='left')
        results_pd.reset_index(level=0, inplace=True)
        results_pd['ds'] = results_pd['ds'].dt.date
        # sku & workspace idをincomingデータセットから取得
        results_pd['sku'] = history_pd['sku'].iloc[0]
        results_pd['workspace_id'] = history_pd['workspace_id'].iloc[0]
    else:
        # 予測に十分なデータがない場合
        for c in ['yhat', 'yhat_upper', 'yhat_lower']:
            history_pd[c] = history_pd['y']
        results_pd = history_pd[['ds','y','dbus','yhat', 'yhat_upper', 'yhat_lower', 'sku', 'workspace_id']]
    return results_pd

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 課金予測テーブルを作成:
# MAGIC CREATE TABLE IF NOT EXISTS billing_forecast (ds DATE, yhat DOUBLE, yhat_upper DOUBLE, yhat_lower DOUBLE, y DOUBLE, dbus DOUBLE, sku STRING, workspace_id STRING, training_date DATE);

# COMMAND ----------

# DBTITLE 1,Forecast for the sum of all DBUS
# すべてのSKUを合計し、ワークスペースでグローバルな消費傾向を見る
# （デフォルトでは、すべてのワークスペースにまたがるすべての請求使用量を表示したいので、特定のモデルをトレーニングする必要がある）
global_forecast = data_to_predict_daily.groupBy(col("ds")).agg(sum('cost_at_list_price').alias("y"), sum('dbus').alias("dbus")) \
                                       .withColumn('sku', lit('ALL')) \
                                       .withColumn('workspace_id', lit('ALL')).toPandas()
global_forecast = generate_forecast(global_forecast)
spark.createDataFrame(global_forecast).withColumn('training_date', current_date()) \
                                      .write.mode('overwrite').option("mergeSchema", "true").saveAsTable("billing_forecast")

# COMMAND ----------

# DBTITLE 1,Distribute training for each DBU SKUs to have specialized models & predictions
def generate_forecast_udf(history_pd):
  return generate_forecast(history_pd, False)

# ワークスペースごとの全DBUを合計したエントリーを追加
all_per_workspace = data_to_predict_daily.groupBy('ds', 'workspace_id').agg(sum('cost_at_list_price').alias("cost_at_list_price"), sum('dbus').alias("dbus")) \
                                         .withColumn('sku', lit('ALL'))
all_skus = data_to_predict_daily.groupBy('ds', 'sku').agg(sum('cost_at_list_price').alias("cost_at_list_price"), sum('dbus').alias("dbus")) \
                                         .withColumn('workspace_id', lit('ALL'))

results = (
  data_to_predict_daily
    .union(all_per_workspace.select(data_to_predict_daily.columns)) # 全sku,全ワークスペースを追加
    .union(all_skus.select(data_to_predict_daily.columns)) # 全てのワークスペースに全skuを追加
    .withColumnRenamed('cost_at_list_price', 'y') # Prophetの予測は'y'を使用
    .groupBy('workspace_id', 'sku') # SKU + Workspaceでグループ化、グループ毎にモデル呼び出し
    .applyInPandas(generate_forecast_udf, schema="ds date, yhat double, yhat_upper double, yhat_lower double, y double, dbus double, sku string, workspace_id string")
    .withColumn('training_date', current_date()))

# ダッシュボードで使用する将来コストの予測テーブルを保存
results.write.mode('append').saveAsTable("billing_forecast")

# COMMAND ----------

# DBTITLE 1,Our forecasting data is ready
# MAGIC %sql 
# MAGIC select * from billing_forecast

# COMMAND ----------

# DBTITLE 1,Create a view on top to simplify access
# MAGIC %sql
# MAGIC create or replace view detailed_billing_forecast as with forecasts as (
# MAGIC   select
# MAGIC     f.ds as date,
# MAGIC     f.workspace_id,
# MAGIC     f.sku,
# MAGIC     f.dbus,
# MAGIC     f.y,
# MAGIC     GREATEST(0, f.yhat) as yhat,
# MAGIC     GREATEST(0, f.yhat_lower) as yhat_lower,
# MAGIC     GREATEST(0, f.yhat_upper) as yhat_upper,
# MAGIC     f.y > f.yhat_upper as upper_anomaly_alert,
# MAGIC     f.y < f.yhat_lower as lower_anomaly_alert,
# MAGIC     f.y >= f.yhat_lower AND f.y <= f.yhat_upper as on_trend,
# MAGIC     f.training_date
# MAGIC   from billing_forecast f
# MAGIC )
# MAGIC select
# MAGIC   `date`,
# MAGIC   workspace_id,
# MAGIC   dbus,
# MAGIC   sku,
# MAGIC   y as past_list_cost, -- 実績
# MAGIC   y is null as is_prediction ,
# MAGIC   coalesce(y, yhat) as list_cost, -- 実績&予測
# MAGIC   yhat as forecast_list_cost,  -- 予測
# MAGIC   yhat_lower as forecast_list_cost_lower,
# MAGIC   yhat_upper as forecast_list_cost_upper,
# MAGIC   -- スムーズな上下バンドで見やすくする
# MAGIC   avg(yhat) OVER (PARTITION BY sku, workspace_id ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS forecast_list_cost_ma, 
# MAGIC   avg(yhat_lower) OVER (PARTITION BY sku, workspace_id ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS forecast_list_cost_upper_ma,
# MAGIC   avg(yhat_upper) OVER (PARTITION BY sku, workspace_id ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS forecast_list_cost_lower_ma,
# MAGIC   upper_anomaly_alert,
# MAGIC   lower_anomaly_alert,
# MAGIC   on_trend,
# MAGIC   training_date
# MAGIC from
# MAGIC   forecasts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from detailed_billing_forecast  where sku = 'ALL' limit 100

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 将来コストの予測テーブルがダッシュボード用に準備できました
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/system_tables/dashboard-governance-billing.png?raw=true" width="500px" style="float: right">
# MAGIC
# MAGIC 将来コストの予測データを調査するために、ダッシュボードを開いてみましょう  
# MAGIC カタログやスキーマを変更した場合は、クエリも更新する必要があることを忘れないでください。  
# MAGIC
# MAGIC ## ノートブックから予測データを探る
# MAGIC Databricksの組み込みウィジェットやpythonのプロットライブラリを使って、ノートブック内で直接探索することができます：  

# COMMAND ----------

# DBTITLE 1,Forecast consumption for all SKU in all our workspaces
# 全ワークスペースにおける全SKUの消費予測
import plotly.graph_objects as go

df = spark.table('detailed_billing_forecast').where("sku = 'ALL' and workspace_id='ALL'").toPandas()
# 最後のデータポイントは、その日が終了していないため削除
df.at[df['past_list_cost'].last_valid_index(), 'past_list_cost'] = None

# トレースの作成
fig = go.Figure()
fig.add_trace(go.Scatter(x=df['date'], y=df['past_list_cost'][:-4], name='actual usage'))
fig.add_trace(go.Scatter(x=df['date'], y=df['forecast_list_cost'], name='forecast cost (pricing list)'))
fig.add_trace(go.Scatter(x=df['date'], y=df['forecast_list_cost_upper'], name='forecast cost up', line = dict(color='grey', width=1, dash='dot')))
fig.add_trace(go.Scatter(x=df['date'], y=df['forecast_list_cost_lower'], name='forecast cost low', line = dict(color='grey', width=1, dash='dot')))

# COMMAND ----------

# DBTITLE 1,Detailed view for each SKU
# 各SKUの詳細表示
import plotly.express as px
df = spark.table('detailed_billing_forecast').where("workspace_id='ALL'").groupBy('sku', 'date').agg(sum('list_cost').alias('list_cost')).orderBy('date').toPandas()
px.line(df, x="date", y="list_cost", color='sku')

# COMMAND ----------

# DBTITLE 1,Top 5 workspaces view
# ワークスペース トップ5
# ALL PURPOSE dbuに注目
df = spark.table('detailed_billing_forecast').where("sku = 'ALL_PURPOSE' and workspace_id != 'ALL'")
# 最も消費量の多いトップ5のワークスペースを取得
top_workspace = df.groupBy('workspace_id').agg(sum('list_cost').alias('list_cost')).orderBy(col('list_cost').desc()).limit(5)
workspace_id = [r['workspace_id'] for r in top_workspace.collect()]
# ワークスペース毎のグループ消費
df = df.where(col('workspace_id').isin(workspace_id)).groupBy('workspace_id', 'date').agg(sum('list_cost').alias('list_cost')).orderBy('date').toPandas()
px.bar(df, x="date", y="list_cost", color="workspace_id", title="Long-Form Input")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 異常アラートの作成
# MAGIC
# MAGIC Databricks SQL Alertsを使用することで、実際の使用量が予測範囲から外れた場合に自動的に検知するシステムを構築することができます。  
# MAGIC 今回のシナリオでは、使用量が予測の下限値や上限値を上回ったり下回ったりした場合にアラートが表示されます。  
# MAGIC 我々の予測アルゴリズムでは、以下のカラムを生成します。  
# MAGIC - `upper_anomaly_alert` : 実際の金額が上限値より大きい場合に`true`を返す。
# MAGIC - `lower_anomaly_alert` : 実際の金額が下限値より小さい場合に`true`を返す。
# MAGIC - `on_trend` : `upper_anomaly_alert` または `lower_anomaly_alert` が`true`の場合に`true`を返す。
