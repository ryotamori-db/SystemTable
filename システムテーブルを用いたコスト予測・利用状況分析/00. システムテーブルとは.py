# Databricks notebook source
# MAGIC %md
# MAGIC # システムテーブルとは
# MAGIC Databricks内の様々なアクティビティを分析ストアとして一元化したものです。  
# MAGIC これには、ユーザーのログイン状況、クラスターの稼働状況、コストなどが含まれ、これらの情報はコスト管理や不審なアクティビティの監視などに活用できます。  
# MAGIC
# MAGIC 不審なアクティビティについては、以下のような例が挙げられます。  
# MAGIC **・ユーザーの権限が意図せず変更される**  
# MAGIC **・予期せぬクラスターが起動して想定外のコストが発生する**  
# MAGIC **・使用量が予想を超えて想定コストを超える**  
# MAGIC
# MAGIC これらのアクティビティは、運用次第である程度の統制が可能ですが、  
# MAGIC システムテーブルも活用することで、**アクティビティを自動的に監視・検知することが可能**となり、   
# MAGIC Databricksの運用をより効率的かつ、安全に実施する事が可能です。
# MAGIC
# MAGIC 詳細は以下ドキュメントをご確認ください。  
# MAGIC https://docs.databricks.com/ja/administration-guide/system-tables/index.html
# MAGIC
# MAGIC ![概観](https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/system_tables/uc-system-tables-explorer.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC # システムテーブルに含まれるデータ
# MAGIC | テーブル | 説明 | 格納場所 | ストリーミングサポート有無 | データ保持期間 |
# MAGIC | ---- | ---- | ---- | ---- | ---- |
# MAGIC | **監査ログ** | アカウント、ユーザー、グループはじめとした、監査イベントのレコード | *system.access.audit* | あり | 365日間 |
# MAGIC | **テーブルリネージ** | Unity Catalog上のテーブルもしくはパス上の読み取りまたは書き込みイベントのレコード | *system.access.table_lineage* | あり | 365日間 |
# MAGIC | **カラムリネージ** | Unity Catalog上の列の読み取りまたは書き込みイベントのレコード | *system.access.column_lineage* | あり | 365日間 |
# MAGIC | **請求対象となる使用量** | アカウント全体の課金利用に関するレコード | *system.billing.usage* | あり | 365日間 |
# MAGIC |　**価格プラン** | SKU価格のレコード | *system.billing.list_prices* | なし | N/A |
# MAGIC | **クラスター** | 構成をはじめとしたクラスターに関するレコード | *system.compute.clusters* | あり | なし |
# MAGIC | **ノードの種類** | 現在使用可能なノードタイプやハードウェア情報に関するレコード | *system.compute.node_types* | なし | N/A |
# MAGIC | **SQLウェアハウスイベント** | SQLウェアハウスの開始、停止、実行、スケールアップとスケールダウンなどのイベントレコード | *system.compute.warehouse_events* | あり | なし |
# MAGIC | **Marketplace ファネルイベント** | Marketplaceのファネルに関するレコード | *system.marketplace.listing_funnel_events* | あり | 365日間 |
# MAGIC | **Marketplace リストへのアクセス** | Marketplaceリスティングに関するレコード | *system.marketplace.listing_access_events* | あり | 365日間 |
# MAGIC | **予測的最適化** | 予測的最適化機能の操作に関するレコード | *system.storage.predictive_optimization_operations_history* | なし | 180日間 |

# COMMAND ----------

# MAGIC %md 
# MAGIC # システムテーブルの有効化
# MAGIC システムテーブルを利用するには、少なくとも1つのワークスペースでUnity Catalogを有効化している必要があります。  
# MAGIC
# MAGIC 提供されるデータは、ワークスペースのUnity Catalogのステータスに関係なく、Databricksアカウント内のすべてのワークスペースから収集されます。  
# MAGIC 例えば、10個のワークスペースがあり、そのうちの1つだけがUnity Catalogを有効にしている場合、すべてのワークスペースのデータが収集され、Unity Catalogが有効になっている1つのワークスペースからシステムテーブルのデータにアクセスできるようになります。  
# MAGIC
# MAGIC また、システムテーブルはデフォルトでアクセスできないため、利用する際には、システムテーブルの利用を有効化する必要があります。  
# MAGIC 有効化はアカウント管理者が実施できます。詳細は、[**99. システムテーブルの有効化**]($./99. システムテーブルの有効化) ノートブックをご確認ください。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # コスト管理分析ダッシュボード
# MAGIC このダッシュボードは、**現在の使用量に加えて将来の使用量を予測し、可視化することができます。**  
# MAGIC
# MAGIC 具体的なビジネスメリットを以下記載します。  
# MAGIC ・請求額が設定した基準を超えた場合にアラートを発する事で、**想定外のコスト発生を未然に防ぐ事が可能**です。  
# MAGIC ・クラスターの起動時間などから**分析の非効率性を特定**し、それに対処する手立てを講じることが可能です。これは、時間の経過とともに**大幅なコスト削減につながります**。  
# MAGIC
# MAGIC 裏側の処理としては、時系列予測ライブラリである**prophet**を使用して、複数のMLモデルをトレーニングしています。  
# MAGIC そのため、予測データを生成するために、**02.将来コストの予測**ノートブックを必ず実行してください。 
# MAGIC
# MAGIC また、予測精度を高めるために、上記ノートブックを**毎日ジョブとして実行することをお勧めします。**
# MAGIC
# MAGIC ![](https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/system_tables/dashboard-governance-billing.png?raw=true)
