# dags/space_risk_daily_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# =============================================================================
# 📋 1. DAG 基礎配置 (對齊 2026 年期末專案時空背景)
# =============================================================================
default_args = {
    'owner': 'leo_space_analyst',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),  # 設定在 2026 年 6 月開始生效
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,                        # 如果外部 API 偶發性斷線，自動重試 2 次
    'retry_delay': timedelta(minutes=5),  # 每次重試間隔 5 分鐘
}

with DAG(
    dag_id='leo_space_environmental_risk_daily_pipeline',
    default_args=default_args,
    description='LEO 軌道擁擠度與太空天氣增量更新與綜合風險精算管線 (方案二生產環境版)',
    schedule_interval='55 23 * * *',     # 每日 UTC 23:55 定時啟動，完美卡點午夜大點名
    catchup=False,                       # 🚨 關閉歷史補追，防止 Airflow 啟動時重刷程序 1 已灌好的歷史
    tags=['space_intelligence', 'production', 'postgreSQL', 'decoupled'],
) as dag:

    print("==================================================")
    print("🧠 Apache Airflow 核心排程大腦已成功掛載 LEO 日常增量 DAG")
    print("==================================================")

    # =============================================================================
    # ⚡ 2. 任務定義 (利用 BashOperator 呼叫容器內 /app 目錄下的方案二增量小程式)
    # =============================================================================
    
    # 🛰️ 任務一：Space-Track 活體大軍午夜單發快照 (程序 2)
    task_space_track = BashOperator(
        task_id='fetch_and_load_space_track',
        bash_command='python /app/daily_pipeline.py',  # 完美對齊 Docker 內置路徑
        do_xcom_push=False
    )

    # 🌤️ 任務二：德國 GFZ 30日天氣滾動覆蓋修正 (程序 2)
    task_gfz_weather = BashOperator(
        task_id='fetch_and_load_gfz_weather',
        bash_command='python /app/daily_gfz_pipeline.py', # 完美對齊 Docker 內置路徑
        do_xcom_push=False
    )

    # 🧠 任務三：跨事實表（大盤表 + 天氣表）綜合風險交叉算分引擎 (程序 2)
    task_risk_assessment = BashOperator(
        task_id='calculate_comprehensive_risk',
        bash_command='python /app/daily_risk_assessment.py', # 完美對齊 Docker 內置路徑
        do_xcom_push=False
    )

    # =============================================================================
    # 🔀 3. 建立微服務網格相依性 (The Directed Acyclic Graph)
    # =============================================================================
    # 讓太空軍軌道數據（daily_pipeline.py）與德國天氣數據（daily_gfz_pipeline.py）
    # 在後台完全「非相依、異步並行」平行抓取，大幅節省 API 網路等待時延；
    # 兩者皆深綠色成功（Success）落地後，最後才觸發風險精算小程式，完成第七張表的合成。
    [task_space_track, task_gfz_weather] >> task_risk_assessment