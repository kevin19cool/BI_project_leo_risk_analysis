import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

print("==================================================")
print("🌤️ LEO 平台增量端 - 德國 GFZ 天氣 30 日滾動修復管線")
print("==================================================")

# 1. 初始化資料庫連線引擎
load_dotenv()
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mysecretpassword") 
# 🌟 內網門牌鎖定：對齊 Docker-Compose 內網服務名
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "leo_risk_db")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 2. 自動計算滾動 30 天窗口範圍
now_utc = datetime.now(timezone.utc)
start_dt = (now_utc - timedelta(days=30)).strftime('%Y-%m-%dT00:00:00Z')
end_dt = now_utc.strftime('%Y-%m-%dT23:59:59Z')

kp_url = f"https://kp.gfz-potsdam.de/app/json/?start={start_dt}&end={end_dt}&index=Kp"
f107_url = f"https://kp.gfz-potsdam.de/app/json/?start={start_dt}&end={end_dt}&index=Fobs"

try:
    # 3. Extract - 抽取 GFZ 數據
    print("📡 正在向德國 GFZ 伺服器請求 30 天 Kp 指數...")
    kp_res = requests.get(kp_url, timeout=15)
    print("📡 正在向德國 GFZ 伺服器請求 30 天 F10.7 指數...")
    f107_res = requests.get(f107_url, timeout=15)

    if kp_res.status_code == 200 and f107_res.status_code == 200:
        print("✅ GFZ 雙水源 JSON 成功獲取。開始進入 Transform 階段...")
        
        kp_data = kp_res.json()
        f107_data = f107_res.json()

        # Transform - 展平為原始 DataFrame
        df_kp_raw = pd.DataFrame({
            'timestamp_utc': pd.to_datetime(kp_data['datetime']),
            'kp_index': kp_data['Kp']
        })
        
        df_f107_raw = pd.DataFrame({
            'timestamp_utc': pd.to_datetime(f107_data['datetime']),
            'f107_value': f107_data['Fobs']
        })

        # 收斂顆粒度至「日 (Date)」
        df_kp_raw['date_str'] = df_kp_raw['timestamp_utc'].dt.strftime('%Y-%m-%d')
        df_f107_raw['date_str'] = df_f107_raw['timestamp_utc'].dt.strftime('%Y-%m-%d')

        # Kp 抓每日最大值 (捕捉暴風尖峰)
        df_kp_daily = df_kp_raw.groupby('date_str')['kp_index'].max().reset_index()
        # F10.7 抓每日平均值 (四捨五入至小數點第一位)
        df_f107_daily = df_f107_raw.groupby('date_str')['f107_value'].mean().reset_index()
        df_f107_daily['f10_7_index'] = df_f107_daily['f107_value'].round(1)

        # 每日級別數據大融合（Join）
        df_daily_weather = pd.merge(df_kp_daily, df_f107_daily[['date_str', 'f10_7_index']], on='date_str', how='inner')

        df_daily_weather['dt_obj'] = pd.to_datetime(df_daily_weather['date_str'])
        df_daily_weather['date_key'] = df_daily_weather['dt_obj'].dt.strftime('%Y%m%d').astype(int)
        
        def get_weather_risk_level(kp):
            if kp >= 5.0: return 'HIGH'
            elif kp >= 3.0: return 'MEDIUM'
            else: return 'LOW'
        df_daily_weather['risk_level'] = df_daily_weather['kp_index'].apply(get_weather_risk_level)

        # =============================================================================
        # 🧠 4. Load - 資料庫交易發動：實施方案二「洗淨型態、局部精準覆蓋」
        # =============================================================================
        with engine.connect() as conn:
            with conn.begin():

                # ─── 任務 A：動態補齊 dim_time 時間字典 ───
                print("\n⏳ 正在動態維護【dim_time 時間維度表】...")
                df_time = pd.DataFrame()
                df_time['date_key'] = df_daily_weather['date_key']
                df_time['year'] = df_daily_weather['dt_obj'].dt.year
                df_time['month'] = df_daily_weather['dt_obj'].dt.month
                df_time['day'] = df_daily_weather['dt_obj'].dt.day
                df_time['is_weekend'] = df_daily_weather['dt_obj'].dt.weekday.isin([5, 6])
                df_time = df_time.drop_duplicates(subset=['date_key'])
                
                existing_date_keys = conn.execute(text("SELECT date_key FROM dim_time")).scalars().all()
                df_time_new = df_time[~df_time['date_key'].isin(existing_date_keys)]
                
                if not df_time_new.empty:
                    df_time_new.to_sql('dim_time', con=conn, if_exists='append', index=False, method='multi')
                    print(f"  ✅ 成功將新登場的 {len(df_time_new)} 天時間門牌補入 dim_time！")
                else:
                    print("  ℹ️ dim_time 時間字典已是最新狀態。")

                # ─── 任務 B：30日天氣事實表局部精準覆蓋 ───
                print("⏳ 正在執行方案二增量覆蓋機制...")
                db_weather_cols = ['date_key', 'kp_index', 'f10_7_index', 'risk_level']
                df_weather_final = df_daily_weather[db_weather_cols].copy()
                
                # 🏥 終極 Bug 修復點：使用列表推導式將 Numpy.int64 強制洗成 Python 原生 int！
                target_keys = tuple(int(x) for x in df_weather_final['date_key'].unique())
                
                # 執行手術刀精準局部清空，保全 30 天以外的所有歷史數據
                if len(target_keys) == 1:
                    conn.execute(text(f"DELETE FROM fact_space_weather WHERE date_key = {target_keys[0]}"))
                else:
                    conn.execute(text(f"DELETE FROM fact_space_weather WHERE date_key IN {target_keys}"))
                
                # 倒水直灌最新觀測值（含今日滾動修正）
                df_weather_final.to_sql('fact_space_weather', con=conn, if_exists='append', index=False, method='multi')
                print(f"  🎉 【太空天氣事實表】自動更新覆蓋大成功！共 {len(df_weather_final)} 天觀測值安全著陸！")

        print("\n🏁 【德國 GFZ 方案二日常更新管線】全線通關！")
        print("==================================================")

    else:
        print(f"❌ GFZ 伺服器請求失敗，狀態碼: Kp={kp_res.status_code}, F107={f107_res.status_code}")
except Exception as e:
    print(f"❌ 執行管線發生連線或解析異常: {e}")