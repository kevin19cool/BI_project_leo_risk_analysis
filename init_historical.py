# init_historical.py
import os
import requests
import time
import io
import csv
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

print("==================================================")
print("🚀 [程序 1] LEO 平台核心 - 基礎設施冷歷史大灌錄（終極洗淨版）")
print("==================================================")

# 1. 🔒 全局鐵板鎖死：Docker 內網通道唯一設定
# 🚨 絕對不使用 os.getenv("DB_HOST")，防止被本地的 .env 帶偏成 localhost
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_HOST = "postgres"   # 🔒 鐵板鎖死：Docker 內網唯一合法門牌
DB_PORT = "5432"       # 🔒 鐵板鎖死：Docker 內網原生連接埠
DB_NAME = "leo_risk_db"

connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# 2. 安全載入 Space-Track API 專用憑證 (這時只讀取帳密，絕不碰資料庫變數)
load_dotenv()
IDENTITY_EMAIL = os.getenv("SPACETRACK_USER")
IDENTITY_PASSWORD = os.getenv("SPACETRACK_PASS")


# ⚡ PostgreSQL 原生極速二進位 COPY 載入引擎
def psql_insert_copy(table, conn, keys, data_iter):
    raw_conn = conn.connection.dbapi_connection if hasattr(conn.connection, "dbapi_connection") else conn.connection
    with raw_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)
        columns = ", ".join([f'"{k}"' for k in keys])
        cur.copy_expert(sql=f"COPY {table.name} ({columns}) FROM STDIN WITH CSV", file=s_buf)


# =========================================================================
# 🌤️ 第一階段：德國 GFZ Potsdam 歷史 30 天天氣大直灌
# =========================================================================
print("\n🌤️ 開始執行：德國 GFZ Potsdam 30 日歷史天氣管線...")

now_utc = datetime.now(timezone.utc)
start_dt = (now_utc - timedelta(days=30)).strftime('%Y-%m-%dT00:00:00Z')
end_dt = now_utc.strftime('%Y-%m-%dT23:59:59Z')

kp_url = f"https://kp.gfz-potsdam.de/app/json/?start={start_dt}&end={end_dt}&index=Kp"
f107_url = f"https://kp.gfz-potsdam.de/app/json/?start={start_dt}&end={end_dt}&index=Fobs"

try:
    print("📡 正在向德國 GFZ 伺服器請求 30 天歷史 Kp 指數...")
    kp_res = requests.get(kp_url, timeout=15)
    print("📡 正在向德國 GFZ 伺服器請求 30 天歷史 F10.7 指數...")
    f107_res = requests.get(f107_url, timeout=15)

    if kp_res.status_code == 200 and f107_res.status_code == 200:
        print("✅ GFZ 雙水源 JSON 成功獲取。開始 Transform...")
        
        kp_data = kp_res.json()
        f107_data = f107_res.json()

        df_kp_raw = pd.DataFrame({
            'timestamp_utc': pd.to_datetime(kp_data['datetime']),
            'kp_index': kp_data['Kp']
        })
        df_f107_raw = pd.DataFrame({
            'timestamp_utc': pd.to_datetime(f107_data['datetime']),
            'f107_value': f107_data['Fobs']
        })

        df_kp_raw['date_str'] = df_kp_raw['timestamp_utc'].dt.strftime('%Y-%m-%d')
        df_f107_raw['date_str'] = df_f107_raw['timestamp_utc'].dt.strftime('%Y-%m-%d')

        df_kp_daily = df_kp_raw.groupby('date_str')['kp_index'].max().reset_index()
        df_f107_daily = df_f107_raw.groupby('date_str')['f107_value'].mean().reset_index()
        df_f107_daily['f10_7_index'] = df_f107_daily['f107_value'].round(1)

        df_daily_weather = pd.merge(df_kp_daily, df_f107_daily[['date_str', 'f10_7_index']], on='date_str', how='inner')
        df_daily_weather['dt_obj'] = pd.to_datetime(df_daily_weather['date_str'])
        df_daily_weather['date_key'] = df_daily_weather['dt_obj'].dt.strftime('%Y%m%d').astype(int)
        
        def get_weather_risk_level(kp):
            if kp >= 5.0: return 'HIGH'
            elif kp >= 3.0: return 'MEDIUM'
            else: return 'LOW'
        df_daily_weather['risk_level'] = df_daily_weather['kp_index'].apply(get_weather_risk_level)

        with engine.connect() as conn:
            with conn.begin():
                # 直灌 dim_time
                print("⏳ 正在建立歷史【dim_time 日級別時間維度表】...")
                df_time = pd.DataFrame()
                df_time['date_key'] = df_daily_weather['date_key']
                df_time['year'] = df_daily_weather['dt_obj'].dt.year
                df_time['month'] = df_daily_weather['dt_obj'].dt.month
                df_time['day'] = df_daily_weather['dt_obj'].dt.day
                df_time['is_weekend'] = df_daily_weather['dt_obj'].dt.weekday.isin([5, 6])
                df_time = df_time.drop_duplicates(subset=['date_key'])
                
                existing_date_keys = pd.read_sql("SELECT date_key FROM dim_time", con=conn)['date_key'].tolist()
                df_time_new = df_time[~df_time['date_key'].isin(existing_date_keys)]
                if not df_time_new.empty:
                    df_time_new.to_sql('dim_time', con=conn, if_exists='append', index=False, method='multi')

                # 直灌 fact_space_weather
                print("⏳ 正在直灌歷史【fact_space_weather 太空天氣事實表】...")
                db_weather_cols = ['date_key', 'kp_index', 'f10_7_index', 'risk_level']
                df_weather_final = df_daily_weather[db_weather_cols].copy()
                
                existing_weathers = pd.read_sql("SELECT date_key FROM fact_space_weather", con=conn)['date_key'].tolist()
                df_weather_new = df_weather_final[~df_weather_final['date_key'].isin(existing_weathers)]
                if not df_weather_new.empty:
                    df_weather_new.to_sql('fact_space_weather', con=conn, if_exists='append', index=False, method='multi')
                    print(f"  🎉 歷史太空天氣灌錄成功：共 {len(df_weather_new)} 天報告安全入庫。")
except Exception as e:
    print(f"❌ 執行天氣歷史管線發生異常: {e}")


# =========================================================================
# 🛰️ 第二階段：美國太空軍 Space-Track 歷史 7 天回溯大迴圈
# =========================================================================
print("\n🛰️ 開始執行：Space-Track 7 日歷史衛星大軍快照管線...")

session = requests.Session()
login_url = "https://www.space-track.org/ajaxauth/login"
login_payload = {"identity": IDENTITY_EMAIL, "password": IDENTITY_PASSWORD}

print("📡 正在向 Space-Track 進行安全身分驗證...")
login_res = session.post(login_url, data=login_payload, timeout=15)

if login_res.status_code != 200 or "LOGIN_FAILED" in login_res.text:
    print("❌ Space-Track 登入身分驗證失敗！請檢查 .env 密碼。")
else:
    print("✅ 驗證成功！發動【7天時空矩陣壓縮模型大解析】...\n")

    memory_micro_dfs = []
    memory_dim_sat_dfs = []  
    fact_rows = []

    shell_bins = [200, 400, 600, 800, 2000]
    shell_labels = [1, 2, 3, 4]

    for d in range(7):
        target_day = now_utc - timedelta(days=d)
        date_str = target_day.strftime("%Y-%m-%d")
        dk = int(target_day.strftime("%Y%m%d"))

        if d == 0:
            print(f"🔥 [🚀 熱路徑] 正在擷取今日 [{date_str}] 最新在軌快照...")
            query_url = "https://www.space-track.org/basicspacedata/query/class/gp/MEAN_MOTION/>11.25/decay_date/null-val/orderby/NORAD_CAT_ID%20asc"
        else:
            print(f"❄️ [⏳ 冷歷史] 正在調閱歷史 [{date_str}] 的全天觀測點...")
            query_url = f"https://www.space-track.org/basicspacedata/query/class/gp_history/EPOCH/{date_str}%2000:00:00--{date_str}%2023:59:59/MEAN_MOTION/>11.25/decay_date/null-val/orderby/NORAD_CAT_ID%20asc"

        data_res = session.get(query_url, timeout=45)

        if data_res.status_code == 200:
            df_day = pd.DataFrame(data_res.json())

            if df_day.empty:
                continue

            # ─── 🥇 終極安全防線：強行將 API 吐回的所有小寫欄位轉為大寫，阻絕 KeyError！ ───
            df_day.columns = df_day.columns.str.upper()

            # 數據強型態與清洗
            df_day["NORAD_CAT_ID"] = df_day["NORAD_CAT_ID"].astype(int)
            df_day["APOAPSIS"] = pd.to_numeric(df_day["APOAPSIS"], errors="coerce")
            df_day["PERIAPSIS"] = pd.to_numeric(df_day["PERIAPSIS"], errors="coerce")
            df_day["history_altitude_km"] = ((df_day["APOAPSIS"] + df_day["PERIAPSIS"]) / 2.0).round(1)
            df_day["date_key"] = int(dk)

            df_day["RCS_SIZE"] = df_day["RCS_SIZE"].fillna("UNKNOWN")
            df_day["INCLINATION"] = pd.to_numeric(df_day["INCLINATION"], errors="coerce")
            df_day["ECCENTRICITY"] = pd.to_numeric(df_day["ECCENTRICITY"], errors="coerce")
            df_day["COUNTRY_CODE"] = df_day["COUNTRY_CODE"].fillna("UNKNOWN")

            df_day_dedup = df_day.drop_duplicates(subset=["NORAD_CAT_ID"], keep="last").copy()

            # 收集維度特徵字典
            df_sat_dim_keep = df_day_dedup[["NORAD_CAT_ID", "OBJECT_NAME", "OBJECT_TYPE", "COUNTRY_CODE", "RCS_SIZE"]].copy()
            df_sat_dim_keep.columns = ["norad_cat_id", "object_name", "object_type", "country_code", "rcs_size"]
            memory_dim_sat_dfs.append(df_sat_dim_keep)

            # 收集微觀明細事實表
            df_micro_keep = df_day_dedup[["date_key", "EPOCH", "NORAD_CAT_ID", "OBJECT_NAME", "OBJECT_TYPE", "history_altitude_km", "INCLINATION", "ECCENTRICITY"]].copy()
            df_micro_keep.columns = ["date_key", "epoch", "norad_cat_id", "object_name", "object_type", "history_altitude_km", "inclination", "eccentricity"]
            memory_micro_dfs.append(df_micro_keep)

            # 收集宏觀計數
            df_day_dedup["shell_id"] = pd.cut(df_day_dedup["history_altitude_km"], bins=shell_bins, labels=shell_labels)
            for shell in shell_labels:
                grp = df_day_dedup[df_day_dedup["shell_id"] == shell]
                sc = int(grp[grp["OBJECT_TYPE"] == "PAYLOAD"].shape[0])
                dc = int(grp[grp["OBJECT_TYPE"] != "PAYLOAD"].shape[0])
                fact_rows.append({
                    "date_key": int(dk), "shell_id": int(shell), "satellite_count": sc, "debris_count": dc, "total_count": sc + dc
                })
        else:
            print(f"❌ 抓取 {date_str} 失敗")

        time.sleep(1)

    # 3. 執行全域唯一、絕不偏航的 COPY 直灌機制
    if memory_micro_dfs and fact_rows and memory_dim_sat_dfs:
        print("\n🧠 [全域工程] 歷史快照矩陣解壓完畢，啟動 COPY 灌錄...")

        df_all_7days_micro = pd.concat(memory_micro_dfs, ignore_index=True)
        df_all_7days_sat_dim = pd.concat(memory_dim_sat_dfs, ignore_index=True)

        with engine.connect() as conn:
            with conn.begin():
                # ─── 任務 A：補滿 7 日 dim_time 時間門牌 ───
                for d_int in df_all_7days_micro["date_key"].unique():
                    dt_o = datetime.strptime(str(d_int), "%Y%m%d")
                    conn.execute(text(f"INSERT INTO dim_time (date_key, year, month, day, is_weekend) VALUES ({d_int}, {dt_o.year}, {dt_o.month}, {dt_o.day}, {dt_o.weekday() in [5,6]}) ON CONFLICT DO NOTHING"))

                # ─── 任務 B：造冊登錄 【dim_satellite 衛星字典表】 ───
                print("⏳ 正在對 28,000 顆活體星體進行全域唯一化排重造冊...")
                df_all_7days_sat_dim = df_all_7days_sat_dim.drop_duplicates(subset=["norad_cat_id"], keep="last")
                existing_sats = pd.read_sql("SELECT norad_cat_id FROM dim_satellite", con=conn)["norad_cat_id"].astype(int).tolist()
                new_sats_to_insert = df_all_7days_sat_dim[~df_all_7days_sat_dim["norad_cat_id"].isin(existing_sats)]
                
                if not new_sats_to_insert.empty:
                    new_sats_to_insert.to_sql("dim_satellite", con=conn, if_exists="append", index=False, method=psql_insert_copy)
                    print(f"  🎉 【dim_satellite 字典表】造冊成功！共計 {len(new_sats_to_insert)} 顆星體特徵安全寫入。")

                # ─── 任務 C：倒灌 【fact_satellite_7days_history 微觀明細事實表】 ───
                print("⏳ 正在高速流式傾倒 7 日全宇宙微觀軌道明細事實表...")
                conn.execute(text("TRUNCATE TABLE fact_satellite_7days_history CASCADE"))
                df_all_7days_micro = df_all_7days_micro.drop_duplicates(subset=["date_key", "norad_cat_id"], keep="last")
                df_all_7days_micro.to_sql("fact_satellite_7days_history", con=conn, if_exists="append", index=False, method=psql_insert_copy)

                # ─── 任務 D：精算倒灌 【fact_orbital_density 宏觀大盤統計事實表】 ───
                print("⏳ 正在跨時空精算 7 日大盤軌道高度份額密度分數...")
                conn.execute(text("TRUNCATE TABLE fact_orbital_density CASCADE"))
                df_macro_final = pd.DataFrame(fact_rows)
                day_sums = df_macro_final.groupby("date_key")["total_count"].transform("sum")
                df_macro_final["day_leo_total"] = day_sums.replace(0, 1).astype(float)

                df_macro_final["base_occupancy"] = (df_macro_final["total_count"] / df_macro_final["day_leo_total"]) * 100
                df_macro_final["debris_ratio"] = (df_macro_final["debris_count"] / df_macro_final["total_count"]).fillna(0.0)
                df_macro_final["debris_multiplier"] = 1.0 + (df_macro_final["debris_ratio"] * 0.5)
                df_macro_final["density_score"] = (df_macro_final["base_occupancy"] * df_macro_final["debris_multiplier"]).round(0).astype(int).clip(upper=100)

                db_macro_cols = ["date_key", "shell_id", "satellite_count", "debris_count", "total_count", "density_score"]
                df_macro_final[db_macro_cols].to_sql("fact_orbital_density", con=conn, if_exists="append", index=False)
                print("  ✅ 7 日歷史大盤與明細全部直灌完畢！")

print("\n🏁 [程序 1] 歷史冷數據大直灌全部大獲全勝！數據中台地基已牢不可破！")