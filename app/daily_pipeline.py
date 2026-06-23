import csv
import io
import os
from datetime import datetime, timezone
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =============================================================================
# ⚡ 0. 極速傳輸通道：PostgreSQL 原生二進位 COPY 機制
# =============================================================================
def psql_insert_copy(table, conn, keys, data_iter):
    raw_conn = conn.connection.dbapi_connection if hasattr(conn.connection, 'dbapi_connection') else conn.connection
    with raw_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)
        columns = ", ".join([f'"{k}"' for k in keys])
        cur.copy_expert(sql=f"COPY {table.name} ({columns}) FROM STDIN WITH CSV", file=s_buf)

print("==================================================")
print("🚀 LEO 平台增量端 - Space-Track 今日快照調度管線")
print("==================================================")

# 1. 環境初始化與內網引擎綁定
load_dotenv()
IDENTITY_EMAIL = os.getenv("SPACETRACK_USER")
IDENTITY_PASSWORD = os.getenv("SPACETRACK_PASS")

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mysecretpassword")
# 🌟 內網門牌鎖定：Airflow 貨櫃大喊 postgres 即可連線資料庫
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "leo_risk_db")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 2. 自動鎖定今日門牌 (純 Python 原生型態，絕無 Numpy 雜訊)
now_utc = datetime.now(timezone.utc)
dk = int(now_utc.strftime("%Y%m%d"))  # 例如：20260623

shell_bins = [200, 400, 600, 800, 2000]
shell_labels = [1, 2, 3, 4]

print(f"⏱️ 今日目標更新門牌: [{dk}]")

# 3. Extract - 發動單發「熱路徑」API 快照攔截
session = requests.Session()
login_url = "https://www.space-track.org/ajaxauth/login"
login_payload = {'identity': IDENTITY_EMAIL, 'password': IDENTITY_PASSWORD}

print("📡 正在向 Space-Track 驗證身分...")
login_res = session.post(login_url, data=login_payload, timeout=15)

if login_res.status_code != 200 or "LOGIN_FAILED" in login_res.text:
    print("❌ 登入身分驗證失敗！日常排程中止。")
else:
    print("✅ 驗證成功！正在擷取今日 LEO 全量即時快照...")
    query_url = "https://www.space-track.org/basicspacedata/query/class/gp/MEAN_MOTION/>11.25/decay_date/null-val/orderby/NORAD_CAT_ID%20asc"
    data_res = session.get(query_url, timeout=45)
    
    if data_res.status_code == 200:
        df_day = pd.DataFrame(data_res.json())
        
        if not df_day.empty:
            print(f"📥 成功攔截最新數據流：共 {len(df_day)} 筆活體紀錄。開始進入 Transform 階段...")
            df_day.columns = df_day.columns.str.upper()
            # --- Transform: 數據清洗與強型態轉換 ---
            df_day["NORAD_CAT_ID"] = df_day["NORAD_CAT_ID"].astype(int)
            df_day["APOAPSIS"] = pd.to_numeric(df_day["APOAPSIS"], errors="coerce")
            df_day["PERIAPSIS"] = pd.to_numeric(df_day["PERIAPSIS"], errors="coerce")
            df_day["history_altitude_km"] = ((df_day["APOAPSIS"] + df_day["PERIAPSIS"]) / 2.0).round(1)
            
            # 🌟 核心靈魂：將今天抓到的活體全量，強行蓋上今天的 date_key，建立我們自己定義的歷史！
            df_day["date_key"] = int(dk)
            
            df_day["RCS_SIZE"] = df_day["RCS_SIZE"].fillna("UNKNOWN")
            df_day["INCLINATION"] = pd.to_numeric(df_day["INCLINATION"], errors="coerce")
            df_day["ECCENTRICITY"] = pd.to_numeric(df_day["ECCENTRICITY"], errors="coerce")
            df_day["COUNTRY_CODE"] = df_day["COUNTRY_CODE"].fillna("UNKNOWN")
            
            # 單星唯一化
            df_day_dedup = df_day.drop_duplicates(subset=["NORAD_CAT_ID"], keep="last").copy()
            
            # =============================================================================
            # 🧠 4. Load - 資料庫交易發動：實施方案二「先精準刪除、後高速直灌」
            # =============================================================================
            with engine.connect() as conn:
                with conn.begin():
                    
                    # ─── 任務 A：動態開闢今天的 dim_time 門牌 ───
                    conn.execute(text(
                        f"INSERT INTO dim_time (date_key, year, month, day, is_weekend) "
                        f"VALUES ({dk}, {now_utc.year}, {now_utc.month}, {now_utc.day}, {now_utc.weekday() in [5,6]}) "
                        f"ON CONFLICT DO NOTHING"
                    ))
                    
                    # ─── 任務 B：新衛星字典自動造冊 ───
                    df_sat_dim = df_day_dedup[["NORAD_CAT_ID", "OBJECT_NAME", "OBJECT_TYPE", "COUNTRY_CODE", "RCS_SIZE"]].copy()
                    df_sat_dim.columns = ["norad_cat_id", "object_name", "object_type", "country_code", "rcs_size"]
                    df_sat_dim = df_sat_dim.drop_duplicates(subset=["norad_cat_id"])
                    
                    existing_sats = pd.read_sql("SELECT norad_cat_id FROM dim_satellite", con=conn)["norad_cat_id"].astype(int).tolist()
                    new_sats_to_insert = df_sat_dim[~df_sat_dim["norad_cat_id"].isin(existing_sats)]
                    
                    if not new_sats_to_insert.empty:
                        print(f"  ✨ [字典增量] 發現今日有 {len(new_sats_to_insert)} 顆全新星體，追加造冊...")
                        new_sats_to_insert.to_sql("dim_satellite", con=conn, if_exists='append', index=False, method=psql_insert_copy)
                    
                    # ─── 任務 C：微觀明細表 ─── 🌟 冪等性手術刀：先精準切除今天，再用 COPY 直灌今天
                    conn.execute(text(f"DELETE FROM fact_satellite_7days_history WHERE date_key = {dk}"))
                    
                    df_micro_keep = df_day_dedup[["date_key", "EPOCH", "NORAD_CAT_ID", "OBJECT_NAME", "OBJECT_TYPE", "history_altitude_km", "INCLINATION", "ECCENTRICITY"]].copy()
                    df_micro_keep.columns = ["date_key", "epoch", "norad_cat_id", "object_name", "object_type", "history_altitude_km", "inclination", "eccentricity"]
                    
                    print(f"  ⚡ [明細 COPY] 正在寫入今日微觀快照（共 {len(df_micro_keep)} 行）...")
                    df_micro_keep.to_sql("fact_satellite_7days_history", con=conn, if_exists='append', index=False, method=psql_insert_copy)
                    
                    # ─── 任務 D：宏觀大盤統計 ─── 🌟 冪等性手術刀：先精準切除今天，再附加精算今日份額
                    conn.execute(text(f"DELETE FROM fact_orbital_density WHERE date_key = {dk}"))
                    
                    df_day_dedup["shell_id"] = pd.cut(df_day_dedup["history_altitude_km"], bins=shell_bins, labels=shell_labels)
                    fact_rows = []
                    for shell in shell_labels:
                        grp = df_day_dedup[df_day_dedup["shell_id"] == shell]
                        sc = int(grp[grp["OBJECT_TYPE"] == "PAYLOAD"].shape[0])
                        dc = int(grp[grp["OBJECT_TYPE"] != "PAYLOAD"].shape[0])
                        fact_rows.append({
                            "date_key": dk, "shell_id": int(shell), "satellite_count": sc, "debris_count": dc, "total_count": sc + dc
                        })
                    
                    df_macro_final = pd.DataFrame(fact_rows)
                    day_sum = df_macro_final["total_count"].sum()
                    df_macro_final["day_leo_total"] = float(day_sum if day_sum > 0 else 1)
                    df_macro_final["base_occupancy"] = (df_macro_final["total_count"] / df_macro_final["day_leo_total"]) * 100
                    df_macro_final["debris_ratio"] = (df_macro_final["debris_count"] / df_macro_final["total_count"]).fillna(0.0)
                    df_macro_final["debris_multiplier"] = 1.0 + (df_macro_final["debris_ratio"] * 0.5)
                    df_macro_final["density_score"] = (df_macro_final["base_occupancy"] * df_macro_final["debris_multiplier"]).round(0).astype(int).clip(upper=100)
                    
                    db_macro_cols = ["date_key", "shell_id", "satellite_count", "debris_count", "total_count", "density_score"]
                    df_macro_final[db_macro_cols].to_sql("fact_orbital_density", con=conn, if_exists='append', index=False)
                    print(f"  ✅ [大盤增量] 今日門牌 {dk} 軌道份額精算落地成功！")

            print(f"\n🏁 【Space-Track 日常更新完勝】今日全量數據已安全咬合入庫！")
    else:
        print(f"❌ 呼叫 Space-Track 失敗，狀態碼: {data_res.status_code}")