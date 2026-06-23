import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

print("==================================================")
print("🚀 LEO 平台核心 - 終極綜合風險評估合成引擎 (方案二生產版)")
print("==================================================\n")

# 1. 初始化資料庫引擎
load_dotenv()
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mysecretpassword") 
# 🌟 門牌大對齊：在 Docker 微服務網路中，主機名稱就是服務名稱 postgres
DB_HOST = os.getenv("DB_HOST", "postgres") 
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_DBNAME", "leo_risk_db")

connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

try:
    # 2. Extract - 從資料庫直接讀取最新落地的兩大事實表
    print("📥 正在讀取資料庫中的【宏觀大盤統計事實表】...")
    df_density = pd.read_sql("SELECT date_key, shell_id, total_count, density_score FROM fact_orbital_density", con=engine)
    
    print("📥 正在讀取資料庫中的【日級別太空天氣事實表】...")
    df_weather = pd.read_sql("SELECT date_key, kp_index, f10_7_index FROM fact_space_weather", con=engine)
    
    if df_density.empty or df_weather.empty:
        print("⚠️ 警告：大盤表或天氣表其中之一尚無資料，日常排程可能尚未結算。")
    else:
        # 3. Transform - 透過黃金主鍵 date_key 進行跨資料源大融合
        print("🔄 正在進行跨源數據時間軸對齊 (Merge by date_key)...")
        df_risk = pd.merge(df_density, df_weather, on='date_key', how='inner')
        
        # 4. 🔥 核心特徵工程：精算加權綜合風險分數 (Composite Risk Score)
        print("🧠 正在執行太空物理風險特徵工程算分 (加權: 大盤密度 60% + 地磁暴 40%)...")
        df_risk['composite_risk_score'] = (df_risk['density_score'] * 0.6 + (df_risk['kp_index'] / 9.0 * 100) * 0.4).round(0).astype(int)
        
        # 修正欄位名稱以完美對齊結構
        df_risk['orbital_density_score'] = df_risk['density_score']
        df_risk['total_object_count'] = df_risk['total_count']
        df_risk['avg_kp_index'] = df_risk['kp_index']
        df_risk['avg_f10_7_index'] = df_risk['f10_7_index']
        
        # 根據分數打上專業的航太警戒分類標籤
        def get_risk_category(score):
            if score >= 85: return 'CRITICAL'
            elif score >= 60: return 'HIGH'
            elif score >= 30: return 'MEDIUM'
            else: return 'LOW'
        df_risk['risk_category'] = df_risk['composite_risk_score'].apply(get_risk_category)
        
        # 整理要塞入資料庫的欄位
        db_risk_cols = [
            'date_key', 'shell_id', 'orbital_density_score', 'total_object_count', 
            'avg_kp_index', 'avg_f10_7_index', 'composite_risk_score', 'risk_category'
        ]
        df_risk_final = df_risk[db_risk_cols].copy()
        
        # 🏥 終極修復：強制轉成 Python 原生 int 避免 Numpy 型態報錯
        target_keys = tuple(int(x) for x in df_risk_final['date_key'].unique())

        # 5. Load - 運用交易機制鎖定發動 ─── 先精準刪除，再覆蓋直灌
        print("⏳ 正在執行方案二「手術刀增量覆蓋機制」...")
        with engine.connect() as conn:
            with conn.begin():
                
                # 🏥 冪等性防禦：抹除當前變更區間的舊計算報告，確保重複跑資料不翻倍、不衝突
                if len(target_keys) == 1:
                    conn.execute(text(f"DELETE FROM fact_orbital_risk_assessment WHERE date_key = {target_keys[0]}"))
                else:
                    conn.execute(text(f"DELETE FROM fact_orbital_risk_assessment WHERE date_key IN {target_keys}"))
                
                # 直灌寫入
                df_risk_final.to_sql('fact_orbital_risk_assessment', con=conn, if_exists='append', index=False, method='multi')
                print(f"🎉 🎉 【終極綜合風險評估表】覆蓋更新成功！共計 {len(df_risk_final)} 筆動態風險報告安全著陸！")
                
    print("\n🏁 【LEO 核心數據大合體】日常調度完畢！請重新整理 Power BI 查看最新綜合風險！")
    print("==================================================")
except Exception as e:
    print(f"❌ 執行合體管線發生異常: {e}")