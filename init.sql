-- ============================================================================
-- 🌌 太空軌道風險與天氣整合專案：GALAXY SCHEMA 一鍵自動初始化腳本
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 🥇 第一階段：核心維度表 (Conformed Dimensions / 基礎字典)
-- ----------------------------------------------------------------------------

-- 1. 衛星與在軌物體字典表
CREATE TABLE IF NOT EXISTS dim_satellite (
    norad_cat_id INT PRIMARY KEY,
    object_name VARCHAR(100) NOT NULL,
    object_type VARCHAR(50),
    country_code VARCHAR(10),
    rcs_size VARCHAR(20)
);

-- 2. 高度殼層別名字典表
CREATE TABLE IF NOT EXISTS dim_shell (
    shell_id INT PRIMARY KEY,
    min_altitude INT NOT NULL,
    max_altitude INT NOT NULL,
    display_name VARCHAR(50) NOT NULL,
    description TEXT
);

-- 3. 黃金時間維度表 (包含小時顆粒度，完美牽起雷達事件與天氣觀測)
CREATE TABLE IF NOT EXISTS dim_time (
    date_key INT PRIMARY KEY,            -- 格式: YYYYMMDD
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- ----------------------------------------------------------------------------
-- 🥈 第二階段：獨立/微觀事實表 (Fact Tables - Independent & Micro)
-- ----------------------------------------------------------------------------

-- 4. 宏觀大盤統計事實表
CREATE TABLE IF NOT EXISTS fact_orbital_density (
    date_key INT NOT NULL,
    shell_id INT NOT NULL,
    satellite_count INT DEFAULT 0,
    debris_count INT DEFAULT 0,
    total_count INT DEFAULT 0,
    density_score INT DEFAULT 0,
    PRIMARY KEY (date_key, shell_id),
    FOREIGN KEY (shell_id) REFERENCES dim_shell(shell_id),
    FOREIGN KEY (date_key) REFERENCES dim_time(date_key)
);

-- 5. 微觀個體追蹤事實表 (記錄過去 7 天全量雷達觀測點位明細)
CREATE TABLE IF NOT EXISTS fact_satellite_7days_history (
    date_key INT NOT NULL,
    epoch TIMESTAMP NOT NULL,
    norad_cat_id INT NOT NULL,
    object_name VARCHAR(100),
    object_type VARCHAR(50),
    history_altitude_km NUMERIC(6,1),
    inclination NUMERIC(5,2),
    eccentricity NUMERIC(7,6),
    PRIMARY KEY (date_key, norad_cat_id),
    FOREIGN KEY (norad_cat_id) REFERENCES dim_satellite(norad_cat_id),
    FOREIGN KEY (date_key) REFERENCES dim_time(date_key)
);

-- 6. GFZ 太空天氣觀測事實表 
CREATE TABLE IF NOT EXISTS fact_space_weather (
    date_key INT PRIMARY KEY,         -- 直接使用 date_key 作為主鍵
    kp_index NUMERIC(3,1),            -- 每日最大 Kp (地磁暴尖峰)
    f10_7_index NUMERIC(5,1),         -- 每日平均 F10.7 (太陽背景輻射)
    risk_level VARCHAR(20),           -- 天氣風險等級 (LOW, MEDIUM, HIGH)
    FOREIGN KEY (date_key) REFERENCES dim_time(date_key)
);

-- ----------------------------------------------------------------------------
-- 🏅 第三階段：跨資料源整合事實表 (Consolidated Bridge Fact Table)
-- ----------------------------------------------------------------------------

-- 7. 綜合風險評估事實表 (由 Python 每日精算融合 擁擠度 與 天氣 欄位)
CREATE TABLE IF NOT EXISTS fact_orbital_risk_assessment (
    date_key INT NOT NULL,            -- 關聯 dim_time 的日期級
    shell_id INT NOT NULL,            -- 關聯 dim_shell
    orbital_density_score INT DEFAULT 0,
    total_object_count INT DEFAULT 0,
    avg_kp_index NUMERIC(3,1),
    avg_f10_7_index NUMERIC(5,1),
    composite_risk_score INT DEFAULT 0,
    risk_category VARCHAR(20),        -- 綜合風險等級 (CRITICAL, HIGH, MEDIUM, LOW)
    PRIMARY KEY (date_key, shell_id),
    FOREIGN KEY (shell_id) REFERENCES dim_shell(shell_id),
    FOREIGN KEY (date_key) REFERENCES dim_time(date_key)
);

-- ----------------------------------------------------------------------------
-- ⚡ 第四階段：性能優化索引 (Performance Optimization Indexes)
-- ----------------------------------------------------------------------------
-- 針對 Dashboard 盲搜單一衛星、切換特定日期與殼層進行高速優化
CREATE INDEX IF NOT EXISTS idx_micro_history_norad_id ON fact_satellite_7days_history(norad_cat_id);
CREATE INDEX IF NOT EXISTS idx_density_search ON fact_orbital_density(date_key, shell_id);
CREATE INDEX IF NOT EXISTS idx_risk_assessment_search ON fact_orbital_risk_assessment(date_key, shell_id);

-- 填補核心高度殼層字典的 4 筆基礎翻譯資料
INSERT INTO dim_shell (shell_id, min_altitude, max_altitude, display_name, description)
VALUES 
(1, 200, 400, 'Shell 1 (極低軌道區)', '國際太空站 (ISS) 部署範圍，受微薄大氣阻力影響，物件易產生失控沉降。'),
(2, 400, 600, 'Shell 2 (星鏈核心區)', 'Starlink 密集部署部署之商業精華區，當前在軌活衛星密度最高。'),
(3, 600, 800, 'Shell 3 (中低軌道區)', '各國政府科研、大型氣象與遙測衛星骨幹區，太空垃圾密度逐步上升。'),
(4, 800, 2000, 'Shell 4 (高低軌道區)', '歷史封存之長壽太空垃圾、報廢火箭殘骸重災區，極易引發連鎖碰撞。')
ON CONFLICT (shell_id) DO NOTHING; -- 確保重複執行腳本時不會報錯