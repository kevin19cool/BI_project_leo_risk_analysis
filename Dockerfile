# 🌟 使用官方 Airflow 2.7.1 搭配 Python 3.10 作為大底
FROM apache/airflow:2.7.1-python3.10

# 將本地的套件清單複製到容器的內部工作目錄
COPY requirements.txt .

# 🚨 企業級安全規範：使用官方指定的 airflow 用戶執行安裝
# 絕對不要用 root 用戶安裝，否則日後 Airflow 執行日常小程式時會發生檔案權限衝突 (Permission Denied)
RUN pip install --no-cache-dir --user -r requirements.txt