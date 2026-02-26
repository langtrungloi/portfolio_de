from datetime import datetime, timedelta
from airflow.decorators import dag, task
import requests
import pandas as pd
from sqlalchemy import create_engine

API_KEY = "8b58ceea3f870838adab5004e9441ad0"
CITY = "Ho Chi Minh City"
DB_URL = "postgresql+psycopg2://airflow:airflow@host.docker.internal:5432/mis"

@dag(
    dag_id="weather_etl_daily",
    start_date=datetime(2025, 1, 1),   # để quá khứ
    schedule="0 */3 * * *",
    catchup=False,
    tags=["etl", "weather"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def weather_etl():

    @task
    def extract() -> dict:
        url = (
            "http://api.openweathermap.org/data/2.5/weather"
            f"?q={CITY}&appid={API_KEY}&units=metric"
        )
        resp = requests.get(url, timeout=30)
        data = resp.json()

        if data.get("cod") != 200:
            raise ValueError(f"API error: {data}")

        return data  # 👈 trả dict, Airflow tự xử lý XCom

    @task
    def transform(data: dict) -> dict:
        record = {
            "city": data["name"],
            "temp": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "description": data["weather"][0]["description"],
            "dt": datetime.utcfromtimestamp(data["dt"]),
        }
        return record  # 👈 dict nhẹ, an toàn

    @task
    def load(record: dict):
        engine = create_engine(DB_URL)
        df = pd.DataFrame([record])

        df.to_sql(
            "weather_data",
            engine,
            schema="marketing",
            if_exists="append",
            index=False,
        )

    # 🔗 pipeline
    raw = extract()
    clean = transform(raw)
    load(clean)


weather_etl()