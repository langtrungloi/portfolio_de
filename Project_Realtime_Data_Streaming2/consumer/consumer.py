import json
import os
import psycopg2
import logging
from kafka import KafkaConsumer
from datetime import datetime

# ---------- logging ----------
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/consumer.log"),
        logging.StreamHandler()
    ],
)

logger = logging.getLogger(__name__)

# ---------- kafka ----------
consumer = KafkaConsumer(
    "sensor_stream",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="sensor-group",
    auto_offset_reset="earliest"
)

# ---------- postgres ----------
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)
cur = conn.cursor()

logger.info("Consumer started")

for msg in consumer:
    data = msg.value

    if data["temperature"] > 35:
        logger.warning("Filtered outlier %s", data)
        continue

    cur.execute(
        """
        INSERT INTO sensor_data(sensor_id, temperature, humidity, created_at)
        VALUES (%s, %s, %s, %s)
        """,
        (
            data["sensor_id"],
            data["temperature"],
            data["humidity"],
            datetime.fromisoformat(data["timestamp"])
        )
    )
    conn.commit()
    logger.info("Inserted %s", data)