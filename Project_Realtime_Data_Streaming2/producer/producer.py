import json
import os
import random
import time
import logging
from datetime import datetime
from kafka import KafkaProducer

# ---------- logging ----------
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/producer.log"),
        logging.StreamHandler()
    ],
)

logger = logging.getLogger(__name__)

# ---------- kafka ----------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "sensor_stream"

logger.info("Producer started")

while True:
    data = {
        "sensor_id": f"sensor_{random.randint(1,5)}",
        "temperature": round(random.uniform(20, 40), 2),
        "humidity": round(random.uniform(30, 90), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send(TOPIC, data)
    logger.info("Sent %s", data)
    time.sleep(1)