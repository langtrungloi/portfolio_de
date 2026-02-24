import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
from datetime import datetime

# ================= LOGGING =================
logging.basicConfig(
    filename="etl_process.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================= DB CONFIG =================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "mis",
    "user": "langloi",
    "password": "loilang"
}

SCHEMA = "marketing"
TABLE = "coffee_sales"

# ================= EXTRACT =================
def extract(file_path: str) -> pd.DataFrame:
    try:
        file_path = os.path.expanduser(file_path)
        df = pd.read_excel(file_path)
        logging.info(f"Extract thành công: {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"Extract lỗi: {e}")
        raise
# ================= VALIDATE =================
def validate_data(df: pd.DataFrame):
    """
    Data Profiling + Data Quality Check
    Fail-fast nếu lỗi nghiêm trọng
    """

    errors = []
    warnings = []

    logging.info("=== DATA VALIDATION START ===")

    # 1️⃣ Schema & metadata
    logging.info(f"Columns: {list(df.columns)}")
    logging.info(f"Dtypes:\n{df.dtypes}")
    logging.info(f"Info:\n{df.info()}")

    required_columns = ["date", "datetime", "amount"]
    missing_cols = [c for c in required_columns if c not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")

    # 2️⃣ Null check
    null_count = df.isnull().sum()
    null_percent = df.isnull().mean() * 100
    logging.info(f"Null count:\n{null_count}")
    logging.info(f"Null percent:\n{null_percent}")

    critical_cols = ["date", "amount"]
    for col in critical_cols:
        if col in df.columns and null_count[col] > 0:
            errors.append(f"Column '{col}' has NULL values: {null_count[col]}")

    # 3️⃣ Data type consistency
    if "date" in df.columns:
        invalid_date = pd.to_datetime(df["date"], errors="coerce").isna().sum()
        if invalid_date > 0:
            errors.append(f"Invalid date values: {invalid_date}")

        logging.info(
            f"Date value types:\n{df['date'].apply(type).value_counts()}"
        )

    if "datetime" in df.columns:
        invalid_dt = pd.to_datetime(df["datetime"], errors="coerce").isna().sum()
        if invalid_dt > 0:
            errors.append(f"Invalid datetime values: {invalid_dt}")

    # 4️⃣ Duplicate check
    dup_rows = df.duplicated().sum()
    logging.info(f"Duplicate rows: {dup_rows}")
    if dup_rows > 0:
        warnings.append(f"Found {dup_rows} duplicated rows")

    if "order_id" in df.columns:
        dup_order = df.duplicated(subset=["order_id"]).sum()
        if dup_order > 0:
            errors.append(f"Duplicate order_id detected: {dup_order}")

    # 5️⃣ Business rule checks
    if "amount" in df.columns:
        negative_amount = (df["amount"] < 0).sum()
        if negative_amount > 0:
            errors.append(f"Negative amount detected: {negative_amount}")

        logging.info(
            f"Amount distribution:\n{df['amount'].describe()}"
        )
        logging.info(
            f"Amount quantiles:\n{df['amount'].quantile([0.01, 0.99])}"
        )

    if "date" in df.columns:
        future_date = (
            pd.to_datetime(df["date"], errors="coerce") > pd.Timestamp.today()
        ).sum()
        if future_date > 0:
            warnings.append(f"Future date detected: {future_date}")

    # 6️⃣ Kết luận
    if errors:
        for e in errors:
            logging.error(f"VALIDATION ERROR: {e}")
        raise ValueError("Data validation failed. Check logs for details.")

    for w in warnings:
        logging.warning(f"VALIDATION WARNING: {w}")

    logging.info("=== DATA VALIDATION PASSED ===")
# ================= TRANSFORM (OPTIONAL) =================
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # convert datetime -> int YYYYMMDD
    if "date" in df.columns:
       df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # DATETIME -> TIMESTAMP (GIỮ NGUYÊN, KHÔNG ÉP INT)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        
    df = df.where(pd.notnull(df), None)
    return df

## ================= LOAD PRO =================
#def load_execute_values(df: pd.DataFrame):
#    conn = None
#    try:
#        conn = psycopg2.connect(**DB_CONFIG)
#        cur = conn.cursor()
#
#        cols = list(df.columns)
#        columns = ", ".join(cols)
#
#        insert_sql = f"""
#            INSERT INTO {SCHEMA}.{TABLE} ({columns})
#            VALUES %s
#        """
#
#        values = [tuple(row) for row in df.to_numpy()]
#
#        start = datetime.now()
#        execute_values(
#            cur,
#            insert_sql,
#            values,
#            page_size=10_000   # chỉnh 5k–20k tùy RAM
#        )
#        conn.commit()
#
#        duration = datetime.now() - start
#        logging.info(f"Load PRO thành công {len(df)} rows trong {duration}")
#
#    except Exception as e:
#        if conn:
#            conn.rollback()
#        logging.error(f"Load PRO lỗi: {e}")
#        raise
#    finally:
#        if conn:
#            cur.close()
#            conn.close()

# ================= MAIN =================
if __name__ == "__main__":
    start_time = datetime.now()
    logging.info("=== ETL PRO START ===")

    FILE_PATH = "~/Project_DataEngineer/Project_Basic_ETL/Coffe_sales.xlsx"

    try:
        df_raw = extract(FILE_PATH)
        validate_data(df_raw)
        #df_clean = transform(df_raw)
        #load_execute_values(df_clean)
    except Exception as e:
        logging.error(f"ETL PRO FAILED: {e}")
    finally:
        logging.info(f"Total runtime: {datetime.now() - start_time}")
        logging.info("=== ETL PRO END ===")