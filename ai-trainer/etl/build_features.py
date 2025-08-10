import os, pandas as pd
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL")
N = int(os.getenv("N_DAYS_BEFORE_CANCEL", "14"))
L = int(os.getenv("LOOKBACK_DAYS", "365"))
sql = (
    open(
        os.path.join(os.path.dirname(__file__), "churn_label_feature.sql"),
        encoding="utf-8",
    )
    .read()
    .replace("14::int", f"{N}::int")
    .replace("365::int", f"{L}::int")
)
df = pd.read_sql(text(sql), create_engine(DB_URL))
os.makedirs("/data/features/churn", exist_ok=True)
df.to_parquet("/data/features/churn/dt=current.parquet", index=False)
print("ok")
