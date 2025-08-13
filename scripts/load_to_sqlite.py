import sqlite3
import pandas as pd
from pathlib import Path

# Paths
db_path = Path(__file__).parent.parent / "data" / "horoscope.db"
csv_dir = Path(__file__).parent.parent / "csv"

# Connect to DB
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create tables if not exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS daily_forecast (
    sign TEXT,
    date TEXT,
    category TEXT,
    forecast TEXT,
    stars INTEGER
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS weekly_forecast (
    sign TEXT,
    week_start TEXT,
    category TEXT,
    forecast TEXT,
    stars INTEGER
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS monthly_forecast (
    sign TEXT,
    month TEXT,
    category TEXT,
    forecast TEXT,
    stars INTEGER
)
""")

# Load CSVs into tables
for csv_file in csv_dir.glob("daily_*.csv"):
    df = pd.read_csv(csv_file)
    df.to_sql("daily_forecast", conn, if_exists="append", index=False)

for csv_file in csv_dir.glob("weekly_*.csv"):
    df = pd.read_csv(csv_file)
    df.to_sql("weekly_forecast", conn, if_exists="append", index=False)

for csv_file in csv_dir.glob("monthly_*.csv"):
    df = pd.read_csv(csv_file)
    df.to_sql("monthly_forecast", conn, if_exists="append", index=False)

conn.commit()
conn.close()

print("✅ All CSVs loaded into SQLite database!")
