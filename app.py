# app.py
import os, glob, time
from datetime import date, datetime
from functools import lru_cache
from typing import Optional, Tuple
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DATA_DIR = os.environ.get("DATA_DIR", "./data")

# ----------------------------
# File naming conventions
# ----------------------------
# Daily:   <sign>_<YYYY-MM>_daily*.csv         (rows: date, sign, category, forecast, stars?)
# Weekly:  <sign>_<YYYY-MM>_weekly*.csv        (rows: week_start, week_end, sign, category, forecast, stars?)
# Monthly: <sign>_<YYYY-MM>_monthly*.csv       (rows: month, sign, category, forecast, stars?)
#
# Examples:
#   aries_2025-08_daily_with_stars_v2.csv
#   aries_2025-08_weekly_with_stars_v2.csv
#   aries_2025-08_monthly_with_stars_v2.csv
#
# You can keep any suffix after "daily/weekly/monthly" (e.g., _with_stars_v2.csv).

# ----------------------------
# Helpers
# ----------------------------
def norm_sign(s: str) -> str:
    return (s or "").strip().lower()

def norm_cat(s: str) -> str:
    return (s or "").strip().lower()

def yyyymm_from_date(d: str) -> Optional[str]:
    try:
        return datetime.fromisoformat(d).strftime("%Y-%m")
    except Exception:
        return None

def to_json_rows(df, fields):
    if df is None or df.empty:
        return []
    keep = [c for c in fields if c in df.columns]
    return df[keep].to_dict(orient="records")

def newest_file_mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except FileNotFoundError:
        return 0.0

# ----------------------------
# CSV Discovery
# ----------------------------
def discover_file(period: str, sign: str, yyyymm_hint: Optional[str]) -> Optional[str]:
    """
    Find a CSV that matches period+sign, prioritizing the provided YYYY-MM.
    If yyyymm_hint is None (e.g., daily without ?date), we pick the newest file for that period/sign.
    """
    pattern = os.path.join(DATA_DIR, f"{sign}_*_ {period}*.csv")  # note: we'll fix the space below
    pattern = pattern.replace("_ ", "_")  # safety for string formatting
    files = glob.glob(pattern)

    if not files:
        return None

    # If a specific month requested, choose that first
    if yyyymm_hint:
        for f in files:
            if f"{sign}_{yyyymm_hint}_" in os.path.basename(f):
                return f

    # Else choose the most recently modified matching file
    files.sort(key=lambda p: newest_file_mtime(p), reverse=True)
    return files[0] if files else None

# ----------------------------
# Caching with automatic invalidation on file mtime
# ----------------------------
_cache = {}  # key: (path, mtime) -> df

def load_csv_cached(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    mtime = os.path.getmtime(path)
    key = (path, mtime)
    if key in _cache:
        return _cache[key]
    # prune old keys for same path
    for k in list(_cache.keys()):
        if isinstance(k, tuple) and k[0] == path and k[1] != mtime:
            del _cache[k]
    df = pd.read_csv(path, encoding="utf-8")
    _cache[key] = df
    return df

# ----------------------------
# Route: DAILY
# ----------------------------
@app.route("/api/v1/forecast/daily")
def api_daily():
    """
    Query:
      sign=aries
      date=YYYY-MM-DD (optional; used to pick the right monthly file; if absent we pick the newest)
      category=... (optional)
    """
    sign = norm_sign(request.args.get("sign", "aries"))
    qdate = request.args.get("date", "")
    yyyymm = yyyymm_from_date(qdate) if qdate else None

    path = discover_file("daily", sign, yyyymm)
    df = load_csv_cached(path)

    if not df.empty and "sign" in df.columns:
        df = df[df["sign"].str.lower() == sign]

    # Choose date rows
    if qdate and "date" in df.columns:
        sel = df[df["date"] == qdate]
        # if date not present, fallback to closest available (first available)
        if sel.empty:
            try:
                first = sorted(df["date"].unique())[0]
                sel = df[df["date"] == first]
            except Exception:
                sel = df
        df = sel
    else:
        # default to today or first available
        if "date" in df.columns:
            today = date.today().isoformat()
            sel = df[df["date"] == today]
            if sel.empty:
                # pick the latest date in this file
                try:
                    last = sorted(df["date"].unique())[-1]
                    sel = df[df["date"] == last]
                except Exception:
                    sel = df
            df = sel

    category = request.args.get("category")
    if category and "category" in df.columns:
        df = df[df["category"].str.lower() == norm_cat(category)]

    return jsonify({
        "period": "daily",
        "sign": sign,
        "date": (df["date"].iloc[0] if not df.empty and "date" in df.columns else qdate or ""),
        "items": to_json_rows(df, ["category", "forecast", "stars"])
    })

# ----------------------------
# Route: WEEKLY
# ----------------------------
@app.route("/api/v1/forecast/weekly")
def api_weekly():
    """
    Query:
      sign=aries
      week_start=YYYY-MM-DD (optional)
      week_end=YYYY-MM-DD   (optional)
      month=YYYY-MM         (optional; helps file discovery)
      category=...          (optional)
    """
    sign = norm_sign(request.args.get("sign", "aries"))
    ws   = request.args.get("week_start", "")
    we   = request.args.get("week_end", "")
    month_hint = request.args.get("month", None)

    # pick file by month hint (or by ws/we month if present)
    yyyymm = month_hint or (yyyymm_from_date(ws) if ws else None) or (yyyymm_from_date(we) if we else None)
    path = discover_file("weekly", sign, yyyymm)
    df = load_csv_cached(path)

    if not df.empty and "sign" in df.columns:
        df = df[df["sign"].str.lower() == sign]

    if ws and we and {"week_start","week_end"} <= set(df.columns):
        df = df[(df["week_start"] == ws) & (df["week_end"] == we)]
    else:
        # pick the earliest week chronologically
        if {"week_start","week_end"} <= set(df.columns) and not df.empty:
            ordered = df.sort_values(["week_start","week_end"])
            ws = ordered["week_start"].iloc[0]
            we = ordered["week_end"].iloc[0]
            df = ordered[(ordered["week_start"] == ws) & (ordered["week_end"] == we)]

    category = request.args.get("category")
    if category and "category" in df.columns:
        df = df[df["category"].str.lower() == norm_cat(category)]

    return jsonify({
        "period": "weekly",
        "sign": sign,
        "week_start": ws,
        "week_end": we,
        "items": to_json_rows(df, ["category", "forecast", "stars"])
    })

# ----------------------------
# Route: MONTHLY
# ----------------------------
@app.route("/api/v1/forecast/monthly")
def api_monthly():
    """
    Query:
      sign=aries
      month=YYYY-MM (optional; if absent, newest file for that sign)
      category=...  (optional)
    """
    sign = norm_sign(request.args.get("sign", "aries"))
    month = request.args.get("month", None)

    path = discover_file("monthly", sign, month)
    df = load_csv_cached(path)

    if not df.empty and "sign" in df.columns:
        df = df[df["sign"].str.lower() == sign]
    if month and "month" in df.columns:
        df = df[df["month"] == month]
    elif "month" in df.columns and not df.empty:
        # fallback to newest month in the file
        try:
            # assume all rows in this file are the same month, just pick first
            month = df["month"].iloc[0]
        except Exception:
            month = ""

    category = request.args.get("category")
    if category and "category" in df.columns:
        df = df[df["category"].str.lower() == norm_cat(category)]

    return jsonify({
        "period": "monthly",
        "sign": sign,
        "month": month or "",
        "items": to_json_rows(df, ["category", "forecast", "stars"])
    })

@app.route("/health")
def health():
    return jsonify({"ok": True, "data_dir": DATA_DIR})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
