# app.py (SQLite version)
import os, sqlite3
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DB_PATH = os.environ.get("DB_PATH", "data/horoscope.db")

def q(sql, params=()):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute(sql, params).fetchall()
    con.close()
    return [dict(r) for r in rows]

def one(sql, params=()):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    row = con.execute(sql, params).fetchone()
    con.close()
    return dict(row) if row else None

def norm(s): return (s or "").strip().lower()

@app.route("/api/v1/forecast/daily")
def daily():
    """
    ?sign=aries
    ?date=YYYY-MM-DD (optional; if omitted, uses latest available date for that sign)
    ?category=...
    """
    sign = norm(request.args.get("sign", "aries"))
    qdate = request.args.get("date", "")

    if not qdate:
        row = one("SELECT MAX(date) AS d FROM daily WHERE sign=?", (sign,))
        qdate = (row["d"] if row and row["d"] else "")

    category = request.args.get("category")
    if category:
        rows = q("""SELECT category, forecast, COALESCE(stars,3) AS stars
                    FROM daily
                    WHERE sign=? AND date=? AND LOWER(category)=?""",
                 (sign, qdate, norm(category)))
    else:
        rows = q("""SELECT category, forecast, COALESCE(stars,3) AS stars
                    FROM daily
                    WHERE sign=? AND date=?
                    ORDER BY category""", (sign, qdate))

    return jsonify({
        "period": "daily",
        "sign": sign,
        "date": qdate,
        "items": rows
    })

@app.route("/api/v1/forecast/weekly")
def weekly():
    """
    ?sign=aries
    ?week_start=YYYY-MM-DD & ?week_end=YYYY-MM-DD (optional)
    ?month=YYYY-MM (optional; helps pick a week)
    ?category=...
    """
    sign = norm(request.args.get("sign", "aries"))
    ws   = request.args.get("week_start", "")
    we   = request.args.get("week_end", "")
    month = request.args.get("month", "")

    if not (ws and we):
        if month:
            row = one("""SELECT week_start, week_end
                         FROM weekly
                         WHERE sign=? AND (substr(week_start,1,7)=? OR substr(week_end,1,7)=?)
                         ORDER BY week_start ASC
                         LIMIT 1""", (sign, month, month))
        else:
            row = one("""SELECT week_start, week_end
                         FROM weekly
                         WHERE sign=?
                         ORDER BY week_start ASC
                         LIMIT 1""", (sign,))
        if row:
            ws, we = row["week_start"], row["week_end"]

    category = request.args.get("category")
    if category:
        rows = q("""SELECT category, forecast, COALESCE(stars,3) AS stars
                    FROM weekly
                    WHERE sign=? AND week_start=? AND week_end=? AND LOWER(category)=?""",
                 (sign, ws, we, norm(category)))
    else:
        rows = q("""SELECT category, forecast, COALESCE(stars,3) AS stars
                    FROM weekly
                    WHERE sign=? AND week_start=? AND week_end=?
                    ORDER BY category""", (sign, ws, we))

    return jsonify({
        "period": "weekly",
        "sign": sign,
        "week_start": ws,
        "week_end": we,
        "items": rows
    })

@app.route("/api/v1/forecast/monthly")
def monthly():
    """
    ?sign=aries
    ?month=YYYY-MM (optional; if omitted, picks latest available month for that sign)
    ?category=...
    """
    sign = norm(request.args.get("sign", "aries"))
    month = request.args.get("month", "")

    if not month:
        row = one("SELECT MAX(month) AS m FROM monthly WHERE sign=?", (sign,))
        month = (row["m"] if row and row["m"] else "")

    category = request.args.get("category")
    if category:
        rows = q("""SELECT category, forecast, COALESCE(stars,3) AS stars
                    FROM monthly
                    WHERE sign=? AND month=? AND LOWER(category)=?""",
                 (sign, month, norm(category)))
    else:
        rows = q("""SELECT category, forecast, COALESCE(stars,3) AS stars
                    FROM monthly
                    WHERE sign=? AND month=?
                    ORDER BY category""", (sign, month))

    return jsonify({
        "period": "monthly",
        "sign": sign,
        "month": month,
        "items": rows
    })

@app.route("/api/v1/availability")
def availability():
    # minimal: months per sign/period from DB
    out = {}
    # daily months
    rows = q("SELECT sign, substr(date,1,7) AS ym FROM daily GROUP BY sign, ym ORDER BY sign, ym")
    for r in rows:
        out.setdefault(r["sign"], {"daily": [], "weekly": [], "monthly": []})
        out[r["sign"]]["daily"].append(r["ym"])
    # weekly months (from either week_start or week_end)
    rows = q("SELECT sign, substr(week_start,1,7) AS ym FROM weekly GROUP BY sign, ym ORDER BY sign, ym")
    for r in rows:
        out.setdefault(r["sign"], {"daily": [], "weekly": [], "monthly": []})
        if r["ym"] not in out[r["sign"]]["weekly"]:
            out[r["sign"]]["weekly"].append(r["ym"])
    rows = q("SELECT sign, substr(week_end,1,7) AS ym FROM weekly GROUP BY sign, ym ORDER BY sign, ym")
    for r in rows:
        out.setdefault(r["sign"], {"daily": [], "weekly": [], "monthly": []})
        if r["ym"] not in out[r["sign"]]["weekly"]:
            out[r["sign"]]["weekly"].append(r["ym"])
    # monthly months
    rows = q("SELECT sign, month FROM monthly GROUP BY sign, month ORDER BY sign, month")
    for r in rows:
        out.setdefault(r["sign"], {"daily": [], "weekly": [], "monthly": []})
        out[r["sign"]]["monthly"].append(r["month"])
    # sort arrays
    for sign in out:
        out[sign]["daily"].sort()
        out[sign]["weekly"].sort()
        out[sign]["monthly"].sort()
    return jsonify(out)

@app.route("/health")
def health():
    d_count = one("SELECT COUNT(*) AS c FROM daily") or {"c": 0}
    return jsonify({"ok": True, "db": DB_PATH, "daily_rows": d_count["c"]})
