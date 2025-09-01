from flask import Blueprint, render_template, request, jsonify, abort
from db import db
from bson import ObjectId
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import re, calendar

debtors_bp = Blueprint("debtors", __name__)

clients_col  = db["clients"]
orders_col   = db["orders"]
payments_col = db["payments"]

# ---------------- helpers ----------------

MONTHS = [{"value": i, "label": calendar.month_name[i]} for i in range(1, 13)]

def _as_dt(val):
    """Accepts datetime, epoch seconds/ms, or string; returns datetime or None."""
    if isinstance(val, datetime):
        return val
    if isinstance(val, (int, float)):
        # Heuristic: treat large values as milliseconds
        if float(val) > 10**11:
            try:
                return datetime.utcfromtimestamp(float(val) / 1000.0)
            except Exception:
                return None
        try:
            return datetime.utcfromtimestamp(float(val))
        except Exception:
            return None
    if isinstance(val, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%b-%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(val, fmt)
            except Exception:
                pass
    return None

def _fmt_date(val, default="-"):
    dt = _as_dt(val)
    return dt.strftime("%Y-%m-%d") if dt else default

def _debt_age(from_date):
    dt = _as_dt(from_date)
    if not dt:
        return "Unknown"
    diff = relativedelta(datetime.utcnow(), dt)
    if diff.years  > 0: return f"{diff.years} year(s)"
    if diff.months > 0: return f"{diff.months} month(s)"
    if diff.days   >= 7: return f"{diff.days // 7} week(s)"
    return f"{diff.days} day(s)"

def _month_to_int(m):
    """Accept 1..12 or names like jan/january; return int or None."""
    if not m: return None
    m = str(m).strip()
    if m.isdigit():
        v = int(m)
        return v if 1 <= v <= 12 else None
    m = m.lower()
    for i in range(1, 13):
        if m in (calendar.month_name[i].lower(), calendar.month_abbr[i].lower()):
            return i
    return None

def _resolve_window(args):
    """
    Precedence:
      1) from/to (custom range)
      2) month/year
      3) range=week|month|year
      4) default: no bounds
    Returns (start_dt, end_dt, selected_month, selected_year)
    """
    # 1) custom range
    f = args.get("from")
    t = args.get("to")
    if f and t:
        sd, ed = _as_dt(f), _as_dt(t)
        if sd and ed:
            ed = ed.replace(hour=23, minute=59, second=59, microsecond=999999)
            return sd, ed, None, None

    # 2) month/year
    m = _month_to_int(args.get("month"))
    y = args.get("year")
    y = int(y) if (y and str(y).isdigit()) else None
    now = datetime.utcnow()
    if m and not y: y = now.year
    if y and not m: m = now.month  # if a year is chosen alone, assume current month
    if m and y:
        sd = datetime(y, m, 1)
        ed = sd + relativedelta(months=1) - timedelta(microseconds=1)
        return sd, ed, m, y

    # 3) quick range
    rng = (args.get("range") or "").lower().strip()
    if rng == "week":
        return now - timedelta(days=7), now, None, None
    if rng == "month":
        sd = datetime(now.year, now.month, 1)
        ed = sd + relativedelta(months=1) - timedelta(microseconds=1)
        return sd, ed, now.month, now.year
    if rng == "year":
        sd = datetime(now.year, 1, 1)
        ed = datetime(now.year, 12, 31, 23, 59, 59, 999999)
        return sd, ed, None, now.year

    # 4) no bounds
    return None, None, None, None

def _find_client(token: str | None):
    """Accept _id hex, client_id, or name (icontains)."""
    if not token: return None
    token = token.strip()

    # _id
    if re.fullmatch(r"[0-9a-fA-F]{24}", token):
        try:
            doc = clients_col.find_one({"_id": ObjectId(token)})
            if doc: return doc
        except Exception:
            pass

    # client_id
    doc = clients_col.find_one({"client_id": token})
    if doc: return doc

    # name icontains
    return clients_col.find_one({"name": {"$regex": re.escape(token), "$options": "i"}})

def _years_range():
    """Build year list from earliest to latest order date; fallback to current year."""
    first = orders_col.find({}, {"date": 1}).sort("date", 1).limit(1)
    last  = orders_col.find({}, {"date": 1}).sort("date", -1).limit(1)
    first_dt = None
    last_dt  = None
    for d in first: first_dt = d.get("date")
    for d in last:  last_dt  = d.get("date")
    now = datetime.utcnow()
    if not first_dt or not last_dt:
        return [now.year]
    y0, y1 = _as_dt(first_dt).year, _as_dt(last_dt).year
    if y0 > y1: y0, y1 = y1, y0
    return list(range(y0, y1 + 1))

# ---------------- list (aggregation) ----------------

@debtors_bp.route("/debtors/list")
def view_debtors_table():
    """
    Aggregated debtors by client for the selected window **based on order 'date'**.
    Filters:
      - ?month=1..12 & ?year=YYYY  (primary UI)
      - OR ?from=YYYY-MM-DD&to=YYYY-MM-DD (custom)
      - OR ?range=week|month|year
      - Optional: ?client=<id|client_id|name>
    Only rows with amount_left > 0 (i.e., orders in window not fully paid).
    """
    start_dt, end_dt, sel_month, sel_year = _resolve_window(request.args)

    client_token = request.args.get("client")
    client = _find_client(client_token) if client_token else None
    client_oid = client["_id"] if client else None

    # Orders match (window + status + optional client)
    order_match = {"status": "approved"}
    if start_dt and end_dt:
        order_match["date"] = {"$gte": start_dt, "$lte": end_dt}
    if client_oid:
        order_match["client_id"] = client_oid

    pipeline = [
        {"$match": order_match},

        # Group orders by client
        {"$group": {
            "_id": "$client_id",
            "order_oids": {"$addToSet": "$_id"},           # Mongo ObjectIds of orders
            "order_refs": {"$addToSet": "$order_id"},      # Short string order IDs (e.g., '97OZD') if present
            "total_debt": {"$sum": {"$toDouble": "$total_debt"}},
            "latest_due_date": {"$max": "$due_date"},
            "oldest_order_date": {"$min": "$date"}
        }},

        # Payments for those orders (support both styles):
        #   payments.order_id == order _id   (ObjectId)
        #   payments.order_id == order_id    (short string like '97OZD')
        {"$lookup": {
            "from": "payments",
            "let": {"orderOids": "$order_oids", "orderRefs": "$order_refs", "cid": "$_id"},
            "pipeline": [
                {"$match": {"$expr": {
                    "$and": [
                        {"$eq": ["$client_id", "$$cid"]},
                        {"$eq": ["$status", "confirmed"]},
                        {"$or": [
                            {"$in": ["$order_id", "$$orderOids"]},  # payments.order_id holds ObjectId
                            {"$in": ["$order_id", "$$orderRefs"]},  # payments.order_id holds string code
                            {"$in": ["$order_ref", "$$orderRefs"]}, # payments.order_ref holds string code
                            {"$in": ["$order_ref", "$$orderOids"]}, # very defensive
                        ]}
                    ]
                }}},
                {"$group": {"_id": None, "paid": {"$sum": {"$toDouble": "$amount"}}}}
            ],
            "as": "pay"
        }},
        {"$addFields": {"total_paid": {"$ifNull": [{"$arrayElemAt": ["$pay.paid", 0]}, 0]}}},
        {"$addFields": {"amount_left": {"$max": [{"$subtract": ["$total_debt", "$total_paid"]}, 0]}}},

        # Only debtors (orders in window not fully paid)
        {"$match": {"amount_left": {"$gt": 0}}},

        # Join client meta
        {"$lookup": {
            "from": "clients",
            "localField": "_id",
            "foreignField": "_id",
            "as": "client"
        }},
        {"$unwind": "$client"},

        {"$project": {
            "_id": 0,
            "client_oid": {"$toString": "$client._id"},
            "client_id": {"$ifNull": ["$client.client_id", {"$toString": "$client._id"}]},
            "name": {"$ifNull": ["$client.name", "Unnamed"]},
            "total_debt": {"$round": ["$total_debt", 2]},
            "total_paid": {"$round": ["$total_paid", 2]},
            "amount_left": {"$round": ["$amount_left", 2]},
            "due_date": "$latest_due_date",
            "debt_age_from": "$oldest_order_date",
            "tag": {"$ifNull": ["$client.tag.label", ""]},
            "tag_color": {"$ifNull": ["$client.tag.color", "#f8f9fa"]}
        }},
        {"$sort": {"amount_left": -1}}
    ]

    rows = list(orders_col.aggregate(pipeline))

    # finalize fields for template
    for r in rows:
        r["debt_age"] = _debt_age(r.pop("debt_age_from", None))
        r["due_date"] = _fmt_date(r.get("due_date"), "-")

    years = _years_range()

    # Build a user-friendly period label
    if start_dt and end_dt and start_dt.month == end_dt.month and start_dt.year == end_dt.year:
        period = start_dt.strftime("%B %Y")
    elif start_dt and end_dt:
        period = f"{_fmt_date(start_dt)} to {_fmt_date(end_dt)}"
    else:
        period = "All time"

    return render_template(
        "partials/debtors_list.html",
        clients=rows,
        months=MONTHS,
        years=years,
        selected_month=sel_month,
        selected_year=sel_year,
        from_date=_fmt_date(start_dt, "") if start_dt else "",
        to_date=_fmt_date(end_dt, "") if end_dt else "",
        period_label=period,
        customer=(client_token or "")
    )

# ---------------- detail (latest order) ----------------

@debtors_bp.route("/debtors")
def view_debtors():
    """
    Detailed per-client (latest approved order) with filters + customer selector.
    Uses the same month/year/from-to logic and filters by the order's 'date'.
    """
    start_dt, end_dt, sel_month, sel_year = _resolve_window(request.args)

    client_token = request.args.get("client")
    if client_token:
        c = _find_client(client_token)
        clients = [c] if c else []
    else:
        clients = list(clients_col.find({"status": "active"}))

    client_data = []
    for client in clients:
        if not client: 
            continue
        cid = client["_id"]

        q = {"client_id": cid, "status": "approved"}
        if start_dt and end_dt:
            q["date"] = {"$gte": start_dt, "$lte": end_dt}

        approved = list(orders_col.find(q).sort("date", -1))
        if not approved:
            continue

        latest = approved[0]
        order_oid  = latest["_id"]
        total_debt = float(latest.get("total_debt", 0) or 0)
        order_type = (latest.get("order_type") or "-").upper()

        # payments linked to this order (support both id styles)
        pays = list(payments_col.find({
            "client_id": cid,
            "status": "confirmed",
            "$or": [
                {"order_id": order_oid},
                {"order_id": latest.get("order_id")},
                {"order_ref": latest.get("order_id")},
                {"order_ref": order_oid},
            ]
        }).sort("date", 1))

        total_paid = 0.0
        payment_data = []
        for p in pays:
            amt = float(p.get("amount", 0) or 0)
            total_paid += amt
            payment_data.append({
                "date": _fmt_date(p.get("date"), "Unknown"),
                "amount": round(amt, 2),
                "bank": p.get("bank_name") or "",
                "note": p.get("note") or ""
            })

        client_data.append({
            "name": client.get("name", "Unnamed"),
            "client_id": str(client.get("client_id", client["_id"])),
            "latest_order": {
                "mongo_id": str(order_oid),
                "order_id": latest.get("order_id") or "",
                "date": _fmt_date(latest.get("date")),
                "due_date": _fmt_date(latest.get("due_date")),
                "order_type": order_type,
                "total_debt": round(total_debt, 2),
                "total_paid": round(total_paid, 2),
                "amount_left": round(max(total_debt - total_paid, 0), 2),
            },
            "payments": payment_data
        })

    years = _years_range()

    # Period label
    if start_dt and end_dt and start_dt.month == end_dt.month and start_dt.year == end_dt.year:
        period = start_dt.strftime("%B %Y")
    elif start_dt and end_dt:
        period = f"{_fmt_date(start_dt)} to {_fmt_date(end_dt)}"
    else:
        period = "All time"

    return render_template(
        "partials/debtors.html",
        client_data=client_data,
        months=MONTHS,
        years=years,
        selected_month=sel_month,
        selected_year=sel_year,
        from_date=_fmt_date(start_dt, "") if start_dt else "",
        to_date=_fmt_date(end_dt, "") if end_dt else "",
        period_label=period,
        customer=(client_token or "")
    )

# ---------------- Statement: customer ledger + PDF page ----------------



# ---------------- JSON (optional for AJAX / autocomplete) ----------------

@debtors_bp.route("/debtors/clients.json")
def clients_lookup():
    term = (request.args.get("q") or "").strip()
    q = {}
    if term:
        q = {"$or": [
            {"name": {"$regex": term, "$options": "i"}},
            {"client_id": {"$regex": term, "$options": "i"}}
        ]}
    docs = list(clients_col.find(q, {"name":1, "client_id":1}).limit(20))
    return jsonify([{
        "id": str(d["_id"]),
        "client_id": d.get("client_id", str(d["_id"])),
        "name": d.get("name", "Unnamed")
    } for d in docs])

@debtors_bp.route("/debtors/tag", methods=["POST"])
def update_tag():
    client_id = request.form.get("client_id")
    tag_label = (request.form.get("tag") or "").strip()
    tag_color = (request.form.get("tag_color") or "#f8f9fa").strip()
    if not client_id:
        return jsonify({"success": False, "error": "client_id required"}), 400
    clients_col.update_one(
        {"client_id": client_id},
        {"$set": {"tag": {"label": tag_label, "color": tag_color, "updated_at": datetime.utcnow()}}}
    )
    return jsonify({"success": True})
