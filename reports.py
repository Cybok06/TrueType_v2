from flask import Blueprint, render_template, request, session, redirect, url_for, flash
from bson import ObjectId
from datetime import datetime
from calendar import monthrange
from db import db

reports_bp = Blueprint("reports", __name__, template_folder="templates")

orders_collection   = db["orders"]
clients_collection  = db["clients"]
payments_collection = db["payments"]
settings_collection = db["settings"]

# ---------- helpers ----------

def _fmt_money(v):
    try:
        return f"{float(v):,.2f}"
    except Exception:
        return "0.00"

def _fmt_int(v):
    try:
        return f"{int(round(float(v) or 0)):,.0f}"
    except Exception:
        return "0"

def _parse_month(month_str):
    """'YYYY-MM' -> (start_dt, end_dt inclusive end-of-day)."""
    if not month_str:
        return None, None
    try:
        y, m = [int(x) for x in month_str.split("-")]
        start = datetime(y, m, 1)
        last_day = monthrange(y, m)[1]
        end = datetime(y, m, last_day, 23, 59, 59, 999999)
        return start, end
    except Exception:
        return None, None

def _period_label(start_dt, end_dt):
    if not start_dt or not end_dt:
        return "—"
    return start_dt.strftime("%B %Y").upper()

def _date_key(dt):
    # Sort helper; dt can be None or non-datetime
    return dt or datetime.min

def _prioritize_products(names):
    """Ensure PMS first, AGO second, keep others after (alphabetically)."""
    names = list(dict.fromkeys(names))  # de-dupe, keep order
    out = []
    for pick in ("PMS", "AGO"):
        for n in list(names):
            if (n or "").strip().upper() == pick:
                out.append(n)
                names.remove(n)
                break
    out.extend(sorted(names, key=lambda x: (x or "").upper()))
    return out or ["PMS", "AGO"]

def _money_or_none(x):
    try:
        return float(x)
    except Exception:
        return None

def _describe_order(o):
    """'BRV / Region / DriverName DriverPhone' fallback to order_id or 'Delivery'."""
    if not o:
        return "Delivery"
    parts = []
    if o.get("vehicle_number"): parts.append(str(o.get("vehicle_number")))
    if o.get("region"): parts.append(str(o.get("region")))
    driver_bits = []
    if o.get("driver_name"): driver_bits.append(str(o.get("driver_name")))
    if o.get("driver_phone"): driver_bits.append(str(o.get("driver_phone")))
    if driver_bits:
        parts.append(" ".join(driver_bits))
    return " / ".join(parts) if parts else (o.get("order_id") or "Delivery")

# ---------- core data builder ----------

def build_statement(client_id=None, month_str=None):
    """
    Returns:
      dict(
        ok: bool,
        meta: {customer_name, period_label},
        products: [product_name1, product_name2, ...],
        rows: [ {date, is_opening, is_payment, description, product, vol, price, amount, payment, balance} ... ],
        totals: {
          per_product: {prod: {vol, amount}},
          opening: float,
          deliveries_total: float,
          payments_total: float,
          grand_total_col: float,  # opening + deliveries
          closing_balance: float
        }
      )
    """
    start_dt, end_dt = _parse_month(month_str)
    has_period = bool(start_dt and end_dt)

    # ---- client meta ----
    customer_name = "—"
    _client_oid = None
    if client_id:
        try:
            _client_oid = ObjectId(client_id)
            c = clients_collection.find_one({"_id": _client_oid}, {"name": 1})
            if c:
                customer_name = c.get("name") or str(c["_id"])
        except Exception:
            _client_oid = None

    # ---- orders in period (approved) ----
    order_q = {"status": "approved"}
    if _client_oid:
        order_q["client_id"] = _client_oid
    if has_period:
        order_q["date"] = {"$gte": start_dt, "$lte": end_dt}

    period_orders = list(orders_collection.find(order_q).sort("date", 1))

    # Map orders by id (both ObjectId & str keys) for payment alignment
    orders_by_id = {}
    for o in period_orders:
        orders_by_id[o["_id"]] = o
        orders_by_id[str(o["_id"])] = o

    # product groups for columns
    product_names = [(o.get("product") or "").strip() for o in period_orders if (o.get("product") or "").strip()]
    product_groups = _prioritize_products(product_names)

    # ---- opening balance (client-only) ----
    opening_balance = 0.0
    if _client_oid and has_period:
        # orders before start
        opening_orders_sum = 0.0
        cur = orders_collection.aggregate([
            {"$match": {"status": "approved", "client_id": _client_oid, "date": {"$lt": start_dt}}},
            {"$group": {"_id": None, "sum": {"$sum": {"$ifNull": ["$total_debt", 0]}}}}
        ])
        d = list(cur)
        if d:
            opening_orders_sum = float(d[0]["sum"])

        # payments before start — prefer client_id, else by order_id (ObjectId or string)
        opening_payments_sum = 0.0
        curp = payments_collection.aggregate([
            {"$match": {"status": "confirmed", "client_id": _client_oid, "date": {"$lt": start_dt}}},
            {"$group": {"_id": None, "sum": {"$sum": {"$ifNull": ["$amount", 0]}}}}
        ])
        dp = list(curp)
        if dp and dp[0].get("sum") is not None:
            opening_payments_sum = float(dp[0]["sum"])
        else:
            prior_orders = list(orders_collection.find(
                {"status": "approved", "client_id": _client_oid, "date": {"$lt": start_dt}},
                {"_id": 1}
            ))
            prior_ids_obj = [o["_id"] for o in prior_orders]
            prior_ids_str = [str(o["_id"]) for o in prior_orders]
            if prior_ids_obj or prior_ids_str:
                agg = payments_collection.aggregate([
                    {"$match": {
                        "status": "confirmed",
                        "date": {"$lt": start_dt},
                        "$or": [
                            {"order_id": {"$in": prior_ids_obj}},
                            {"order_id": {"$in": prior_ids_str}},
                        ]
                    }},
                    {"$group": {"_id": None, "sum": {"$sum": {"$ifNull": ["$amount", 0]}}}}
                ])
                dd = list(agg)
                if dd:
                    opening_payments_sum = float(dd[0]["sum"])

        opening_balance = round(opening_orders_sum - opening_payments_sum, 2)

    # ---- payments in period (aligned by order_id) ----
    payments = []
    if has_period:
        if _client_oid:
            # For a specific client: include ALL their confirmed payments in the month,
            # even if the linked order was from a different month.
            pay_q = {
                "status": "confirmed",
                "client_id": _client_oid,
                "date": {"$gte": start_dt, "$lte": end_dt}
            }
            payments = list(payments_collection.find(pay_q).sort("date", 1))

            # Bring in any orders referenced by these payments (not already in period_orders)
            missing_oids = []
            for p in payments:
                oid = p.get("order_id")
                if oid is None:
                    continue
                if (oid not in orders_by_id) and (str(oid) not in orders_by_id):
                    if isinstance(oid, ObjectId):
                        missing_oids.append(oid)
                    else:
                        try:
                            missing_oids.append(ObjectId(str(oid)))
                        except Exception:
                            pass
            if missing_oids:
                extra_orders = list(orders_collection.find({"_id": {"$in": missing_oids}}))
                for eo in extra_orders:
                    orders_by_id[eo["_id"]] = eo
                    orders_by_id[str(eo["_id"])] = eo
        else:
            # No client selected: show only payments related to orders in the period (all clients).
            if period_orders:
                order_ids_obj = [o["_id"] for o in period_orders]
                order_ids_str = [str(o["_id"]) for o in period_orders]
                pay_q = {
                    "status": "confirmed",
                    "date": {"$gte": start_dt, "$lte": end_dt},
                    "$or": [
                        {"order_id": {"$in": order_ids_obj}},
                        {"order_id": {"$in": order_ids_str}},
                    ]
                }
                payments = list(payments_collection.find(pay_q).sort("date", 1))
            else:
                payments = []

    # ---- build event rows ----
    events = []

    # Opening balance row (client statements only)
    if _client_oid and has_period:
        events.append({
            "date": start_dt,
            "is_opening": True,
            "is_payment": False,
            "description": "Balance b/f",
            "product": None,
            "vol": None, "price": None, "amount": opening_balance,
            "payment": None,
            "balance": None
        })

    # Delivery/order rows (approved orders within the month)
    for o in period_orders:
        dt = o.get("date")
        prod = (o.get("product") or "").strip() or "—"
        qty = _money_or_none(o.get("quantity")) or 0.0
        total_debt = _money_or_none(o.get("total_debt")) or 0.0
        unit_price = round(total_debt / qty, 2) if qty else 0.0

        desc = _describe_order(o)

        events.append({
            "date": dt,
            "is_opening": False,
            "is_payment": False,
            "description": desc,
            "product": prod,
            "vol": qty,
            "price": unit_price,
            "amount": total_debt,
            "payment": None,
            "balance": None
        })

    # Payment rows (each aligned to its order via order_id)
    for p in payments:
        dt = p.get("date")
        label = (
            p.get("note") or p.get("details") or p.get("method") or p.get("type")
            or (p.get("bank_name") or "").strip() or "Payment"
        )
        if p.get("account_last4"):
            label = f"{label} {p['account_last4']}"

        linked_order = orders_by_id.get(p.get("order_id")) or orders_by_id.get(str(p.get("order_id")))
        suffix = f" / {_describe_order(linked_order)}" if linked_order else ""
        amount = _money_or_none(p.get("amount")) or 0.0

        events.append({
            "date": dt,
            "is_opening": False,
            "is_payment": True,
            "description": f"Payment – {label}{suffix}",
            "product": None,
            "vol": None, "price": None, "amount": None,
            "payment": amount,
            "balance": None
        })

    # ---- sort then compute running balances ----
    events.sort(key=lambda r: (_date_key(r["date"]), 0 if r["is_payment"] else 1))
    running = opening_balance
    deliveries_total = 0.0
    payments_total = 0.0

    for ev in events:
        if ev["is_opening"]:
            ev["balance"] = running
            continue
        if ev["is_payment"]:
            amt = ev["payment"] or 0.0
            payments_total += amt
            running = round(running - amt, 2)
            ev["balance"] = running
        else:
            amt = ev["amount"] or 0.0
            deliveries_total += amt
            running = round(running + amt, 2)
            ev["balance"] = running

    # ---- totals per product ----
    per_product = {name: {"vol": 0.0, "amount": 0.0} for name in product_groups}
    for ev in events:
        if (not ev["is_payment"]) and (not ev["is_opening"]) and ev["product"] in per_product:
            per_product[ev["product"]]["vol"] += float(ev["vol"] or 0)
            per_product[ev["product"]]["amount"] += float(ev["amount"] or 0)

    totals = {
        "per_product": {k: {"vol": round(v["vol"], 2), "amount": round(v["amount"], 2)} for k, v in per_product.items()},
        "opening": round(opening_balance, 2),
        "deliveries_total": round(deliveries_total, 2),
        "payments_total": round(payments_total, 2),
        "grand_total_col": round(opening_balance + deliveries_total, 2),
        "closing_balance": round(running, 2),
    }

    return {
        "ok": True,
        "meta": {
            "customer_name": customer_name if _client_oid else "— (All Clients)" if has_period else "—",
            "period_label": _period_label(start_dt, end_dt) if has_period else "—",
        },
        "products": product_groups,
        "rows": events,
        "totals": totals
    }

# ---------- routes ----------

@reports_bp.app_template_filter("fmt_money")
def jinja_fmt_money(v):
    return _fmt_money(v)

@reports_bp.app_template_filter("fmt_int")
def jinja_fmt_int(v):
    return _fmt_int(v)

@reports_bp.route("/reports", methods=["GET"])
def reports():
    # Access control like your approved_orders page
    if "role" not in session or session["role"] not in ["admin", "assistant"]:
        flash("Access denied.", "danger")
        return redirect(url_for("login.login"))

    settings_doc = settings_collection.find_one() or {}
    month = (request.args.get("month") or "").strip()      # YYYY-MM
    client_id = (request.args.get("client_id") or "").strip()

    # Clients for filter
    clients = list(clients_collection.find({}, {"name": 1}).sort("name", 1))
    for c in clients:
        c["_id"] = str(c["_id"])
        c["name"] = c.get("name") or c["_id"]

    result = None
    if month or client_id:
        result = build_statement(client_id=client_id or None, month_str=month or None)

    return render_template(
        "reports.html",
        clients=clients,
        month=month,
        client_id=client_id,
        result=result,
        reports_enabled=settings_doc.get("approve_orders", True)
    )
