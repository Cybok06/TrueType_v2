from flask import Blueprint, render_template, session, redirect, url_for
from bson import ObjectId
from datetime import datetime
from db import clients_collection, orders_collection, payments_collection

client_profile_bp = Blueprint("client_profile", __name__, template_folder="templates")

def _f(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default

def _parse_dt(v):
    """Handle Mongo extended JSON and naive datetimes."""
    if isinstance(v, datetime):
        return v
    if isinstance(v, (int, float)):
        # value already in ms/seconds? Heuristic: treat > 10^12 as ms
        if v > 10**12:  # ms
            return datetime.fromtimestamp(v / 1000.0)
        return datetime.fromtimestamp(v)
    if isinstance(v, dict) and "$date" in v:
        d = v["$date"]
        # could be {"$numberLong":"..."} or int ms
        if isinstance(d, dict) and "$numberLong" in d:
            try:
                return datetime.fromtimestamp(int(d["$numberLong"]) / 1000.0)
            except Exception:
                return None
        try:
            # sometimes "$date" can be millis directly
            return datetime.fromtimestamp(int(d) / 1000.0)
        except Exception:
            # or ISO string
            try:
                return datetime.fromisoformat(d.replace("Z", "+00:00"))
            except Exception:
                return None
    return None

@client_profile_bp.route('/client/<client_id>')
def client_profile(client_id):
    # ✅ Validate ObjectId
    if not ObjectId.is_valid(client_id):
        return "Invalid client ID", 400

    oid = ObjectId(client_id)

    # ✅ Fetch client
    client = clients_collection.find_one({"_id": oid})
    if not client:
        return "Client not found", 404

    # ✅ Fetch all orders for this client (support both ObjectId and string storage)
    orders = list(
        orders_collection.find({"client_id": {"$in": [oid, str(oid)]}})
                         .sort("date", -1)
    )

    # ---- Aggregate confirmed payments for these orders (payments-only) ----
    order_ids_obj = [o["_id"] for o in orders]

    paid_map = {}
    if order_ids_obj:
        pipeline = [
            {
                "$match": {
                    "status": {"$regex": "^confirmed$", "$options": "i"},
                    "client_id": oid,                     # payments use ObjectId client_id
                    "order_id": {"$in": order_ids_obj}    # payments use ObjectId order_id
                }
            },
            {
                # robust if amount sometimes stored as string
                "$addFields": {
                    "amount_num": {
                        "$convert": {"input": "$amount", "to": "double", "onError": 0, "onNull": 0}
                    }
                }
            },
            {
                "$group": {
                    "_id": "$order_id",
                    "total_paid": {"$sum": "$amount_num"}
                }
            }
        ]
        paid_map = {row["_id"]: _f(row.get("total_paid")) for row in payments_collection.aggregate(pipeline)}

    # ✅ Decorate each order (dates, margins, returns, paid/left)
    for o in orders:
        # Dates
        o["date"] = _parse_dt(o.get("date"))
        o["due_date"] = _parse_dt(o.get("due_date"))

        # Ensure numeric base fields
        p     = _f(o.get("p_bdc_omc"), None)
        s     = _f(o.get("s_bdc_omc"), None)
        p_tax = _f(o.get("p_tax"), None)
        s_tax = _f(o.get("s_tax"), None)
        q     = _f(o.get("quantity"), 0.0)

        # Derive per-L margins (don’t write to DB here; just decorate for view)
        margin_price = (s - p) if (s is not None and p is not None) else None
        margin_tax   = (s_tax - p_tax) if (s_tax is not None and p_tax is not None) else None

        # Expose useful fields for template (if you want to show them)
        o["margin_price"] = round(margin_price, 2) if margin_price is not None else None
        o["margin_tax"]   = round(margin_tax, 2) if margin_tax is not None else None

        # Returns total using the new rule:
        # - If both per-L margins exist: (margin_price + margin_tax) * Q
        # - Else if only one exists:     that_margin * Q
        # - Else:                        0
        per_l_sum = 0.0
        if margin_price is not None: per_l_sum += margin_price
        if margin_tax   is not None: per_l_sum += margin_tax
        returns_total = round(per_l_sum * q, 2) if per_l_sum != 0 else 0.0

        # Keep existing returns if already stored, else use derived
        if o.get("returns_total") is None and o.get("returns") is None:
            o["returns_total"] = returns_total
            o["returns"] = returns_total
        else:
            # prefer explicit returns_total if present; otherwise fall back to returns
            o["returns_total"] = _f(o.get("returns_total"), _f(o.get("returns"), returns_total))

        # Debt (display only, do not change db here)
        o["total_debt"] = _f(o.get("total_debt"))

        # Payments ONLY from payments collection
        paid_external = _f(paid_map.get(o["_id"]))  # defaults to 0.0 when missing
        o["amount_paid"] = round(paid_external, 2)
        o["amount_left"] = round(o["total_debt"] - o["amount_paid"], 2)

    # ✅ Latest approved order (if any) and summary box values
    latest_approved = next((x for x in orders if (x.get("status") or "").lower() == "approved"), None)
    if latest_approved:
        total_paid = _f(latest_approved.get("amount_paid"))
        amount_left = max(_f(latest_approved.get("total_debt")) - total_paid, 0.0)
    else:
        total_paid = 0.0
        amount_left = 0.0

    return render_template(
        "partials/client_profile.html",
        client=client,
        orders=orders,
        latest_approved=latest_approved,
        total_paid=round(total_paid, 2),
        amount_left=round(amount_left, 2)
    )
