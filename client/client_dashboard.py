from flask import Blueprint, render_template, session, redirect, url_for, flash
from bson import ObjectId
from db import db
from datetime import datetime

client_dashboard_bp = Blueprint('client_dashboard', __name__, template_folder='templates')

clients_collection   = db.clients
orders_collection    = db.orders
payments_collection  = db.payments

def _f(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default

@client_dashboard_bp.route('/dashboard')
def dashboard():
    if 'client_id' not in session:
        flash("Please log in first", "warning")
        return redirect(url_for('login.login'))

    client_id = session['client_id']
    if not ObjectId.is_valid(client_id):
        flash("Invalid session. Please log in again.", "danger")
        return redirect(url_for('login.login'))

    oid = ObjectId(client_id)

    # ✅ Fetch client and name
    client = clients_collection.find_one({"_id": oid})
    if not client:
        flash("Client not found. Please contact support.", "danger")
        return redirect(url_for('login.login'))

    client_name = client.get("name", "Client")   # <-- get name here

    # Fetch orders for this client (support both ObjectId and string storage on orders)
    orders = list(
        orders_collection.find({"client_id": {"$in": [oid, client_id]}}).sort("date", -1)
    )

    order_ids_obj = [o["_id"] for o in orders]

    payments_pipe = [
        {
            "$match": {
                "status": {"$regex": "^confirmed$", "$options": "i"},
                "client_id": oid,
                "order_id": {"$in": order_ids_obj}
            }
        },
        {
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

    paid_map = {row["_id"]: _f(row.get("total_paid")) for row in payments_collection.aggregate(payments_pipe)}

    total_orders = len(orders)
    total_debt   = 0.0
    total_paid   = 0.0

    for o in orders:
        o["total_debt"] = _f(o.get("total_debt"))
        total_debt += o["total_debt"]

        paid_external = _f(paid_map.get(o["_id"]))
        o["amount_paid"] = round(paid_external, 2)
        o["amount_left"] = round(o["total_debt"] - o["amount_paid"], 2)
        total_paid += o["amount_paid"]

        for field in ("date", "due_date", "delivered_date"):
            v = o.get(field)
            if isinstance(v, dict) and "$date" in v:
                try:
                    ms = int(v["$date"].get("$numberLong", 0))
                    o[field] = datetime.fromtimestamp(ms / 1000.0)
                except Exception:
                    pass

    amount_left = round(total_debt - total_paid, 2)
    latest_order = orders[0] if orders else None

    return render_template(
        'client/client_dashboard.html',
        client=client,
        client_name=client_name,             # ✅ pass name into template
        total_orders=total_orders,
        total_debt=round(total_debt, 2),
        total_paid=round(total_paid, 2),
        amount_left=amount_left,
        latest_order=latest_order,
        recent_orders=orders[:5]
    )
