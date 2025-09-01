from flask import Blueprint, render_template, session, redirect, url_for
from db import db

external_orders_bp = Blueprint("external_orders", __name__, template_folder="templates")

truck_orders_col = db["truck_orders"]

@external_orders_bp.route("/external/orders")
def external_orders():
    # Ensure user is logged in and is an external client
    if session.get("role") != "external":
        return redirect(url_for("login.login"))

    client_id = session.get("external_id")
    if not client_id:
        return redirect(url_for("login.login"))

    # Fetch orders that belong to this external client
    orders = list(truck_orders_col.find({"client_id": client_id}).sort("created_at", -1))

    for order in orders:
        order["_id"] = str(order["_id"])
        order["created_at"] = order.get("created_at").strftime("%Y-%m-%d %H:%M")
        order["total_debt"] = float(order.get("total_debt", 0))
        order["status"] = order.get("status", "pending").capitalize()

    return render_template("external_orders.html", orders=orders)
