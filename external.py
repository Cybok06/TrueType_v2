from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from db import db
from datetime import datetime
from bson import ObjectId

external_bp = Blueprint("external", __name__, template_folder="templates")

# MongoDB collections
truck_payments_col = db["truck_payments"]
truck_orders_col = db["truck_orders"]
bank_accounts_col = db["bank_accounts"]

# ✅ External Dashboard
@external_bp.route("/external/dashboard")
def external_dashboard():
    if session.get("role") != "external":
        return redirect(url_for("login.login"))

    external_name = session.get("external_name", "Guest")
    return render_template("external_dashboard.html", name=external_name)

# ✅ Truck Payment Page (External Clients)
@external_bp.route("/external/payment", methods=["GET", "POST"])
def external_truck_payment():
    external_id = session.get("external_id")
    if not external_id:
        flash("⚠ Session expired. Please log in again.", "warning")
        return redirect(url_for("login.login"))

    if request.method == "POST":
        amount = request.form.get("amount", "").strip()
        bank_name = request.form.get("bank_name", "").strip()
        account_last4 = request.form.get("account_last4", "").strip()
        proof_url = request.form.get("proof_url", "").strip()

        if not all([amount, bank_name, account_last4, proof_url]):
            flash("⚠ All fields are required.", "danger")
            return redirect(url_for("external.external_truck_payment"))

        try:
            amount = float(amount)
        except ValueError:
            flash("⚠ Invalid amount format.", "danger")
            return redirect(url_for("external.external_truck_payment"))

        payment_data = {
            "client_id": ObjectId(external_id),
            "amount": amount,
            "bank_name": bank_name,
            "account_last4": account_last4,
            "proof_url": proof_url,
            "status": "pending",
            "date": datetime.utcnow()
        }

        truck_payments_col.insert_one(payment_data)
        flash("✅ Truck payment submitted successfully!", "success")
        return redirect(url_for("external.external_truck_payment"))

    # ✅ Fetch payment history
    payment_history = list(truck_payments_col.find({"client_id": ObjectId(external_id)}).sort("date", -1))
    formatted_payments = [
        {
            "date": p["date"].strftime("%Y-%m-%d %H:%M:%S"),
            "amount": float(p.get("amount", 0)),
            "bank_name": p.get("bank_name", "-"),
            "account_last4": p.get("account_last4", ""),
            "proof_url": p.get("proof_url", "#"),
            "status": p.get("status", "pending"),
            "feedback": p.get("feedback", "")
        }
        for p in payment_history
    ]

    bank_accounts = list(bank_accounts_col.find({}, {
        "bank_name": 1,
        "account_name": 1,
        "account_number": 1,
        "_id": 0
    }).sort("bank_name"))

    return render_template(
        "client/client_truck_payment.html",
        payments=formatted_payments,
        bank_accounts=bank_accounts
    )

# ✅ View Orders Page (External Clients)
@external_bp.route("/external/orders")
def external_orders():
    external_id = session.get("external_id")
    if not external_id:
        flash("⚠ Session expired. Please log in again.", "warning")
        return redirect(url_for("login.login"))

    orders = list(truck_orders_col.find({"client_id": str(external_id)}).sort("created_at", -1))

    formatted_orders = []
    for o in orders:
        formatted_orders.append({
            "truck_number": o.get("truck_number", ""),
            "driver_name": o.get("driver_name", ""),
            "driver_phone": o.get("driver_phone", ""),
            "destination": o.get("destination", ""),
            "total_debt": f"{o.get('total_debt', 0):,.2f}",
            "status": o.get("status", "pending").capitalize(),
            "created_at": o.get("created_at", "").strftime("%Y-%m-%d %H:%M:%S") if o.get("created_at") else "N/A",
            "started_at": o.get("started_at", "").strftime("%Y-%m-%d %H:%M:%S") if o.get("started_at") else "Not started",
            "delivered_at": o.get("delivered_at", "").strftime("%Y-%m-%d %H:%M:%S") if o.get("delivered_at") else "Not delivered"
        })

    return render_template("external_orders.html", orders=formatted_orders)
