from flask import Blueprint, render_template, request, jsonify
from db import db
from bson import ObjectId
from datetime import datetime
import math
from collections import defaultdict

admin_truck_payments_bp = Blueprint("admin_truck_payments", __name__)

truck_payments_col = db["truck_payments"]
clients_col = db["clients"]
truck_orders_col = db["truck_orders"]
truck_expenses_col = db["truck_expenses"]

@admin_truck_payments_bp.route("/admin/truck_payments")
def admin_view_truck_payments():
    page = int(request.args.get("page", 1))
    per_page = 10
    skip = (page - 1) * per_page

    # Load orders
    orders = list(truck_orders_col.find())
    total_debt = 0
    debt_map = defaultdict(float)

    for order in orders:
        amount = float(order.get("total_debt", 0))
        client_key = str(order.get("client_id"))
        total_debt += amount
        debt_map[client_key] += amount

    # Total order count
    total_orders = len(orders)

    # Load expenses
    expenses = list(truck_expenses_col.find())
    total_expense = sum(float(e.get("amount", 0)) for e in expenses)

    # Load confirmed payments
    confirmed_payments_cursor = truck_payments_col.find({"status": "confirmed"})
    payments_map = defaultdict(float)
    total_paid = 0

    for p in confirmed_payments_cursor:
        amount = float(p.get("amount", 0))
        client_key = str(p.get("client_id"))
        payments_map[client_key] += amount
        total_paid += amount

    # Collection Efficiency (%)
    collection_efficiency = (total_paid / total_debt * 100) if total_debt > 0 else 0
    total_settled = total_debt - total_expense

    # Paginated payments for display
    total_count = truck_payments_col.count_documents({})
    total_pages = math.ceil(total_count / per_page)
    payments_cursor = truck_payments_col.find().sort("date", -1).skip(skip).limit(per_page)

    payments = []
    for p in payments_cursor:
        client_id_raw = p.get("client_id")
        client_id_str = str(client_id_raw)
        client_info = clients_col.find_one({"_id": ObjectId(client_id_raw)}) if ObjectId.is_valid(client_id_str) else None

        total_debt_for_client = debt_map.get(client_id_str, 0)
        total_paid_for_client = payments_map.get(client_id_str, 0)
        amount_left = total_debt_for_client - total_paid_for_client

        last4 = p.get("account_last4", "")
        payments.append({
            "_id": str(p["_id"]),
            "client_name": client_info.get("name") if client_info else "External Client",
            "client_phone": client_info.get("phone") if client_info else "-",
            "amount": p.get("amount", 0),
            "bank_name": p.get("bank_name", "-"),
            "account_last4": last4,
            "bank_and_last4": f"{p.get('bank_name', '-')} (xxx{last4})",
            "proof_url": p.get("proof_url", "#"),
            "status": p.get("status", "pending"),
            "formatted_date": p["date"].strftime("%Y-%m-%d %H:%M"),
            "total_debt": total_debt_for_client,
            "total_paid": total_paid_for_client,
            "amount_left": amount_left
        })

    return render_template(
        "partials/admin_truck_payments.html",
        payments=payments,
        current_page=page,
        total_pages=total_pages,
        total_debt=total_debt,
        total_paid=total_paid,
        total_orders=total_orders,
        total_expense=total_expense,
        total_settled=total_settled,
        collection_efficiency=round(collection_efficiency, 2)
    )

@admin_truck_payments_bp.route("/admin/truck_payments/confirm/<payment_id>", methods=["POST"])
def confirm_truck_payment(payment_id):
    truck_payments_col.update_one(
        {"_id": ObjectId(payment_id)},
        {"$set": {"status": "confirmed"}}
    )
    return jsonify({"success": True})
