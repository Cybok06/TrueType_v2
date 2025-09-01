from flask import Blueprint, render_template, request
from db import db
from bson import ObjectId
from collections import defaultdict
import math

truck_debtors_bp = Blueprint("truck_debtors", __name__)

# Collections
truck_orders_col = db["truck_orders"]
truck_expenses_col = db["truck_expenses"]
truck_payments_col = db["truck_payments"]

def get_filtered_debtors(search_term="", unpaid_only=False):
    orders = list(truck_orders_col.find())
    client_data = defaultdict(lambda: {
        "total_debt": 0,
        "orders": [],
        "client_name": "",
        "client_phone": ""
    })

    for order in orders:
        client_key = str(order.get("client_id"))
        client_data[client_key]["client_name"] = order.get("client_name", "")
        client_data[client_key]["client_phone"] = order.get("client_phone", "")
        client_data[client_key]["orders"].append(order)
        client_data[client_key]["total_debt"] += float(order.get("total_debt", 0))

    payments = truck_payments_col.find({"status": "confirmed"})
    payments_map = defaultdict(float)
    for p in payments:
        payments_map[str(p.get("client_id"))] += float(p.get("amount", 0))

    expenses = truck_expenses_col.find()
    expenses_map = defaultdict(float)
    for e in expenses:
        expenses_map[e.get("order_id")] += float(e.get("amount", 0))

    result = []
    for client_key, data in client_data.items():
        total_expense = sum(expenses_map.get(str(o["_id"]), 0) for o in data["orders"])
        total_debt = data["total_debt"]
        total_paid = payments_map.get(client_key, 0)
        amount_left = total_debt - total_paid
        settled_amount = total_debt - total_expense

        if (
            (not search_term or search_term in data["client_name"].lower() or search_term in data["client_phone"])
            and (not unpaid_only or amount_left > 0)
        ):
            result.append({
                "client_name": data["client_name"],
                "client_phone": data["client_phone"],
                "total_debt": total_debt,
                "total_paid": total_paid,
                "amount_left": amount_left,
                "total_expense": total_expense,
                "settled_amount": settled_amount,
                "order_count": len(data["orders"])
            })

    result.sort(key=lambda x: x["amount_left"], reverse=True)
    return result

@truck_debtors_bp.route("/truck_debtors")
def view_truck_debtors():
    page = int(request.args.get("page", 1))
    per_page = 10
    search_term = request.args.get("search", "").lower().strip()
    unpaid_only = request.args.get("unpaid_only", "false").lower() == "true"

    debtors = get_filtered_debtors(search_term, unpaid_only)
    total_pages = math.ceil(len(debtors) / per_page)
    paginated = debtors[(page - 1) * per_page: page * per_page]

    return render_template("partials/truck_debtors.html",
        debtors=paginated,
        current_page=page,
        total_pages=total_pages,
        search=search_term,
        unpaid_only=unpaid_only,
        partial=False
    )

@truck_debtors_bp.route("/truck_debtors/ajax")
def ajax_truck_debtors():
    search_term = request.args.get("search", "").lower().strip()
    unpaid_only = request.args.get("unpaid_only", "false").lower() == "true"
    debtors = get_filtered_debtors(search_term, unpaid_only)

    return render_template("partials/truck_debtors.html",
        debtors=debtors,
        current_page=1,
        total_pages=1,
        search=search_term,
        unpaid_only=unpaid_only,
        partial=True  # flag for rendering just the table
    )
