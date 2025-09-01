from flask import Blueprint, render_template, jsonify
from db import db
from datetime import datetime

navbar_bp = Blueprint("navbar", __name__, template_folder="templates")

# Collections
clients_collection = db["clients"]
orders_collection = db["orders"]
payments_collection = db["payments"]
truck_payments_collection = db["truck_payments"]
tax_records_collection = db["tax_records"]     # OMC S-Tax paid
sbdc_collection = db["s_bdc_payment"]          # BDC bank payments

def _compute_counts():
    unapproved_orders_count = orders_collection.count_documents({"status": "pending"})
    overdue_clients_count = clients_collection.count_documents({"status": "overdue"})
    unconfirmed_payments_count = payments_collection.count_documents({"status": "pending"})
    unconfirmed_truck_payments_count = truck_payments_collection.count_documents({"status": "pending"})

    # Truck debtors count
    pipeline = [
        {"$group": {"_id": "$client_id", "total_debt": {"$sum": "$total_debt"}, "total_paid": {"$sum": "$paid"}}},
        {"$project": {"amount_left": {"$subtract": ["$total_debt", "$total_paid"]}}},
        {"$match": {"amount_left": {"$gt": 0}}},
        {"$count": "truck_debtors_count"}
    ]
    agg = list(db["orders"].aggregate(pipeline))
    truck_debtors_count = agg[0]["truck_debtors_count"] if agg else 0

    # OMC unpaid S-Tax summary
    omc_pipe = [
        {"$match": {
            "$or": [
                {"order_type": "s_tax"},
                {"order_type": "combo"},
                {"s_tax": {"$gt": 0}},
                {"s-tax": {"$gt": 0}},
            ]
        }},
        {"$addFields": {
            "rate_raw": {"$ifNull": ["$s_tax", {"$ifNull": ["$s-tax", 0]}]},
            "qty_raw": {"$ifNull": ["$quantity", 0]}
        }},
        {"$addFields": {"rate": {"$toDouble": "$rate_raw"}, "qty": {"$toDouble": "$qty_raw"}}},
        {"$addFields": {"due": {"$round": [{"$multiply": ["$rate", "$qty"]}, 2]}}},
        {"$lookup": {
            "from": "tax_records",
            "let": {"oid": "$_id"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$order_oid", "$$oid"]},
                            {"$regexMatch": {"input": "$type", "regex": r"^s[\s_-]*tax$", "options": "i"}}
                        ]
                    }
                }},
                {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
            ],
            "as": "tax"
        }},
        {"$addFields": {"paid": {"$ifNull": [{"$arrayElemAt": ["$tax.total", 0]}, 0]}}},
        {"$addFields": {"remain": {"$round": [{"$subtract": ["$due", {"$toDouble": "$paid"}]}, 2]}}}
    ]
    omc_rows = list(orders_collection.aggregate(omc_pipe))
    omc_debtors_count = len([r for r in omc_rows if (r.get("remain", 0) or 0) > 0])
    omc_outstanding_total = float(sum((r.get("remain") or 0) for r in omc_rows))

    # BDC unpaid bank-payment summary (credit/from account)
    bdc_pipe = [
        {"$match": {"payment_type": {"$regex": r"^(credit|from\s*account)$", "$options": "i"}}},
        {"$lookup": {"from": "orders", "localField": "order_id", "foreignField": "_id", "as": "ord"}},
        {"$addFields": {
            "bdc_id_eff": {"$ifNull": ["$bdc_id", {"$arrayElemAt": ["$ord.bdc_id", 0]}]},
            "amount_d": {"$toDouble": "$amount"},
            "paid_d": {"$toDouble": {"$ifNull": ["$bank_paid_total", 0]}}
        }},
        {"$addFields": {"remain": {"$subtract": ["$amount_d", "$paid_d"]}}}
    ]
    bdc_rows = list(sbdc_collection.aggregate(bdc_pipe))
    bdc_debtors_count = len([r for r in bdc_rows if r.get("bdc_id_eff") is not None and (r.get("remain", 0) or 0) > 0])
    bdc_outstanding_total = float(sum((r.get("remain") or 0) for r in bdc_rows if r.get("bdc_id_eff") is not None))

    return {
        "unapproved_orders_count": int(unapproved_orders_count),
        "overdue_clients_count": int(overdue_clients_count),
        "unconfirmed_payments_count": int(unconfirmed_payments_count),
        "unconfirmed_truck_payments_count": int(unconfirmed_truck_payments_count),
        "truck_debtors_count": int(truck_debtors_count),
        "omc_debtors_count": int(omc_debtors_count),
        "omc_outstanding_total": float(omc_outstanding_total),
        "bdc_debtors_count": int(bdc_debtors_count),
        "bdc_outstanding_total": float(bdc_outstanding_total),
        "ts": datetime.utcnow().isoformat() + "Z",
    }

@navbar_bp.route("/nav/fragment")
def nav_fragment():
    return render_template("partials/navbar.html", **_compute_counts())

@navbar_bp.route("/nav/pulse")
def nav_pulse():
    return jsonify({"ok": True, "counts": _compute_counts()})
