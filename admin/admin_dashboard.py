# admin_dashboard.py
from flask import Blueprint, render_template, redirect, url_for, session
from db import db, users_collection

admin_dashboard_bp = Blueprint('admin_dashboard', __name__, template_folder='templates')

# Collections
clients_collection = db["clients"]
orders_collection = db["orders"]
payments_collection = db["payments"]
truck_payments_collection = db["truck_payments"]
tax_records_collection = db["tax_records"]
sbdc_collection = db["s_bdc_payment"]

def _load_current_user():
    """
    Use session only to identify *who* is logged in (username/role),
    but NEVER to read permissions. Permissions come directly from DB.
    """
    username = session.get("username")
    if not username:
        return None
    user = users_collection.find_one(
        {"username": username},
        {"username": 1, "role": 1, "access": 1, "perms": 1}
    )
    return user

def _allowed_slugs_for(user):
    """
    Compute allowed slugs from user's stored access map or legacy perms list.
    Superadmin is considered only by username=='admin' or role flag (optional).
    """
    if not user:
        return set(), False

    is_super = (user.get("username") == "admin") or (user.get("role") == "superadmin")

    allowed = set()
    # New model: {"access": {"slug": true/false, ...}}
    access = user.get("access")
    if isinstance(access, dict):
        allowed = {k for k, v in access.items() if v}
    else:
        # Legacy model: ["slug", "slug2", ...]
        perms = user.get("perms") or []
        allowed = set(perms)

    return allowed, bool(is_super)

@admin_dashboard_bp.route('/dashboard')
def dashboard():
    # If no user identified => send to login
    user = _load_current_user()
    if not user:
        return redirect(url_for('login.login'))

    # Live counts (unchanged)
    unapproved_orders_count = orders_collection.count_documents({"status": "pending"})
    overdue_clients_count = clients_collection.count_documents({"status": "overdue"})
    unconfirmed_payments_count = payments_collection.count_documents({"status": "pending"})
    unconfirmed_truck_payments_count = truck_payments_collection.count_documents({"status": "pending"})

    # Truck debtors count
    pipeline = [
        {"$group": {"_id": "$client_id", "total_debt": {"$sum": "$total_debt"}, "total_paid": {"$sum": "$paid"}}},
        {"$project": {"amount_left": {"$subtract": ["$total_debt", "$total_paid"]}}},  # debt - paid
        {"$match": {"amount_left": {"$gt": 0}}},
        {"$count": "truck_debtors_count"}
    ]
    agg_result = list(db["orders"].aggregate(pipeline))
    truck_debtors_count = agg_result[0]["truck_debtors_count"] if agg_result else 0

    # OMC S-Tax unpaid summary
    omc_pipe = [
        {"$match": {"$or": [
            {"order_type": "s_tax"}, {"order_type": "combo"},
            {"s_tax": {"$gt": 0}}, {"s-tax": {"$gt": 0}}
        ]}},
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
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$order_oid", "$$oid"]},
                    {"$regexMatch": {"input": "$type", "regex": r"^s[\s_-]*tax$", "options": "i"}}
                ]}}},
                {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
            ],
            "as": "tax"
        }},
        {"$addFields": {"paid": {"$ifNull": [{"$arrayElemAt": ["$tax.total", 0]}, 0]}}},
        {"$addFields": {"remain": {"$round": [{"$subtract": ["$due", {"$toDouble": "$paid"}]}, 2]}}},

        {"$match": {"remain": {"$gt": 0}}},
        {"$group": {"_id": "$omc", "outstanding": {"$sum": "$remain"}}},
        {"$project": {"_id": 0, "omc": {"$ifNull": ["$_id", "—"]}, "outstanding": {"$round": ["$outstanding", 2]}}},
        {"$sort": {"outstanding": -1}}
    ]
    omc_rows = list(orders_collection.aggregate(omc_pipe))
    omc_debtors_count = len(omc_rows)
    omc_outstanding_total = float(sum((row.get("outstanding") or 0) for row in omc_rows))

    # BDC unpaid bank payment summary
    bdc_pipe = [
        {"$match": {"payment_type": {"$regex": r"^(credit|from\s*account)$", "$options": "i"}}},
        {"$lookup": {"from": "orders", "localField": "order_id", "foreignField": "_id", "as": "ord"}},
        {"$addFields": {
            "bdc_id_eff": {"$ifNull": ["$bdc_id", {"$arrayElemAt": ["$ord.bdc_id", 0]}]},
            "amount_d": {"$toDouble": "$amount"},
            "paid_d": {"$toDouble": {"$ifNull": ["$bank_paid_total", 0]}}
        }},
        {"$addFields": {"remain": {"$subtract": ["$amount_d", "$paid_d"]}}},
        {"$match": {"bdc_id_eff": {"$ne": None}, "remain": {"$gt": 0}}},
        {"$group": {"_id": "$bdc_id_eff", "outstanding": {"$sum": "$remain"}}},
        {"$project": {"_id": 0, "outstanding": {"$round": ["$outstanding", 2]}}},
        {"$sort": {"outstanding": -1}}
    ]
    bdc_rows = list(sbdc_collection.aggregate(bdc_pipe))
    bdc_debtors_count = len(bdc_rows)
    bdc_outstanding_total = float(sum((row.get("outstanding") or 0) for row in bdc_rows))

    # 🔒 Pull permissions straight from DB for the logged user
    allowed_slugs, is_superadmin = _allowed_slugs_for(user)

    return render_template(
        'admin/admin_dashboard.html',
        unapproved_orders_count=unapproved_orders_count,
        overdue_clients_count=overdue_clients_count,
        unconfirmed_payments_count=unconfirmed_payments_count,
        unconfirmed_truck_payments_count=unconfirmed_truck_payments_count,
        truck_debtors_count=truck_debtors_count,
        omc_debtors_count=omc_debtors_count,
        omc_outstanding_total=omc_outstanding_total,
        bdc_debtors_count=bdc_debtors_count,
        bdc_outstanding_total=bdc_outstanding_total,

        # 👉 server-render permissions (no client fetch, no session perms)
        allowed_slugs=list(allowed_slugs),
        is_superadmin=is_superadmin
    )
