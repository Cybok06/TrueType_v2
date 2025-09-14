from flask import Blueprint, render_template, session, redirect, url_for, flash, jsonify
from db import db
from bson import ObjectId
from datetime import datetime, timedelta

home_bp = Blueprint('home', __name__, template_folder='templates')

# Core collections (existing)
orders_collection    = db['orders']
clients_collection   = db['clients']
payments_collection  = db['payments']        # inbound receipts (confirmed)

# Extra collections (for KPIs; safe to reference even if currently empty)
tax_collection       = db['tax_records']     # P-Tax outflows
sbdc_collection      = db['s_bdc_payment']   # BDC payables (amount, bank_paid_total)

# ---------------- helpers ----------------

def _sum_returns_total():
    """Sum saved returns_total across APPROVED orders only."""
    pipeline = [
        {"$match": {"status": {"$regex": "^approved$", "$options": "i"}}},
        {"$addFields": {
            "rt": {"$convert": {"input": "$returns_total", "to": "double", "onError": 0, "onNull": 0}}
        }},
        {"$group": {"_id": None, "total": {"$sum": "$rt"}}}
    ]
    doc = next(orders_collection.aggregate(pipeline), None)
    return round(float((doc or {}).get("total", 0) or 0), 2)

def _sum_total_bank_balance():
    """
    Total Bank Balance = all confirmed receipts
                         - all P-Tax outflows (tax_records.type ~ 'p-tax')
                         - all BDC bank payments (s_bdc_payment).
    """
    # Inbound
    in_doc = next(payments_collection.aggregate([
        {"$match": {"status": {"$regex": "^confirmed$", "$options": "i"}}},
        {"$addFields": {"amt": {"$convert": {"input": "$amount", "to": "double", "onError": 0, "onNull": 0}}}},
        {"$group": {"_id": None, "total_in": {"$sum": "$amt"}}}
    ]), None)
    total_in = float((in_doc or {}).get("total_in", 0) or 0)

    # P-Tax out
    tax_doc = next(tax_collection.aggregate([
        {"$match": {"type": {"$regex": r"^p[\s_-]*tax$", "$options": "i"}}},
        {"$addFields": {"amt": {"$convert": {"input": "$amount", "to": "double", "onError": 0, "onNull": 0}}}},
        {"$group": {"_id": None, "total_tax": {"$sum": "$amt"}}}
    ]), None)
    total_tax = float((tax_doc or {}).get("total_tax", 0) or 0)

    # BDC bank payments (prefer bank_paid_total; fallback to history sum)
    bdc_doc = next(sbdc_collection.aggregate([
        {"$addFields": {
            "paid_total": {"$convert": {"input": {"$ifNull": ["$bank_paid_total", 0]}, "to": "double", "onError": 0, "onNull": 0}},
            "hist_sum": {
                "$sum": {
                    "$map": {
                        "input": {"$ifNull": ["$bank_paid_history", []]},
                        "as": "h",
                        "in": {"$convert": {"input": "$$h.amount", "to": "double", "onError": 0, "onNull": 0}}
                    }
                }
            }
        }},
        {"$addFields": {"paid_effective": {"$cond": [{"$gt": ["$paid_total", 0]}, "$paid_total", "$hist_sum"]}}},
        {"$group": {"_id": None, "total_bdc_out": {"$sum": "$paid_effective"}}}
    ]), None)
    total_bdc_out = float((bdc_doc or {}).get("total_bdc_out", 0) or 0)

    return round(total_in - total_tax - total_bdc_out, 2)

def _sum_total_omc_debt():
    """
    Unpaid P-Tax across approved orders with p_tax > 0:
      due = p_tax_per_litre * quantity; remaining = max(due - sum(p-tax payments), 0)
    """
    pipe = [
        {"$match": {
            "status": {"$regex": "^approved$", "$options": "i"},
            "$or": [{"p_tax": {"$gt": 0}}, {"p-tax": {"$gt": 0}}]
        }},
        {"$addFields": {
            "q": {"$convert": {"input": "$quantity", "to": "double", "onError": 0, "onNull": 0}},
            "p_tax_pl": {"$ifNull": [
                {"$convert": {"input": "$p_tax", "to": "double", "onError": 0, "onNull": 0}},
                {"$convert": {"input": "$p-tax", "to": "double", "onError": 0, "onNull": 0}}
            ]}
        }},
        {"$addFields": {"due": {"$multiply": ["$q", "$p_tax_pl"]}}},
        {"$lookup": {
            "from": "tax_records",
            "let": {"oid": "$_id"},
            "pipeline": [
                {"$match": {"$expr": {
                    "$and": [
                        {"$eq": ["$order_oid", "$$oid"]},
                        {"$regexMatch": {"input": "$type", "regex": r"^p[\s_-]*tax$", "options": "i"}}
                    ]
                }}},
                {"$addFields": {"amt": {"$convert": {"input": "$amount", "to": "double", "onError": 0, "onNull": 0}}}},
                {"$group": {"_id": None, "paid": {"$sum": "$amt"}}}
            ],
            "as": "ptax_paid"
        }},
        {"$addFields": {"paid": {"$ifNull": [{"$arrayElemAt": ["$ptax_paid.paid", 0]}, 0]}}},
        {"$addFields": {"rem": {"$max": [{"$subtract": ["$due", "$paid"]}, 0]}}}, 
        {"$group": {"_id": None, "total_omc_debt": {"$sum": "$rem"}}}
    ]
    doc = next(orders_collection.aggregate(pipe), None)
    return round(float((doc or {}).get("total_omc_debt", 0) or 0), 2)

def _sum_total_bdc_debt():
    """
    Unpaid BDC payables across s_bdc_payment where payment_type in (cash|credit|from account):
      remain = max(amount - bank_paid_total, 0)
    """
    pipe = [
        {"$match": {"payment_type": {"$regex": r"^(cash|credit|from\s*account)$", "$options": "i"}}},
        {"$addFields": {
            "amount_d": {"$convert": {"input": "$amount", "to": "double", "onError": 0, "onNull": 0}},
            "paid_d":   {"$convert": {"input": {"$ifNull": ["$bank_paid_total", 0]}, "to": "double", "onError": 0, "onNull": 0}}
        }},
        {"$addFields": {"remain": {"$max": [{"$subtract": ["$amount_d", "$paid_d"]}, 0]}}},
        {"$group": {"_id": None, "total_bdc_debt": {"$sum": "$remain"}}}
    ]
    doc = next(sbdc_collection.aggregate(pipe), None)
    return round(float((doc or {}).get("total_bdc_debt", 0) or 0), 2)

def _sum_total_debtors_amount():
    """
    For each approved order: left = max(total_debt - sum(confirmed payments linked to it), 0)
    Links support ObjectId or string order_id via 'order_id' or 'order_ref'.
    """
    pipe = [
        {"$match": {"status": {"$regex": "^approved$", "$options": "i"}}},
        {"$addFields": {
            "order_oid": "$_id",
            "order_ref": {"$ifNull": ["$order_id", None]},
            "debt_num":  {"$convert": {"input": "$total_debt", "to": "double", "onError": 0, "onNull": 0}}
        }},
        {"$lookup": {
            "from": "payments",
            "let": {"oid": "$order_oid", "ref": "$order_ref"},
            "pipeline": [
                {"$match": {"$expr": {
                    "$and": [
                        {"$eq": ["$status", "confirmed"]},
                        {"$or": [
                            {"$eq": ["$order_id", "$$oid"]},
                            {"$eq": ["$order_ref", "$$oid"]},
                            {"$and": [{"$ne": ["$$ref", None]}, {"$eq": ["$order_id", "$$ref"]}]},
                            {"$and": [{"$ne": ["$$ref", None]}, {"$eq": ["$order_ref", "$$ref"]}]}
                        ]}
                    ]
                }}},
                {"$addFields": {"amt": {"$convert": {"input": "$amount", "to": "double", "onError": 0, "onNull": 0}}}},
                {"$group": {"_id": None, "paid": {"$sum": "$amt"}}}
            ],
            "as": "pays"
        }},
        {"$addFields": {"paid": {"$ifNull": [{"$arrayElemAt": ["$pays.paid", 0]}, 0]}}},
        {"$addFields": {"left": {"$max": [{"$subtract": ["$debt_num", "$paid"]}, 0]}}}, 
        {"$group": {"_id": None, "total_left": {"$sum": "$left"}}}
    ]
    doc = next(orders_collection.aggregate(pipe), None)
    return round(float((doc or {}).get("total_left", 0) or 0), 2)

# ---------------- routes ----------------

@home_bp.route('/home')
def dashboard_home():
    if 'role' not in session or session['role'] != 'admin':
        flash("Access denied.", "danger")
        return redirect(url_for('login.login'))

    total_clients = clients_collection.estimated_document_count()
    total_orders = orders_collection.estimated_document_count()
    total_approved_orders = orders_collection.count_documents(
        {'status': {'$regex': '^approved$', '$options': 'i'}}
    )
    approval_rate = round((total_approved_orders / total_orders) * 100, 1) if total_orders else 0

    # Core total
    total_returns = _sum_returns_total()

    # NEW KPI cards
    kpi_bank_balance = _sum_total_bank_balance()
    kpi_omc_debt     = _sum_total_omc_debt()
    kpi_bdc_debt     = _sum_total_bdc_debt()
    kpi_debtors      = _sum_total_debtors_amount()

    # Orders today / yesterday (kept)
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    orders_today = orders_collection.count_documents({"date": {"$gte": today}})
    orders_yesterday = orders_collection.count_documents({"date": {"$gte": yesterday, "$lt": today}})

    return render_template(
        'partials/home.html',
        total_clients=total_clients,
        total_orders=total_orders,
        total_approved_orders=total_approved_orders,
        approval_rate=approval_rate,
        total_returns=total_returns,

        # expose KPIs to template (no JS needed)
        kpi_bank_balance=kpi_bank_balance,
        kpi_omc_debt=kpi_omc_debt,
        kpi_bdc_debt=kpi_bdc_debt,
        kpi_debtors=kpi_debtors,

        orders_today=orders_today,
        orders_yesterday=orders_yesterday
    )

@home_bp.route('/home/details')
def dashboard_details():
    if 'role' not in session or session['role'] != 'admin':
        return jsonify({'error': 'Unauthorized'}), 403

    try:
        now = datetime.now()

        # Totals for charts
        total_debt_cursor = orders_collection.aggregate([
            {"$match": {"status": {"$regex": "^approved$", "$options": "i"}}},
            {"$addFields": {"debt_num": {"$convert": {"input": "$total_debt", "to": "double", "onError": 0, "onNull": 0}}}},
            {"$group": {"_id": None, "total_debt": {"$sum": "$debt_num"}}}
        ])
        total_debt = round(float(next(total_debt_cursor, {}).get("total_debt", 0) or 0), 2)

        total_paid_cursor = payments_collection.aggregate([
            {"$match": {"status": {"$regex": "^confirmed$", "$options": "i"}}},
            {"$addFields": {"amount_num": {"$convert": {"input": "$amount", "to": "double", "onError": 0, "onNull": 0}}}},
            {"$group": {"_id": None, "total": {"$sum": "$amount_num"}}}
        ])
        total_paid = round(float(next(total_paid_cursor, {}).get("total", 0) or 0), 2)

        total_returns = _sum_returns_total()

        # Monthly Orders (current year)
        months, order_counts = [], []
        for i in range(1, 13):
            start = datetime(now.year, i, 1)
            end = datetime(now.year + 1, 1, 1) if i == 12 else datetime(now.year, i + 1, 1)
            months.append(start.strftime('%B'))
            order_counts.append(orders_collection.count_documents({'date': {'$gte': start, '$lt': end}}))

        # Top Clients
        top_clients_agg = list(orders_collection.aggregate([
            {"$group": {"_id": "$client_id", "order_count": {"$sum": 1}}},
            {"$sort": {"order_count": -1}},
            {"$limit": 5}
        ]))
        client_ids = [entry['_id'] for entry in top_clients_agg if entry['_id']]
        safe_ids = [cid if isinstance(cid, ObjectId) else ObjectId(cid) for cid in client_ids]
        client_map = {str(c['_id']): c.get('name', 'Unknown') for c in clients_collection.find({"_id": {"$in": safe_ids}})}
        top_clients_names = [client_map.get(str(entry['_id']), 'Unknown') for entry in top_clients_agg]
        top_clients_orders = [entry['order_count'] for entry in top_clients_agg]

        # Recent activity (3 days)
        three_days_ago = now - timedelta(days=3)
        orders = list(orders_collection.find(
            {'status': {'$regex': '^approved$', '$options': 'i'}, 'date': {'$gte': three_days_ago}}
        ).sort('date', -1).limit(5))
        pays = list(payments_collection.find(
            {'status': {'$regex': '^confirmed$', '$options': 'i'}, 'date': {'$gte': three_days_ago}}
        ).sort('date', -1).limit(5))
        overdues = list(orders_collection.find(
            {'status': {'$ne': 'completed'}, 'due_date': {'$lt': now}}
        ).sort('due_date', -1).limit(5))

        recent_client_ids = list({o.get('client_id') for o in orders + pays + overdues})
        safe_client_ids = [cid if isinstance(cid, ObjectId) else ObjectId(cid) for cid in recent_client_ids if cid]
        client_lookup = {str(c['_id']): c.get('name', 'Unknown') for c in clients_collection.find({"_id": {"$in": safe_client_ids}})}

        def fmt_time(dt): return (dt.isoformat() if dt else now.isoformat())

        recent_activities = []
        for o in orders:
            name = client_lookup.get(str(o.get('client_id')), 'Unknown')
            amt = round(float(o.get('total_debt', 0) or 0), 2)
            recent_activities.append({"icon": "<i class='bi bi-check-circle-fill'></i>", "text": f"Order approved for {name} — {o.get('product','N/A')} (GHS {amt})", "time": fmt_time(o.get('date')), "color": "text-success"})
        for p in pays:
            name = client_lookup.get(str(p.get('client_id')), 'Unknown')
            amt = round(float(p.get('amount', 0) or 0), 2)
            method = p.get('method') or p.get('bank_name') or 'N/A'
            recent_activities.append({"icon": "<i class='bi bi-cash-stack'></i>", "text": f"Payment of GHS {amt} confirmed from {name} via {method}", "time": fmt_time(p.get('date')), "color": "text-primary"})
        for o in overdues:
            name = client_lookup.get(str(o.get('client_id')), 'Unknown')
            amt = round(float(o.get('total_debt', 0) or 0), 2)
            recent_activities.append({"icon": "<i class='bi bi-exclamation-circle'></i>", "text": f"{name} missed due for {o.get('product','N/A')} — GHS {amt}", "time": fmt_time(o.get('due_date')), "color": "text-danger"})

        recent_activities = sorted(recent_activities, key=lambda x: x['time'], reverse=True)[:8]

        return jsonify({
            "total_debt": total_debt,
            "total_paid": total_paid,
            "total_returns": total_returns,
            "months": months,
            "order_counts": order_counts,
            "top_clients_names": top_clients_names,
            "top_clients_orders": top_clients_orders,
            "recent_activities": recent_activities
        })

    except Exception as e:
        print("Error in /home/details:", str(e))
        return jsonify({"error": "Failed to load dashboard details.", "details": str(e)}), 500
