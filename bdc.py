from flask import Blueprint, render_template, request, jsonify, session
from db import db
from datetime import datetime
from bson import ObjectId

# 📦 Collections
bdc_col           = db["bdc"]
bdc_txn_col       = db["bdc_transactions"]
orders_col        = db["orders"]            # legacy join for payments with only order_id
s_bdc_payment_col = db["s_bdc_payment"]     # ✅ central BDC payments (authoritative)

bdc_bp = Blueprint('bdc', __name__)

# ---------- Helpers ----------
def _to_f(x):
    try:
        if isinstance(x, str):
            x = x.replace("GHS", "").replace(",", "").strip()
        return float(x)
    except Exception:
        return 0.0

def _norm_status(p):
    """
    Normalize an individual payment's 'paid'/'pending' status.
    Priority: explicit bank_status -> else (cash => paid, others => pending)
    """
    s = (p.get("bank_status") or "").strip().lower()
    if s in ("paid", "pending"):
        return s
    t = (p.get("payment_type") or "").strip().lower()
    return "paid" if t == "cash" else "pending"

def _fetch_bdc_payments(oid: ObjectId):
    """
    Return deduped, date-desc list of payments for a BDC.
    Looks for direct bdc_id matches and legacy rows linked by order_id.
    Ensures each dict has normalized 'bank_status' and a datetime 'date' (or None).
    """
    direct = list(s_bdc_payment_col.find({"bdc_id": oid}))
    order_ids = list(orders_col.find({"bdc_id": oid}, {"_id": 1}))
    order_id_set = {o["_id"] for o in order_ids}
    legacy = list(s_bdc_payment_col.find({"order_id": {"$in": list(order_id_set)}})) if order_id_set else []

    seen, out = set(), []
    for p in direct + legacy:
        pid = str(p.get("_id"))
        if pid in seen:
            continue
        seen.add(pid)

        # normalize status (not persisted; just for computation/UI)
        p["bank_status"] = _norm_status(p)

        # coerce date
        dt = p.get("date")
        if isinstance(dt, str):
            try:
                p["date"] = datetime.fromisoformat(dt)
            except Exception:
                p["date"] = None
        elif not isinstance(dt, datetime):
            p["date"] = None

        out.append(p)

    out.sort(key=lambda x: x.get("date") or datetime.min, reverse=True)
    return out

def _compute_current_balance(bdc_id: ObjectId):
    """
    Balance model:
      balance = deposits_total - (paid_from_account + paid_credit)
    'Pending' amounts are reported but not deducted from balance.
    """
    oid = ObjectId(bdc_id)

    # Sum deposits
    deposits_total = 0.0
    agg = list(bdc_txn_col.aggregate([
        {"$match": {"bdc_id": oid, "type": "deposit"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
    ]))
    if agg:
        deposits_total = _to_f(agg[0].get("total"))

    # Tally payments (exclude cash from deduction logic)
    from_acc_paid = 0.0
    from_acc_pending = 0.0
    credit_paid = 0.0
    credit_pending = 0.0

    for p in _fetch_bdc_payments(oid):
        ptype = (p.get("payment_type") or "").strip().lower()
        if ptype not in ("from account", "credit"):
            # cash is ignored here (cash is handled via deposits flow)
            continue

        amount = _to_f(p.get("amount"))
        status = _norm_status(p)  # 'paid' / 'pending'

        if ptype == "from account":
            if status == "paid":
                from_acc_paid += amount
            else:
                from_acc_pending += amount
        else:  # credit
            if status == "paid":
                credit_paid += amount
            else:
                credit_pending += amount

    balance = round(deposits_total - (from_acc_paid + credit_paid), 2)

    return {
        "deposits_total": round(deposits_total, 2),
        "from_account_total_paid": round(from_acc_paid, 2),
        "credit_total_paid": round(credit_paid, 2),
        "from_account_total_pending": round(from_acc_pending, 2),
        "credit_total_pending": round(credit_pending, 2),
        "balance": balance
    }

def _pending_map_for_all_bdcs():
    """
    Efficiently compute unpaid (pending) totals per BDC for flags on the list page.
    Counts only 'from account' and 'credit' that are not marked bank_status='paid'.
    If 'bank_status' is missing, treat it as pending.
    Returns: {bdc_id(str): {"pending_amount": float, "pending_count": int}}
    """
    pipeline = [
        {"$match": {
            "payment_type": {"$in": ["from account", "credit"]},
            # consider missing bank_status as pending too
            "$or": [{"bank_status": {"$exists": False}}, {"bank_status": {"$ne": "paid"}}]
        }},
        {"$group": {
            "_id": "$bdc_id",
            "pending_amount": {"$sum": "$amount"},
            "pending_count": {"$sum": 1}
        }}
    ]
    result = {}
    for row in s_bdc_payment_col.aggregate(pipeline):
        key = str(row["_id"]) if row["_id"] else None
        if key:
            result[key] = {
                "pending_amount": round(_to_f(row.get("pending_amount")), 2),
                "pending_count": int(row.get("pending_count") or 0)
            }
    return result

# 📄 View All BDCs (shows unpaid flags; card no longer shows balance)
@bdc_bp.route('/bdc')
def bdc_list():
    bdcs = list(bdc_col.find().sort("name", 1))
    pending_map = _pending_map_for_all_bdcs()
    for b in bdcs:
        key = str(b["_id"])
        b["pending_amount"] = (pending_map.get(key, {}) or {}).get("pending_amount", 0.0)
        b["pending_count"]  = (pending_map.get(key, {}) or {}).get("pending_count", 0)
    return render_template("partials/bdc.html", bdcs=bdcs)

# ➕ Add New BDC
@bdc_bp.route('/bdc/add', methods=['POST'])
def add_bdc():
    data = request.json or {}
    name = (data.get('name', '') or '').strip()
    phone = (data.get('phone', '') or '').strip()
    location = (data.get('location', '') or '').strip()
    rep_name = (data.get('rep_name', '') or '').strip()
    rep_phone = (data.get('rep_phone', '') or '').strip()
    if not all([name, phone, location, rep_name, rep_phone]):
        return jsonify({"status": "error", "message": "All fields are required."}), 400
    if bdc_col.find_one({"name": name}):
        return jsonify({"status": "error", "message": "BDC already exists"}), 400
    bdc_col.insert_one({
        "name": name,
        "phone": phone,
        "location": location,
        "rep_name": rep_name,
        "rep_phone": rep_phone,
        "payment_details": [],  # legacy
        "date_created": datetime.utcnow()
    })
    return jsonify({"status": "success"})

# ✏️ Edit/Update BDC details
@bdc_bp.route('/bdc/update/<bdc_id>', methods=['POST'])
def update_bdc(bdc_id):
    data = request.json or {}
    fields = {
        "name": (data.get('name') or '').strip(),
        "phone": (data.get('phone') or '').strip(),
        "location": (data.get('location') or '').strip(),
        "rep_name": (data.get('rep_name') or '').strip(),
        "rep_phone": (data.get('rep_phone') or '').strip(),
    }
    updates = {k: v for k, v in fields.items() if v}
    if not updates:
        return jsonify({"status": "error", "message": "No valid fields to update."}), 400

    # Prevent name collision
    if "name" in updates:
        existing = bdc_col.find_one({"name": updates["name"], "_id": {"$ne": ObjectId(bdc_id)}})
        if existing:
            return jsonify({"status": "error", "message": "Another BDC already uses that name."}), 400

    res = bdc_col.update_one({"_id": ObjectId(bdc_id)}, {"$set": updates})
    if not res.matched_count:
        return jsonify({"status": "error", "message": "BDC not found"}), 404
    return jsonify({"status": "success"})

# 💰 Manual Deposit
@bdc_bp.route('/bdc/txn/<bdc_id>', methods=['POST'])
def add_transaction(bdc_id):
    try:
        data = request.json or {}
        amount = _to_f(data.get('amount'))
        note = (data.get('note') or '').strip()
        txn_type = (data.get('type') or '').strip().lower()
        if amount <= 0 or txn_type != 'add':
            return jsonify({"status": "error", "message": "Invalid transaction type or amount."}), 400
        if not bdc_col.find_one({"_id": ObjectId(bdc_id)}):
            return jsonify({"status": "error", "message": "BDC not found"}), 404

        bdc_txn_col.insert_one({
            "bdc_id": ObjectId(bdc_id),
            "amount": amount,
            "type": "deposit",
            "note": note,
            "timestamp": datetime.utcnow()
        })
        comp = _compute_current_balance(ObjectId(bdc_id))
        return jsonify({"status": "success", "new_balance": comp["balance"]})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 🧾 Manual BDC Payment (stored centrally)
@bdc_bp.route('/bdc/payment/<bdc_id>', methods=['POST'])
def record_bdc_payment(bdc_id):
    try:
        data = request.json or {}
        payment_type = (data.get("payment_type") or "").strip().lower()
        amount = _to_f(data.get("amount"))
        client_name = (data.get("client_name") or "").strip()
        product = (data.get("product") or "").strip()
        vehicle_number = (data.get("vehicle_number") or "").strip()
        driver_name = (data.get("driver_name") or "").strip()
        driver_phone = (data.get("driver_phone") or "").strip()
        quantity = data.get("quantity", "")
        region = (data.get("region") or "").strip()
        bank_status = (data.get("bank_status") or "").strip().lower()
        if bank_status not in ("paid", "pending"):
            bank_status = "paid" if payment_type == "cash" else "pending"

        if payment_type not in ["cash", "from account", "credit"] or amount <= 0:
            return jsonify({"status": "error", "message": "Invalid payment type or amount"}), 400
        if not bdc_col.find_one({"_id": ObjectId(bdc_id)}):
            return jsonify({"status": "error", "message": "BDC not found"}), 404

        s_bdc_payment_col.insert_one({
            "bdc_id": ObjectId(bdc_id),
            "payment_type": payment_type,
            "amount": amount,
            "client_name": client_name or "—",
            "product": product,
            "vehicle_number": vehicle_number,
            "driver_name": driver_name,
            "driver_phone": driver_phone,
            "quantity": quantity,
            "region": region,
            "delivery_status": "pending",
            "bank_status": bank_status,
            "date": datetime.utcnow()
        })
        comp = _compute_current_balance(ObjectId(bdc_id))
        return jsonify({"status": "success", "new_balance": comp["balance"]})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 👤 BDC Profile (computed balance; not stored)
@bdc_bp.route('/bdc/profile/<bdc_id>')
def bdc_profile(bdc_id):
    bdc = bdc_col.find_one({"_id": ObjectId(bdc_id)})
    if not bdc:
        return "BDC not found", 404

    role = session.get("role", "assistant")
    dashboard_url = "/admin/dashboard" if role == "admin" else "/assistant/dashboard"

    # Optional date filtering for transactions
    start = request.args.get("start")
    end = request.args.get("end")
    query = {"bdc_id": ObjectId(bdc_id)}
    try:
        if start:
            query["timestamp"] = {"$gte": datetime.strptime(start, "%Y-%m-%d")}
        if end:
            end_date = datetime.strptime(end, "%Y-%m-%d")
            query["timestamp"] = query.get("timestamp", {})
            query["timestamp"]["$lte"] = end_date
    except ValueError:
        pass

    transactions = list(bdc_txn_col.find(query).sort("timestamp", -1))
    payments = _fetch_bdc_payments(ObjectId(bdc_id))
    comp = _compute_current_balance(ObjectId(bdc_id))

    return render_template(
        "partials/bdc_profile.html",
        bdc=bdc,
        transactions=transactions,
        payments=payments,
        credit_balance=comp["balance"],
        deposits_total=comp["deposits_total"],
        from_account_total=comp["from_account_total_paid"],
        credit_total=comp["credit_total_paid"],
        from_account_total_pending=comp["from_account_total_pending"],
        credit_total_pending=comp["credit_total_pending"],
        dashboard_url=dashboard_url
    )

# ✅ Update delivery status
@bdc_bp.route('/bdc/update_delivery/<payment_id>', methods=['POST'])
def update_delivery_status(payment_id):
    try:
        data = request.json or {}
        status = (data.get("status") or "").strip()
        if not status:
            return jsonify({"status": "error", "message": "Missing status"}), 400

        res = s_bdc_payment_col.update_one({"_id": ObjectId(payment_id)}, {"$set": {"delivery_status": status}})
        if not res.matched_count:
            return jsonify({"status": "error", "message": "Payment not found"}), 404

        pay = s_bdc_payment_col.find_one({"_id": ObjectId(payment_id)}, {"order_id": 1})
        if pay and pay.get("order_id"):
            db["orders"].update_one({"_id": ObjectId(pay["order_id"])}, {"$set": {"delivery_status": status}})
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ✅ Update bank status
@bdc_bp.route('/bdc/payment_status/<payment_id>', methods=['POST'])
def update_bank_status(payment_id):
    try:
        data = request.json or {}
        new_status = (data.get("bank_status") or "").strip().lower()
        if new_status not in ("paid", "pending"):
            return jsonify({"status": "error", "message": "Invalid bank_status"}), 400

        res = s_bdc_payment_col.update_one({"_id": ObjectId(payment_id)}, {"$set": {"bank_status": new_status}})
        if not res.matched_count:
            return jsonify({"status": "error", "message": "Payment not found"}), 404
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
