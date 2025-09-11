from flask import Blueprint, render_template, jsonify, request
from bson import ObjectId
from datetime import datetime
from db import db

# Collections
payments_col = db["payments"]
clients_col  = db["clients"]
orders_col   = db["orders"]   # <-- NEW: we pull order info

payments_bp = Blueprint("payments_bp", __name__)

DEFAULT_PAGE_SIZE = 10  # for UI only

def _to_oid(val):
    if isinstance(val, ObjectId):
        return val
    try:
        return ObjectId(val)
    except Exception:
        return None

def _fmt_date(dt):
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M")
    try:
        return datetime.fromisoformat(str(dt)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(dt) if dt else "N/A"

def _nz(x):
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0

@payments_bp.route("/payments")
def view_payments():
    # Search term (only search is server-side; pagination will be client-side)
    q = (request.args.get("q") or "").strip()

    filt = {}
    if q:
        client_sub = {"$or": [
            {"name":      {"$regex": q, "$options": "i"}},
            {"phone":     {"$regex": q, "$options": "i"}},
            {"client_id": {"$regex": q, "$options": "i"}},
        ]}
        match_ids = [c["_id"] for c in clients_col.find(client_sub, {"_id": 1})]
        filt["client_id"] = {"$in": match_ids or ["__none__"]}

    # Pull ALL results (client paginates)
    cur = payments_col.find(
        filt,
        {
            "client_id": 1,
            "order_id": 1,           # <-- NEW
            "amount": 1,
            "bank_name": 1,
            "status": 1,
            "account_last4": 1,
            "proof_url": 1,
            "date": 1
        }
    ).sort("date", -1)

    docs = list(cur)

    # Batch fetch clients (map by _id)
    client_ids = list({ _to_oid(p.get("client_id")) for p in docs if _to_oid(p.get("client_id")) })
    client_map = {}
    if client_ids:
        for c in clients_col.find({"_id": {"$in": client_ids}}, {"name":1, "client_id":1, "phone":1}):
            client_map[str(c["_id"])] = c

    # Batch fetch orders referenced by these payments
    order_ids = list({ _to_oid(p.get("order_id")) for p in docs if _to_oid(p.get("order_id")) })
    order_map = {}
    if order_ids:
        for o in orders_col.find({"_id": {"$in": order_ids}}, {"order_id":1, "total_debt":1, "product":1}):
            order_map[str(o["_id"])] = o

    # Build confirmed totals per order (over the whole collection for accuracy)
    paid_map = {}
    if order_ids:
        pipe = [
            {"$match": {"order_id": {"$in": order_ids}, "status": "confirmed"}},
            {"$group": {"_id": "$order_id", "total_paid": {"$sum": "$amount"}}}
        ]
        for row in payments_col.aggregate(pipe):
            paid_map[str(row["_id"])] = _nz(row.get("total_paid"))

    # View models
    payments = []
    for p in docs:
        cid = _to_oid(p.get("client_id"))
        c   = client_map.get(str(cid)) if cid else None

        # Order details (if this payment is for an order)
        oid  = _to_oid(p.get("order_id"))
        o    = order_map.get(str(oid)) if oid else None
        ocode = (o.get("order_id") if o else None) or (str(oid)[-8:].upper() if oid else "—")
        o_total = _nz(o.get("total_debt") if o else 0)
        o_paid  = _nz(paid_map.get(str(oid), 0)) if oid else 0.0
        o_left  = max(o_total - o_paid, 0.0) if oid else 0.0

        payments.append({
            "_id":            str(p["_id"]),
            "client_name":    (c.get("name") if c else "Unknown") or "Unknown",
            "client_id_str":  (c.get("client_id") if c else "Unknown") or "Unknown",
            "phone":          (c.get("phone") if c else "Unknown") or "Unknown",
            "amount":         _nz(p.get("amount")),
            "bank_name":      p.get("bank_name") or "-",
            "account_last4":  p.get("account_last4") or "",
            "status":         p.get("status") or "pending",
            "proof_url":      p.get("proof_url") or "#",
            "date_str":       _fmt_date(p.get("date")),
            # NEW fields for the table:
            "order_code":     ocode,
            "order_total_paid": round(o_paid, 2),
            "order_amount_left": round(o_left, 2),
            "has_order":      bool(oid),
        })

    return render_template(
        "partials/payments.html",
        payments=payments,
        total_count=len(payments),
        q=q,
        default_page_size=DEFAULT_PAGE_SIZE,
    )

@payments_bp.route("/confirm_payment/<payment_id>", methods=["POST"])
def confirm_payment(payment_id):
    """
    Confirms a payment and returns updated order aggregates (if any):
    { success, order_code, total_paid, amount_left }
    """
    try:
        feedback = (request.form.get("feedback") or "").strip()
        update_fields = {"status": "confirmed", "confirmed_at": datetime.utcnow()}
        if feedback:
            update_fields["feedback"] = feedback

        # Apply the update
        result = payments_col.update_one({"_id": ObjectId(payment_id)}, {"$set": update_fields})
        if result.modified_count != 1:
            return jsonify({"success": False, "error": "No matching payment found."})

        # Fetch the payment to know which order it belongs to
        pay_doc = payments_col.find_one({"_id": ObjectId(payment_id)}, {"order_id":1})
        oid = _to_oid(pay_doc.get("order_id") if pay_doc else None)
        if not oid:
            # Truck / non-order payment
            return jsonify({"success": True})

        # Pull order + recompute aggregates
        o = orders_col.find_one({"_id": oid}, {"order_id":1, "total_debt":1})
        order_code = (o.get("order_id") if o else None) or str(oid)[-8:].upper()
        total_debt = _nz(o.get("total_debt") if o else 0)

        agg = payments_col.aggregate([
            {"$match": {"order_id": oid, "status": "confirmed"}},
            {"$group": {"_id": "$order_id", "total_paid": {"$sum": "$amount"}}}
        ])
        total_paid = 0.0
        for row in agg:
            total_paid = _nz(row.get("total_paid"))
        amount_left = max(total_debt - total_paid, 0.0)

        return jsonify({
            "success": True,
            "order_code": order_code,
            "total_paid": round(total_paid, 2),
            "amount_left": round(amount_left, 2),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
