from flask import Blueprint, render_template, jsonify, request
from bson import ObjectId
from datetime import datetime
from db import db

# Collections
payments_col = db["payments"]
clients_col  = db["clients"]

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

@payments_bp.route("/payments")
def view_payments():
    # Search term (only search is server-side; pagination will be client-side)
    q = (request.args.get("q") or "").strip()

    filt = {}
    if q:
        client_sub = {"$or": [
            {"name":     {"$regex": q, "$options": "i"}},
            {"phone":    {"$regex": q, "$options": "i"}},
            {"client_id":{"$regex": q, "$options": "i"}},
        ]}
        match_ids = [c["_id"] for c in clients_col.find(client_sub, {"_id": 1})]
        filt["client_id"] = {"$in": match_ids or ["__none__"]}

    # Pull ALL results (client paginates)
    cur = payments_col.find(
        filt,
        {
            "client_id": 1,
            "amount": 1,
            "bank_name": 1,
            "status": 1,
            "account_last4": 1,
            "proof_url": 1,
            "date": 1
        }
    ).sort("date", -1)

    docs = list(cur)

    # Batch fetch clients
    client_ids = list({ _to_oid(p.get("client_id")) for p in docs if _to_oid(p.get("client_id")) })
    client_map = {}
    if client_ids:
        for c in clients_col.find({"_id": {"$in": client_ids}}, {"name":1, "client_id":1, "phone":1}):
            client_map[str(c["_id"])] = c

    # View models
    payments = []
    for p in docs:
        cid = _to_oid(p.get("client_id"))
        c   = client_map.get(str(cid)) if cid else None
        payments.append({
            "_id":            str(p["_id"]),
            "client_name":    (c.get("name") if c else "Unknown") or "Unknown",
            "client_id_str":  (c.get("client_id") if c else "Unknown") or "Unknown",
            "phone":          (c.get("phone") if c else "Unknown") or "Unknown",
            "amount":         float(p.get("amount") or 0),
            "bank_name":      p.get("bank_name") or "-",
            "account_last4":  p.get("account_last4") or "",
            "status":         p.get("status") or "pending",
            "proof_url":      p.get("proof_url") or "#",
            "date_str":       _fmt_date(p.get("date")),
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
    try:
        feedback = (request.form.get("feedback") or "").strip()
        update_fields = {"status": "confirmed", "confirmed_at": datetime.utcnow()}
        if feedback:
            update_fields["feedback"] = feedback

        result = payments_col.update_one({"_id": ObjectId(payment_id)}, {"$set": update_fields})
        if result.modified_count == 1:
            return jsonify({"success": True})
        return jsonify({"success": False, "error": "No matching payment found."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
