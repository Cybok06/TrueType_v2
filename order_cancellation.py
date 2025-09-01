from flask import Blueprint, render_template, request, jsonify, session, abort
from bson import ObjectId
from datetime import datetime
from db import db

cancel_bp = Blueprint("order_cancel", __name__, template_folder="templates")

orders_col        = db["orders"]
clients_col       = db["clients"]
payments_col      = db["payments"]            # client-side payments
s_bdc_payment_col = db["s_bdc_payment"]       # central BDC payments
bdc_col           = db["bdc"]
cancellations_col = db["cancellations"]

# ✅ Optional collections (existence-checked)
_existing = set(db.list_collection_names())
bdc_txn_col = db["bdc_transactions"] if "bdc_transactions" in _existing else None
tax_col     = db["tax_records"]       if "tax_records"       in _existing else None

def _oid(v):
    try:
        return v if isinstance(v, ObjectId) else ObjectId(str(v))
    except Exception:
        return None

def _role_ok():
    return "role" in session and session["role"] in ("admin", "assistant")

@cancel_bp.route("/orders/cancel", methods=["GET"])
def page_cancel_orders():
    if not _role_ok(): abort(403)
    recent_clients = list(clients_col.find({}, {"name":1}).sort("name", 1).limit(200))
    return render_template("partials/cancel_orders.html", clients=recent_clients)

@cancel_bp.route("/orders/cancel/client/<client_id>/recent", methods=["GET"])
def recent_orders_for_client(client_id):
    if not _role_ok(): abort(403)
    coid = _oid(client_id)
    if not coid:
        return jsonify({"success": False, "error": "Invalid client id"}), 400

    orders = list(
        orders_col.find(
            {"client_id": coid},
            {
                "product":1,"region":1,"quantity":1,"date":1,"status":1,
                "delivery_status":1,"total_debt":1,"npa_status":1,"tts_status":1
            }
        ).sort("date",-1).limit(5)
    )

    # build paid_total map (confirmed only), handling ObjectId and string order_id
    paid_map = {}
    if orders:
        oid_list = [o["_id"] for o in orders]
        str_list = [str(x) for x in oid_list]
        for r in payments_col.aggregate([
            {"$match": {"order_id": {"$in": oid_list}, "status": "confirmed"}},
            {"$group": {"_id": "$order_id", "sum": {"$sum": "$amount"}}}
        ]):
            paid_map[str(r["_id"])] = float(r.get("sum") or 0.0)
        for r in payments_col.aggregate([
            {"$match": {"order_id": {"$in": str_list}, "status": "confirmed"}},
            {"$group": {"_id": "$order_id", "sum": {"$sum": "$amount"}}}
        ]):
            k = str(r["_id"])
            paid_map[k] = paid_map.get(k, 0.0) + float(r.get("sum") or 0.0)

    out = []
    for o in orders:
        k = str(o["_id"])
        out.append({
            "order_id": k,
            "product": o.get("product",""),
            "region": o.get("region",""),
            "quantity": float(o.get("quantity") or 0),
            "status": o.get("status",""),
            "delivery_status": o.get("delivery_status",""),
            "npa_status": o.get("npa_status",""),
            "tts_status": o.get("tts_status",""),
            "total_debt": float(o.get("total_debt") or 0),
            "paid_total": float(paid_map.get(k, 0.0)),
            "date": (o.get("date") or datetime.utcnow()).isoformat()
        })
    return jsonify({"success": True, "orders": out})

@cancel_bp.route("/orders/cancel/impact/<order_id>", methods=["GET"])
def cancel_impact(order_id):
    if not _role_ok(): abort(403)
    oid = _oid(order_id)
    if not oid:
        return jsonify({"success": False, "error": "Invalid order id"}), 400

    order = orders_col.find_one({"_id": oid})
    if not order:
        return jsonify({"success": False, "error": "Order not found"}), 404

    oid_str = str(oid)

    paid_cursor = payments_col.aggregate([
        {"$match": {"order_id": {"$in":[oid, oid_str]}, "status": "confirmed"}},
        {"$group": {"_id": None, "sum": {"$sum": "$amount"}, "count": {"$sum": 1}}}
    ])
    paid_doc = next(paid_cursor, None) or {"sum":0.0, "count":0}

    impact = {
        "order": {
            "status": order.get("status"),
            "delivery_status": order.get("delivery_status"),
            "npa_status": order.get("npa_status"),
            "tts_status": order.get("tts_status"),
            "total_debt": float(order.get("total_debt") or 0),
        },
        "payments": {
            "count": int(paid_doc["count"]),
            "sum": float(paid_doc["sum"] or 0.0),
        },
        "s_bdc_payment": s_bdc_payment_col.count_documents({"order_id": {"$in":[oid, oid_str]}}),
        "bdc_payment_details_hits": bdc_col.count_documents(
            {"payment_details.order_id": {"$in":[oid, oid_str]}}
        ),
        "bdc_transactions": (bdc_txn_col.count_documents({"order_id":{"$in":[oid,oid_str]}})
                             if bdc_txn_col else 0),
        "tax_records": (tax_col.count_documents({"order_id":{"$in":[oid,oid_str]}})
                        if tax_col else 0),
        "has_confirmed_payments": bool(paid_doc["count"] > 0),
        "delivered": (str(order.get("delivery_status","")).lower() == "delivered")
    }
    return jsonify({"success": True, "impact": impact})

@cancel_bp.route("/orders/cancel/refund/<order_id>", methods=["POST"])
def refund_client_payments(order_id):
    if not _role_ok(): abort(403)
    oid = _oid(order_id)
    if not oid:
        return jsonify({"success": False, "error": "Invalid order id"}), 400

    user = session.get("email") or session.get("user") or "system"
    now = datetime.utcnow()
    with db.client.start_session() as sess:
        with sess.start_transaction():
            res = payments_col.update_many(
                {"order_id": {"$in":[oid, str(oid)]}, "status": "confirmed"},
                {"$set": {"feedback": "refunded", "amount": 0.0,
                          "refunded_at": now, "refunded_by": user}},
                session=sess
            )
            orders_col.update_one({"_id": oid},{"$set": {"total_debt": 0.0}}, session=sess)
    return jsonify({"success": True, "refunded_count": res.modified_count})

@cancel_bp.route("/orders/cancel/execute/<order_id>", methods=["POST"])
def execute_cancel(order_id):
    """Delete side-effects and mark order/NPA/TTS cancelled (with audit + snapshot)."""
    if not _role_ok(): abort(403)
    data = request.get_json(silent=True) or {}
    reason = (data.get("reason") or "").strip()
    force  = bool(data.get("force"))
    if not reason:
        return jsonify({"success": False, "error": "Reason is required"}), 400

    oid = _oid(order_id)
    if not oid:
        return jsonify({"success": False, "error": "Invalid order id"}), 400

    order = orders_col.find_one({"_id": oid})
    if not order:
        return jsonify({"success": False, "error": "Order not found"}), 404

    delivered = str(order.get("delivery_status","")).lower() == "delivered"
    has_confirmed = payments_col.count_documents(
        {"order_id": {"$in":[oid, str(oid)]}, "status": "confirmed", "amount": {"$gt": 0}}
    ) > 0

    if delivered and not force:
        return jsonify({"success": False, "error": "Delivered orders require force to cancel"}), 409
    if has_confirmed and not force:
        return jsonify({"success": False, "error": "Confirmed payments exist. Refund or force to continue"}), 409

    user = session.get("email") or session.get("user") or "system"
    now = datetime.utcnow()
    oid_str = str(oid)

    # capture paid_sum (confirmed only) for snapshot
    paid_doc = payments_col.aggregate([
        {"$match": {"order_id": {"$in":[oid, oid_str]}, "status": "confirmed"}},
        {"$group": {"_id": None, "sum": {"$sum": "$amount"}}}
    ])
    paid_sum = float((next(paid_doc, {}) or {}).get("sum", 0.0) or 0.0)

    with db.client.start_session() as s:
        with s.start_transaction():
            # Delete side-effects
            sbdc_del = s_bdc_payment_col.delete_many({"order_id": {"$in":[oid, oid_str]}}, session=s)
            bdc_pull = bdc_col.update_many(
                {"payment_details.order_id": {"$in":[oid, oid_str]}},
                {"$pull": {"payment_details": {"order_id": {"$in":[oid, oid_str]}}}},
                session=s
            )
            bdc_txn_del = None
            if bdc_txn_col:
                bdc_txn_del = bdc_txn_col.delete_many({"order_id": {"$in":[oid, oid_str]}}, session=s)
            tax_del = None
            if tax_col:
                tax_del = tax_col.delete_many({"order_id": {"$in":[oid, oid_str]}}, session=s)

            # Flip order to cancelled + zero debt + NPA/TTS cancelled
            orders_col.update_one(
                {"_id": oid},
                {"$set": {
                    "status": "cancelled",
                    "delivery_status": "cancelled",
                    "npa_status": "cancelled",
                    "tts_status": "cancelled",
                    "total_debt": 0.0,
                    "cancelled_at": now,
                    "cancelled_by": user,
                    "cancel_reason": reason
                }},
                session=s
            )

            # Optional: clear S‑Tax partial fields
            orders_col.update_one(
                {"_id": oid},
                {"$unset": {
                    "s_tax_paid_amount": "",
                    "s_tax_paid_at": "",
                    "s_tax_paid_by": "",
                    "s_tax_payment": "",
                    "s_tax_reference": ""
                }},
                session=s
            )

            # Build snapshot for durable history
            client_doc = clients_col.find_one({"_id": order.get("client_id")}, {"name":1})
            snapshot = {
                "client_id": order.get("client_id"),
                "client_name": (client_doc or {}).get("name"),
                "product": order.get("product"),
                "quantity": float(order.get("quantity") or 0),
                "region": order.get("region"),
                "status_before": order.get("status"),
                "delivery_before": order.get("delivery_status"),
                "npa_before": order.get("npa_status"),
                "tts_before": order.get("tts_status"),
                "debt_before": float(order.get("total_debt") or 0),
                "paid_sum": paid_sum
            }

            # Audit record
            cancellations_col.insert_one({
                "order_id": oid,
                "reason": reason,
                "by_user": user,
                "at": now,
                "deleted": {
                    "s_bdc_payment_count": sbdc_del.deleted_count,
                    "bdc_payment_details_updates": bdc_pull.modified_count,
                    "bdc_transactions_deleted": (bdc_txn_del.deleted_count if bdc_txn_del else 0),
                    "tax_records_deleted": (tax_del.deleted_count if tax_del else 0),
                },
                "snapshot": snapshot
            }, session=s)

    return jsonify({"success": True, "message": "Order cancelled and postings removed"})

# ===== Cancellation History APIs =====

@cancel_bp.route("/orders/cancel/history", methods=["GET"])
def cancel_history():
    """Paginated history with filters: q, client_id, from, to."""
    if not _role_ok(): abort(403)

    try:
        page = max(int(request.args.get("page", 1)), 1)
        size = min(max(int(request.args.get("size", 10)), 1), 50)
    except Exception:
        page, size = 1, 10

    q = (request.args.get("q") or "").strip()
    client_id = request.args.get("client_id") or ""
    date_from = request.args.get("from") or ""
    date_to   = request.args.get("to") or ""

    filt = {}
    # date range
    dt = {}
    if date_from:
        try: dt["$gte"] = datetime.fromisoformat(date_from)
        except Exception: pass
    if date_to:
        try:
            # include entire day for "to"
            dt["$lte"] = datetime.fromisoformat(date_to) .replace(hour=23, minute=59, second=59, microsecond=999999)
        except Exception: pass
    if dt: filt["at"] = dt

    # free text (reason / product / order id / client name)
    ors = []
    if q:
        ors.append({"reason": {"$regex": q, "$options": "i"}})
        # try order id substring
        ors.append({"order_id": {"$eq": _oid(q)}} if _oid(q) else {"order_id": {"$regex": q, "$options": "i"}})
        # product/client via snapshot (if present)
        ors.append({"snapshot.product": {"$regex": q, "$options": "i"}})
        ors.append({"snapshot.client_name": {"$regex": q, "$options": "i"}})
    if ors: filt["$or"] = ors

    if client_id and _oid(client_id):
        filt["$or"] = (filt.get("$or") or []) + [
            {"snapshot.client_id": _oid(client_id)},
            {"order.client_id": _oid(client_id)}  # fallback through lookup
        ]

    pipeline = [
        {"$match": filt},
        {"$sort": {"at": -1}},
        {"$facet": {
            "data": [
                {"$skip": (page-1)*size},
                {"$limit": size},
                # look up order (live) to show product if snapshot missing
                {"$lookup": {
                    "from": "orders",
                    "localField": "order_id",
                    "foreignField": "_id",
                    "as": "order"
                }},
                {"$unwind": {"path": "$order", "preserveNullAndEmptyArrays": True}},
                # lookup client for display if needed
                {"$lookup": {
                    "from": "clients",
                    "localField": "order.client_id",
                    "foreignField": "_id",
                    "as": "client"
                }},
                {"$unwind": {"path": "$client", "preserveNullAndEmptyArrays": True}},
                # shape minimal fields to reduce payload
                {"$project": {
                    "_id": 1, "order_id": 1, "reason": 1, "by_user": 1, "at": 1, "deleted": 1,
                    "snapshot": 1, "order.product": 1, "order.quantity": 1, "order.cancelled_at":1, "order.client_id":1,
                    "client.name": 1
                }}
            ],
            "count": [{"$count": "total"}]
        }}
    ]

    res = list(cancellations_col.aggregate(pipeline))
    data = (res[0]["data"] if res else [])
    total = (res[0]["count"][0]["total"] if res and res[0]["count"] else 0)

    # Convert ObjectIds to strings for frontend safety
    for x in data:
        x["_id"] = str(x["_id"])
        if isinstance(x.get("order_id"), ObjectId):
            x["order_id"] = str(x["order_id"])

    return jsonify({"success": True, "items": data, "total": total, "page": page, "size": size})

@cancel_bp.route("/orders/cancel/history/item/<cid>", methods=["GET"])
def cancel_history_item(cid):
    """Single item detail (with lookups)."""
    if not _role_ok(): abort(403)
    oid = _oid(cid)
    if not oid: return jsonify({"success": False, "error": "Invalid id"}), 400

    pipeline = [
        {"$match": {"_id": oid}},
        {"$lookup": {"from": "orders", "localField": "order_id", "foreignField": "_id", "as": "order"}},
        {"$unwind": {"path":"$order", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {"from": "clients", "localField": "order.client_id", "foreignField": "_id", "as": "client"}},
        {"$unwind": {"path":"$client", "preserveNullAndEmptyArrays": True}},
    ]
    x = next(cancellations_col.aggregate(pipeline), None)
    if not x: return jsonify({"success": False, "error": "Not found"}), 404
    x["_id"] = str(x["_id"])
    if isinstance(x.get("order_id"), ObjectId): x["order_id"] = str(x["order_id"])
    return jsonify({"success": True, "item": x})
