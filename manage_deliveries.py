from flask import Blueprint, render_template, request, jsonify, url_for
from bson import ObjectId
from db import db
from datetime import datetime
from urllib.parse import urlencode
import math

manage_deliveries_bp = Blueprint("manage_deliveries", __name__, template_folder="templates")

orders_collection = db["orders"]
clients_collection = db["clients"]
bdc_collection = db["bdc"]

STATUS_OPTIONS = [
    "Ordered", "Approved", "GoodStanding", "Depot Manager",
    "BRV check pass", "BRV check unpass", "Loading", "Loaded",
    "Moved", "Released",
]

def _safe_oid(val):
    try:
        return ObjectId(val)
    except Exception:
        return None

def _qargs_with(**overrides):
    args = request.args.to_dict(flat=True)
    args.update({k: v for k, v in overrides.items() if v is not None})
    args = {k: v for k, v in args.items() if v is not None}
    return "?" + urlencode(args)

@manage_deliveries_bp.route("/deliveries", methods=["GET"])
def view_deliveries():
    filters = {"status": "approved"}

    region = request.args.get("region")
    if region: filters["region"] = region

    bdc_name = request.args.get("bdc")
    if bdc_name: filters["bdc_name"] = bdc_name

    tts = request.args.get("tts")
    if tts: filters["tts_status"] = tts

    npa = request.args.get("npa")
    if npa: filters["npa_status"] = npa

    # ---------- Pagination (default 20 per page) ----------
    try:
        page = max(int(request.args.get("page", 1)), 1)
    except Exception:
        page = 1

    try:
        per_page = int(request.args.get("per_page", 20))   # default 20
    except Exception:
        per_page = 20
    per_page = max(20, min(per_page, 200))                 # clamp: 20..200
    # ------------------------------------------------------

    total = orders_collection.count_documents(filters)
    pages = max(1, math.ceil(total / per_page))
    if page > pages:
        page = pages

    skip = (page - 1) * per_page
    limit = per_page

    projection = {
        "_id": 1, "client_id": 1, "bdc_name": 1, "product": 1,
        "vehicle_number": 1, "driver_name": 1, "driver_phone": 1,
        "quantity": 1, "region": 1, "depot": 1,
        "delivery_status": 1, "tts_status": 1, "npa_status": 1,
        "date": 1, "delivered_date": 1,
    }

    cursor = (
        orders_collection.find(filters, projection)
        .sort("date", -1).skip(skip).limit(limit)
    )
    orders = list(cursor)

    # Page-scoped client lookup
    client_ids = []
    for o in orders:
        cid = o.get("client_id")
        if isinstance(cid, ObjectId):
            client_ids.append(cid)
        else:
            oid = _safe_oid(cid)
            if oid: client_ids.append(oid)
    client_ids = list(set(client_ids))

    client_map = {
        str(c["_id"]): c.get("name", "Unknown")
        for c in clients_collection.find({"_id": {"$in": client_ids}}, {"name": 1})
    }

    deliveries, pending_count, delivered_count = [], 0, 0
    for order in orders:
        legacy_status = str(order.get("delivery_status", "pending")).lower()
        if legacy_status == "delivered": delivered_count += 1
        else: pending_count += 1

        cid_str = str(order.get("client_id") or "")
        deliveries.append({
            "order_id": str(order["_id"]),
            "bdc_name": order.get("bdc_name", "Unknown BDC"),
            "client_name": client_map.get(cid_str, "Unknown"),
            "product": order.get("product", ""),
            "vehicle_number": order.get("vehicle_number", ""),
            "driver_name": order.get("driver_name", ""),
            "driver_phone": order.get("driver_phone", ""),
            "quantity": order.get("quantity", ""),
            "region": order.get("region", ""),
            "depot": order.get("depot", ""),
            "delivery_status": legacy_status,
            "tts_status": order.get("tts_status"),
            "npa_status": order.get("npa_status"),
            "date": order.get("date"),
            "delivered_date": order.get("delivered_date"),
        })

    # Filter dropdown data (from all approved)
    regions = sorted([r for r in orders_collection.distinct("region", {"status": "approved"}) if r])
    bdcs    = sorted([b for b in orders_collection.distinct("bdc_name", {"status": "approved"}) if b])

    first_item = 0 if total == 0 else skip + 1
    last_item  = min(skip + len(orders), total)

    window = 2
    start = max(1, page - window)
    end   = min(pages, page + window)
    page_numbers = list(range(start, end + 1))

    base_url = url_for("manage_deliveries.view_deliveries")
    def page_url(p):
        return base_url + _qargs_with(page=p, per_page=per_page)

    pagination = {
        "page": page,
        "per_page": per_page,
        "total": total,
        "pages": pages,
        "first_item": first_item,
        "last_item": last_item,
        "has_prev": page > 1,
        "has_next": page < pages,
        "prev_url": page_url(page - 1) if page > 1 else None,
        "next_url": page_url(page + 1) if page < pages else None,
        "first_url": page_url(1),
        "last_url": page_url(pages),
        "page_numbers": page_numbers,
        "page_url": page_url,
    }

    return render_template(
        "partials/manage_deliveries.html",
        deliveries=deliveries,
        regions=regions,
        bdcs=bdcs,
        status_options=STATUS_OPTIONS,
        summary={"pending": pending_count, "delivered": delivered_count},
        pagination=pagination,
    )

@manage_deliveries_bp.route("/deliveries/update_status/<order_id>", methods=["POST"])
def update_delivery_status(order_id):
    tts_status = (request.form.get("tts_status") or "").strip()
    npa_status = (request.form.get("npa_status") or "").strip()

    if not tts_status and not npa_status:
        return jsonify({"success": False, "message": "Provide at least one of TTS or NPA status."}), 400

    oid = _safe_oid(order_id)
    if not oid:
        return jsonify({"success": False, "message": "Invalid order id."}), 400

    order = orders_collection.find_one({"_id": oid}, {"depot": 1, "driver_name": 1})
    if not order:
        return jsonify({"success": False, "message": "Order not found."}), 404

    update_fields = {}
    if tts_status: update_fields["tts_status"] = tts_status
    if npa_status: update_fields["npa_status"] = npa_status

    history_entry = {
        "tts_status": tts_status or None,
        "npa_status": npa_status or None,
        "depot": order.get("depot", None),
        "driver_name": order.get("driver_name", None),
        "timestamp": datetime.utcnow(),
    }

    orders_result = orders_collection.update_one(
        {"_id": oid},
        {"$set": update_fields, "$push": {"delivery_history": history_entry}},
    )

    bdc_result_1 = bdc_collection.update_one(
        {"payment_details.order_id": oid},
        {"$set": {"payment_details.$.tts_status": tts_status or None,
                  "payment_details.$.npa_status": npa_status or None}},
    )
    bdc_result_2 = bdc_collection.update_one(
        {"payment_details.order_id": str(oid)},
        {"$set": {"payment_details.$.tts_status": tts_status or None,
                  "payment_details.$.npa_status": npa_status or None}},
    )

    if orders_result.modified_count == 1 or bdc_result_1.modified_count == 1 or bdc_result_2.modified_count == 1:
        return jsonify({"success": True, "message": "Statuses updated."})
    else:
        return jsonify({"success": False, "message": "No update made."})

@manage_deliveries_bp.route("/deliveries/history/<order_id>", methods=["GET"])
def get_delivery_history(order_id):
    oid = _safe_oid(order_id)
    if not oid:
        return jsonify({"success": False, "message": "Invalid order id."}), 400

    try:
        order = orders_collection.find_one(
            {"_id": oid}, {"delivery_history": 1, "depot": 1, "driver_name": 1}
        )
        history = order.get("delivery_history", []) if order else []
        sorted_history = sorted(history, key=lambda x: x.get("timestamp", datetime.min), reverse=True)

        depot = order.get("depot") if order else None
        driver_name = order.get("driver_name") if order else None

        return jsonify({
            "success": True,
            "history": [{
                "tts_status": h.get("tts_status"),
                "npa_status": h.get("npa_status"),
                "depot": h.get("depot") if h.get("depot") is not None else depot,
                "driver_name": h.get("driver_name") if h.get("driver_name") is not None else driver_name,
                "timestamp": (h.get("timestamp") or datetime.utcnow()).strftime("%Y-%m-%d %H:%M:%S"),
            } for h in sorted_history],
        })
    except Exception:
        return jsonify({"success": False, "message": "Error fetching history."}), 500
