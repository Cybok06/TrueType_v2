from flask import Blueprint, render_template, request, jsonify
from bson import ObjectId
from db import db
from datetime import datetime

manage_deliveries_bp = Blueprint("manage_deliveries", __name__, template_folder="templates")

orders_collection = db["orders"]
clients_collection = db["clients"]
bdc_collection = db["bdc"]

# Shared status options for both TTS and NPA
STATUS_OPTIONS = [
    "Ordered",
    "Approved",
    "GoodStanding",
    "Depot Manager",
    "BRV check pass",
    "BRV check unpass",
    "Loading",
    "Loaded",
    "Moved",
    "Released",
]

def _safe_oid(val):
    try:
        return ObjectId(val)
    except Exception:
        return None

@manage_deliveries_bp.route("/deliveries", methods=["GET"])
def view_deliveries():
    # Keep your business rule for visible orders
    filters = {"status": "approved"}

    # Optional filters
    region = request.args.get("region")
    if region:
        filters["region"] = region

    bdc_name = request.args.get("bdc")
    if bdc_name:
        filters["bdc_name"] = bdc_name

    tts = request.args.get("tts")
    if tts:
        filters["tts_status"] = tts

    npa = request.args.get("npa")
    if npa:
        filters["npa_status"] = npa

    # Fetch only needed fields
    projection = {
        "_id": 1,
        "client_id": 1,
        "bdc_name": 1,
        "product": 1,
        "vehicle_number": 1,
        "driver_name": 1,
        "driver_phone": 1,
        "quantity": 1,
        "region": 1,
        "depot": 1,                    # NEW: include depot
        "delivery_status": 1,          # legacy single delivery status for summary cards
        "tts_status": 1,
        "npa_status": 1,
        "date": 1,
        "delivered_date": 1,
    }

    orders_cursor = orders_collection.find(filters, projection).sort("date", -1)
    orders = list(orders_cursor)

    # Batch-fetch clients (handle both ObjectId and string IDs safely)
    client_ids = []
    for o in orders:
        cid = o.get("client_id")
        if isinstance(cid, ObjectId):
            client_ids.append(cid)
        else:
            oid = _safe_oid(cid)
            if oid:
                client_ids.append(oid)

    client_map = {
        str(c["_id"]): c.get("name", "Unknown")
        for c in clients_collection.find({"_id": {"$in": list(set(client_ids))}}, {"name": 1})
    }

    deliveries = []
    pending_count = 0
    delivered_count = 0

    for order in orders:
        # legacy delivery_status used for summary
        legacy_status = str(order.get("delivery_status", "pending")).lower()
        if legacy_status == "delivered":
            delivered_count += 1
        else:
            pending_count += 1

        cid = order.get("client_id")
        cid_str = str(cid) if cid is not None else ""

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
            "depot": order.get("depot", ""),                # NEW: send depot to template
            "delivery_status": legacy_status,
            "tts_status": order.get("tts_status"),
            "npa_status": order.get("npa_status"),
            "date": order.get("date"),
            "delivered_date": order.get("delivered_date"),
        })

    regions = sorted(set(d["region"] for d in deliveries if d["region"]))
    bdcs = sorted(set(d["bdc_name"] for d in deliveries if d["bdc_name"]))

    return render_template(
        "partials/manage_deliveries.html",
        deliveries=deliveries,
        regions=regions,
        bdcs=bdcs,
        status_options=STATUS_OPTIONS,
        summary={"pending": pending_count, "delivered": delivered_count},
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

    # Fetch order (we'll also include depot/driver_name into history snapshot)
    order = orders_collection.find_one({"_id": oid}, {"depot": 1, "driver_name": 1})
    if not order:
        return jsonify({"success": False, "message": "Order not found."}), 404

    update_fields = {}
    if tts_status:
        update_fields["tts_status"] = tts_status
    if npa_status:
        update_fields["npa_status"] = npa_status

    # History entry (combined) + snapshot of depot/driver
    history_entry = {
        "tts_status": tts_status if tts_status else None,
        "npa_status": npa_status if npa_status else None,
        "depot": order.get("depot", None),             # NEW
        "driver_name": order.get("driver_name", None), # NEW
        "timestamp": datetime.utcnow(),
    }

    # Apply updates
    orders_result = orders_collection.update_one(
        {"_id": oid},
        {
            "$set": update_fields,
            "$push": {"delivery_history": history_entry},
        },
    )

    # Optional: reflect in bdc.payment_details (if you keep this mirror)
    bdc_result_1 = bdc_collection.update_one(
        {"payment_details.order_id": oid},
        {"$set": {
            "payment_details.$.tts_status": tts_status if tts_status else None,
            "payment_details.$.npa_status": npa_status if npa_status else None,
        }},
    )
    bdc_result_2 = bdc_collection.update_one(
        {"payment_details.order_id": str(oid)},
        {"$set": {
            "payment_details.$.tts_status": tts_status if tts_status else None,
            "payment_details.$.npa_status": npa_status if npa_status else None,
        }},
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
        # Include depot/driver_name for the modal
        order = orders_collection.find_one(
            {"_id": oid},
            {"delivery_history": 1, "depot": 1, "driver_name": 1},
        )
        history = order.get("delivery_history", []) if order else []
        sorted_history = sorted(history, key=lambda x: x.get("timestamp", datetime.min), reverse=True)

        depot = order.get("depot") if order else None
        driver_name = order.get("driver_name") if order else None

        return jsonify({
            "success": True,
            "history": [
                {
                    "tts_status": h.get("tts_status"),
                    "npa_status": h.get("npa_status"),
                    # Prefer per-entry snapshot if present; else fall back to current order fields
                    "depot": h.get("depot") if h.get("depot") is not None else depot,                # NEW
                    "driver_name": h.get("driver_name") if h.get("driver_name") is not None else driver_name,  # NEW
                    "timestamp": (h.get("timestamp") or datetime.utcnow()).strftime("%Y-%m-%d %H:%M:%S"),
                }
                for h in sorted_history
            ],
        })
    except Exception:
        return jsonify({"success": False, "message": "Error fetching history."}), 500
