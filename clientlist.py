from flask import Blueprint, render_template, request, jsonify, session
from bson import ObjectId
from db import db
from datetime import datetime

clientlist_bp = Blueprint('clientlist', __name__, template_folder='templates')

clients_collection = db["clients"]
deleted_collection = db["deleted"]

ALLOWED_STATUSES = {"active", "inactive", "blocked"}


# ---------------------------
# Helpers
# ---------------------------
def _as_object_id(id_str):
    if not id_str or not ObjectId.is_valid(id_str):
        return None
    return ObjectId(id_str)

def _actor():
    return {
        "username": session.get("username", "unknown"),
        "role": session.get("role", "unknown"),
    }

def _log_client_document(client_id, action, note=None, extra=None):
    """
    Append an action log entry into client's 'documents' array.
    This satisfies 'post it in their documents'.
    """
    entry = {
        "type": "client_action",
        "action": action,                          # e.g., 'status_change', 'update', 'delete'
        "by": _actor(),                            # who did it
        "timestamp": datetime.utcnow(),
    }
    if note:
        entry["note"] = note
    if isinstance(extra, dict) and extra:
        entry["meta"] = extra

    clients_collection.update_one(
        {"_id": client_id},
        {"$push": {"documents": entry}}
    )


# ---------------------------
# Views
# ---------------------------

# ✅ Render shared client list partial
@clientlist_bp.route('/client_list_partial')
def client_list_partial():
    role = session.get('role', 'admin')
    clients = list(clients_collection.find().sort("date_registered", -1))
    return render_template('partials/client_list.html', clients=clients, role=role)


# ✅ Load clients with pagination, search, filter (server-side)
@clientlist_bp.route('/clients/load', methods=['GET'])
def load_clients():
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 20))
        page = max(page, 1)
        per_page = max(min(per_page, 200), 1)  # cap to prevent abuse
    except ValueError:
        page, per_page = 1, 20

    skip = (page - 1) * per_page

    search = (request.args.get('search') or '').strip()
    status = (request.args.get('status') or '').strip().lower()
    start_date = (request.args.get('start_date') or '').strip()
    end_date = (request.args.get('end_date') or '').strip()

    query = {}

    if search:
        # case-insensitive regex for name/phone/client_id/location
        regex = {"$regex": search, "$options": "i"}
        query["$or"] = [
            {"name": regex},
            {"phone": regex},
            {"client_id": regex},
            {"location": regex},
        ]

    if status in ALLOWED_STATUSES:
        query["status"] = status

    if start_date and end_date:
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            # include the end day entirely
            end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999000)
            query["date_registered"] = {"$gte": start, "$lte": end}
        except ValueError:
            pass

    total = clients_collection.count_documents(query)
    cursor = clients_collection.find(query).sort("date_registered", -1).skip(skip).limit(per_page)
    clients = []
    for c in cursor:
        c["_id"] = str(c["_id"])
        if c.get("date_registered"):
            try:
                c["date_registered"] = c["date_registered"].strftime('%Y-%m-%d')
            except Exception:
                # if it was stored as str already, leave it
                pass
        clients.append(c)

    return jsonify({
        "items": clients,
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": (total + per_page - 1) // per_page,
        "has_next": (skip + per_page) < total,
        "has_prev": page > 1
    })


# ✅ Update client info (partial update – from modal or programmatically)
@clientlist_bp.route('/clients/update', methods=['POST'])
def update_client():
    # accept form OR JSON
    payload = request.form if request.form else (request.get_json(silent=True) or {})
    client_id = payload.get('id')
    oid = _as_object_id(client_id)
    if not oid:
        return jsonify(success=False, error="Invalid client ID"), 400

    # Build $set only for provided fields
    set_fields = {}
    name = payload.get('name')
    phone = payload.get('phone')
    status = payload.get('status')

    if name is not None:
        set_fields["name"] = name.strip()
    if phone is not None:
        set_fields["phone"] = phone.strip()
    if status is not None:
        status = status.strip().lower()
        if status not in ALLOWED_STATUSES:
            return jsonify(success=False, error=f"Invalid status. Allowed: {', '.join(ALLOWED_STATUSES)}"), 400
        set_fields["status"] = status

    if not set_fields:
        return jsonify(success=False, error="No fields to update"), 400

    result = clients_collection.update_one({"_id": oid}, {"$set": set_fields})

    if result.matched_count == 0:
        return jsonify(success=False, error="Client not found"), 404

    # Post to client's documents (audit log)
    _log_client_document(
        oid,
        action="update",
        note="Client fields updated",
        extra={"changed_fields": list(set_fields.keys()), "new_values": set_fields}
    )

    return jsonify(success=True, updated=set_fields)


# ✅ Status-only endpoint (Activate / Deactivate / Block / Unblock)
@clientlist_bp.route('/clients/status/<client_id>', methods=['POST'])
def set_client_status(client_id):
    oid = _as_object_id(client_id)
    if not oid:
        return jsonify(success=False, error="Invalid client ID"), 400

    # accept form OR JSON
    payload = request.form if request.form else (request.get_json(silent=True) or {})
    status = (payload.get('status') or '').strip().lower()

    if status not in ALLOWED_STATUSES:
        return jsonify(success=False, error=f"Invalid status. Allowed: {', '.join(ALLOWED_STATUSES)}"), 400

    result = clients_collection.update_one({"_id": oid}, {"$set": {"status": status}})
    if result.matched_count == 0:
        return jsonify(success=False, error="Client not found"), 404

    # Log
    _log_client_document(
        oid,
        action="status_change",
        note=f"Status set to {status}",
        extra={"status": status}
    )

    return jsonify(success=True, status=status)


# ✅ Bulk status (optional – pass JSON: { ids: [...], status: "active|inactive|blocked" })
@clientlist_bp.route('/clients/status/bulk', methods=['POST'])
def bulk_status():
    data = request.get_json(silent=True) or {}
    ids = data.get("ids") or []
    status = (data.get("status") or "").strip().lower()

    if not ids:
        return jsonify(success=False, error="No ids provided"), 400
    if status not in ALLOWED_STATUSES:
        return jsonify(success=False, error=f"Invalid status. Allowed: {', '.join(ALLOWED_STATUSES)}"), 400

    oids = [_as_object_id(i) for i in ids if _as_object_id(i)]
    if not oids:
        return jsonify(success=False, error="No valid ObjectIds"), 400

    res = clients_collection.update_many({"_id": {"$in": oids}}, {"$set": {"status": status}})

    # Log per client (keeps audit accurate)
    for oid in oids:
        _log_client_document(
            oid,
            action="status_change",
            note=f"Bulk status set to {status}",
            extra={"status": status}
        )

    return jsonify(success=True, matched=res.matched_count, modified=res.modified_count, status=status)


# ✅ Delete (archive) client
@clientlist_bp.route('/clients/delete/<client_id>', methods=['POST'])
def delete_client(client_id):
    oid = _as_object_id(client_id)
    if not oid:
        return jsonify(success=False, error="Invalid client ID"), 400

    client = clients_collection.find_one({"_id": oid})
    if not client:
        return jsonify(success=False, error="Client not found"), 404

    # add deletion metadata
    client["deleted_by"] = {
        **_actor(),
        "timestamp": datetime.utcnow()
    }

    # write delete log to documents before moving
    try:
        _log_client_document(oid, action="delete", note="Client archived to deleted collection")
    except Exception:
        # If logging fails, continue with deletion
        pass

    deleted_collection.insert_one(client)
    clients_collection.delete_one({"_id": oid})

    return jsonify(success=True)
