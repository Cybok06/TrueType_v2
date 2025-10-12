# frontdesk/meetings.py
from flask import Blueprint, render_template, request, jsonify
from bson import ObjectId
from datetime import datetime
from db import db  # ✅ uses your existing Mongo connection: truetype DB

MEETINGS = db["meetings"]

meetings_bp = Blueprint(
    "meetings_bp",
    __name__,
    url_prefix="/frontdesk",  # routes -> /frontdesk/...
)

# ---------- Helpers ----------
def _to_dt(dt_str: str):
    """Accepts 'YYYY-MM-DDTHH:MM' or ISO string; returns datetime or None."""
    if not dt_str:
        return None
    try:
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M")
    except Exception:
        pass
    try:
        return datetime.fromisoformat(dt_str.replace("Z", ""))
    except Exception:
        return None

def _to_attendees(value):
    """Accept 'name, name2' or list; return clean list."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(a).strip() for a in value if str(a).strip()]
    # string
    return [a.strip() for a in str(value).split(",") if a.strip()]

def _serialize(doc):
    return {
        "id": str(doc["_id"]),
        "title": doc.get("title"),
        "description": doc.get("description"),
        "location": doc.get("location"),
        "start": doc.get("start").isoformat() if doc.get("start") else None,
        "end": doc.get("end").isoformat() if doc.get("end") else None,
        "color": doc.get("color") or None,
        "category": doc.get("category") or "",
        "attendees": doc.get("attendees") or [],
    }

# ---------- Page ----------
@meetings_bp.get("/meetings")
def meetings_page():
    return render_template("frontdesk_pages/meetings.html")

# ---------- API: list & create ----------
@meetings_bp.route("/api/meetings", methods=["GET", "POST"])
def meetings_collection():
    if request.method == "GET":
        start_q = request.args.get("start")
        end_q = request.args.get("end")
        query = {}
        if start_q and end_q:
            try:
                s = datetime.fromisoformat(start_q.replace("Z", ""))
                e = datetime.fromisoformat(end_q.replace("Z", ""))
                query = {"start": {"$lt": e}, "end": {"$gt": s}}
            except Exception:
                query = {}

        docs = list(MEETINGS.find(query).sort("start", 1))
        return jsonify([_serialize(d) for d in docs])

    # POST (create)
    data = request.get_json(force=True)
    title = (data.get("title") or "").strip()
    start = _to_dt(data.get("start"))
    end = _to_dt(data.get("end"))
    if not title or not start or not end:
        return jsonify({"error": "title, start, and end are required"}), 400

    doc = {
        "title": title,
        "description": (data.get("description") or "").strip(),
        "location": (data.get("location") or "").strip(),
        "start": start,
        "end": end,
        "color": (data.get("color") or "").strip() or None,
        "category": (data.get("category") or "").strip(),
        "attendees": _to_attendees(data.get("attendees")),
        "created_at": datetime.utcnow(),
    }
    inserted = MEETINGS.insert_one(doc)
    doc["_id"] = inserted.inserted_id
    return jsonify(_serialize(doc)), 201

# ---------- API: read / update / delete ----------
@meetings_bp.route("/api/meetings/<id>", methods=["GET", "PUT", "DELETE"])
def meetings_resource(id):
    try:
        _id = ObjectId(id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400

    if request.method == "GET":
        doc = MEETINGS.find_one({"_id": _id})
        if not doc:
            return jsonify({"error": "not found"}), 404
        return jsonify(_serialize(doc))

    if request.method == "DELETE":
        res = MEETINGS.delete_one({"_id": _id})
        if res.deleted_count == 0:
            return jsonify({"error": "not found"}), 404
        return jsonify({"ok": True})

    # PUT (update)
    data = request.get_json(force=True)
    updates = {}
    for key in ["title", "description", "location", "color", "category"]:
        if key in data:
            updates[key] = (data.get(key) or "").strip()

    if "attendees" in data:
        updates["attendees"] = _to_attendees(data.get("attendees"))

    if "start" in data:
        st = _to_dt(data.get("start"))
        if not st:
            return jsonify({"error": "invalid start"}), 400
        updates["start"] = st

    if "end" in data:
        en = _to_dt(data.get("end"))
        if not en:
            return jsonify({"error": "invalid end"}), 400
        updates["end"] = en

    if not updates:
        return jsonify({"error": "no fields to update"}), 400

    res = MEETINGS.update_one({"_id": _id}, {"$set": updates})
    if res.matched_count == 0:
        return jsonify({"error": "not found"}), 404
    doc = MEETINGS.find_one({"_id": _id})
    return jsonify(_serialize(doc))
