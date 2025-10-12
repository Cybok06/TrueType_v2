# frontdesk/admin_meetings.py
from flask import Blueprint, render_template, request, jsonify
from bson import ObjectId
from datetime import datetime
from db import db  # uses your existing Mongo connection

MEETINGS = db["meetings"]

# Exported blueprint (this is what app.py should import)
admin_meetings_bp = Blueprint(
    "admin_meetings_bp",
    __name__,
    url_prefix="/admin",
)

# ----------------- Utilities -----------------
def _serialize(doc):
    return {
        "id": str(doc["_id"]),
        "title": doc.get("title") or "",
        "description": doc.get("description") or "",
        "location": doc.get("location") or "",
        "start": doc.get("start").isoformat() if doc.get("start") else None,
        "end": doc.get("end").isoformat() if doc.get("end") else None,
        "color": doc.get("color") or None,
        "category": doc.get("category") or "",
        "attendees": doc.get("attendees") or [],
    }

def _parse_iso(dt_str):
    if not dt_str:
        return None
    try:
        # Accept YYYY-MM-DD or full ISO
        if len(dt_str) == 10:
            return datetime.strptime(dt_str, "%Y-%m-%d")
        # tolerate trailing 'Z'
        return datetime.fromisoformat(dt_str.replace("Z", ""))
    except Exception:
        return None

# ----------------- Pages -----------------
@admin_meetings_bp.get("/meetings")
def admin_meetings_page():
    # template: templates/admin_pages/admin_meetings.html
    return render_template("admin_pages/admin_meetings.html")

# ----------------- APIs (read-only) -----------------
@admin_meetings_bp.get("/api/meetings")
def api_list_meetings():
    """
    Optional query params:
      q, start, end, category, status(upcoming|ongoing|ended)
    """
    qtext = (request.args.get("q") or "").strip().lower()
    cat = (request.args.get("category") or "").strip()
    status = (request.args.get("status") or "").strip().lower()
    start_q = _parse_iso(request.args.get("start"))
    end_q = _parse_iso(request.args.get("end"))

    mongo_q = {}
    if start_q and end_q:
        mongo_q["start"] = {"$lt": end_q}
        mongo_q["end"] = {"$gt": start_q}
    elif start_q:
        mongo_q["end"] = {"$gte": start_q}
    elif end_q:
        mongo_q["start"] = {"$lte": end_q}
    if cat:
        mongo_q["category"] = cat

    docs = list(MEETINGS.find(mongo_q).sort("start", 1))
    items = [_serialize(d) for d in docs]

    if qtext:
        def match_text(it):
            hay = " ".join([
                it.get("title",""), it.get("location",""),
                it.get("description",""),
                " ".join(it.get("attendees") or [])
            ]).lower()
            return qtext in hay
        items = [i for i in items if match_text(i)]

    if status in {"upcoming","ongoing","ended"}:
        now = datetime.utcnow().timestamp() * 1000
        def state(it):
            if not it.get("start") or not it.get("end"):
                return "unknown"
            st = datetime.fromisoformat(it["start"]).timestamp()*1000
            en = datetime.fromisoformat(it["end"]).timestamp()*1000
            if now < st: return "upcoming"
            if now > en: return "ended"
            return "ongoing"
        items = [i for i in items if state(i) == status]

    return jsonify(items)

@admin_meetings_bp.get("/api/meetings/<id>")
def api_get_meeting(id):
    try:
        _id = ObjectId(id)
    except Exception:
        return jsonify({"error":"invalid id"}), 400
    doc = MEETINGS.find_one({"_id": _id})
    if not doc:
        return jsonify({"error":"not found"}), 404
    return jsonify(_serialize(doc))

# Make explicit what this module exports
__all__ = ["admin_meetings_bp"]
