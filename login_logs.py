# login_logs.py
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, abort, session
from bson import ObjectId
from datetime import datetime, timezone, timedelta
from db import db

login_logs_bp = Blueprint("login_logs_bp", __name__, template_folder="templates")

LOGS = db["login_logs"]

# --- Security helpers (keep in sync with your app) ---
def _current_is_superadmin():
    return (session.get("username") == "admin") or bool(session.get("is_superadmin"))

def _require_superadmin():
    if not _current_is_superadmin():
        abort(403)

# --- Ensure helpful indexes (safe if already exist) ---
try:
    LOGS.create_index([("ts", -1)])
    LOGS.create_index([("who.username", 1), ("ts", -1)])
    LOGS.create_index([("req.ip", 1), ("ts", -1)])
    LOGS.create_index([("result.success", 1), ("ts", -1)])
except Exception:
    pass

# --- Purge helper: delete logs older than 30 days ---
def _purge_old():
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    try:
        LOGS.delete_many({"ts": {"$lt": cutoff}})
    except Exception:
        # never break the page because of maintenance
        pass

# --- Utility: convert ObjectId to str for template ---
def _to_view(doc):
    if not doc:
        return doc
    d = dict(doc)
    d["_id"] = str(d.get("_id"))
    return d

# --- View + auto-purge route ---
@login_logs_bp.get("/logs/login")
def login_logs():
    _require_superadmin()
    _purge_old()  # auto-purge on every view

    # Filters
    q_username = (request.args.get("username") or "").strip()
    q_ip       = (request.args.get("ip") or "").strip()
    q_success  = (request.args.get("success") or "").strip().lower()  # "", "true", "false"
    q_reason   = (request.args.get("reason") or "").strip().lower()

    find = {}
    if q_username:
        find["who.username"] = {"$regex": f"{q_username}", "$options": "i"}
    if q_ip:
        find["req.ip"] = {"$regex": f"{q_ip}", "$options": "i"}
    if q_success in ("true", "false"):
        find["result.success"] = (q_success == "true")
    if q_reason:
        find["result.reason"] = {"$regex": f"{q_reason}", "$options": "i"}

    # Pagination
    try:
        page = max(1, int(request.args.get("page", "1")))
    except Exception:
        page = 1
    try:
        per_page = int(request.args.get("per_page", "50"))
        per_page = max(10, min(per_page, 200))
    except Exception:
        per_page = 50

    skip = (page - 1) * per_page
    cursor = LOGS.find(find).sort("ts", -1).skip(skip).limit(per_page + 1)
    rows = list(cursor)

    has_next = len(rows) > per_page
    if has_next:
        rows = rows[:per_page]

    rows = [_to_view(r) for r in rows]

    return render_template(
        "login_logs.html",
        rows=rows,
        page=page,
        per_page=per_page,
        has_next=has_next,
        q_username=q_username,
        q_ip=q_ip,
        q_success=q_success,
        q_reason=q_reason
    )

# --- Manual purge endpoint (kept simple) ---
@login_logs_bp.post("/logs/login/purge")
def purge_login_logs():
    _require_superadmin()
    _purge_old()
    # stay on same filter/page if present
    return redirect(url_for("login_logs_bp.login_logs", **request.args))

# --- Detail as JSON for modal ---
@login_logs_bp.get("/logs/login/<log_id>/json")
def login_log_json(log_id):
    _require_superadmin()
    try:
        oid = ObjectId(log_id)
    except Exception:
        return jsonify({"ok": False, "error": "invalid id"}), 400
    doc = LOGS.find_one({"_id": oid})
    if not doc:
        return jsonify({"ok": False, "error": "not found"}), 404

    # Convert ObjectId fields to strings to make them JSON-friendly
    if "session_user_id" in doc and isinstance(doc["session_user_id"], ObjectId):
        doc["session_user_id"] = str(doc["session_user_id"])
    if "client_id" in doc and isinstance(doc["client_id"], ObjectId):
        doc["client_id"] = str(doc["client_id"])

    doc["_id"] = str(doc["_id"])
    # Convert datetime to ISO for the UI
    if isinstance(doc.get("ts"), datetime):
        doc["ts"] = doc["ts"].astimezone(timezone.utc).isoformat()

    return jsonify({"ok": True, "log": doc})
