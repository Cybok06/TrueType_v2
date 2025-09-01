from flask import Blueprint, render_template, request, jsonify, session, abort
from bson import ObjectId
from werkzeug.security import generate_password_hash
from db import db
import re
import json

users_bp = Blueprint("users_bp", __name__)
USERS = db["users"]

# ---- Permission catalog ----
PERMISSIONS = [
    {"slug": "dashboard:view",   "label": "Dashboard"},
    {"slug": "clients:register", "label": "Register Client"},
    {"slug": "clients:view",     "label": "View Clients"},
    {"slug": "orders:review",    "label": "Review Orders"},
    {"slug": "orders:approved",  "label": "Approved Orders"},
    {"slug": "payments:receive", "label": "Receive Payments"},
    {"slug": "omc:view",         "label": "OMC Companies"},
    {"slug": "bdc:view",         "label": "BDC Accounts"},
    {"slug": "tax:view",         "label": "Tax (OMC)"},
    {"slug": "bank:view",        "label": "Bank Accounts"},
    {"slug": "deliveries:view",  "label": "Deliveries"},
    {"slug": "shareholders:view","label": "Shareholders"},
    {"slug": "trucks:manage",    "label": "Manage Trucks"},
    {"slug": "trucks:payments",  "label": "Freights Transactions"},
    {"slug": "trucks:debtors",   "label": "Truck Debtors"},
    {"slug": "products:view",    "label": "Product Setup"},
    # --- NEW PAGES ---
    {"slug": "reports:view",     "label": "Statements / Reports"},
    {"slug": "debtors:view",     "label": "Debtors (Detail)"},
    {"slug": "debtors:list",     "label": "Debtors List"},
    # -----------------
    {"slug": "settings:view",    "label": "Settings"},
]

# ---------- Helpers ----------
def _oid(s):
    try: return ObjectId(s)
    except Exception: return None

def _clean(s): return (s or "").strip()
def _clean_username(s): return (s or "").strip().lower()

def _current_is_superadmin():
    return (session.get("username") == "admin") or bool(session.get("is_superadmin"))

def _require_superadmin():
    if not _current_is_superadmin(): abort(403)

def _valid_slugs():
    return [p["slug"] for p in PERMISSIONS]

def _default_access_all_on():
    return {p["slug"]: True for p in PERMISSIONS}

def _parse_access_from_form(form):
    access = {}
    for slug in _valid_slugs():
        key = f"status_{slug}"
        access[slug] = (form.get(key) == "on")
    return access

_pw_re_lower = re.compile(r"[a-z]")
_pw_re_upper = re.compile(r"[A-Z]")
_pw_re_digit = re.compile(r"\d")
_pw_re_symbol= re.compile(r"[^A-Za-z0-9]")

def _is_strong_password(pw: str) -> bool:
    if not pw or len(pw) < 10: return False
    if not _pw_re_lower.search(pw): return False
    if not _pw_re_upper.search(pw): return False
    if not _pw_re_digit.search(pw): return False
    if not _pw_re_symbol.search(pw): return False
    return True

# Ensure unique username
try:
    USERS.create_index("username", unique=True)
except Exception:
    pass

# ---------- Routes ----------
@users_bp.get("/users")
def users_page():
    _require_superadmin()
    admins = list(USERS.find(
        {"role": "admin"},
        {"username": 1, "position": 1, "role": 1, "access": 1, "perms": 1, "status": 1}
    ).sort("username", 1))

    for a in admins:
        a["_id"] = str(a["_id"])
        access = a.get("access")
        if not isinstance(access, dict):
            legacy = set(a.get("perms") or [])
            access = {slug: (slug in legacy) for slug in _valid_slugs()}
        a["access"] = access
        a["count_on"] = sum(1 for v in access.values() if v)
        if not a.get("status"): a["status"] = "active"  # default

    return render_template("users.html", admins=admins, PERMISSIONS=PERMISSIONS)

@users_bp.post("/users/create")
def create_user():
    _require_superadmin()
    username = _clean_username(request.form.get("username"))
    position = _clean(request.form.get("position"))
    password = request.form.get("password") or ""

    if not username or not password:
      return jsonify({"ok": False, "error": "Username and password are required."}), 400

    if not _is_strong_password(password):
      return jsonify({"ok": False, "error": "Password not strong. Min 10 chars incl. upper, lower, number & symbol."}), 400

    if USERS.count_documents({"username": username}) > 0:
      return jsonify({"ok": False, "error": "Username already exists."}), 400

    posted_any_status = any(k.startswith("status_") for k in request.form.keys())
    access = _parse_access_from_form(request.form) if posted_any_status else _default_access_all_on()

    doc = {
        "username": username,
        "password": generate_password_hash(password, method="scrypt"),
        "role": "admin",
        "position": position,
        "access": access,
        "status": "active",   # default active
    }
    ins = USERS.insert_one(doc)
    return jsonify({"ok": True, "id": str(ins.inserted_id)})

@users_bp.get("/users/<uid>/json")
def get_user(uid):
    _require_superadmin()
    oid = _oid(uid)
    if not oid:
        return jsonify({"ok": False, "error": "Invalid id"}), 400

    u = USERS.find_one({"_id": oid}, {"username": 1, "position": 1, "role": 1, "access": 1, "perms": 1, "status": 1})
    if not u:
        return jsonify({"ok": False, "error": "Not found"}), 404

    access = u.get("access")
    if not isinstance(access, dict):
        legacy = set(u.get("perms") or [])
        access = {slug: (slug in legacy) for slug in _valid_slugs()}

    out = {
        "_id": str(u["_id"]),
        "username": u.get("username"),
        "position": u.get("position"),
        "role": u.get("role"),
        "status": u.get("status") or "active",
        "access": access
    }
    return jsonify({"ok": True, "user": out})

@users_bp.post("/users/<uid>/update")
def update_user(uid):
    _require_superadmin()
    oid = _oid(uid)
    if not oid:
        return jsonify({"ok": False, "error": "Invalid id"}), 400

    position = _clean(request.form.get("position"))
    password = request.form.get("password") or ""
    access = _parse_access_from_form(request.form)

    update = {"position": position, "access": access}
    if password.strip():
        if not _is_strong_password(password):
            return jsonify({"ok": False, "error": "Password not strong. Min 10 chars incl. upper, lower, number & symbol."}), 400
        update["password"] = generate_password_hash(password, method="scrypt")

    res = USERS.update_one({"_id": oid, "role": "admin"}, {"$set": update, "$unset": {"perms": ""}})
    if res.matched_count != 1:
        return jsonify({"ok": False, "error": "User not found"}), 404
    return jsonify({"ok": True})

@users_bp.post("/users/<uid>/delete")
def delete_user(uid):
    _require_superadmin()
    oid = _oid(uid)
    if not oid:
        return jsonify({"ok": False, "error": "Invalid id"}), 400

    user = USERS.find_one({"_id": oid}, {"username": 1})
    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 404
    if user.get("username") == "admin":
        return jsonify({"ok": False, "error": "Cannot delete super-admin user."}), 400

    res = USERS.delete_one({"_id": oid, "role": "admin"})
    if res.deleted_count != 1:
        return jsonify({"ok": False, "error": "Delete failed"}), 400
    return jsonify({"ok": True})

# ---- Block / Unblock ----
@users_bp.post("/users/<uid>/status")
def set_user_status(uid):
    """
    Body: { "status": "blocked" | "active" }
    """
    _require_superadmin()
    oid = _oid(uid)
    if not oid:
        return jsonify({"ok": False, "error": "Invalid id"}), 400

    try:
        payload = request.get_json(silent=True) or {}
    except Exception:
        payload = {}
    status = (payload.get("status") or "").strip().lower()
    if status not in ("blocked", "active"):
        return jsonify({"ok": False, "error": "Invalid status"}), 400

    user = USERS.find_one({"_id": oid}, {"username": 1})
    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 404
    if user.get("username") == "admin" and status == "blocked":
        return jsonify({"ok": False, "error": "Cannot block super-admin user."}), 400

    res = USERS.update_one({"_id": oid, "role": "admin"}, {"$set": {"status": status}})
    if res.matched_count != 1:
        return jsonify({"ok": False, "error": "Update failed"}), 400
    return jsonify({"ok": True, "status": status})

# ---- Session perms (unchanged contract) ----
@users_bp.get("/auth/session-perms")
def session_perms():
    allowed = []
    access = session.get("access")
    if isinstance(access, dict):
        allowed = [slug for slug, on in access.items() if on]
    else:
        allowed = list(session.get("perms") or [])
    return jsonify({"ok": True, "perms": allowed, "is_superadmin": _current_is_superadmin()})
