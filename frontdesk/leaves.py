# frontdesk/leaves.py
from flask import Blueprint, render_template, request, jsonify, send_file, abort
from bson import ObjectId
from datetime import datetime, timedelta
from io import BytesIO
import gridfs

from db import db  # your existing Mongo connection

# Collections
EMPLOYEES     = db["employees"]
LEAVES        = db["fd_leaves"]
LEAVE_FILES   = db["fd_leave_files"]   # metadata mirror for GridFS files
fs = gridfs.GridFS(db, collection="hr_storage")

leaves_bp = Blueprint(
    "leaves_bp",
    __name__,
    url_prefix="/frontdesk"
)

# -------------------- Constants --------------------
LEAVE_TYPES = ["annual", "sick", "maternity", "paternity", "study", "unpaid"]
DEFAULT_BALANCES = {
    "annual": 20,
    "sick": 10,
    "maternity": 90,
    "paternity": 10,
    "study": 20,
    "unpaid": 0,  # unlimited; not capped in remaining
}
APPROVED_STATES = {"manager_approved", "hr_confirmed"}  # count towards used

# -------------------- Helpers --------------------
def _now():
    return datetime.utcnow()

def _parse_date(s):
    if not s:
        return None
    try:
        if len(s) == 10:
            return datetime.strptime(s, "%Y-%m-%d")
        return datetime.fromisoformat(s.replace("Z", ""))
    except Exception:
        return None

def _serialize_leave(d):
    return {
        "id": str(d["_id"]),
        "employee_id": str(d["employee_id"]),
        "employee_name": d.get("employee_name"),
        "type": d.get("type"),
        "start_date": d.get("start_date").date().isoformat() if d.get("start_date") else None,
        "end_date": d.get("end_date").date().isoformat() if d.get("end_date") else None,
        "days": d.get("days", 0),
        "reason": d.get("reason") or "",
        "status": d.get("status"),
        "manager_username": d.get("manager_username"),
        "manager_decision_at": d.get("manager_decision_at").isoformat() if d.get("manager_decision_at") else None,
        "hr_decision_at": d.get("hr_decision_at").isoformat() if d.get("hr_decision_at") else None,
        "created_at": d.get("created_at").isoformat() if d.get("created_at") else None,
        "attachments": [str(x) for x in (d.get("attachments") or [])],
    }

def _employee_name(e):
    fn = (e or {}).get("first_name") or ""
    ln = (e or {}).get("last_name") or ""
    n = (fn + " " + ln).strip()
    return n or (e or {}).get("email") or "—"

def _working_days(start_date, end_date):
    if not start_date or not end_date:
        return 0
    return (end_date - start_date).days + 1  # simple inclusive day count

def _get_balances_for_employee(emp):
    overrides = (emp or {}).get("leave_balances") or {}
    out = DEFAULT_BALANCES.copy()
    for k, v in overrides.items():
        try:
            out[k] = int(v)
        except Exception:
            pass
    return out

def _calc_used_days(emp_id: ObjectId, year: int):
    """Sum days by type in a given year for states that count."""
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31, 23, 59, 59)
    pipeline = [
        {"$match": {
            "employee_id": emp_id,
            "start_date": {"$lte": end},
            "end_date": {"$gte": start},
            "status": {"$in": list(APPROVED_STATES)},
        }},
        {"$group": {"_id": "$type", "days": {"$sum": "$days"}}},
    ]
    return {x["_id"]: int(x["days"]) for x in LEAVES.aggregate(pipeline)}

def _overlaps(emp_id: ObjectId, start_dt: datetime, end_dt: datetime, exclude_id: ObjectId | None = None):
    q = {
        "employee_id": emp_id,
        "start_date": {"$lte": end_dt},
        "end_date": {"$gte": start_dt},
        "status": {"$nin": ["manager_rejected", "hr_rejected", "cancelled"]},
    }
    if exclude_id:
        q["_id"] = {"$ne": exclude_id}
    res = []
    for d in LEAVES.find(q).sort("start_date", 1):
        res.append(_serialize_leave(d))
    return res

# -------------------- Page --------------------
@leaves_bp.get("/leaves")
def leaves_page():
    return render_template("frontdesk_pages/leaves.html")

# -------------------- Lookups --------------------
@leaves_bp.get("/api/leaves/types")
def api_leave_types():
    return jsonify(LEAVE_TYPES)

@leaves_bp.get("/api/leaves/employees")
def api_leave_employees():
    rows = []
    for e in EMPLOYEES.find({}, {"first_name": 1, "last_name": 1, "email": 1}).sort([("last_name", 1), ("first_name", 1)]):
        rows.append({"id": str(e["_id"]), "name": _employee_name(e), "email": e.get("email")})
    return jsonify(rows)

@leaves_bp.get("/api/leaves/balances/<emp_id>")
def api_leave_balances(emp_id):
    try:
        _id = ObjectId(emp_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400
    emp = EMPLOYEES.find_one({"_id": _id})
    if not emp:
        return jsonify({"error": "not found"}), 404

    year = int(request.args.get("year") or _now().year)
    policy = _get_balances_for_employee(emp)
    used = _calc_used_days(_id, year)

    out = {}
    for t in LEAVE_TYPES:
        cap = policy.get(t, 0)
        if t == "unpaid":
            out[t] = {"entitled": 0, "used": used.get(t, 0), "remaining": None}
        else:
            u = used.get(t, 0)
            out[t] = {"entitled": cap, "used": u, "remaining": max(0, cap - u)}
    return jsonify({"year": year, "balances": out})

# -------------------- List / Query --------------------
@leaves_bp.get("/api/leaves")
def api_leaves_list():
    """
    Filters: q, employee_id, type, status, start, end, page, per, sort
    """
    qtext = (request.args.get("q") or "").strip().lower()
    emp_id = (request.args.get("employee_id") or "").strip()
    ltype = (request.args.get("type") or "").strip().lower()
    status = (request.args.get("status") or "").strip().lower()
    start = _parse_date(request.args.get("start"))
    end = _parse_date(request.args.get("end"))
    page = max(1, int(request.args.get("page") or 1))
    per = min(100, max(10, int(request.args.get("per") or 25)))
    sort = (request.args.get("sort") or "-start_date").strip().lower()

    q = {}
    if emp_id:
        try:
            q["employee_id"] = ObjectId(emp_id)
        except Exception:
            return jsonify({"items": [], "page": 1, "per": per, "total": 0})
    if ltype:
        q["type"] = ltype
    if status:
        q["status"] = status
    if start and end:
        q["start_date"] = {"$lt": end}
        q["end_date"] = {"$gt": start}
    elif start:
        q["end_date"] = {"$gte": start}
    elif end:
        q["start_date"] = {"$lte": end}

    sort_spec = [("start_date", -1)]
    if sort == "start_date":
        sort_spec = [("start_date", 1)]
    elif sort == "created_at":
        sort_spec = [("created_at", 1)]
    elif sort == "-created_at":
        sort_spec = [("created_at", -1)]

    cur = LEAVES.find(q).sort(sort_spec).skip((page - 1) * per).limit(per)
    items = []
    for d in cur:
        it = _serialize_leave(d)
        if qtext:
            hay = " ".join([
                it.get("employee_name") or "",
                it.get("type") or "",
                it.get("reason") or "",
                it.get("status") or "",
            ]).lower()
            if qtext not in hay:
                continue
        items.append(it)

    total = LEAVES.count_documents(q)
    return jsonify({"items": items, "page": page, "per": per, "total": total})

# -------------------- Create & Workflow --------------------
@leaves_bp.post("/api/leaves")
def api_leave_create():
    """
    Create request ALWAYS starts as pending_manager.
    Must be explicitly approved then confirmed.
    """
    data = request.form if request.form else (request.json or {})
    emp_id = data.get("employee_id")
    ltype = (data.get("type") or "").strip().lower()
    start = _parse_date(data.get("start_date"))
    end = _parse_date(data.get("end_date"))
    reason = data.get("reason") or ""
    manager_username = (data.get("manager_username") or "").strip() or None

    if not emp_id or not ltype or not start or not end:
        return jsonify({"error": "employee_id, type, start_date, end_date are required"}), 400
    if ltype not in LEAVE_TYPES:
        return jsonify({"error": "invalid leave type"}), 400

    try:
        _eid = ObjectId(emp_id)
    except Exception:
        return jsonify({"error": "invalid employee_id"}), 400

    emp = EMPLOYEES.find_one({"_id": _eid})
    if not emp:
        return jsonify({"error": "employee not found"}), 404

    start = datetime.combine(start.date(), datetime.min.time())
    end = datetime.combine(end.date(), datetime.max.time())
    if end < start:
        return jsonify({"error": "end_date must be >= start_date"}), 400

    # Enforce approval before start:
    # (We still allow creating a request for any future date, but status is always pending.)
    days = _working_days(start.date(), end.date())
    overlaps = _overlaps(_eid, start, end, None)

    doc = {
        "employee_id": _eid,
        "employee_name": _employee_name(emp),
        "type": ltype,
        "start_date": start,
        "end_date": end,
        "days": days,
        "reason": reason,
        "status": "pending_manager",
        "manager_username": manager_username,  # optional text reference; actual auth handled externally
        "manager_decision_at": None,
        "hr_decision_at": None,
        "attachments": [],
        "created_at": _now(),
    }
    ins = LEAVES.insert_one(doc)

    # Optional first attachment
    if "file" in request.files:
        file = request.files["file"]
        blob_id = fs.put(file.stream, filename=file.filename, content_type=file.mimetype)
        meta = {
            "leave_id": ins.inserted_id,
            "file_id": blob_id,
            "filename": file.filename,
            "mimetype": file.mimetype,
            "size": file.content_length,
            "uploaded_at": _now(),
        }
        m = LEAVE_FILES.insert_one(meta)
        LEAVES.update_one({"_id": ins.inserted_id}, {"$push": {"attachments": m.inserted_id}})

    created = LEAVES.find_one({"_id": ins.inserted_id})
    return jsonify({"ok": True, "leave": _serialize_leave(created), "conflicts": overlaps}), 201

@leaves_bp.patch("/api/leaves/<leave_id>/manager_decision")
def api_leave_manager_decision(leave_id):
    data = request.json or {}
    decision = (data.get("decision") or "").strip().lower()  # approve|reject
    username = (data.get("username") or "").strip() or None

    if decision not in ["approve", "reject"]:
        return jsonify({"error": "decision must be approve|reject"}), 400
    try:
        _lid = ObjectId(leave_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400

    L = LEAVES.find_one({"_id": _lid})
    if not L:
        return jsonify({"error": "not found"}), 404
    if L.get("status") != "pending_manager":
        return jsonify({"error": "not in manager pending state"}), 400

    upd = {
        "manager_decision_at": _now(),
        "manager_username": username or L.get("manager_username"),
        "status": "manager_approved" if decision == "approve" else "manager_rejected",
    }
    LEAVES.update_one({"_id": _lid}, {"$set": upd})
    return jsonify({"ok": True, "leave": _serialize_leave(LEAVES.find_one({"_id": _lid}))})

@leaves_bp.patch("/api/leaves/<leave_id>/hr_decision")
def api_leave_hr_decision(leave_id):
    data = request.json or {}
    decision = (data.get("decision") or "").strip().lower()  # confirm|reject
    if decision not in ["confirm", "reject"]:
        return jsonify({"error": "decision must be confirm|reject"}), 400
    try:
        _lid = ObjectId(leave_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400

    L = LEAVES.find_one({"_id": _lid})
    if not L:
        return jsonify({"error": "not found"}), 404

    # Enforce sequence: must have manager_approved first (no auto-skip)
    if L.get("status") != "manager_approved":
        return jsonify({"error": "request must be manager_approved first"}), 400

    # Extra guard: cannot confirm AFTER it already ended
    if decision == "confirm" and _now().date() > L["end_date"].date():
        return jsonify({"error": "cannot confirm a leave after it ended"}), 400

    upd = {
        "hr_decision_at": _now(),
        "status": "hr_confirmed" if decision == "confirm" else "hr_rejected",
    }
    LEAVES.update_one({"_id": _lid}, {"$set": upd})
    return jsonify({"ok": True, "leave": _serialize_leave(LEAVES.find_one({"_id": _lid}))})

@leaves_bp.delete("/api/leaves/<leave_id>")
def api_leave_delete(leave_id):
    try:
        _lid = ObjectId(leave_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400

    # delete attachments
    metas = list(LEAVE_FILES.find({"leave_id": _lid}))
    for m in metas:
        try:
            fs.delete(m["file_id"])
        except Exception:
            pass
    LEAVE_FILES.delete_many({"leave_id": _lid})
    LEAVES.delete_one({"_id": _lid})
    return jsonify({"ok": True})

# -------------------- Attachments --------------------
@leaves_bp.get("/api/leaves/<leave_id>/files")
def api_leave_files(leave_id):
    try:
        _lid = ObjectId(leave_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400
    out = []
    for m in LEAVE_FILES.find({"leave_id": _lid}).sort("uploaded_at", -1):
        out.append({
            "meta_id": str(m["_id"]),
            "filename": m.get("filename"),
            "mimetype": m.get("mimetype"),
            "size": m.get("size"),
            "uploaded_at": m.get("uploaded_at").isoformat(),
        })
    return jsonify(out)

@leaves_bp.post("/api/leaves/<leave_id>/files")
def api_leave_upload_file(leave_id):
    try:
        _lid = ObjectId(leave_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400
    if "file" not in request.files:
        return jsonify({"error": "no file"}), 400
    if not LEAVES.find_one({"_id": _lid}):
        return jsonify({"error": "not found"}), 404

    f = request.files["file"]
    blob_id = fs.put(f.stream, filename=f.filename, content_type=f.mimetype)
    meta = {
        "leave_id": _lid,
        "file_id": blob_id,
        "filename": f.filename,
        "mimetype": f.mimetype,
        "size": f.content_length,
        "uploaded_at": _now(),
    }
    m = LEAVE_FILES.insert_one(meta)
    LEAVES.update_one({"_id": _lid}, {"$push": {"attachments": m.inserted_id}})
    return jsonify({"ok": True})

@leaves_bp.get("/api/leavefiles/<meta_id>/download")
def api_leavefile_download(meta_id):
    try:
        mid = ObjectId(meta_id)
    except Exception:
        return abort(400)
    m = LEAVE_FILES.find_one({"_id": mid})
    if not m:
        return abort(404)
    try:
        blob = fs.get(m["file_id"])
    except Exception:
        return abort(404)
    return send_file(
        BytesIO(blob.read()),
        mimetype=m.get("mimetype") or "application/octet-stream",
        download_name=m.get("filename") or "file.bin",
        as_attachment=True,
    )

@leaves_bp.delete("/api/leavefiles/<meta_id>")
def api_leavefile_delete(meta_id):
    try:
        mid = ObjectId(meta_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400
    m = LEAVE_FILES.find_one({"_id": mid})
    if not m:
        return jsonify({"error": "not found"}), 404
    try:
        fs.delete(m["file_id"])
    except Exception:
        pass
    LEAVE_FILES.delete_one({"_id": mid})
    LEAVES.update_one({"_id": m["leave_id"]}, {"$pull": {"attachments": mid}})
    return jsonify({"ok": True})

# -------------------- Analytics --------------------
@leaves_bp.get("/api/leaves/metrics")
def api_leaves_metrics():
    """
    Returns:
      - monthly_totals: [{month: 1..12, days: int}]
      - top_employees: [{employee_id, employee_name, days}]
    Filters:
      year (default: current), status (default: hr_confirmed only)
    """
    year = int(request.args.get("year") or _now().year)
    status = (request.args.get("status") or "hr_confirmed").strip().lower()
    status_filter = [status] if status in {"pending_manager", "manager_approved", "manager_rejected", "hr_confirmed", "hr_rejected", "cancelled"} else ["hr_confirmed"]

    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31, 23, 59, 59)

    # Monthly totals (by days)
    pipeline_month = [
        {"$match": {
            "start_date": {"$lte": end},
            "end_date": {"$gte": start},
            "status": {"$in": status_filter},
        }},
        {"$project": {
            "days": 1,
            "month": {"$month": "$start_date"}
        }},
        {"$group": {"_id": "$month", "days": {"$sum": "$days"}}},
        {"$sort": {"_id": 1}}
    ]
    monthly = {m["_id"]: int(m["days"]) for m in LEAVES.aggregate(pipeline_month)}
    monthly_totals = [{"month": m, "days": monthly.get(m, 0)} for m in range(1, 13)]

    # Top employees by days
    pipeline_top = [
        {"$match": {
            "start_date": {"$lte": end},
            "end_date": {"$gte": start},
            "status": {"$in": status_filter},
        }},
        {"$group": {
            "_id": {"eid": "$employee_id", "name": "$employee_name"},
            "days": {"$sum": "$days"}
        }},
        {"$sort": {"days": -1}},
        {"$limit": 10}
    ]
    top = [{"employee_id": str(x["_id"]["eid"]), "employee_name": x["_id"]["name"], "days": int(x["days"])} for x in LEAVES.aggregate(pipeline_top)]

    return jsonify({"year": year, "monthly_totals": monthly_totals, "top_employees": top})

__all__ = ["leaves_bp"]
