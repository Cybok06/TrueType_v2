# frontdesk/hris_employees.py
from flask import Blueprint, render_template, request, jsonify, send_file, abort
from bson import ObjectId
from datetime import datetime
from io import BytesIO
import gridfs

from db import db  # existing Mongo connection

# Collections
EMPLOYEES  = db["employees"]
FILES_META = db["hr_files"]        # metadata mirror for GridFS
fs = gridfs.GridFS(db, collection="hr_storage")

# Blueprint (front desk namespace)
hris_employees_bp = Blueprint(
    "hris_employees_bp",
    __name__,
    url_prefix="/frontdesk"
)

# ---------------- Utilities ----------------
def _parse_date(s: str | None):
    if not s:
        return None
    try:
        # Accept YYYY-MM-DD
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        try:
            # Accept full ISO; tolerate trailing Z
            return datetime.fromisoformat(s.replace("Z", ""))
        except Exception:
            return None

def _norm_groups(v):
    """Accept list or comma string and return a clean list of unique non-empty group names."""
    if not v:
        return []
    if isinstance(v, str):
        parts = [p.strip() for p in v.split(",")]
    else:
        parts = [str(p).strip() for p in v]
    # keep order while de-duplicating
    out, seen = [], set()
    for p in parts:
        if p and p.lower() not in seen:
            seen.add(p.lower())
            out.append(p)
    return out

def _norm_custom_fields(v):
    """Accept list of dicts or pairs; coerce to [{name, value}] dropping empties."""
    out = []
    if isinstance(v, list):
        for it in v:
            if isinstance(it, dict):
                name = str(it.get("name", "")).strip()
                val  = str(it.get("value", "")).strip()
            elif isinstance(it, (list, tuple)) and len(it) >= 2:
                name = str(it[0]).strip()
                val  = str(it[1]).strip()
            else:
                continue
            if name or val:
                out.append({"name": name, "value": val})
    return out

def _serialize(emp: dict):
    if not emp:
        return None
    return {
        "id": str(emp["_id"]),
        "staff_id": emp.get("staff_id"),
        "first_name": emp.get("first_name"),
        "last_name": emp.get("last_name"),
        "email": emp.get("email"),
        "phone": emp.get("phone"),
        "role_title": emp.get("role_title"),
        "status": emp.get("status", "active"),
        "employment_type": emp.get("employment_type", "full_time"),
        "start_date": emp.get("start_date").isoformat() if emp.get("start_date") else None,
        "probation_end": emp.get("probation_end").isoformat() if emp.get("probation_end") else None,
        "confirmation_date": emp.get("confirmation_date").isoformat() if emp.get("confirmation_date") else None,
        "location": emp.get("location"),
        "notes": emp.get("notes", ""),
        "groups": emp.get("groups", []),                  # NEW
        "custom_fields": emp.get("custom_fields", []),    # NEW [{name, value}]
        "docs_count": emp.get("docs_count", 0),
        "avatar_url": emp.get("avatar_url"),
        "created_at": emp.get("created_at").isoformat() if emp.get("created_at") else None,
        "updated_at": emp.get("updated_at").isoformat() if emp.get("updated_at") else None,
    }

# ---------------- Pages ----------------
@hris_employees_bp.get("/employees")
def employees_directory_page():
    return render_template("frontdesk_pages/hris_employees.html")

@hris_employees_bp.get("/employees/<emp_id>")
def employee_profile_page(emp_id):
    return render_template("frontdesk_pages/hris_profile.html", emp_id=emp_id)

# ---------------- Employee APIs ----------------
@hris_employees_bp.get("/api/employees")
def api_list_employees():
    q       = (request.args.get("q") or "").strip().lower()
    status  = (request.args.get("status") or "").strip().lower()
    group   = (request.args.get("group") or "").strip()

    query = {}
    if status:
        query["status"] = status
    if group:
        query["groups"] = group  # exact match in array

    docs = EMPLOYEES.find(query).sort([("last_name", 1), ("first_name", 1)])
    items = []
    for d in docs:
        it = _serialize(d)
        if q:
            hay = " ".join([
                it.get("first_name") or "",
                it.get("last_name") or "",
                it.get("email") or "",
                it.get("phone") or "",
                it.get("staff_id") or "",
                it.get("role_title") or "",
                " ".join(it.get("groups") or []),
                " ".join([f"{cf.get('name','')} {cf.get('value','')}" for cf in (it.get("custom_fields") or [])]),
            ]).lower()
            if q not in hay:
                continue
        items.append(it)
    return jsonify(items)

@hris_employees_bp.post("/api/employees")
def api_create_employee():
    data = request.json or {}
    now = datetime.utcnow()
    doc = {
        "staff_id": data.get("staff_id"),
        "first_name": data.get("first_name"),
        "last_name": data.get("last_name"),
        "email": data.get("email"),
        "phone": data.get("phone"),
        "role_title": data.get("role_title"),
        "status": (data.get("status") or "active").lower(),
        "employment_type": data.get("employment_type", "full_time"),
        "start_date": _parse_date(data.get("start_date")),
        "probation_end": _parse_date(data.get("probation_end")),
        "confirmation_date": _parse_date(data.get("confirmation_date")),
        "location": data.get("location"),
        "notes": data.get("notes", ""),
        "groups": _norm_groups(data.get("groups")),                 # NEW
        "custom_fields": _norm_custom_fields(data.get("custom_fields")),  # NEW
        "docs_count": 0,
        "avatar_url": data.get("avatar_url"),
        "created_at": now,
        "updated_at": now,
    }
    ins = EMPLOYEES.insert_one(doc)
    doc["_id"] = ins.inserted_id
    return jsonify(_serialize(doc)), 201

@hris_employees_bp.get("/api/employees/<emp_id>")
def api_get_employee(emp_id):
    try:
        _id = ObjectId(emp_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400
    doc = EMPLOYEES.find_one({"_id": _id})
    if not doc:
        return jsonify({"error": "not found"}), 404
    return jsonify(_serialize(doc))

@hris_employees_bp.patch("/api/employees/<emp_id>")
def api_update_employee(emp_id):
    try:
        _id = ObjectId(emp_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400

    data = request.json or {}
    upd = {"updated_at": datetime.utcnow()}

    fields = [
        "staff_id","first_name","last_name","email","phone","role_title",
        "status","employment_type","location","notes","avatar_url"
    ]
    for f in fields:
        if f in data:
            upd[f] = data[f]

    # dates
    for df in ["start_date","probation_end","confirmation_date"]:
        if df in data:
            upd[df] = _parse_date(data[df])

    # groups & custom fields
    if "groups" in data:
        upd["groups"] = _norm_groups(data.get("groups"))
    if "custom_fields" in data:
        upd["custom_fields"] = _norm_custom_fields(data.get("custom_fields"))

    res = EMPLOYEES.update_one({"_id": _id}, {"$set": upd})
    if res.matched_count == 0:
        return jsonify({"error": "not found"}), 404

    doc = EMPLOYEES.find_one({"_id": _id})
    return jsonify(_serialize(doc))

@hris_employees_bp.delete("/api/employees/<emp_id>")
def api_delete_employee(emp_id):
    try:
        _id = ObjectId(emp_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400

    # delete attachments
    metas = list(FILES_META.find({"employee_id": _id}))
    for m in metas:
        try:
            fs.delete(m["file_id"])
        except Exception:
            pass
    FILES_META.delete_many({"employee_id": _id})

    EMPLOYEES.delete_one({"_id": _id})
    return jsonify({"ok": True})

# ---------------- Groups helper (for filter dropdown) ----------------
@hris_employees_bp.get("/api/groups")
def api_groups():
    # distinct groups from employees
    groups = set()
    for d in EMPLOYEES.find({}, {"groups": 1}):
        for g in d.get("groups") or []:
            if g:
                groups.add(g)
    return jsonify(sorted(groups, key=lambda s: s.lower()))

# ---------------- File APIs (GridFS) ----------------
@hris_employees_bp.post("/api/employees/<emp_id>/files")
def api_upload_file(emp_id):
    try:
        _id = ObjectId(emp_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400

    if "file" not in request.files:
        return jsonify({"error": "no file"}), 400

    file = request.files["file"]
    doc_type = request.form.get("doc_type") or "other"
    title = request.form.get("title") or file.filename

    blob_id = fs.put(file.stream, filename=file.filename, content_type=file.mimetype)

    meta = {
        "employee_id": _id,
        "file_id": blob_id,
        "filename": file.filename,
        "title": title,
        "doc_type": doc_type,
        "mimetype": file.mimetype,
        "size": file.content_length,
        "uploaded_at": datetime.utcnow(),
    }
    ins = FILES_META.insert_one(meta)
    meta["_id"] = ins.inserted_id

    EMPLOYEES.update_one({"_id": _id}, {"$inc": {"docs_count": 1}})

    return jsonify({
        "ok": True,
        "file": {
            "meta_id": str(meta["_id"]),
            "title": title,
            "doc_type": doc_type,
            "filename": file.filename,
            "mimetype": file.mimetype,
            "size": meta["size"],
            "uploaded_at": meta["uploaded_at"].isoformat(),
        }
    })

@hris_employees_bp.get("/api/employees/<emp_id>/files")
def api_list_files(emp_id):
    try:
        _id = ObjectId(emp_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400

    files = []
    for m in FILES_META.find({"employee_id": _id}).sort("uploaded_at", -1):
        files.append({
            "meta_id": str(m["_id"]),
            "filename": m.get("filename"),
            "title": m.get("title"),
            "doc_type": m.get("doc_type"),
            "mimetype": m.get("mimetype"),
            "size": m.get("size"),
            "uploaded_at": m.get("uploaded_at").isoformat(),
        })
    return jsonify(files)

@hris_employees_bp.get("/api/files/<meta_id>/download")
def api_download_file(meta_id):
    try:
        mid = ObjectId(meta_id)
    except Exception:
        return abort(400)
    meta = FILES_META.find_one({"_id": mid})
    if not meta:
        return abort(404)
    try:
        gridout = fs.get(meta["file_id"])
    except Exception:
        return abort(404)

    return send_file(
        BytesIO(gridout.read()),
        mimetype=meta.get("mimetype") or "application/octet-stream",
        download_name=meta.get("filename") or "download.bin",
        as_attachment=True,
    )

@hris_employees_bp.delete("/api/files/<meta_id>")
def api_delete_file(meta_id):
    try:
        mid = ObjectId(meta_id)
    except Exception:
        return jsonify({"error": "invalid id"}), 400

    meta = FILES_META.find_one({"_id": mid})
    if not meta:
        return jsonify({"error": "not found"}), 404

    try:
        fs.delete(meta["file_id"])
    except Exception:
        pass
    FILES_META.delete_one({"_id": mid})
    EMPLOYEES.update_one({"_id": meta["employee_id"]}, {"$inc": {"docs_count": -1}})
    return jsonify({"ok": True})

__all__ = ["hris_employees_bp"]
