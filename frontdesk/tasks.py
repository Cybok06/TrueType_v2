from flask import Blueprint, render_template, request, jsonify, send_file, abort
from bson import ObjectId
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from io import BytesIO
import gridfs
import json

from db import db  # your existing Mongo connection

# --- Collections ---
TASKS       = db["fd_tasks"]
FILES_META  = db["fd_task_files"]
EMPLOYEES   = db["employees"]          # (optional link)
MEETINGS    = db["meetings"]           # (optional link)
DOCS_META   = db["fd_docs_meta"]       # (optional link)
fs = gridfs.GridFS(db, collection="hr_storage")

tasks_bp = Blueprint("tasks_bp", __name__, url_prefix="/frontdesk")

# ---------- helpers ----------
PRIORITIES = ["low", "normal", "high", "urgent"]
STATUSES   = ["todo", "in_progress", "blocked", "done"]
VIEWS      = ["myday", "upcoming", "overdue", "completed"]

def _now():
    return datetime.utcnow()

def _parse_date(s):
    """
    Accept YYYY-MM-DD or full ISO (with or without 'Z').
    Supports date + time so the UI can schedule exact time.
    """
    if not s:
        return None
    try:
        if len(s) == 10:
            return datetime.strptime(s, "%Y-%m-%d")
        return datetime.fromisoformat(s.replace("Z",""))
    except Exception:
        return None

def _serialize(t):
    return {
        "id": str(t["_id"]),
        "title": t.get("title") or "",
        "description": t.get("description") or "",
        "status": t.get("status") or "todo",
        "priority": t.get("priority") or "normal",
        "labels": t.get("labels") or [],
        "due_date": t.get("due_date").isoformat() if t.get("due_date") else None,
        "completed_at": t.get("completed_at").isoformat() if t.get("completed_at") else None,
        "is_recurring": bool(t.get("recurrence")),
        "recurrence": t.get("recurrence") or None,  # {"freq":"daily|weekly|monthly|yearly","interval":1}
        "assignee_id": str(t.get("assignee_id")) if t.get("assignee_id") else None,
        "linked": {
            "employee_id": str(t["linked"].get("employee_id")) if t.get("linked",{}).get("employee_id") else None,
            "meeting_id": str(t["linked"].get("meeting_id")) if t.get("linked",{}).get("meeting_id") else None,
            "document_id": str(t["linked"].get("document_id")) if t.get("linked",{}).get("document_id") else None,
        },
        "attachments": [str(x) for x in (t.get("attachments") or [])],
        "created_at": t.get("created_at").isoformat() if t.get("created_at") else None,
        "updated_at": t.get("updated_at").isoformat() if t.get("updated_at") else None,
    }

def _advance_due(due, rec):
    if not due or not rec:
        return None
    freq = (rec.get("freq") or "").lower()
    interval = int(rec.get("interval") or 1)
    if freq == "daily":
        return due + timedelta(days=interval)
    if freq == "weekly":
        return due + timedelta(weeks=interval)
    if freq == "monthly":
        return due + relativedelta(months=+interval)
    if freq == "yearly":
        return due + relativedelta(years=+interval)
    return None

def _apply_view_filter(view, query):
    today = _now().date()
    start_today = datetime(today.year, today.month, today.day)
    end_today = start_today + timedelta(days=1)

    if view == "myday":
        query["status"] = {"$in": ["todo","in_progress","blocked"]}
        query["due_date"] = {"$gte": start_today, "$lt": end_today}
    elif view == "upcoming":
        query["status"] = {"$in": ["todo","in_progress","blocked"]}
        query["due_date"] = {"$gte": end_today}
    elif view == "overdue":
        query["status"] = {"$in": ["todo","in_progress","blocked"]}
        query["due_date"] = {"$lt": start_today}
    elif view == "completed":
        query["status"] = "done"

# ---------- page ----------
@tasks_bp.get("/tasks")
def tasks_page():
    return render_template("frontdesk_pages/tasks.html")

# ---------- lookups (optional linking; UI keeps it simple but available) ----------
@tasks_bp.get("/api/tasks/lookups")
def api_task_lookups():
    kind = (request.args.get("type") or "").strip().lower()
    qtxt = (request.args.get("q") or "").strip().lower()

    if kind == "employees":
        cur = EMPLOYEES.find({}).sort([("last_name",1),("first_name",1)]).limit(50)
        out = []
        for e in cur:
            fn = (e.get("first_name") or "").strip()
            ln = (e.get("last_name") or "").strip()
            name = (fn + " " + ln).strip() or e.get("email") or e.get("phone") or "—"
            if qtxt and qtxt not in (" ".join([fn, ln, e.get("email",""), e.get("phone","")]).lower()):
                continue
            out.append({"id": str(e["_id"]), "label": name})
        return jsonify(out)

    if kind == "meetings":
        cur = MEETINGS.find({}).sort("start",-1).limit(50)
        out = []
        for m in cur:
            label = (m.get("title") or "Meeting")
            if qtxt and qtxt not in (" ".join([label, m.get("location","")]).lower()):
                continue
            out.append({"id": str(m["_id"]), "label": label})
        return jsonify(out)

    if kind == "documents":
        cur = DOCS_META.find({}).sort("uploaded_at",-1).limit(50)
        out = []
        for d in cur:
            label = d.get("title") or d.get("filename") or "Document"
            if qtxt and qtxt not in (label.lower()):
                continue
            out.append({"id": str(d["_id"]), "label": label})
        return jsonify(out)

    return jsonify([])

# ---------- list ----------
@tasks_bp.get("/api/tasks")
def api_tasks_list():
    qtxt   = (request.args.get("q") or "").strip().lower()
    view   = (request.args.get("view") or "").strip().lower()
    status = (request.args.get("status") or "").strip().lower()
    prio   = (request.args.get("priority") or "").strip().lower()
    label  = (request.args.get("label") or "").strip()
    snd    = (request.args.get("sort") or "-due_date").strip().lower()
    page   = max(1, int(request.args.get("page") or 1))
    per    = min(100, max(10, int(request.args.get("per") or 24)))

    q = {}
    if view in VIEWS:
        _apply_view_filter(view, q)
    if status in STATUSES:
        q["status"] = status
    if prio in PRIORITIES:
        q["priority"] = prio
    if label:
        q["labels"] = label

    # sort
    field = snd.replace("-", "")
    direction = -1 if snd.startswith("-") else 1
    sort_field = field if field in {"due_date","created_at","priority","title"} else "due_date"

    cur = TASKS.find(q).sort(sort_field, direction).skip((page-1)*per).limit(per)
    items = []
    for t in cur:
        it = _serialize(t)
        if qtxt:
            hay = " ".join([
                it["title"], it["description"],
                " ".join(it.get("labels") or []),
                it.get("priority",""), it.get("status","")
            ]).lower()
            if qtxt not in hay:
                continue
        items.append(it)
    total = TASKS.count_documents(q)
    return jsonify({"items": items, "page": page, "per": per, "total": total})

# ---------- create ----------
@tasks_bp.post("/api/tasks")
def api_tasks_create():
    is_multipart = bool(request.files) or (request.content_type or "").startswith("multipart/")
    data = request.form if is_multipart else (request.json or {})
    now  = _now()

    labels = []
    if isinstance(data.get("labels"), list):
        labels = [str(x).strip() for x in data.get("labels") if str(x).strip()]
    elif isinstance(data.get("labels"), str) and data.get("labels").strip():
        labels = [x.strip() for x in data.get("labels").split(",") if x.strip()]

    rec = None
    rec_raw = data.get("recurrence")
    if rec_raw:
        try:
            rec = json.loads(rec_raw) if isinstance(rec_raw, str) else rec_raw
        except Exception:
            rec = None
    if rec and (rec.get("freq") not in ["daily","weekly","monthly","yearly"]):
        rec = None

    doc = {
        "title": (data.get("title") or "").strip(),
        "description": (data.get("description") or "").strip(),
        "status": (data.get("status") or "todo").lower() if (data.get("status") or "").lower() in STATUSES else "todo",
        "priority": (data.get("priority") or "normal").lower() if (data.get("priority") or "").lower() in PRIORITIES else "normal",
        "labels": labels,
        "due_date": _parse_date(data.get("due_date")),
        "recurrence": rec,
        "assignee_id": ObjectId(data["assignee_id"]) if data.get("assignee_id") else None,
        "linked": {
            "employee_id": ObjectId(data["employee_id"]) if data.get("employee_id") else None,
            "meeting_id": ObjectId(data["meeting_id"]) if data.get("meeting_id") else None,
            "document_id": ObjectId(data["document_id"]) if data.get("document_id") else None,
        },
        "attachments": [],
        "completed_at": None,
        "created_at": now,
        "updated_at": now,
    }
    if not doc["title"]:
        return jsonify({"error":"Title is required"}), 400

    ins = TASKS.insert_one(doc)
    task_id = ins.inserted_id

    # optional attachment on create
    if "file" in request.files:
        f = request.files["file"]
        if f and getattr(f, "filename", ""):
            blob_id = fs.put(f.stream, filename=f.filename, content_type=f.mimetype)
            meta = {
                "task_id": task_id,
                "file_id": blob_id,
                "filename": f.filename,
                "mimetype": f.mimetype,
                "size": f.content_length,
                "uploaded_at": now,
            }
            m = FILES_META.insert_one(meta)
            TASKS.update_one({"_id": task_id}, {"$push": {"attachments": m.inserted_id}})

    return jsonify(_serialize(TASKS.find_one({"_id": task_id}))), 201

# ---------- update ----------
@tasks_bp.patch("/api/tasks/<task_id>")
def api_tasks_update(task_id):
    try:
        _id = ObjectId(task_id)
    except Exception:
        return jsonify({"error":"invalid id"}), 400
    data = request.json or {}
    upd = {"updated_at": _now()}

    for f in ["title","description"]:
        if f in data:
            upd[f] = (data.get(f) or "").strip()

    if "priority" in data and (data.get("priority") or "").lower() in PRIORITIES:
        upd["priority"] = (data.get("priority") or "").lower()

    if "status" in data and (data.get("status") or "").lower() in STATUSES:
        upd["status"] = (data.get("status") or "").lower()
        # if user flips away from done, clear completed_at
        if upd["status"] != "done":
            upd["completed_at"] = None

    if "labels" in data:
        labels = data.get("labels") or []
        if isinstance(labels, str):
            labels = [x.strip() for x in labels.split(",") if x.strip()]
        upd["labels"] = labels

    if "due_date" in data:
        upd["due_date"] = _parse_date(data.get("due_date"))

    if "recurrence" in data:
        rec = data.get("recurrence")
        if isinstance(rec, str):
            try: rec = json.loads(rec)
            except Exception: rec = None
        if rec and rec.get("freq") in ["daily","weekly","monthly","yearly"]:
            upd["recurrence"] = {"freq": rec["freq"], "interval": int(rec.get("interval") or 1)}
        else:
            upd["recurrence"] = None

    # links (kept for extensibility)
    for key, field in [("employee_id","employee_id"),("meeting_id","meeting_id"),("document_id","document_id")]:
        if key in data:
            upd.setdefault("linked", {})[field] = ObjectId(data[key]) if data[key] else None

    TASKS.update_one({"_id": _id}, {"$set": upd})
    return jsonify(_serialize(TASKS.find_one({"_id": _id})))

# ---------- status move (recurring + completed_at) ----------
@tasks_bp.patch("/api/tasks/<task_id>/status")
def api_tasks_status(task_id):
    try:
        _id = ObjectId(task_id)
    except Exception:
        return jsonify({"error":"invalid id"}), 400
    data = request.json or {}
    st = (data.get("status") or "").lower()
    if st not in STATUSES:
        return jsonify({"error":"invalid status"}), 400

    now = _now()
    doc = TASKS.find_one({"_id": _id})

    # set completed_at when done; clear otherwise
    upd = {"status": st, "updated_at": now, "completed_at": (now if st == "done" else None)}
    TASKS.update_one({"_id": _id}, {"$set": upd})

    # If recurring and moved to done → clone next occurrence
    if st == "done" and doc and doc.get("recurrence") and doc.get("due_date"):
        nxt = _advance_due(doc["due_date"], doc.get("recurrence"))
        if nxt:
            clone = {k:v for k,v in doc.items() if k not in ["_id","created_at","updated_at","attachments","completed_at"]}
            clone["status"] = "todo"
            clone["due_date"] = nxt
            clone["created_at"] = now
            clone["updated_at"] = now
            clone["completed_at"] = None
            clone["attachments"] = []  # fresh
            TASKS.insert_one(clone)

    return jsonify({"ok": True})

# ---------- delete ----------
@tasks_bp.delete("/api/tasks/<task_id>")
def api_tasks_delete(task_id):
    try:
        _id = ObjectId(task_id)
    except Exception:
        return jsonify({"error":"invalid id"}), 400

    # delete files
    for m in FILES_META.find({"task_id": _id}):
        try:
            fs.delete(m["file_id"])
        except Exception:
            pass
    FILES_META.delete_many({"task_id": _id})

    TASKS.delete_one({"_id": _id})
    return jsonify({"ok": True})

# ---------- attachments ----------
@tasks_bp.post("/api/tasks/<task_id>/files")
def api_tasks_upload_file(task_id):
    try:
        _id = ObjectId(task_id)
    except Exception:
        return jsonify({"error":"invalid id"}), 400
    if "file" not in request.files:
        return jsonify({"error":"no file"}), 400
    f = request.files["file"]
    blob_id = fs.put(f.stream, filename=f.filename, content_type=f.mimetype)
    meta = {
        "task_id": _id,
        "file_id": blob_id,
        "filename": f.filename,
        "mimetype": f.mimetype,
        "size": f.content_length,
        "uploaded_at": _now(),
    }
    m = FILES_META.insert_one(meta)
    TASKS.update_one({"_id": _id}, {"$push": {"attachments": m.inserted_id}})
    return jsonify({"ok": True})

@tasks_bp.get("/api/tasks/<task_id>/files")
def api_tasks_list_files(task_id):
    try:
        _id = ObjectId(task_id)
    except Exception:
        return jsonify({"error":"invalid id"}), 400
    out=[]
    for m in FILES_META.find({"task_id": _id}).sort("uploaded_at",-1):
        out.append({
            "meta_id": str(m["_id"]),
            "filename": m.get("filename"),
            "mimetype": m.get("mimetype"),
            "size": m.get("size"),
            "uploaded_at": m.get("uploaded_at").isoformat()
        })
    return jsonify(out)

@tasks_bp.get("/api/taskfiles/<meta_id>/download")
def api_taskfile_download(meta_id):
    try:
        mid = ObjectId(meta_id)
    except Exception:
        return abort(400)
    m = FILES_META.find_one({"_id": mid})
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

@tasks_bp.delete("/api/taskfiles/<meta_id>")
def api_taskfile_delete(meta_id):
    try:
        mid = ObjectId(meta_id)
    except Exception:
        return jsonify({"error":"invalid id"}), 400
    m = FILES_META.find_one({"_id": mid})
    if not m:
        return jsonify({"error":"not found"}), 404
    try:
        fs.delete(m["file_id"])
    except Exception:
        pass
    FILES_META.delete_one({"_id": mid})
    TASKS.update_one({"_id": m["task_id"]}, {"$pull": {"attachments": mid}})
    return jsonify({"ok": True})

__all__ = ["tasks_bp"]
