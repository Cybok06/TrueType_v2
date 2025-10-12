# frontdesk/frontdesk_dashboard.py
from flask import Blueprint, render_template, session, redirect, url_for
from datetime import datetime, timedelta
from pymongo import ASCENDING, DESCENDING
from db import db  # your Mongo connection

# Collections
EMPLOYEES = db["employees"]
TASKS     = db["fd_tasks"]
MEETINGS  = db["meetings"]
DOCS      = db["fd_docs"]

frontdesk_dashboard_bp = Blueprint("frontdesk_dashboard_bp", __name__)

# ---------- Indexes (safe, idempotent) ----------
def _ensure_indexes():
    try:
        # Compound indexes aligned with dashboard queries
        TASKS.create_index([("status", ASCENDING), ("due_date", ASCENDING)], name="status_1_due_1")
        TASKS.create_index([("created_at", DESCENDING)], name="created_desc")
        MEETINGS.create_index([("start", ASCENDING)], name="start_1")
        DOCS.create_index([("uploaded_at", DESCENDING)], name="uploaded_desc")
        EMPLOYEES.create_index([("_id", ASCENDING)], name="_id_1")
    except Exception:
        pass

_ensure_indexes()

# ---------- Small format helpers ----------
def _fmt_when(dt: datetime) -> str:
    if not dt:
        return "—"
    return dt.strftime("%b %d, %Y %I:%M %p")

def _fmt_due(dt: datetime) -> str:
    if not dt:
        return "no due date"
    today = datetime.utcnow().date()
    d = dt.date()
    if d == today:
        return f"today · {dt.strftime('%I:%M %p')}"
    if d == today + timedelta(days=1):
        return f"tomorrow · {dt.strftime('%I:%M %p')}"
    return dt.strftime("%b %d · %I:%M %p")

# ---------- Route ----------
@frontdesk_dashboard_bp.route("/dashboard")
def dashboard():
    # Gate
    role = session.get("role")
    if role not in ("front_desk", "frontdesk", "admin", "superadmin"):
        return redirect(url_for("login.login"))

    name = session.get("name", "Front Desk")
    now = datetime.utcnow()

    # -------- Employees / Documents: O(1) fast estimates
    total_employees = 0
    total_documents = 0
    try:
        total_employees = EMPLOYEES.estimated_document_count()
    except Exception:
        # fallback if needed
        total_employees = EMPLOYEES.count_documents({})

    try:
        total_documents = DOCS.estimated_document_count()
    except Exception:
        total_documents = DOCS.count_documents({})

    # -------- Tasks: count + top list in ONE aggregate
    undone_tasks_cnt = 0
    top_tasks = []
    match_tasks = {"status": {"$in": ["todo", "in_progress", "blocked"]}}
    tasks_pipeline = [
        {"$match": match_tasks},
        {
            "$facet": {
                "count": [{"$count": "n"}],
                "list": [
                    {"$sort": {"due_date": 1, "created_at": -1}},
                    {"$limit": 6},
                    {"$project": {
                        "_id": 0,
                        "title": {"$ifNull": ["$title", "(untitled)"]},
                        "status": 1,
                        "priority": {"$ifNull": ["$priority", "normal"]},
                        "due_date": 1
                    }}
                ]
            }
        }
    ]
    try:
        agg_opts = {"allowDiskUse": False}
        cur = TASKS.aggregate(tasks_pipeline, **agg_opts)
        res = next(cur, {"count": [], "list": []})
        undone_tasks_cnt = (res.get("count") or [{}])[0].get("n", 0) if res.get("count") else 0
        raw_list = res.get("list") or []
        # Light formatting only for the 6 items
        for t in raw_list:
            dd = t.get("due_date")
            top_tasks.append({
                "title": (t.get("title") or "(untitled)").strip(),
                "status": t.get("status") or "todo",
                "priority": t.get("priority") or "normal",
                "due_date": dd,
                "due_human": _fmt_due(dd)
            })
    except Exception:
        # Hint retry (older Mongo or missing index can cause slow path—skip hint to be safe)
        try:
            cur = TASKS.aggregate(tasks_pipeline)
            res = next(cur, {"count": [], "list": []})
            undone_tasks_cnt = (res.get("count") or [{}])[0].get("n", 0) if res.get("count") else 0
            raw_list = res.get("list") or []
            for t in raw_list:
                dd = t.get("due_date")
                top_tasks.append({
                    "title": (t.get("title") or "(untitled)").strip(),
                    "status": t.get("status") or "todo",
                    "priority": t.get("priority") or "normal",
                    "due_date": dd,
                    "due_human": _fmt_due(dd)
                })
        except Exception:
            undone_tasks_cnt = 0
            top_tasks = []

    # -------- Meetings: count + upcoming list in ONE aggregate
    upcoming_meet_cnt = 0
    next_meetings = []
    match_meetings = {"start": {"$gte": now}}
    meetings_pipeline = [
        {"$match": match_meetings},
        {
            "$facet": {
                "count": [{"$count": "n"}],
                "list": [
                    {"$sort": {"start": 1}},
                    {"$limit": 6},
                    {"$project": {
                        "_id": 0,
                        "title": {"$ifNull": ["$title", "Meeting"]},
                        "start": 1,
                        "end": 1,
                        "location": {"$ifNull": ["$location", ""]},
                        "category": {"$ifNull": ["$category", ""]}
                    }}
                ]
            }
        }
    ]
    try:
        cur = MEETINGS.aggregate(meetings_pipeline, allowDiskUse=False)
        res = next(cur, {"count": [], "list": []})
        upcoming_meet_cnt = (res.get("count") or [{}])[0].get("n", 0) if res.get("count") else 0
        raw_list = res.get("list") or []
        for m in raw_list:
            st = m.get("start")
            next_meetings.append({
                "title": (m.get("title") or "Meeting").strip(),
                "when": _fmt_when(st) if st else "—",
                "location": (m.get("location") or "").strip(),
                "category": (m.get("category") or "").strip(),
            })
    except Exception:
        try:
            cur = MEETINGS.aggregate(meetings_pipeline)
            res = next(cur, {"count": [], "list": []})
            upcoming_meet_cnt = (res.get("count") or [{}])[0].get("n", 0) if res.get("count") else 0
            raw_list = res.get("list") or []
            for m in raw_list:
                st = m.get("start")
                next_meetings.append({
                    "title": (m.get("title") or "Meeting").strip(),
                    "when": _fmt_when(st) if st else "—",
                    "location": (m.get("location") or "").strip(),
                    "category": (m.get("category") or "").strip(),
                })
        except Exception:
            upcoming_meet_cnt = 0
            next_meetings = []

    # -------- Render
    return render_template(
        "frontdesk_pages/dashboard.html",
        name=name,
        kpis={
            "employees": total_employees,
            "tasks": undone_tasks_cnt,
            "meetings": upcoming_meet_cnt,
            "documents": total_documents,
        },
        top_tasks=top_tasks,
        next_meetings=next_meetings,
        now=now.isoformat()
    )
