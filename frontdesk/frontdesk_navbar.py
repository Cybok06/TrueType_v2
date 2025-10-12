# frontdesk/frontdesk_navbar.py
from flask import Blueprint, jsonify
from datetime import datetime, timedelta
from db import db

# Match your current collection names
TASKS    = db["fd_tasks"]
MEETINGS = db["meetings"]
DOCS     = db["fd_docs"]

frontdesk_navbar_bp = Blueprint(
    "frontdesk_navbar_bp",
    __name__,
    url_prefix="/frontdesk/api/navbar"
)

def _now():
    return datetime.utcnow()

@frontdesk_navbar_bp.get("/counters")
def counters():
    """
    Returns:
      - tasks_undone: tasks not done
      - meetings_upcoming: meetings starting from now to +14 days
      - documents_total: total docs (optional for future badges)
    """
    try:
        undone = TASKS.count_documents({"status": {"$ne": "done"}})
    except Exception:
        undone = 0

    try:
        now = _now()
        soon = now + timedelta(days=14)
        meetings_upcoming = MEETINGS.count_documents({"start": {"$gte": now, "$lte": soon}})
    except Exception:
        meetings_upcoming = 0

    try:
        docs_total = DOCS.estimated_document_count()
    except Exception:
        docs_total = 0

    return jsonify({
        "tasks_undone": int(undone),
        "meetings_upcoming": int(meetings_upcoming),
        "documents_total": int(docs_total),
    })
