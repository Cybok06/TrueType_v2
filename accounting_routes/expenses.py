from __future__ import annotations

from flask import Blueprint, render_template, request, jsonify
from datetime import datetime, date, time, timedelta
from typing import Any, Dict, List

from bson import ObjectId
from db import db

acc_expenses = Blueprint(
    "acc_expenses",
    __name__,
    template_folder="../templates",
)

expenses_col = db["expenses"]
expense_categories_col = db["expense_categories"]


# ---------- helpers ----------

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        # treat as local date at midnight
        d = datetime.strptime(s, "%Y-%m-%d").date()
        return datetime.combine(d, time.min)
    except Exception:
        return None


def _serialize_expense(doc: Dict[str, Any]) -> Dict[str, Any]:
    _id = doc.get("_id")
    dt = doc.get("date")
    if isinstance(dt, datetime):
        d = dt.date()
    elif isinstance(dt, date):
        d = dt
    else:
        d = None

    return {
        "id": str(_id) if isinstance(_id, ObjectId) else "",
        "date": d.isoformat() if isinstance(d, date) else "",
        "amount": float(doc.get("amount", 0.0) or 0.0),
        "category": (doc.get("category") or "").strip() or "Uncategorized",
        "description": doc.get("description") or "",
        "payment_method": doc.get("payment_method") or "",
    }


def _compute_totals(docs: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = 0.0
    cat_map: Dict[str, float] = {}
    for d in docs:
        amt = float(d.get("amount", 0.0) or 0.0)
        total += amt
        cat = (d.get("category") or "").strip() or "Uncategorized"
        cat_map[cat] = cat_map.get(cat, 0.0) + amt

    cat_totals = [
        {"category": k, "total": float(v)}
        for k, v in sorted(cat_map.items(), key=lambda kv: kv[1], reverse=True)
    ]

    return {
        "count": len(docs),
        "total_amount": float(total),
        "category_totals": cat_totals,
    }


def _date_range_for_month(today: date) -> tuple[datetime, datetime]:
    start = today.replace(day=1)
    # end is today end-of-day
    end = datetime.combine(today, time.max)
    return datetime.combine(start, time.min), end


# ---------- pages ----------

@acc_expenses.get("/expenses")
def expenses_page():
    today = date.today()
    start_dt, end_dt = _date_range_for_month(today)

    query: Dict[str, Any] = {
        "date": {"$gte": start_dt, "$lte": end_dt}
    }

    docs = list(
        expenses_col.find(query)
        .sort("date", -1)
        .limit(500)
    )

    totals_info = _compute_totals(docs)
    initial_data = {
        "expenses": [_serialize_expense(d) for d in docs],
        "totals": {
            "count": totals_info["count"],
            "total_amount": totals_info["total_amount"],
        },
        "category_totals": totals_info["category_totals"],
        "default_range": {
            "start": start_dt.date().isoformat(),
            "end": today.isoformat(),
        },
    }

    # distinct categories (from dedicated collection first)
    cat_docs = list(expense_categories_col.find({}).sort("name", 1))
    categories = [c.get("name") for c in cat_docs if c.get("name")]

    # also merge categories from existing docs just in case
    seen = set(categories)
    for d in docs:
        cat = (d.get("category") or "").strip()
        if cat and cat not in seen:
            seen.add(cat)
            categories.append(cat)

    default_start = start_dt.date().isoformat()
    default_end = today.isoformat()

    return render_template(
        "accounting/expenses.html",
        initial_data=initial_data,
        categories=categories,
        default_start=default_start,
        default_end=default_end,
    )


# ---------- API: list with filters ----------

@acc_expenses.get("/expenses/list")
def expenses_list():
    start_str = (request.args.get("start") or "").strip()
    end_str = (request.args.get("end") or "").strip()
    category = (request.args.get("category") or "").strip()
    search = (request.args.get("search") or "").strip()
    min_str = (request.args.get("min") or "").strip()
    max_str = (request.args.get("max") or "").strip()

    query: Dict[str, Any] = {}

    # Date range
    start_dt = _parse_date(start_str)
    end_dt = _parse_date(end_str)
    if end_dt:
        # make end inclusive end-of-day
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    if start_dt or end_dt:
        query["date"] = {}
        if start_dt:
            query["date"]["$gte"] = start_dt
        if end_dt:
            query["date"]["$lte"] = end_dt

    # Category
    if category:
        query["category"] = category

    # Search in description / reference
    if search:
        query["$or"] = [
            {"description": {"$regex": search, "$options": "i"}},
            {"reference": {"$regex": search, "$options": "i"}},
        ]

    # Amount range handled client-side after serialization for simplicity
    docs = list(
        expenses_col.find(query)
        .sort("date", -1)
        .limit(1000)
    )

    # Apply amount filter in Python (since values are float anyway)
    min_amt = _safe_float(min_str, None) if min_str else None
    max_amt = _safe_float(max_str, None) if max_str else None

    filtered_docs: List[Dict[str, Any]] = []
    for d in docs:
        amt = float(d.get("amount", 0.0) or 0.0)
        if min_amt is not None and amt < min_amt:
            continue
        if max_amt is not None and amt > max_amt:
            continue
        filtered_docs.append(d)

    totals_info = _compute_totals(filtered_docs)
    data = {
        "expenses": [_serialize_expense(d) for d in filtered_docs],
        "totals": {
            "count": totals_info["count"],
            "total_amount": totals_info["total_amount"],
        },
        "category_totals": totals_info["category_totals"],
    }

    return jsonify(ok=True, data=data)


# ---------- API: create expense ----------

@acc_expenses.post("/expenses/create")
def create_expense():
    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        return jsonify(ok=False, message="Invalid JSON body."), 400

    if not isinstance(data, dict):
        return jsonify(ok=False, message="Invalid payload format."), 400

    date_str = (data.get("date") or "").strip()
    amount_str = (data.get("amount") or "").strip()
    category = (data.get("category") or "").strip()
    payment_method = (data.get("payment_method") or "").strip()
    description = (data.get("description") or "").strip()

    if not date_str or not amount_str or not category:
        return jsonify(ok=False, message="Date, amount and category are required."), 400

    dt = _parse_date(date_str) or datetime.utcnow()
    amount = _safe_float(amount_str)
    if amount <= 0:
        return jsonify(ok=False, message="Amount must be greater than zero."), 400

    now = datetime.utcnow()

    doc: Dict[str, Any] = {
        "date": dt,
        "amount": amount,
        "category": category,
        "payment_method": payment_method,
        "description": description,
        "created_at": now,
        "updated_at": now,
    }

    # optional reference if client ever sends it in future
    if "reference" in data:
        doc["reference"] = (data.get("reference") or "").strip()

    res = expenses_col.insert_one(doc)
    doc["_id"] = res.inserted_id

    # upsert category for future suggestions
    if category:
        expense_categories_col.update_one(
            {"name": category},
            {"$setOnInsert": {"name": category, "created_at": now}},
            upsert=True,
        )

    expense_out = _serialize_expense(doc)
    return jsonify(ok=True, expense=expense_out), 200
