# admin_reports.py
from __future__ import annotations

from flask import (
    Blueprint, render_template, request,
    session, redirect, url_for, flash, jsonify
)
from datetime import datetime, timedelta
from bson import ObjectId
from db import db

admin_reports_bp = Blueprint(
    "admin_reports",
    __name__,
    url_prefix="/admin/reports",
    template_folder="templates",
)

# Collections
orders_col          = db["orders"]
payments_col        = db["payments"]
truck_payments_col  = db["truck_payments"]
s_bdc_payment_col   = db["s_bdc_payment"]
omc_payment_col     = db["omc_payment"]
clients_col         = db["clients"]
bdc_col             = db["bdc"]


# ---------- Helpers ----------

def _start_of_day(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _parse_date_str(s: str | None) -> datetime | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            d = datetime.strptime(s, fmt)
            return _start_of_day(d)
        except Exception:
            continue
    return None


def _date_range_from_param(range_key: str, start_s: str | None, end_s: str | None):
    """
    Returns (start_dt, end_dt, label)
    end_dt is exclusive (start <= x < end).
    """
    today = _start_of_day(datetime.utcnow())

    range_key = (range_key or "month").lower()

    if range_key == "today":
        start = today
        end = start + timedelta(days=1)
        label = "Today"

    elif range_key == "week":
        # Monday–Sunday of current week (UTC)
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=7)
        label = "This Week"

    elif range_key == "custom":
        start = _parse_date_str(start_s) or today
        end = _parse_date_str(end_s) or start
        end = end + timedelta(days=1)
        label = f"{start.strftime('%d-%b-%Y')} to {(end - timedelta(days=1)).strftime('%d-%b-%Y')}"

    else:  # "month" or default
        start = today.replace(day=1)
        # next month
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        label = start.strftime("Month of %b %Y")

    return start, end, label


def _to_f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _compute_trading():
    """
    Core logic used by both the HTML page and the AJAX JSON endpoint.
    Reads query params from request.args.
    Returns a context dict.
    """
    # -------- Main period params --------
    range_key = request.args.get("range", "month")
    start_s   = request.args.get("start")
    end_s     = request.args.get("end")

    start_dt, end_dt, range_label = _date_range_from_param(range_key, start_s, end_s)

    # --- ORDERS in main range ---
    order_filter = {"date": {"$gte": start_dt, "$lt": end_dt}}
    orders = list(
        orders_col.find(
            order_filter,
            {
                "client_id": 1,
                "product": 1,
                "quantity": 1,
                "date": 1,
                "status": 1,
                "order_type": 1,
                "depot": 1,
                "bdc_name": 1,
                "omc": 1,
                "region": 1,
                "total_debt": 1,
                "returns_total": 1,
                "returns_sbdc": 1,
                "returns_stax": 1,
                "margin": 1,
                "margin_tax": 1,
                "order_id": 1,
                "due_date": 1,
                "shareholder": 1,
            },
        ).sort("date", -1)
    )

    order_ids = [o["_id"] for o in orders]

    # --- Attach client names (bulk, not N+1) ---
    client_oid_ids = []
    client_code_ids = []
    for o in orders:
        cid = o.get("client_id")
        if isinstance(cid, ObjectId):
            client_oid_ids.append(cid)
        elif cid:
            client_code_ids.append(str(cid))

    clients_by_oid = {}
    if client_oid_ids:
        for c in clients_col.find(
            {"_id": {"$in": list(set(client_oid_ids))}},
            {"name": 1, "client_id": 1},
        ):
            clients_by_oid[c["_id"]] = c

    clients_by_code = {}
    if client_code_ids:
        for c in clients_col.find(
            {"client_id": {"$in": list(set(client_code_ids))}},
            {"name": 1, "client_id": 1},
        ):
            if c.get("client_id"):
                clients_by_code[c["client_id"]] = c

    for o in orders:
        cid = o.get("client_id")
        client_name = "Unknown"
        client_code = ""
        if isinstance(cid, ObjectId):
            c = clients_by_oid.get(cid)
            if c:
                client_name = c.get("name") or "Unknown"
                client_code = c.get("client_id") or ""
        elif cid:
            c = clients_by_code.get(str(cid))
            if c:
                client_name = c.get("name") or "Unknown"
                client_code = c.get("client_id") or ""
        o["client_name"] = client_name
        o["client_code"] = client_code

    # --- Payments in date range (for chart & collections) ---
    pay_filter = {"date": {"$gte": start_dt, "$lt": end_dt}}

    order_payments = list(
        payments_col.find(pay_filter).sort("date", -1)
    )
    truck_payments = list(
        truck_payments_col.find(pay_filter).sort("date", -1)
    )

    # Totals for confirmed order payments per order (for outstanding)
    paid_by_order = {}
    confirmed_cursor = payments_col.find(
        {
            "order_id": {"$in": order_ids},
            "status": "confirmed",
        }
    )
    for p in confirmed_cursor:
        oid = p.get("order_id")
        amt = _to_f(p.get("amount"))
        if not isinstance(oid, ObjectId):
            continue
        paid_by_order[oid] = paid_by_order.get(oid, 0.0) + amt

    # --- Summaries (main period) ---
    total_orders    = len(orders)
    approved_orders = sum(1 for o in orders if (o.get("status") or "").lower() == "approved")
    pending_orders  = sum(1 for o in orders if (o.get("status") or "").lower() == "pending")
    declined_orders = sum(1 for o in orders if (o.get("status") or "").lower() == "declined")

    total_qty     = sum(_to_f(o.get("quantity")) for o in orders)
    total_debt    = sum(_to_f(o.get("total_debt")) for o in orders)
    total_returns = sum(_to_f(o.get("returns_total")) for o in orders)

    order_payments_total = sum(
        _to_f(p.get("amount"))
        for p in order_payments
        if (p.get("status") or "pending") == "confirmed"
    )
    truck_payments_total = sum(
        _to_f(p.get("amount"))
        for p in truck_payments
        if (p.get("status") or "pending") == "confirmed"
    )

    outstanding_total = 0.0
    for o in orders:
        td = _to_f(o.get("total_debt"))
        paid = paid_by_order.get(o["_id"], 0.0)
        outstanding = max(td - paid, 0.0)
        outstanding_total += outstanding
        o["paid_amount"] = paid
        o["outstanding"] = outstanding

    # --- BDC & OMC summary (created in this range) ---
    bdc_filter = {"created_at": {"$gte": start_dt, "$lt": end_dt}}
    omc_filter = {"created_at": {"$gte": start_dt, "$lt": end_dt}}

    s_bdc_payments = list(s_bdc_payment_col.find(bdc_filter))
    omc_payments   = list(omc_payment_col.find(omc_filter))

    # Map BDC id -> name
    bdc_ids = [p.get("bdc_id") for p in s_bdc_payments if isinstance(p.get("bdc_id"), ObjectId)]
    bdc_names_by_id = {}
    if bdc_ids:
        for b in bdc_col.find({"_id": {"$in": list(set(bdc_ids))}}, {"name": 1}):
            bdc_names_by_id[b["_id"]] = b.get("name") or "BDC"

    bdc_summary = {}
    for p in s_bdc_payments:
        bdc_id = p.get("bdc_id")
        name = bdc_names_by_id.get(bdc_id, "BDC")
        row = bdc_summary.setdefault(
            name,
            {"bdc_name": name, "count": 0, "total_amount": 0.0},
        )
        row["count"] += 1
        row["total_amount"] += _to_f(p.get("amount"))

    bdc_summary_list = sorted(
        bdc_summary.values(),
        key=lambda r: r["total_amount"],
        reverse=True,
    )

    # OMC summary (omc_name stored directly in docs)
    omc_summary = {}
    for p in omc_payments:
        name = p.get("omc_name") or "OMC"
        row = omc_summary.setdefault(
            name,
            {"omc_name": name, "count": 0, "total_amount": 0.0},
        )
        row["count"] += 1
        row["total_amount"] += _to_f(p.get("amount"))

    omc_summary_list = sorted(
        omc_summary.values(),
        key=lambda r: r["total_amount"],
        reverse=True,
    )

    # --- Chart data (per day) ---
    labels = []
    orders_count = []
    debt_per_day = []
    collection_per_day = []

    day_index = {}
    cur = start_dt
    idx = 0
    while cur < end_dt:
        label = cur.strftime("%d-%b")
        labels.append(label)
        orders_count.append(0)
        debt_per_day.append(0.0)
        collection_per_day.append(0.0)
        day_index[cur.date()] = idx
        idx += 1
        cur = cur + timedelta(days=1)

    # Orders per day
    for o in orders:
        d = o.get("date")
        if isinstance(d, datetime):
            k = d.date()
            if k in day_index:
                i = day_index[k]
                orders_count[i] += 1
                debt_per_day[i] += _to_f(o.get("total_debt"))

    # Collections per day (order + truck confirmed)
    for p in order_payments + truck_payments:
        if (p.get("status") or "pending") != "confirmed":
            continue
        d = p.get("date")
        if isinstance(d, datetime):
            k = d.date()
            if k in day_index:
                i = day_index[k]
                collection_per_day[i] += _to_f(p.get("amount"))

    charts_data = {
        "labels": labels,
        "orders_count": orders_count,
        "debt_per_day": debt_per_day,
        "collection_per_day": collection_per_day,
    }

    summary = {
        "range_label": range_label,
        "total_orders": total_orders,
        "approved_orders": approved_orders,
        "pending_orders": pending_orders,
        "declined_orders": declined_orders,
        "total_quantity": total_qty,
        "total_debt": total_debt,
        "total_returns": total_returns,
        "order_payments_total": order_payments_total,
        "truck_payments_total": truck_payments_total,
        "outstanding_total": outstanding_total,
    }

    # ---------- Comparison period (optional) ----------
    compare_enabled = request.args.get("compare") == "1"
    compare_summary = None
    cmp_range_key   = request.args.get("cmp_range") or "today"
    cmp_start_date  = None
    cmp_end_date    = None

    if compare_enabled:
        cmp_start_s = request.args.get("cmp_start")
        cmp_end_s   = request.args.get("cmp_end")

        cmp_start_dt, cmp_end_dt, cmp_label = _date_range_from_param(
            cmp_range_key,
            cmp_start_s,
            cmp_end_s,
        )
        cmp_start_date = cmp_start_dt.date()
        cmp_end_date   = (cmp_end_dt - timedelta(days=1)).date()

        # Orders in comparison range
        cmp_order_filter = {"date": {"$gte": cmp_start_dt, "$lt": cmp_end_dt}}
        cmp_orders = list(
            orders_col.find(
                cmp_order_filter,
                {
                    "quantity": 1,
                    "date": 1,
                    "status": 1,
                    "total_debt": 1,
                    "returns_total": 1,
                    "order_id": 1,
                },
            )
        )
        cmp_order_ids = [o["_id"] for o in cmp_orders]

        # Payments in comparison range
        cmp_pay_filter = {"date": {"$gte": cmp_start_dt, "$lt": cmp_end_dt}}
        cmp_order_payments = list(payments_col.find(cmp_pay_filter))
        cmp_truck_payments = list(truck_payments_col.find(cmp_pay_filter))

        # per-order confirmed payments for comparison period
        cmp_paid_by_order = {}
        cmp_confirmed = payments_col.find(
            {"order_id": {"$in": cmp_order_ids}, "status": "confirmed"}
        )
        for p in cmp_confirmed:
            oid = p.get("order_id")
            amt = _to_f(p.get("amount"))
            if not isinstance(oid, ObjectId):
                continue
            cmp_paid_by_order[oid] = cmp_paid_by_order.get(oid, 0.0) + amt

        cmp_total_orders    = len(cmp_orders)
        cmp_approved_orders = sum(
            1 for o in cmp_orders if (o.get("status") or "").lower() == "approved"
        )
        cmp_pending_orders  = sum(
            1 for o in cmp_orders if (o.get("status") or "").lower() == "pending"
        )
        cmp_declined_orders = sum(
            1 for o in cmp_orders if (o.get("status") or "").lower() == "declined"
        )

        cmp_total_qty     = sum(_to_f(o.get("quantity")) for o in cmp_orders)
        cmp_total_debt    = sum(_to_f(o.get("total_debt")) for o in cmp_orders)
        cmp_total_returns = sum(_to_f(o.get("returns_total")) for o in cmp_orders)

        cmp_order_payments_total = sum(
            _to_f(p.get("amount"))
            for p in cmp_order_payments
            if (p.get("status") or "pending") == "confirmed"
        )
        cmp_truck_payments_total = sum(
            _to_f(p.get("amount"))
            for p in cmp_truck_payments
            if (p.get("status") or "pending") == "confirmed"
        )

        cmp_outstanding_total = 0.0
        for o in cmp_orders:
            td = _to_f(o.get("total_debt"))
            paid = cmp_paid_by_order.get(o["_id"], 0.0)
            outstanding = max(td - paid, 0.0)
            cmp_outstanding_total += outstanding

        compare_summary = {
            "range_label": cmp_label,
            "total_orders": cmp_total_orders,
            "approved_orders": cmp_approved_orders,
            "pending_orders": cmp_pending_orders,
            "declined_orders": cmp_declined_orders,
            "total_quantity": cmp_total_qty,
            "total_debt": cmp_total_debt,
            "total_returns": cmp_total_returns,
            "order_payments_total": cmp_order_payments_total,
            "truck_payments_total": cmp_truck_payments_total,
            "outstanding_total": cmp_outstanding_total,
        }

    # Combine payments for the history table (main period)
    combined_payments = []
    for p in order_payments:
        combined_payments.append({
            "type": "Order",
            "date": p.get("date"),
            "amount": _to_f(p.get("amount")),
            "bank_name": p.get("bank_name", "-"),
            "account_last4": p.get("account_last4", ""),
            "status": p.get("status", "pending"),
        })
    for p in truck_payments:
        combined_payments.append({
            "type": "Truck",
            "date": p.get("date"),
            "amount": _to_f(p.get("amount")),
            "bank_name": p.get("bank_name", "-"),
            "account_last4": p.get("account_last4", ""),
            "status": p.get("status", "pending"),
        })
    combined_payments.sort(key=lambda x: x["date"] or datetime.min, reverse=True)

    return {
        "summary": summary,
        "orders": orders,
        "bdc_summary": bdc_summary_list,
        "omc_summary": omc_summary_list,
        "payments": combined_payments,
        "charts_data": charts_data,
        "range_key": range_key,
        "start_dt": start_dt,
        "end_dt": end_dt,
        "compare_enabled": compare_enabled,
        "compare_summary": compare_summary,
        "cmp_range_key": cmp_range_key,
        "cmp_start_date": cmp_start_date,
        "cmp_end_date": cmp_end_date,
    }


# ---------- Routes ----------

@admin_reports_bp.route("/trading", methods=["GET"])
def trading_report():
    # Access control: admin only
    if session.get("role") != "admin":
        flash("Access denied. Admin only.", "danger")
        return redirect(url_for("login.login"))

    ctx = _compute_trading()

    return render_template(
        "admin_trading_report.html",
        summary=ctx["summary"],
        orders=ctx["orders"],
        bdc_summary=ctx["bdc_summary"],
        omc_summary=ctx["omc_summary"],
        payments=ctx["payments"],
        charts_data=ctx["charts_data"],
        range_key=ctx["range_key"],
        start_date=ctx["start_dt"].date(),
        end_date=(ctx["end_dt"] - timedelta(days=1)).date(),
        compare_enabled=ctx["compare_enabled"],
        compare_summary=ctx["compare_summary"],
        cmp_range_key=ctx["cmp_range_key"],
        cmp_start_date=ctx["cmp_start_date"],
        cmp_end_date=ctx["cmp_end_date"],
    )


@admin_reports_bp.route("/trading/data", methods=["GET"])
def trading_report_data():
    """
    AJAX endpoint – returns JSON so front-end can update
    KPIs, charts, and tables without full page reload.
    """
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "Access denied"}), 403

    ctx = _compute_trading()

    # Build JSON-safe projections
    def _fmt_date(d, fmt):
        return d.strftime(fmt) if isinstance(d, datetime) else ""

    orders_json = []
    for o in ctx["orders"]:
        d = o.get("date")
        orders_json.append({
            "date": _fmt_date(d, "%d-%b-%y"),
            "order_id": str(o.get("order_id") or o.get("_id")),
            "client_name": o.get("client_name", ""),
            "client_code": o.get("client_code", ""),
            "product": o.get("product") or "",
            "order_type": (o.get("order_type") or "").upper(),
            "quantity": _to_f(o.get("quantity")),
            "total_debt": _to_f(o.get("total_debt")),
            "paid_amount": _to_f(o.get("paid_amount")),
            "outstanding": _to_f(o.get("outstanding")),
            "status": (o.get("status") or "").lower(),
        })

    payments_json = []
    for p in ctx["payments"]:
        d = p.get("date")
        payments_json.append({
            "date": _fmt_date(d, "%d-%b-%y %H:%M"),
            "type": p.get("type"),
            "amount": _to_f(p.get("amount")),
            "bank_name": p.get("bank_name"),
            "account_last4": p.get("account_last4"),
            "status": (p.get("status") or "").lower(),
        })

    bdc_json = [
        {
            "bdc_name": r.get("bdc_name", ""),
            "count": int(r.get("count", 0)),
            "total_amount": _to_f(r.get("total_amount")),
        }
        for r in ctx["bdc_summary"]
    ]

    omc_json = [
        {
            "omc_name": r.get("omc_name", ""),
            "count": int(r.get("count", 0)),
            "total_amount": _to_f(r.get("total_amount")),
        }
        for r in ctx["omc_summary"]
    ]

    payload = {
        "ok": True,
        "summary": ctx["summary"],
        "compare_enabled": ctx["compare_enabled"],
        "compare_summary": ctx["compare_summary"],
        "charts_data": ctx["charts_data"],
        "orders": orders_json,
        "bdc_summary": bdc_json,
        "omc_summary": omc_json,
        "payments": payments_json,
        "range_key": ctx["range_key"],
        "start_date": ctx["start_dt"].strftime("%Y-%m-%d"),
        "end_date": (ctx["end_dt"] - timedelta(days=1)).strftime("%Y-%m-%d"),
        "cmp_range_key": ctx["cmp_range_key"],
        "cmp_start_date": ctx["cmp_start_date"].strftime("%Y-%m-%d") if ctx["cmp_start_date"] else None,
        "cmp_end_date": ctx["cmp_end_date"].strftime("%Y-%m-%d") if ctx["cmp_end_date"] else None,
    }

    return jsonify(payload)
