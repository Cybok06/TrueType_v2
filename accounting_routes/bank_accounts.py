from __future__ import annotations

from flask import Blueprint, render_template, request, url_for, Response, jsonify
from datetime import datetime, date
import io, csv
from typing import Any, Dict, List

from bson import ObjectId
from db import db

bank_accounts_bp = Blueprint("bank_accounts", __name__, template_folder="../templates")

# Collections
accounts_col = db["bank_accounts"]
payments_col = db["payments"]        # confirmed inbound cash-ins
tax_col      = db["tax_records"]     # P-Tax outflows (source_bank_id)
sbdc_col     = db["s_bdc_payment"]   # BDC bank payments (bank_paid_history)


# ----------------- helpers -----------------
def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _currency_symbol(code: str) -> str:
    code = (code or "").upper()
    if code in ("GHS", "GH₵", "GHC"):
        return "GH₵"
    if code == "USD":
        return "$"
    if code == "EUR":
        return "€"
    if code == "GBP":
        return "£"
    return ""


def _last4(acc_number: str | None) -> str:
    s = str(acc_number or "")
    return s[-4:] if len(s) >= 4 else s


def _sum_confirmed_in(bank_name: str, last4: str) -> float:
    """Sum of confirmed inbound payments for this bank (by name + last4)."""
    try:
        pipe = [
            {
                "$match": {
                    "bank_name": bank_name,
                    "account_last4": last4,
                    "status": "confirmed",
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(payments_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _sum_ptax_out(bank_oid: ObjectId) -> float:
    """Sum of P-Tax payments made from this bank (tax_records.source_bank_id)."""
    try:
        pipe = [
            {
                "$match": {
                    "source_bank_id": bank_oid,
                    "type": {"$regex": r"^p[\s_-]*tax$", "$options": "i"},
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(tax_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


def _sum_bdc_out(bank_oid: ObjectId) -> float:
    """Sum of BDC bank payments (sum bank_paid_history.amount where bank_id==bank_oid)."""
    try:
        pipe = [
            {"$match": {"bank_paid_history": {"$exists": True, "$ne": []}}},
            {"$unwind": "$bank_paid_history"},
            {"$match": {"bank_paid_history.bank_id": bank_oid}},
            {"$group": {"_id": None, "total": {"$sum": "$bank_paid_history.amount"}}},
        ]
        row = next(sbdc_col.aggregate(pipe), None)
        return _safe_float(row["total"]) if row else 0.0
    except Exception:
        return 0.0


# ----------------- pages -----------------
@bank_accounts_bp.get("/bank-accounts")
def list_accounts():
    """
    Bank & Cash Accounts dashboard.
    """
    docs = list(accounts_col.find({}).sort("bank_name", 1))

    accounts: List[Dict[str, Any]] = []
    total_live_balance = 0.0

    for d in docs:
        bank_oid = d.get("_id")
        if not isinstance(bank_oid, ObjectId):
            continue

        bank_name = d.get("bank_name") or ""

        raw_acc_no = d.get("account_no") or d.get("account_number") or ""
        last4 = _last4(raw_acc_no)

        opening = _safe_float(d.get("opening_balance"))
        total_in = _sum_confirmed_in(bank_name, last4)
        ptax_out = _sum_ptax_out(bank_oid)
        bdc_out = _sum_bdc_out(bank_oid)
        total_out = ptax_out + bdc_out

        live_balance = opening + total_in - total_out
        total_live_balance += live_balance

        cur = (d.get("currency") or "GHS").upper()
        sym = d.get("currency_symbol") or _currency_symbol(cur)

        account_no_masked = f"…{last4}" if last4 else ""

        acc_dict: Dict[str, Any] = dict(d)
        acc_dict["id"] = str(bank_oid)
        acc_dict["opening_balance"] = opening
        acc_dict["balance"] = live_balance
        acc_dict["currency"] = cur
        acc_dict["currency_symbol"] = sym
        acc_dict["account_no_masked"] = account_no_masked
        acc_dict["last_reconciled"] = d.get("last_reconciled")
        acc_dict["metrics"] = {
            "total_in": round(total_in, 2),
            "ptax_out": round(ptax_out, 2),
            "bdc_out": round(bdc_out, 2),
            "net_flow": round(total_in - total_out, 2),
        }

        accounts.append(acc_dict)

    today = date.today().isoformat()

    if accounts:
        first = accounts[0]
        sym = first.get("currency_symbol") or _currency_symbol(first.get("currency", "GHS"))
    else:
        sym = "GH₵"

    total_live_balance = float(total_live_balance)
    total_display = f"{sym}{total_live_balance:0.2f}"

    # 🔴 FIXED: use the correct blueprint endpoint name
    export_url = url_for("acc_bank_accounts.export_excel")

    return render_template(
        "accounting/bank_accounts.html",
        accounts=accounts,
        total_display=total_display,
        today=today,
        export_url=export_url,
    )


@bank_accounts_bp.post("/bank-accounts/quick-create")
def quick_create():
    """
    Handles the slide-over 'Add Bank Account' form.
    """
    data = {
        "account_name": (request.form.get("account_name") or "").strip(),
        "bank_name": (request.form.get("bank_name") or "").strip(),
        "account_no": (request.form.get("account_no") or "").strip(),
        "currency": (request.form.get("currency") or "GHS").upper(),
        "opening_balance": _safe_float(request.form.get("opening_balance")),
        "as_of_date": request.form.get("as_of_date") or None,
        "notes": (request.form.get("notes") or "").strip(),
        "created_at": datetime.utcnow(),
    }

    if data["as_of_date"]:
        try:
            data["as_of_date"] = datetime.fromisoformat(data["as_of_date"])
        except Exception:
            data["as_of_date"] = None

    data["balance"] = data["opening_balance"]
    data["currency_symbol"] = _currency_symbol(data["currency"])
    data["last_reconciled"] = None

    if not data["account_name"] or not data["bank_name"]:
        return jsonify(ok=False, message="Account name and bank name are required."), 400

    res = accounts_col.insert_one(data)
    return jsonify(ok=True, id=str(res.inserted_id))


@bank_accounts_bp.get("/bank-accounts/export")
def export_excel():
    """
    Export bank accounts as CSV (Excel-compatible).
    """
    docs = list(accounts_col.find({}).sort("bank_name", 1))

    out = io.StringIO()
    w = csv.writer(out)

    w.writerow([
        "Bank Name",
        "Account Name",
        "Account Number",
        "Currency",
        "Opening Balance",
        "Total Inflow (confirmed)",
        "P-Tax Out",
        "BDC Out",
        "Net Flow (In - Out)",
        "Live Balance",
        "Last Reconciled",
    ])

    for d in docs:
        bank_oid = d.get("_id")
        if not isinstance(bank_oid, ObjectId):
            continue

        bank_name = d.get("bank_name") or ""
        raw_acc_no = d.get("account_no") or d.get("account_number") or ""
        last4 = _last4(raw_acc_no)

        opening = _safe_float(d.get("opening_balance"))
        total_in = _sum_confirmed_in(bank_name, last4)
        ptax_out = _sum_ptax_out(bank_oid)
        bdc_out = _sum_bdc_out(bank_oid)
        total_out = ptax_out + bdc_out
        net_flow = total_in - total_out
        live_balance = opening + net_flow

        cur = (d.get("currency") or "GHS").upper()
        last = d.get("last_reconciled")
        if isinstance(last, datetime):
            last = last.strftime("%Y-%m-%d")

        w.writerow([
            bank_name,
            d.get("account_name", ""),
            raw_acc_no,
            cur,
            f"{opening:0.2f}",
            f"{total_in:0.2f}",
            f"{ptax_out:0.2f}",
            f"{bdc_out:0.2f}",
            f"{net_flow:0.2f}",
            f"{live_balance:0.2f}",
            last or "",
        ])

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": 'attachment; filename="bank_accounts.csv"'},
    )
