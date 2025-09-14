from flask import Blueprint, render_template, request, jsonify
from bson import ObjectId
from datetime import datetime
from db import db

bank_accounts_bp = Blueprint("bank_accounts", __name__, template_folder="templates")

accounts_col = db["bank_accounts"]
payments_col = db["payments"]      # confirmed inbound cash-ins
tax_col      = db["tax_records"]   # P-Tax outflows (source_bank_id)
sbdc_col     = db["s_bdc_payment"] # BDC bank payments (bank_paid_history)

def _f(v, default=0.0):
    try:
        if v is None or v == "": return default
        return float(v)
    except Exception:
        return default

def _last4(acc_number: str) -> str:
    s = str(acc_number or "")
    return s[-4:] if len(s) >= 4 else s

def _sum_confirmed_in(bank_name: str, last4: str) -> float:
    """Sum of confirmed inbound payments for this bank (by name + last4)."""
    try:
        pipe = [
            {"$match": {"bank_name": bank_name, "account_last4": last4, "status": "confirmed"}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ]
        row = next(payments_col.aggregate(pipe), None)
        return _f(row["total"]) if row else 0.0
    except Exception:
        return 0.0

def _sum_ptax_out(bank_oid: ObjectId) -> float:
    """Sum of P-Tax payments made from this bank (tax_records.source_bank_id)."""
    try:
        pipe = [
            {"$match": {
                "source_bank_id": bank_oid,
                "type": {"$regex": r"^p[\s_-]*tax$", "$options": "i"}
            }},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ]
        row = next(tax_col.aggregate(pipe), None)
        return _f(row["total"]) if row else 0.0
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
        return _f(row["total"]) if row else 0.0
    except Exception:
        return 0.0

# ✅ View All Bank Accounts (with live balances)
@bank_accounts_bp.route("/bank-accounts", methods=["GET"])
def bank_accounts():
    accounts = list(accounts_col.find().sort("bank_name"))

    # Compute metrics per account
    enriched = []
    for acc in accounts:
        bank_oid = acc.get("_id")
        bn = acc.get("bank_name") or ""
        last4 = _last4(acc.get("account_number"))
        total_in = _sum_confirmed_in(bn, last4)
        ptax_out = _sum_ptax_out(bank_oid)
        bdc_out  = _sum_bdc_out(bank_oid)

        # attach metrics for the template
        acc["_metrics"] = {
            "total_in": round(total_in, 2),
            "ptax_out": round(ptax_out, 2),
            "bdc_out":  round(bdc_out, 2),
        }
        enriched.append(acc)

    return render_template("partials/bank_accounts.html", accounts=enriched)

# ✅ Add New Account
@bank_accounts_bp.route("/bank-accounts/add", methods=["POST"])
def add_bank_account():
    data = request.form
    new_account = {
        "bank_name": data.get("bank_name"),
        "account_name": data.get("account_name"),
        "account_number": data.get("account_number"),
        "branch": data.get("branch")
    }
    accounts_col.insert_one(new_account)
    return jsonify({"success": True, "message": "Bank account added"})

# ✅ Edit Account
@bank_accounts_bp.route("/bank-accounts/edit/<id>", methods=["POST"])
def edit_bank_account(id):
    data = request.form
    update = {
        "bank_name": data.get("bank_name"),
        "account_name": data.get("account_name"),
        "account_number": data.get("account_number"),
        "branch": data.get("branch")
    }
    accounts_col.update_one({"_id": ObjectId(id)}, {"$set": update})
    return jsonify({"success": True, "message": "Bank account updated"})

# ✅ Delete Account
@bank_accounts_bp.route("/bank-accounts/delete/<id>", methods=["POST"])
def delete_bank_account(id):
    accounts_col.delete_one({"_id": ObjectId(id)})
    return jsonify({"success": True, "message": "Bank account deleted"})
