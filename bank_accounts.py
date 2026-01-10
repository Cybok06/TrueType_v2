from flask import Blueprint, render_template, request, jsonify, session
from bson import ObjectId
from datetime import datetime
import uuid
from db import db

bank_accounts_bp = Blueprint("bank_accounts", __name__, template_folder="templates")

accounts_col = db["bank_accounts"]
payments_col = db["payments"]      # confirmed inbound cash-ins
tax_col      = db["tax_records"]   # P-Tax outflows (source_bank_id)
sbdc_col     = db["s_bdc_payment"] # BDC bank payments (bank_paid_history)
bank_txn_col = db["bank_transactions"]  # manual deposits/withdrawals/transfers

def _ensure_txn_indexes() -> None:
    try:
        bank_txn_col.create_index([("bank_id", 1), ("txn_date", -1)])
        bank_txn_col.create_index([("transfer_id", 1)])
    except Exception:
        pass

_ensure_txn_indexes()
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

def _sum_manual_in(bank_oid: ObjectId) -> float:
    try:
        pipe = [
            {"$match": {"bank_id": bank_oid, "type": {"$in": ["deposit", "transfer_in"]}}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ]
        row = next(bank_txn_col.aggregate(pipe), None)
        return _f(row["total"]) if row else 0.0
    except Exception:
        return 0.0

def _sum_manual_out(bank_oid: ObjectId) -> float:
    try:
        pipe = [
            {"$match": {"bank_id": bank_oid, "type": {"$in": ["withdrawal", "transfer_out"]}}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ]
        row = next(bank_txn_col.aggregate(pipe), None)
        return _f(row["total"]) if row else 0.0
    except Exception:
        return 0.0

def _can_manage_bank_txn() -> bool:
    role = (session.get("role") or "").lower()
    return role in ("admin", "superadmin", "accounting") or session.get("username") == "admin"

def _parse_txn_date(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None

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
        manual_in = _sum_manual_in(bank_oid)
        manual_out = _sum_manual_out(bank_oid)

        # attach metrics for the template
        acc["_metrics"] = {
            "total_in": round(total_in, 2),
            "ptax_out": round(ptax_out, 2),
            "bdc_out":  round(bdc_out, 2),
            "manual_in": round(manual_in, 2),
            "manual_out": round(manual_out, 2),
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

# Manual deposit
@bank_accounts_bp.route("/bank-accounts/<bank_id>/manual-deposit", methods=["POST"])
def manual_deposit(bank_id):
    if not _can_manage_bank_txn():
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    if not ObjectId.is_valid(bank_id):
        return jsonify({"ok": False, "error": "Invalid bank id"}), 400

    data = request.get_json(silent=True) or request.form
    amount = _f(data.get("amount"))
    txn_date = _parse_txn_date(data.get("txn_date"))
    reference = (data.get("reference") or "").strip()
    narration = (data.get("narration") or "").strip()

    if amount <= 0:
        return jsonify({"ok": False, "error": "Amount must be greater than 0"}), 400
    if not txn_date:
        return jsonify({"ok": False, "error": "Transaction date is required"}), 400

    bank = accounts_col.find_one({"_id": ObjectId(bank_id)}, {"currency": 1})
    if not bank:
        return jsonify({"ok": False, "error": "Bank not found"}), 404

    user_name = session.get("full_name") or session.get("username") or ""
    doc = {
        "type": "deposit",
        "bank_id": ObjectId(bank_id),
        "amount": float(amount),
        "currency": (bank.get("currency") or "GHS").upper(),
        "txn_date": txn_date,
        "reference": reference or None,
        "narration": narration or None,
        "created_by": {"id": session.get("user_id"), "name": user_name},
        "created_at": datetime.utcnow(),
    }
    bank_txn_col.insert_one(doc)
    return jsonify({"ok": True})

# Manual withdrawal
@bank_accounts_bp.route("/bank-accounts/<bank_id>/manual-withdraw", methods=["POST"])
def manual_withdraw(bank_id):
    if not _can_manage_bank_txn():
        return jsonify({"ok": False, "error": "Unauthorized"}), 403
    if not ObjectId.is_valid(bank_id):
        return jsonify({"ok": False, "error": "Invalid bank id"}), 400

    data = request.get_json(silent=True) or request.form
    amount = _f(data.get("amount"))
    txn_date = _parse_txn_date(data.get("txn_date"))
    reference = (data.get("reference") or "").strip()
    narration = (data.get("narration") or "").strip()

    if amount <= 0:
        return jsonify({"ok": False, "error": "Amount must be greater than 0"}), 400
    if not txn_date:
        return jsonify({"ok": False, "error": "Transaction date is required"}), 400

    bank = accounts_col.find_one({"_id": ObjectId(bank_id)}, {"currency": 1})
    if not bank:
        return jsonify({"ok": False, "error": "Bank not found"}), 404

    user_name = session.get("full_name") or session.get("username") or ""
    doc = {
        "type": "withdrawal",
        "bank_id": ObjectId(bank_id),
        "amount": float(amount),
        "currency": (bank.get("currency") or "GHS").upper(),
        "txn_date": txn_date,
        "reference": reference or None,
        "narration": narration or None,
        "created_by": {"id": session.get("user_id"), "name": user_name},
        "created_at": datetime.utcnow(),
    }
    bank_txn_col.insert_one(doc)
    return jsonify({"ok": True})

# Transfer between banks
@bank_accounts_bp.route("/bank-accounts/transfer", methods=["POST"])
def transfer_between_banks():
    if not _can_manage_bank_txn():
        return jsonify({"ok": False, "error": "Unauthorized"}), 403

    data = request.get_json(silent=True) or request.form
    from_bank_id = (data.get("from_bank_id") or "").strip()
    to_bank_id = (data.get("to_bank_id") or "").strip()
    amount = _f(data.get("amount"))
    txn_date = _parse_txn_date(data.get("txn_date"))
    reference = (data.get("reference") or "").strip()
    narration = (data.get("narration") or "").strip()

    if not ObjectId.is_valid(from_bank_id) or not ObjectId.is_valid(to_bank_id):
        return jsonify({"ok": False, "error": "Invalid bank id"}), 400
    if from_bank_id == to_bank_id:
        return jsonify({"ok": False, "error": "From/To bank cannot be the same"}), 400
    if amount <= 0:
        return jsonify({"ok": False, "error": "Amount must be greater than 0"}), 400
    if not txn_date:
        return jsonify({"ok": False, "error": "Transaction date is required"}), 400

    from_bank = accounts_col.find_one({"_id": ObjectId(from_bank_id)}, {"currency": 1})
    to_bank = accounts_col.find_one({"_id": ObjectId(to_bank_id)}, {"currency": 1})
    if not from_bank or not to_bank:
        return jsonify({"ok": False, "error": "Bank not found"}), 404
    from_cur = (from_bank.get("currency") or "GHS").upper()
    to_cur = (to_bank.get("currency") or "GHS").upper()
    if from_cur != to_cur:
        return jsonify({"ok": False, "error": "Transfer requires same currency"}), 400

    transfer_id = str(uuid.uuid4())
    if not reference:
        reference = f"TRF-{datetime.utcnow().strftime('%Y%m%d')}-{transfer_id[:6].upper()}"

    user_name = session.get("full_name") or session.get("username") or ""
    base = {
        "amount": float(amount),
        "currency": from_cur,
        "txn_date": txn_date,
        "reference": reference,
        "narration": narration or None,
        "created_by": {"id": session.get("user_id"), "name": user_name},
        "created_at": datetime.utcnow(),
        "transfer_id": transfer_id,
    }
    out_doc = dict(base, type="transfer_out", bank_id=ObjectId(from_bank_id), other_bank_id=ObjectId(to_bank_id))
    in_doc = dict(base, type="transfer_in", bank_id=ObjectId(to_bank_id), other_bank_id=ObjectId(from_bank_id))

    bank_txn_col.insert_many([out_doc, in_doc])
    return jsonify({"ok": True})
