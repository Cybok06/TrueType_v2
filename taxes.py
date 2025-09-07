from flask import Blueprint, render_template, request, jsonify
from db import db
from bson import ObjectId
from datetime import datetime

taxes_bp = Blueprint("taxes", __name__, template_folder="templates")

# --- collections ---
accounts_col = db["bank_accounts"]
orders_col   = db["orders"]            # for price/tax/qty + legacy bdc_id
tax_col      = db["tax_records"]       # P-Tax payments (outflows)
bdc_col      = db["bdc"]               # BDC master
sbdc_col     = db["s_bdc_payment"]     # central S-BDC payments (from orders/manual)

# ---- numeric tolerance for float sums ----
_EPS = 0.005

# ---------------- helpers ----------------
def _f(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default

def _fmt2(n):
    try:
        return f"{float(n):,.2f}"
    except Exception:
        return "0.00"

def _ptax_per_l(order):
    """Read per-litre P-Tax from the order (accept p_tax or p-tax)."""
    for k in ("p_tax", "p-tax"):
        if k in order and order.get(k) is not None:
            try:
                return float(order.get(k))
            except Exception:
                pass
    return 0.0

def _stax_per_l(order):
    """Read per-litre S-Tax from the order (accept s_tax or s-tax)."""
    for k in ("s_tax", "s-tax"):
        if k in order and order.get(k) is not None:
            try:
                return float(order.get(k))
            except Exception:
                pass
    return 0.0

def _order_due(order):
    """Total P-Tax due for an order = p_tax_per_l * quantity."""
    qty = _f(order.get("quantity"), 0.0)
    return round(_ptax_per_l(order) * qty, 2)

def _paid_sum_for_order(oid: ObjectId) -> float:
    """Sum all P-Tax payments logged for this order in tax_records."""
    try:
        row = next(
            tax_col.aggregate([
                {"$match": {"order_oid": oid, "type": {"$regex": r"^p[\s_-]*tax$", "$options": "i"}}},
                {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
            ]),
            None
        )
        return float(row.get("total", 0.0)) if row else 0.0
    except Exception:
        return 0.0

# ---------------- page ----------------
@taxes_bp.route("/taxes", methods=["GET"])
def taxes_home():
    # Banks for the "pay from" select
    banks = list(accounts_col.find({}, {"bank_name":1, "account_name":1, "account_number":1}).sort("bank_name", 1))

    # BDC list for BDC tab select
    bdcs = list(bdc_col.find({}, {"name": 1}).sort("name", 1))

    # OMC list (distinct from orders)
    try:
        omc_names = orders_col.distinct("omc", {"omc": {"$ne": None, "$ne": ""}})
    except Exception:
        omc_names = []

    return render_template("partials/taxes.html", banks=banks, bdcs=bdcs, omc_names=sorted([o for o in omc_names if isinstance(o, str)]))

# ---------------- API: OMC P-Tax debts (includes avg S-Tax & total qty) ----------------
@taxes_bp.route("/taxes/omc-debts", methods=["GET"])
def taxes_omc_debts():
    try:
        eligible = list(orders_col.find({
            "$or": [{"p_tax": {"$gt": 0}}, {"p-tax": {"$gt": 0}}],
        }, {"_id":1, "omc":1, "quantity":1, "p_tax":1, "p-tax":1, "s_tax":1, "s-tax":1, "date":1}))

        omc_map = {}
        for o in eligible:
            due = _order_due(o)
            paid = _paid_sum_for_order(o["_id"])
            rem  = max(0.0, round(due - paid, 2))
            if rem <= 0:
                continue

            omc = o.get("omc") or "—"
            qty = _f(o.get("quantity"))
            s_tax_pl = _stax_per_l(o)

            slot = omc_map.setdefault(omc, {
                "outstanding": 0.0,
                "unpaid_orders": 0,
                "s_tax_sum": 0.0,
                "s_tax_count": 0,
                "total_quantity": 0.0
            })
            slot["outstanding"] += rem
            slot["unpaid_orders"] += 1
            slot["s_tax_sum"] += s_tax_pl
            slot["s_tax_count"] += 1
            slot["total_quantity"] += qty

        debts = []
        for k, v in omc_map.items():
            avg_s_tax = (v["s_tax_sum"] / v["s_tax_count"]) if v["s_tax_count"] > 0 else 0.0
            debts.append({
                "omc": k,
                "outstanding": round(v["outstanding"], 2),
                "unpaid_orders": v["unpaid_orders"],
                "avg_s_tax_per_l": round(avg_s_tax, 4),
                "total_quantity": round(v["total_quantity"], 2)
            })
        debts.sort(key=lambda x: x["outstanding"], reverse=True)
        return jsonify({"status": "success", "debts": debts})
    except Exception as e:
        return jsonify({"status":"error", "message": str(e)}), 500

# ---------------- API: pay OMC P-Tax (select bank -> allocate oldest-first) ----------------
@taxes_bp.route("/taxes/pay-omc", methods=["POST"])
def taxes_pay_omc():
    try:
        data = request.get_json(force=True)
        bank_id = (data.get("bank_id") or "").strip()
        omc     = (data.get("omc") or "").strip()
        amount  = _f(data.get("amount"))
        ref     = (data.get("reference") or "").strip()
        paid_by = (data.get("paid_by") or "").strip()
        date_s  = (data.get("payment_date") or "").strip()

        if not bank_id or not ObjectId.is_valid(bank_id):
            return jsonify({"status":"error", "message":"Invalid bank id"}), 400
        if not omc:
            return jsonify({"status":"error", "message":"OMC is required"}), 400
        if amount <= 0:
            return jsonify({"status":"error", "message":"Amount must be greater than 0"}), 400

        pay_dt = datetime.utcnow()
        if date_s:
            try:
                pay_dt = datetime.strptime(date_s, "%Y-%m-%d")
            except ValueError:
                return jsonify({"status":"error", "message":"Invalid payment date"}), 400

        # Oldest-first orders for this OMC that carry P-Tax per litre
        orders = list(orders_col.find({
            "omc": omc,
            "$or": [{"p_tax": {"$gt": 0}}, {"p-tax": {"$gt": 0}}],
        }, {"_id":1, "order_id":1, "quantity":1, "p_tax":1, "p-tax":1, "date":1}).sort("date", 1))

        alloc_list, total_outstanding = [], 0.0
        for o in orders:
            due = _order_due(o)
            paid = _paid_sum_for_order(o["_id"])
            rem  = max(0.0, round(due - paid, 2))
            if rem > 0:
                alloc_list.append({"order": o, "remaining": rem})
                total_outstanding += rem

        if total_outstanding <= 0:
            return jsonify({"status":"error", "message":"No outstanding P-Tax for this OMC"}), 400
        if amount > total_outstanding + _EPS:
            return jsonify({"status":"error", "message": f"Amount exceeds OMC outstanding (GHS {_fmt2(total_outstanding)})"}), 400

        left, created = round(amount, 2), []
        bank_oid = ObjectId(bank_id)

        for a in alloc_list:
            if left <= 0:
                break
            portion = min(left, a["remaining"])
            portion = round(portion, 2)
            o = a["order"]

            # Log as P-Tax in tax_records
            tax_col.insert_one({
                "type": "P-Tax",
                "amount": portion,
                "payment_date": pay_dt,
                "reference": ref or None,
                "paid_by": paid_by or None,
                "omc": omc,
                "order_id": o.get("order_id"),
                "order_oid": o["_id"],
                "source_bank_id": bank_oid,
                "submitted_at": datetime.utcnow()
            })

            # Recompute paid/remaining after this portion
            new_paid = _paid_sum_for_order(o["_id"])
            due      = _order_due(o)
            remaining= max(0.0, round(due - new_paid, 2))

            # Update order flags (keep legacy mirrors)
            update_doc = {
                "p_tax_paid_amount": round(new_paid, 2),
                "p_tax_paid_at": pay_dt,
                "p_tax_reference": ref or o.get("p_tax_reference"),
                "p_tax_paid_by": paid_by or o.get("p_tax_paid_by"),
                "p_tax_payment": "paid" if remaining <= 0 else "partial",
                "p-tax-payment": "paid" if remaining <= 0 else "partial",

                "s_tax_paid_amount": round(new_paid, 2),
                "s_tax_paid_at": pay_dt,
                "s_tax_reference": ref or o.get("s_tax_reference"),
                "s_tax_paid_by": paid_by or o.get("s_tax_paid_by"),
                "s_tax_payment": "paid" if remaining <= 0 else "partial",
                "s-tax-payment": "paid" if remaining <= 0 else "partial",
            }
            orders_col.update_one({"_id": o["_id"]}, {"$set": update_doc})

            created.append({
                "order_id":  str(o.get("order_id")) if o.get("order_id") else None,
                "order_oid": str(o["_id"]),
                "applied":   portion,
                "remaining_after": remaining
            })
            left = round(left - portion, 2)

        return jsonify({"status":"success", "allocated": created, "omc": omc, "amount": round(amount,2)})

    except Exception as e:
        return jsonify({"status":"error", "message": str(e)}), 500

# ---------------- API: BDC debts (sum across s_bdc_payment) ----------------
@taxes_bp.route("/taxes/bdc-debts", methods=["GET"])
def taxes_bdc_debts():
    try:
        pipe = [
            {"$match": {"payment_type": {"$regex": r"^(cash|credit|from\s*account)$", "$options": "i"}}},
            {"$lookup": {"from": "orders", "localField": "order_id", "foreignField": "_id", "as": "ord"}},
            {"$addFields": {
                "bdc_id_eff": {"$ifNull": ["$bdc_id", {"$arrayElemAt": ["$ord.bdc_id", 0]}]},
                "amount_d": {"$toDouble": "$amount"},
                "paid_d": {"$toDouble": {"$ifNull": ["$bank_paid_total", 0]}},
            }},
            {"$addFields": {"remain": {"$subtract": ["$amount_d", "$paid_d"]}}},
            {"$match": {"bdc_id_eff": {"$ne": None}, "remain": {"$gt": 0}}},
            {"$group": {
                "_id": "$bdc_id_eff",
                "outstanding": {"$sum": "$remain"},
                "unpaid_items": {"$sum": 1},
            }},
            {"$lookup": {"from": "bdc", "localField": "_id", "foreignField": "_id", "as": "bdc"}},
            {"$addFields": {"bdc_name": {"$arrayElemAt": ["$bdc.name", 0]}}},
            {"$project": {"_id": 0, "bdc_id": {"$toString": "$_id"}, "bdc": "$bdc_name",
                          "outstanding": {"$round": ["$outstanding", 2]}, "unpaid_items": 1}},
            {"$sort": {"outstanding": -1}},
        ]
        rows = list(sbdc_col.aggregate(pipe))
        return jsonify({"status":"success", "debts": rows})
    except Exception as e:
        return jsonify({"status":"error", "message": str(e)}), 500

# ---------------- API: pay BDC (select bank -> allocate oldest-first) ----------------
@taxes_bp.route("/taxes/pay-bdc", methods=["POST"])
def taxes_pay_bdc():
    try:
        data = request.get_json(force=True)
        bank_id = (data.get("bank_id") or "").strip()
        bdc_id  = (data.get("bdc_id") or "").strip()
        amount  = _f(data.get("amount"))
        ref     = (data.get("reference") or "").strip()
        paid_by = (data.get("paid_by") or "").strip()
        date_s  = (data.get("payment_date") or "").strip()

        # ---- validation ----
        if not bank_id or not ObjectId.is_valid(bank_id):
            return jsonify({"status":"error", "message":"Invalid bank id"}), 400
        if not bdc_id or not ObjectId.is_valid(bdc_id):
            return jsonify({"status":"error", "message":"Invalid BDC id"}), 400
        if amount <= 0:
            return jsonify({"status":"error", "message":"Amount must be greater than 0"}), 400

        pay_dt = datetime.utcnow()
        if date_s:
            try:
                pay_dt = datetime.strptime(date_s, "%Y-%m-%d")
            except ValueError:
                return jsonify({"status":"error", "message":"Invalid payment date"}), 400

        bdc_oid = ObjectId(bdc_id)
        bank_oid = ObjectId(bank_id)

        # ---- compute total outstanding for this BDC (cash/credit/from account) ----
        debt_pipe = [
            {"$match": {
                "$or": [
                    {"payment_type": {"$regex": r"^cash$", "$options": "i"}},
                    {"payment_type": {"$regex": r"^credit$", "$options": "i"}},
                    {"payment_type": {"$regex": r"^from\\s*account$", "$options": "i"}},
                ],
                "bdc_id": bdc_oid
            }},
            {"$addFields": {
                "amount_d": {"$toDouble": "$amount"},
                "paid_d": {"$toDouble": {"$ifNull": ["$bank_paid_total", 0]}},
            }},
            {"$addFields": {"remain": {"$subtract": ["$amount_d", "$paid_d"]}}},
            {"$match": {"remain": {"$gt": 0}}},
            {"$group": {"_id": None, "total_outstanding": {"$sum": "$remain"}}}
        ]
        row = next(sbdc_col.aggregate(debt_pipe), None)
        total_outstanding = float(row["total_outstanding"]) if row else 0.0

        if total_outstanding <= 0:
            return jsonify({"status":"error", "message":"No outstanding BDC items for this BDC"}), 400
        if amount > total_outstanding + _EPS:
            return jsonify({"status":"error", "message": f"Amount exceeds BDC outstanding (GHS {_fmt2(total_outstanding)})"}), 400

        # ---- fetch unpaid items oldest-first to allocate against ----
        items = list(sbdc_col.aggregate([
            {"$match": {
                "$or": [
                    {"payment_type": {"$regex": r"^cash$", "$options": "i"}},
                    {"payment_type": {"$regex": r"^credit$", "$options": "i"}},
                    {"payment_type": {"$regex": r"^from\\s*account$", "$options": "i"}},
                ],
                "bdc_id": bdc_oid
            }},
            {"$addFields": {
                "amount_d": {"$toDouble": "$amount"},
                "paid_d": {"$toDouble": {"$ifNull": ["$bank_paid_total", 0]}},
                "sort_dt": {"$ifNull": ["$date", {"$toDate": "$_id"}]}
            }},
            {"$addFields": {"remain": {"$subtract": ["$amount_d", "$paid_d"]}}},
            {"$match": {"remain": {"$gt": 0}}},
            {"$sort": {"sort_dt": 1}}
        ]))

        left = round(amount, 2)
        allocated = []

        for it in items:
            if left <= 0:
                break

            portion = min(left, float(it["remain"]))
            portion = round(portion, 2)

            # push to history & increment totals atomically
            sbdc_col.update_one(
                {"_id": it["_id"]},
                {
                    "$push": {"bank_paid_history": {
                        "bank_id": bank_oid,
                        "amount": portion,
                        "date": pay_dt,
                        "reference": ref or None,
                        "paid_by": paid_by or None
                    }},
                    "$inc": {"bank_paid_total": portion},
                    "$set": {"bank_paid_last_at": pay_dt}
                }
            )

            # compute remaining after this allocation
            new_doc = sbdc_col.find_one(
                {"_id": it["_id"]},
                {"amount":1, "bank_paid_total":1, "order_id":1, "payment_type":1, "bank_status":1, "status":1}
            )
            amt_d = _f(new_doc.get("amount"))
            bank_paid_total = _f(new_doc.get("bank_paid_total"))
            remaining_after = max(0.0, round(amt_d - bank_paid_total, 2))

            # if fully paid, flip statuses to "paid"
            if remaining_after <= _EPS:
                sbdc_col.update_one(
                    {"_id": it["_id"]},
                    {"$set": {
                        "bank_status": "paid",
                        "status": "paid",  # optional global status for UIs
                        "bank_paid_completed_at": pay_dt,
                        "bank_paid_completed_by": paid_by or None,
                        "bank_paid_reference": ref or None
                    }}
                )
                remaining_after = 0.0  # clamp for response

            # try to surface the order code if present
            order_code = new_doc.get("order_id")
            if isinstance(order_code, ObjectId):
                ord_doc = orders_col.find_one({"_id": order_code}, {"order_id":1})
                order_code = (ord_doc or {}).get("order_id") or str(order_code)

            allocated.append({
                "sbdc_oid": str(it["_id"]),
                "order_id": str(order_code) if order_code else None,
                "payment_type": (new_doc.get("payment_type") or "").title(),
                "applied": portion,
                "remaining_after": remaining_after
            })

            left = round(left - portion, 2)

        return jsonify({
            "status": "success",
            "bdc_id": bdc_id,
            "amount": round(amount, 2),
            "allocated": allocated
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
