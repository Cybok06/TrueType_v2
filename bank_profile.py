from flask import Blueprint, render_template, request, jsonify
from db import db
from bson import ObjectId
from datetime import datetime

bank_profile_bp = Blueprint("bank_profile", __name__, template_folder="templates")

accounts_col = db["bank_accounts"]
payments_col = db["payments"]          # inbound receipts (confirmed)
orders_col   = db["orders"]            # for price/tax/qty + legacy bdc_id
tax_col      = db["tax_records"]       # P-Tax payments (outflows)
bdc_col      = db["bdc"]               # BDC master
sbdc_col     = db["s_bdc_payment"]     # central S-BDC payments (from orders/manual)

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

def _sbdc_per_l(order):
    """Read per-litre S-BDC from the order (accept s_bdc_omc or s-bdc or s_bdc)."""
    for k in ("s_bdc_omc", "s-bdc", "s_bdc"):
        if k in order and order.get(k) is not None:
            try:
                return float(order.get(k))
            except Exception:
                pass
    return 0.0

def _order_due(order):
    """Total P-Tax due for an order = p_tax_per_l * quantity."""
    return round(_ptax_per_l(order) * _f(order.get("quantity"), 0.0), 2)

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

def _find_order_from_tax_doc(doc):
    """
    Best-effort order resolution for a tax_record:
      1) prefer 'order_oid'
      2) fallback to first order matching 'order_id' (code string)
    """
    ord_doc = None
    try:
        ooid = doc.get("order_oid")
        if ooid and ObjectId.is_valid(str(ooid)):
            ord_doc = orders_col.find_one({"_id": ObjectId(ooid)})
    except Exception:
        ord_doc = None

    if not ord_doc:
        oid_code = doc.get("order_id")
        if oid_code:
            ord_doc = orders_col.find_one({"order_id": oid_code})

    return ord_doc or {}

# ---------------- page ----------------
@bank_profile_bp.route("/bank-profile/<bank_id>")
def bank_profile(bank_id):
    bank = accounts_col.find_one({"_id": ObjectId(bank_id)})
    if not bank:
        return "Bank not found", 404

    bank_id_str = str(bank["_id"])
    bank_name = bank.get("bank_name")
    last4 = (bank.get("account_number") or "")[-4:]

    start_str = request.args.get("start_date")
    end_str   = request.args.get("end_date")

    query = {"bank_name": bank_name, "account_last4": last4, "status": "confirmed"}
    if start_str and end_str:
        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d")
            end_date   = datetime.strptime(end_str, "%Y-%m-%d")
            query["date"] = {"$gte": start_date, "$lte": end_date}
        except ValueError:
            pass

    payments = list(payments_col.find(query).sort("date", -1))
    total_received = sum(_f(p.get("amount")) for p in payments)

    # ---------- P-Tax history paid from this bank (JOIN orders for qty & s_tax) ----------
    bank_tax_rows = []
    tax_pipe = [
        {"$match": {"source_bank_id": ObjectId(bank_id), "type": {"$regex": r"^p[\s_-]*tax$", "$options": "i"}}},
        {"$sort": {"payment_date": -1}},
        {"$lookup": {"from": "orders", "localField": "order_oid", "foreignField": "_id", "as": "ord"}}
    ]
    for r in tax_col.aggregate(tax_pipe):
        # If lookup by order_oid failed (older records), attempt fallback by order_id
        ord_doc = r.get("ord", [{}])[0] if r.get("ord") else _find_order_from_tax_doc(r)
        qty = _f(ord_doc.get("quantity"))
        s_tax_per_l = _stax_per_l(ord_doc)
        pd = r.get("payment_date")
        pd_str = pd.strftime("%Y-%m-%d") if isinstance(pd, datetime) else str(pd or "—")
        order_code = ord_doc.get("order_id") or (str(r.get("order_id")) if r.get("order_id") else "—")
        bank_tax_rows.append({
            "amount": _f(r.get("amount")),
            "payment_date_str": pd_str,
            "reference": r.get("reference") or "—",
            "paid_by": r.get("paid_by") or "—",
            "omc": r.get("omc") or (ord_doc.get("omc") or "—"),
            "order_id": str(order_code),
            "quantity": qty,
            "s_tax_per_l": s_tax_per_l
        })

    # ---------- BDC payment history (JOIN orders for qty & s_bdc & order code) ----------
    bank_bdc_rows = []
    pipe = [
        {"$match": {"bank_paid_history": {"$exists": True, "$ne": []}}},
        {"$unwind": "$bank_paid_history"},
        {"$match": {"bank_paid_history.bank_id": ObjectId(bank_id)}},
        {"$lookup": {"from": "orders", "localField": "order_id", "foreignField": "_id", "as": "ord"}},
        {"$addFields": {
            "bdc_id_eff": {"$ifNull": ["$bdc_id", {"$arrayElemAt": ["$ord.bdc_id", 0]}]},
            "qty_d": {"$toDouble": {"$ifNull": [{"$arrayElemAt": ["$ord.quantity", 0]}, 0]}},
            "s_bdc_per_l_d": {
                "$toDouble": {
                    "$ifNull": [
                        {"$arrayElemAt": ["$ord.s_bdc_omc", 0]},
                        {"$ifNull": [{"$arrayElemAt": ["$ord.s-bdc", 0]}, 0]}
                    ]
                }
            },
            "order_code": {"$ifNull": [{"$arrayElemAt": ["$ord.order_id", 0]}, "—"]}
        }},
        {"$sort": {"bank_paid_history.date": -1}}
    ]
    rows = list(sbdc_col.aggregate(pipe))

    # resolve bdc names
    bdc_map = {}
    needed_ids = list({r.get("bdc_id_eff") for r in rows if r.get("bdc_id_eff")})
    if needed_ids:
        for b in bdc_col.find({"_id": {"$in": needed_ids}}, {"name": 1}):
            bdc_map[b["_id"]] = b.get("name")

    for r in rows:
        dt = r.get("bank_paid_history", {}).get("date") or r.get("date")
        amt = (r.get("bank_paid_history", {}).get("amount")
               if isinstance(r.get("bank_paid_history"), dict) else r.get("amount"))
        ref = (r.get("bank_paid_history", {}).get("reference")
               if isinstance(r.get("bank_paid_history"), dict) else r.get("reference"))
        by  = (r.get("bank_paid_history", {}).get("paid_by")
               if isinstance(r.get("bank_paid_history"), dict) else r.get("paid_by"))

        bank_bdc_rows.append({
            "amount": _f(amt),
            "payment_date_str": dt.strftime("%Y-%m-%d") if isinstance(dt, datetime) else "—",
            "reference": ref or "—",
            "paid_by": by or "—",
            "bdc": bdc_map.get(r.get("bdc_id_eff"), "—"),
            "ptype": (r.get("payment_type") or "").title(),
            "order_id": str(r.get("order_code") or "—"),
            "quantity": float(r.get("qty_d") or 0),
            "s_bdc_per_l": float(r.get("s_bdc_per_l_d") or 0),
        })

    return render_template(
        "partials/bank_profile.html",
        bank=bank,
        bank_id_str=bank_id_str,
        payments=payments,
        total_received=total_received,
        start_date=start_str,
        end_date=end_str,
        bank_tax_rows=bank_tax_rows,
        bank_bdc_rows=bank_bdc_rows
    )

# ---------------- API: OMC P-Tax debts (now includes avg S-Tax & total qty) ----------------
@bank_profile_bp.route("/bank-profile/<bank_id>/omc-debts", methods=["GET"])
def omc_debts(bank_id):
    try:
        # Only orders that actually have a P-Tax per litre > 0
        eligible = list(orders_col.find({
            "$or": [
                {"p_tax": {"$gt": 0}},
                {"p-tax": {"$gt": 0}},
            ]
        }, {"_id":1, "omc":1, "quantity":1, "p_tax":1, "p-tax":1, "s_tax":1, "s-tax":1, "date":1}))

        omc_map = {}
        for o in eligible:
            due = _order_due(o)                 # uses P-Tax × Q
            paid = _paid_sum_for_order(o["_id"])# sums P-Tax payments
            rem  = max(0.0, round(due - paid, 2))
            if rem <= 0:
                continue
            omc = o.get("omc") or "—"
            qty = _f(o.get("quantity"))
            s_tax_pl = _stax_per_l(o)

            slot = omc_map.setdefault(omc, {
                "outstanding": 0.0,
                "unpaid_orders": 0,
                "s_tax_sum": 0.0,      # for average per-litre
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

# ---------------- API: pay OMC P-Tax (unchanged core; still logs order_oid + order_id) ----------------
@bank_profile_bp.route("/bank-profile/pay-omc", methods=["POST"])
def pay_omc_from_bank():
    try:
        data = request.get_json(force=True)
        bank_id = data.get("bank_id")
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
            "$or": [
                {"p_tax": {"$gt": 0}},
                {"p-tax": {"$gt": 0}},
            ]
        }, {"_id":1, "order_id":1, "quantity":1, "p_tax":1, "p-tax":1, "date":1}).sort("date", 1))

        alloc_list, total_outstanding = [], 0.0
        for o in orders:
            due = _order_due(o)                  # P-Tax × Q
            paid = _paid_sum_for_order(o["_id"]) # P-Tax already paid
            rem  = max(0.0, round(due - paid, 2))
            if rem > 0:
                alloc_list.append({"order": o, "remaining": rem})
                total_outstanding += rem

        if total_outstanding <= 0:
            return jsonify({"status":"error", "message":"No outstanding P-Tax for this OMC"}), 400
        if amount > total_outstanding:
            return jsonify({"status":"error", "message": f"Amount exceeds OMC outstanding (GHS {_fmt2(total_outstanding)})"}), 400

        left, created = amount, []
        for a in alloc_list:
            if left <= 0:
                break
            portion = min(left, a["remaining"])
            o = a["order"]

            # Log as P-Tax in tax_records
            tax_col.insert_one({
                "type": "P-Tax",
                "amount": round(portion, 2),
                "payment_date": pay_dt,
                "reference": ref or None,
                "paid_by": paid_by or None,
                "omc": omc,
                "order_id": o.get("order_id"),
                "order_oid": o["_id"],
                "source_bank_id": ObjectId(bank_id),
                "submitted_at": datetime.utcnow()
            })

            # Recompute paid/remaining after this portion
            new_paid = _paid_sum_for_order(o["_id"])
            due      = _order_due(o)
            remaining= max(0.0, round(due - new_paid, 2))

            # Update order: write both p_tax_* and s_tax_* for backward compatibility
            update_doc = {
                "p_tax_paid_amount": round(new_paid, 2),
                "p_tax_paid_at": pay_dt,
                "p_tax_reference": ref or o.get("p_tax_reference"),
                "p_tax_paid_by": paid_by or o.get("p_tax_paid_by"),
                "p_tax_payment": "paid" if remaining <= 0 else "partial",
                "p-tax-payment": "paid" if remaining <= 0 else "partial",
                # mirrors
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
                "applied":   round(portion, 2),
                "remaining_after": remaining
            })
            left = round(left - portion, 2)

        return jsonify({"status":"success", "allocated": created, "omc": omc, "amount": round(amount,2)})
    except Exception as e:
        return jsonify({"status":"error", "message": str(e)}), 500

# ---------------- API: BDC debts (now includes avg S-BDC & total qty & sample order) ----------------
@bank_profile_bp.route("/bank-profile/<bank_id>/bdc-debts", methods=["GET"])
def bdc_debts(bank_id):
    try:
        # Outstanding = amount - bank_paid_total, across cash/from account/credit
        pipe = [
            {"$match": {"payment_type": {"$regex": r"^(cash|credit|from\s*account)$", "$options": "i"}}},
            {"$lookup": {"from": "orders", "localField": "order_id", "foreignField": "_id", "as": "ord"}},
            {"$addFields": {
                "bdc_id_eff": {"$ifNull": ["$bdc_id", {"$arrayElemAt": ["$ord.bdc_id", 0]}]},
                "amount_d": {"$toDouble": "$amount"},
                "paid_d": {"$toDouble": {"$ifNull": ["$bank_paid_total", 0]}},
                "qty_d": {"$toDouble": {"$ifNull": [{"$arrayElemAt": ["$ord.quantity", 0]}, 0]}},
                "s_bdc_per_l_d": {
                    "$toDouble": {
                        "$ifNull": [
                            {"$arrayElemAt": ["$ord.s_bdc_omc", 0]},
                            {"$ifNull": [{"$arrayElemAt": ["$ord.s-bdc", 0]}, 0]}
                        ]
                    }
                },
                "order_code": {"$ifNull": [{"$arrayElemAt": ["$ord.order_id", 0]}, "—"]}
            }},
            {"$addFields": {"remain": {"$subtract": ["$amount_d", "$paid_d"]}}},
            {"$match": {"bdc_id_eff": {"$ne": None}, "remain": {"$gt": 0}}},
            {"$group": {
                "_id": "$bdc_id_eff",
                "outstanding": {"$sum": "$remain"},
                "unpaid_items": {"$sum": 1},
                "total_quantity": {"$sum": "$qty_d"},
                "avg_s_bdc_per_l": {"$avg": "$s_bdc_per_l_d"},
                "sample_order_id": {"$first": "$order_code"}
            }},
            {"$lookup": {"from": "bdc", "localField": "_id", "foreignField": "_id", "as": "bdc"}},
            {"$addFields": {"bdc_name": {"$arrayElemAt": ["$bdc.name", 0]}}},
            {"$project": {
                "_id": 0,
                "bdc_id": {"$toString": "$_id"},
                "bdc": "$bdc_name",
                "outstanding": {"$round": ["$outstanding", 2]},
                "unpaid_items": 1,
                "total_quantity": {"$round": ["$total_quantity", 2]},
                "avg_s_bdc_per_l": {"$round": ["$avg_s_bdc_per_l", 4]},
                "sample_order_id": 1
            }},
            {"$sort": {"outstanding": -1}},
        ]
        rows = list(sbdc_col.aggregate(pipe))
        return jsonify({"status":"success", "debts": rows})
    except Exception as e:
        return jsonify({"status":"error", "message": str(e)}), 500
# ---------------- API: pay BDC from this bank (allocates oldest-first) ----------------
@bank_profile_bp.route("/bank-profile/pay-bdc", methods=["POST"])
def pay_bdc_from_bank():
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
                    {"payment_type": {"$regex": r"^from\s*account$", "$options": "i"}},
                ],
                "$or": [
                    {"bdc_id": bdc_oid},
                    # some docs store bdc_id on joined order; we already normalized via bdc_debts, but be defensive:
                ]
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
        if amount > total_outstanding + 0.005:
            return jsonify({"status":"error", "message": f"Amount exceeds BDC outstanding (GHS {_fmt2(total_outstanding)})"}), 400

        # ---- fetch unpaid items oldest-first to allocate against ----
        # We prefer a stable "date" field; fallback to _id time if missing.
        items = list(sbdc_col.aggregate([
            {"$match": {
                "$or": [
                    {"payment_type": {"$regex": r"^cash$", "$options": "i"}},
                    {"payment_type": {"$regex": r"^credit$", "$options": "i"}},
                    {"payment_type": {"$regex": r"^from\s*account$", "$options": "i"}},
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
            upd = sbdc_col.update_one(
                {"._id": it["_id"]} if False else {"_id": it["_id"]},
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
            new_doc = sbdc_col.find_one({"_id": it["_id"]}, {"amount":1, "bank_paid_total":1, "order_id":1, "payment_type":1})
            amt_d = _f(new_doc.get("amount"))
            bank_paid_total = _f(new_doc.get("bank_paid_total"))
            remaining_after = max(0.0, round(amt_d - bank_paid_total, 2))

            # try to surface the order code if present
            order_code = new_doc.get("order_id")
            if isinstance(order_code, ObjectId):
                # if order_id is an OID, try to look up readable code
                ord_doc = orders_col.find_one({"_id": order_code}, {"order_id":1})
                order_code = ord_doc.get("order_id") if ord_doc else str(order_code)

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
