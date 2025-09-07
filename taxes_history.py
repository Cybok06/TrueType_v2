from flask import Blueprint, render_template, request, jsonify
from bson import ObjectId
from datetime import datetime
from db import db

taxes_hist_bp = Blueprint("taxes_history", __name__, template_folder="templates")

# --- collections ---
accounts_col = db["bank_accounts"]
orders_col   = db["orders"]
tax_col      = db["tax_records"]        # OMC P-Tax allocations (outflows)
bdc_col      = db["bdc"]
sbdc_col     = db["s_bdc_payment"]      # BDC allocations (bank_paid_history)
omc_col      = db["bd_omc"]             # OMC master (name, phones, etc.)

# ---------------- helpers ----------------
def _f(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default

def _s(v):
    return (v or "").strip()

def _fmt2(n):
    try:
        return f"{float(n):,.2f}"
    except Exception:
        return "0.00"

def _oid_or_none(s):
    try:
        return ObjectId(str(s))
    except Exception:
        return None

# Resolve a bank id -> display "Bank — Account (…1234)"
def _bank_display_map(bank_ids):
    if not bank_ids:
        return {}
    rows = accounts_col.find({"_id": {"$in": list(bank_ids)}}, {"bank_name": 1, "account_name": 1, "account_number": 1})
    m = {}
    for r in rows:
        last4 = (r.get("account_number") or "")[-4:]
        m[r["_id"]] = f'{r.get("bank_name","Bank")} — {r.get("account_name","Acct")} (…{last4})'
    return m

# ---------------- page ----------------
@taxes_hist_bp.route("/taxes-history", methods=["GET"])
def taxes_history_page():
    # Dropdown data
    banks = list(accounts_col.find({}, {"bank_name":1, "account_name":1, "account_number":1}).sort("bank_name", 1))
    bdcs  = list(bdc_col.find({}, {"name": 1}).sort("name", 1))
    omcs  = list(omc_col.find({}, {"name": 1}).sort("name", 1))
    # For quick order search we don’t prefetch orders; filter API will handle it.
    return render_template("partials/taxes_history.html", banks=banks, bdcs=bdcs, omcs=omcs)

# ---------------- API: payments history (BDC & OMC) ----------------
@taxes_hist_bp.route("/taxes-history/data", methods=["GET"])
def taxes_history_data():
    """
    Query params:
      kind: 'bdc' | 'omc'                  (required)
      start_date: YYYY-MM-DD               (optional)
      end_date:   YYYY-MM-DD               (optional; inclusive)
      bank_id:    ObjectId                 (optional)
      bdc_id:     ObjectId                 (kind=bdc optional)
      omc_name:   exact string name        (kind=omc optional; from bd_omc.name)
      order_id:   exact order code         (optional)
    """
    try:
        kind = _s(request.args.get("kind")).lower()
        if kind not in ("bdc", "omc"):
            return jsonify({"status": "error", "message": "kind must be 'bdc' or 'omc'"}), 400

        # date window (inclusive end)
        sd_s = _s(request.args.get("start_date"))
        ed_s = _s(request.args.get("end_date"))
        sd = None
        ed = None
        if sd_s:
            try:
                sd = datetime.strptime(sd_s, "%Y-%m-%d")
            except ValueError:
                return jsonify({"status":"error","message":"Invalid start_date"}), 400
        if ed_s:
            try:
                ed = datetime.strptime(ed_s, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                return jsonify({"status":"error","message":"Invalid end_date"}), 400

        bank_oid = _oid_or_none(_s(request.args.get("bank_id")))
        order_code_filter = _s(request.args.get("order_id")) or None

        rows = []
        total = 0.0

        if kind == "omc":
            # Inputs specific to OMC
            omc_name = _s(request.args.get("omc_name")) or None

            # Build match on tax_records (P-Tax)
            match = {"type": {"$regex": r"^p[\s_-]*tax$", "$options": "i"}}
            if bank_oid:
                match["source_bank_id"] = bank_oid
            if sd and ed:
                match["payment_date"] = {"$gte": sd, "$lte": ed}
            elif sd:
                match["payment_date"] = {"$gte": sd}
            elif ed:
                match["payment_date"] = {"$lte": ed}
            if omc_name:
                # Prefer exact OMC name saved on the tax record; otherwise we’ll also match via joined order.omc below
                match["$or"] = [{"omc": omc_name}, {"omc": {"$exists": False}}]

            pipe = [
                {"$match": match},
                {"$sort": {"payment_date": -1}},
                {"$lookup": {"from": "orders", "localField": "order_oid", "foreignField": "_id", "as": "ord"}},
                {"$addFields": {
                    "ord0": {"$arrayElemAt": ["$ord", 0]},
                    "order_code": {"$ifNull": [{"$arrayElemAt": ["$ord.order_id", 0]}, "$order_id"]},
                    "omc_name_j": {"$ifNull": ["$omc", {"$ifNull": [{"$arrayElemAt": ["$ord.omc", 0]}, "—"]}]},
                    "per_litre": {
                        "$toDouble": {
                            "$ifNull": [
                                {"$arrayElemAt": ["$ord.p_tax", 0]},
                                {"$ifNull": [{"$arrayElemAt": ["$ord.p-tax", 0]}, 0]}
                            ]
                        }
                    }
                }},
            ]

            # If an order code filter is given, filter by the computed order_code after $addFields
            if order_code_filter:
                pipe.append({"$match": {"order_code": order_code_filter}})

            # If omc_name was provided and wasn't present directly on tax record, also filter joined value:
            if omc_name:
                pipe.append({"$match": {"omc_name_j": omc_name}})

            pipe.append({"$project": {
                "_id": 0,
                "amount": 1,
                "reference": 1,
                "paid_by": 1,
                "payment_date": 1,
                "bank_id": "$source_bank_id",
                "order_id": "$order_code",
                "per_litre": {"$round": ["$per_litre", 4]},
                "party": "$omc_name_j",
            }})

            data = list(tax_col.aggregate(pipe))

            # Resolve bank names
            bank_ids = {r.get("bank_id") for r in data if r.get("bank_id")}
            bank_map = _bank_display_map(bank_ids)

            for r in data:
                amt = _f(r.get("amount"))
                total += amt
                dt = r.get("payment_date")
                rows.append({
                    "date": dt.strftime("%Y-%m-%d") if isinstance(dt, datetime) else str(dt or "—"),
                    "party": r.get("party") or "—",
                    "order_id": r.get("order_id") or "—",
                    "amount": round(amt, 2),
                    "per_litre": r.get("per_litre") or 0.0,
                    "per_litre_label": "P_OMC (₵/L)",
                    "reference": r.get("reference") or "—",
                    "paid_by": r.get("paid_by") or "—",
                    "bank": bank_map.get(r.get("bank_id"), "—"),
                })

        else:
            # kind == "bdc"
            bdc_oid = _oid_or_none(_s(request.args.get("bdc_id")))

            # Base: only docs with bank_paid_history
            pipe = [
                {"$match": {"bank_paid_history": {"$exists": True, "$ne": []}}},
                {"$unwind": "$bank_paid_history"},
            ]

            # Date filter on bank_paid_history.date
            date_match = {}
            if sd and ed:
                date_match["bank_paid_history.date"] = {"$gte": sd, "$lte": ed}
            elif sd:
                date_match["bank_paid_history.date"] = {"$gte": sd}
            elif ed:
                date_match["bank_paid_history.date"] = {"$lte": ed}
            if date_match:
                pipe.append({"$match": date_match})

            # Bank filter
            if bank_oid:
                pipe.append({"$match": {"bank_paid_history.bank_id": bank_oid}})

            # BDC filter
            if bdc_oid:
                pipe.append({"$match": {"bdc_id": bdc_oid}})

            # Join order (for order code + p_bdc_omc) and BDC (name)
            pipe.extend([
                {"$lookup": {"from": "orders", "localField": "order_id", "foreignField": "_id", "as": "ord"}},
                {"$lookup": {"from": "bdc", "localField": "bdc_id", "foreignField": "_id", "as": "bdc"}},
                {"$addFields": {
                    "order_code": {"$ifNull": [{"$arrayElemAt": ["$ord.order_id", 0]}, "$order_id"]},
                    "per_litre": {
                        "$toDouble": {
                            "$ifNull": [
                                {"$arrayElemAt": ["$ord.p_bdc_omc", 0]},
                                0
                            ]
                        }
                    },
                    "bdc_name": {"$ifNull": [{"$arrayElemAt": ["$bdc.name", 0]}, "—"]},
                }},
            ])

            if order_code_filter:
                pipe.append({"$match": {"order_code": order_code_filter}})

            pipe.append({"$project": {
                "_id": 0,
                "amount": "$bank_paid_history.amount",
                "reference": "$bank_paid_history.reference",
                "paid_by": "$bank_paid_history.paid_by",
                "payment_date": "$bank_paid_history.date",
                "bank_id": "$bank_paid_history.bank_id",
                "order_id": "$order_code",
                "per_litre": {"$round": ["$per_litre", 4]},
                "party": "$bdc_name",
            }})
            pipe.append({"$sort": {"payment_date": -1}})

            data = list(sbdc_col.aggregate(pipe))

            # Resolve bank names
            bank_ids = {r.get("bank_id") for r in data if r.get("bank_id")}
            bank_map = _bank_display_map(bank_ids)

            for r in data:
                amt = _f(r.get("amount"))
                total += amt
                dt = r.get("payment_date")
                rows.append({
                    "date": dt.strftime("%Y-%m-%d") if isinstance(dt, datetime) else str(dt or "—"),
                    "party": r.get("party") or "—",
                    "order_id": r.get("order_id") if not isinstance(r.get("order_id"), ObjectId) else str(r.get("order_id")),
                    "amount": round(amt, 2),
                    "per_litre": r.get("per_litre") or 0.0,
                    "per_litre_label": "P_BDC (₵/L)",
                    "reference": r.get("reference") or "—",
                    "paid_by": r.get("paid_by") or "—",
                    "bank": bank_map.get(r.get("bank_id"), "—"),
                })

        return jsonify({
            "status": "success",
            "kind": kind,
            "total": round(total, 2),
            "rows": rows
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
