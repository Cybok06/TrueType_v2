# approved_orders.py
from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify
from db import db
from bson import ObjectId
from datetime import datetime, date

approved_orders_bp = Blueprint('approved_orders', __name__, template_folder='templates')

# Collections
orders_collection            = db['orders']
clients_collection           = db['clients']
payments_collection          = db['payments']      # only to compute "amount_paid" in table
bdc_collection               = db['bdc']           # BDC list (value = _id)
omc_collection               = db['bd_omc']        # OMC list (value/label = name)
s_bdc_payment_collection     = db['s_bdc_payment'] # BDC payable (key: order_oid + bdc_id)
omc_payment_collection       = db['omc_payment']   # OMC returns (key: order_oid + omc_name)

# ---------------- helpers ----------------
def as_objid_or_none(v):
    try:
        if isinstance(v, ObjectId): return v
        if v is None: return None
        s = str(v)
        return ObjectId(s) if ObjectId.is_valid(s) else None
    except Exception:
        return None

def as_float(x, default=0.0):
    try:
        f = float(x)
        if f != f:  # NaN
            return default
        return f
    except (TypeError, ValueError):
        return default

def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

def _nz(v):
    return v if v is not None else 0.0

def _as_dt(d):
    if isinstance(d, datetime): return d
    if isinstance(d, date):     return datetime(d.year, d.month, d.day)
    if isinstance(d, str):
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%dT%H:%M:%S"):
            try: return datetime.strptime(d, fmt)
            except Exception: pass
    return None

def human_order_id(order) -> str:
    for k in ("order_id", "order_no", "order_code", "order_number", "order_ref", "public_id"):
        v = order.get(k)
        if v: return str(v)
    created = _as_dt(order.get("date")) or datetime.utcnow()
    ts = created.strftime("%y%m%d")
    tail = str(order.get("_id"))[-6:].upper()
    return f"ORD-{ts}-{tail}"

# -------------- page: list + edit --------------
@approved_orders_bp.route('/approved_orders')
def view_approved_orders():
    if 'role' not in session or session['role'] != 'admin':
        flash("Access denied.", "danger")
        return redirect(url_for('login.login'))

    projection = {
        'date': 1, 'approved_at': 1,
        'client_id': 1, 'client_name': 1,
        'vehicle_number': 1, 'driver_name': 1, 'driver_phone': 1,
        'region': 1, 'product': 1,
        'order_type': 1, 'order_id': 1,
        'omc': 1,
        'bdc_id': 1, 'bdc_name': 1,
        'depot': 1, 'quantity': 1,
        'p_bdc_omc': 1, 's_bdc_omc': 1,
        'p_tax': 1, 's_tax': 1,
        'margin': 1, 'returns': 1, 'returns_total': 1,
        'total_debt': 1,
        'shareholder': 1, 'delivery_status': 1,
        'due_date': 1,
    }

    orders = list(
        orders_collection
        .find({'status': 'approved'}, projection)
        .sort('date', -1)
    )

    # Select lists for the edit modal
    bdcs = list(bdc_collection.find({}, {'name': 1, 'rep_phone': 1, 'phone': 1}).sort('name', 1))
    omcs = list(omc_collection.find({}, {'name': 1, 'rep_phone': 1}).sort('name', 1))

    for order in orders:
        # Client name/link
        client_oid = as_objid_or_none(order.get('client_id'))
        client = clients_collection.find_one({'_id': client_oid}) if client_oid else None
        order['client_name'] = (client or {}).get('name', order.get('client_name', 'Unknown'))
        order['client_mongo_id'] = str((client or {}).get('_id', ''))

        # Numbers
        margin   = as_float(order.get('margin'))
        quantity = as_float(order.get('quantity'))
        if order.get('returns') is None:
            order['returns'] = round(margin * quantity, 2) if margin is not None else 0.0

        order['p_tax']      = as_float(order.get('p_tax'))
        order['s_tax']      = as_float(order.get('s_tax'))
        order['p_bdc_omc']  = as_float(order.get('p_bdc_omc'))
        order['s_bdc_omc']  = as_float(order.get('s_bdc_omc'))
        order['total_debt'] = as_float(order.get('total_debt'))

        # Paid / Left (from central payments)
        oid = order.get('_id')
        match_ids = [oid, str(oid)]
        rows = list(payments_collection.aggregate([
            {'$match': {'order_id': {'$in': match_ids}, 'status': 'confirmed'}},
            {'$group': {'_id': None, 'total_paid': {'$sum': '$amount'}}}
        ]))
        order['amount_paid'] = round(as_float(rows[0]['total_paid']) if rows else 0.0, 2)
        order['amount_left'] = round(order['total_debt'] - order['amount_paid'], 2)

        # Coerce date if needed
        dt = order.get('date')
        if isinstance(dt, dict) and '$date' in dt:
            try:
                ms = int(dt['$date'].get('$numberLong', 0))
                order['date'] = datetime.fromtimestamp(ms / 1000.0)
            except Exception:
                pass

    return render_template('approved_orders.html', orders=orders, bdcs=bdcs, omcs=omcs)

# ----------- edit handler (also updates payment collections) -----------
@approved_orders_bp.route('/approved_orders/update/<order_id>', methods=['POST'])
def update_approved_order(order_id):
    """
    Edit an already-approved order.
    IMPORTANT: Also updates the linked payment docs:
      - s_bdc_payment (BDC payable)  -> by (order_oid, bdc_id)
      - omc_payment   (OMC returns)  -> by (order_oid, omc_name)
    """
    if 'role' not in session or session['role'] != 'admin':
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    try:
        oid = ObjectId(order_id)
    except Exception:
        return jsonify({"success": False, "error": "Invalid order id"}), 400

    order = orders_collection.find_one({"_id": oid})
    if not order:
        return jsonify({"success": False, "error": "Order not found"}), 404

    # ----- form values -----
    form = request.form

    mode = (form.get("order_type") or order.get("order_type") or "combo").strip().lower()
    # mode in {"s_bdc","s_tax","combo"}

    new_omc_name = (form.get("omc") or "").strip()          # string, may be empty
    new_bdc_raw  = (form.get("bdc") or "").strip()          # ObjectId string
    new_bdc_id   = as_objid_or_none(new_bdc_raw) if new_bdc_raw else None

    depot        = (form.get("depot") or order.get("depot") or "").strip()
    shareholder  = (form.get("shareholder") or "").strip() or None

    p_bdc        = _f(form.get("p_bdc_omc"))
    s_bdc        = _f(form.get("s_bdc_omc"))
    p_tax        = _f(form.get("p_tax"))
    s_tax        = _f(form.get("s_tax"))

    payment_type = (form.get("payment_type") or "").strip()  # (BDC) optional
    # payment_amount (manual) is ignored; we always recompute from qty × P-BDC for consistency

    # Validate basic requireds
    if not depot:
        return jsonify({"success": False, "error": "DEPOT is required."}), 400

    if mode not in ("s_bdc", "s_tax", "combo"):
        return jsonify({"success": False, "error": "Invalid order type."}), 400

    if mode == "s_tax":
        if not new_omc_name:
            return jsonify({"success": False, "error": "OMC is required for S-Tax order type."}), 400
    elif mode == "s_bdc":
        if not new_bdc_id:
            return jsonify({"success": False, "error": "BDC is required for S-BDC order type."}), 400
    else:  # combo
        if not new_omc_name or not new_bdc_id:
            return jsonify({"success": False, "error": "OMC and BDC are required for Combo order type."}), 400

    # Client name (for display in BDC doc)
    client = None
    client_name = ""
    try:
        client = clients_collection.find_one({"_id": as_objid_or_none(order.get("client_id"))})
        client_name = client.get("name", "") if client else ""
    except Exception:
        pass

    human_id = human_order_id(order)
    qty = _f(order.get("quantity")) or 0.0

    # per-L margins
    margin_price = (s_bdc - p_bdc) if (s_bdc is not None and p_bdc is not None) else None
    margin_tax   = (s_tax - p_tax) if (s_tax is not None and p_tax is not None) else None

    # total debt by order type
    if mode == "s_bdc":
        total_debt = _nz(s_bdc) * qty
    elif mode == "s_tax":
        total_debt = _nz(s_tax) * qty
    else:  # combo
        total_debt = (_nz(s_bdc) + _nz(s_tax)) * qty

    # RETURNS
    returns_price = _nz(margin_price) * qty
    returns_tax   = _nz(margin_tax) * qty
    returns_total = returns_price + returns_tax

    # ---- previous ids/names to detect changes ----
    prev_bdc_id   = as_objid_or_none(order.get("bdc_id"))
    prev_omc_name = (order.get("omc") or "").strip()

    # ---- update the order itself first ----
    update_doc = {
        "depot": depot,
        "shareholder": shareholder,
        "order_type": mode,
        "omc": new_omc_name or None,
        "p_bdc_omc": p_bdc,
        "s_bdc_omc": s_bdc,
        "p_tax": p_tax,
        "s_tax": s_tax,
        "total_debt": round(total_debt, 2),
        "returns_sbdc": round(returns_price, 2),
        "returns_stax": round(returns_tax, 2),
        "returns_total": round(returns_total, 2),
        "returns": round(returns_total, 2),  # legacy alias
        "delivery_status": order.get("delivery_status") or "pending",
        "updated_at": datetime.utcnow(),
    }
    if margin_price is not None:
        update_doc["margin_price"] = round(margin_price, 2)
        update_doc["margin"] = round(margin_price, 2)  # legacy alias
    if margin_tax is not None:
        update_doc["margin_tax"] = round(margin_tax, 2)

    # attach BDC fields if relevant
    if mode != "s_tax":
        if not new_bdc_id:
            return jsonify({"success": False, "error": "Invalid BDC ID"}), 400
        bdc = bdc_collection.find_one({"_id": new_bdc_id})
        if not bdc:
            return jsonify({"success": False, "error": "BDC not found"}), 404
        update_doc["bdc_id"] = new_bdc_id
        update_doc["bdc_name"] = bdc.get("name", "")

    # keep status approved; if it somehow wasn’t, set it
    update_doc["status"] = "approved"
    orders_collection.update_one({"_id": oid}, {"$set": update_doc})

    # ===========================
    # Sync payment collections
    # ===========================

    # ---- 1) BDC payable in s_bdc_payment ----
    # Only if mode != s_tax (S-BDC or COMBO)
    if mode != "s_tax":
        # Calculate new payable amount (qty × P-BDC)
        # If P-BDC not provided we do not touch amount to avoid overwriting.
        new_payable_amount = None
        if p_bdc is not None:
            new_payable_amount = round(qty * p_bdc, 2)

        # If BDC changed, move/rename payment doc to new bdc_id.
        if prev_bdc_id and new_bdc_id and str(prev_bdc_id) != str(new_bdc_id):
            old_doc = s_bdc_payment_collection.find_one({"order_oid": oid, "bdc_id": prev_bdc_id})
            if old_doc:
                # Try to reassign to the new bdc_id; handle unique key collisions by merging.
                already = s_bdc_payment_collection.find_one({"order_oid": oid, "bdc_id": new_bdc_id})
                if already:
                    # Merge: prefer recalculated amount (if provided), keep/append histories, then drop old one
                    set_updates = {
                        "updated_at": datetime.utcnow(),
                        "shareholder": shareholder,
                        "payment_type": payment_type or already.get("payment_type"),
                        "client_name": client_name or already.get("client_name", ""),
                        "product": order.get("product", already.get("product", "")),
                        "vehicle_number": order.get("vehicle_number", already.get("vehicle_number", "")),
                        "driver_name": order.get("driver_name", already.get("driver_name", "")),
                        "driver_phone": order.get("driver_phone", already.get("driver_phone", "")),
                        "quantity": order.get("quantity", already.get("quantity", "")),
                        "region": order.get("region", already.get("region", "")),
                        "delivery_status": already.get("delivery_status", "pending"),
                    }
                    if new_payable_amount is not None:
                        set_updates["amount"] = new_payable_amount

                    # Merge bank histories if both have them
                    old_hist = old_doc.get("bank_paid_history") or []
                    new_hist = already.get("bank_paid_history") or []
                    merged_hist = new_hist + old_hist if old_hist else new_hist
                    if merged_hist:
                        set_updates["bank_paid_history"] = merged_hist
                        # Try to keep totals/last_at sensible if present
                        set_updates["bank_paid_total"] = (
                            (already.get("bank_paid_total") or 0.0) + (old_doc.get("bank_paid_total") or 0.0)
                        )
                        set_updates["bank_paid_last_at"] = already.get("bank_paid_last_at") or old_doc.get("bank_paid_last_at")

                    s_bdc_payment_collection.update_one(
                        {"_id": already["_id"]},
                        {"$set": set_updates}
                    )
                    s_bdc_payment_collection.delete_one({"_id": old_doc["_id"]})
                else:
                    # Just change the bdc_id on the same doc
                    set_updates = {
                        "bdc_id": new_bdc_id,
                        "updated_at": datetime.utcnow(),
                        "shareholder": shareholder,
                        "payment_type": payment_type or old_doc.get("payment_type"),
                        "client_name": client_name or old_doc.get("client_name", ""),
                        "product": order.get("product", old_doc.get("product", "")),
                        "vehicle_number": order.get("vehicle_number", old_doc.get("vehicle_number", "")),
                        "driver_name": order.get("driver_name", old_doc.get("driver_name", "")),
                        "driver_phone": order.get("driver_phone", old_doc.get("driver_phone", "")),
                        "quantity": order.get("quantity", old_doc.get("quantity", "")),
                        "region": order.get("region", old_doc.get("region", "")),
                    }
                    if new_payable_amount is not None:
                        set_updates["amount"] = new_payable_amount

                    s_bdc_payment_collection.update_one(
                        {"_id": old_doc["_id"]},
                        {"$set": set_updates}
                    )
        else:
            # BDC didn’t change; just update amount/fields if doc exists
            doc = s_bdc_payment_collection.find_one({"order_oid": oid, "bdc_id": new_bdc_id or prev_bdc_id})
            if doc:
                set_updates = {
                    "updated_at": datetime.utcnow(),
                    "shareholder": shareholder,
                    "payment_type": payment_type or doc.get("payment_type"),
                    "client_name": client_name or doc.get("client_name", ""),
                    "product": order.get("product", doc.get("product", "")),
                    "vehicle_number": order.get("vehicle_number", doc.get("vehicle_number", "")),
                    "driver_name": order.get("driver_name", doc.get("driver_name", "")),
                    "driver_phone": order.get("driver_phone", doc.get("driver_phone", "")),
                    "quantity": order.get("quantity", doc.get("quantity", "")),
                    "region": order.get("region", doc.get("region", "")),
                }
                if new_payable_amount is not None:
                    set_updates["amount"] = new_payable_amount

                s_bdc_payment_collection.update_one({"_id": doc["_id"]}, {"$set": set_updates})
            else:
                # If no doc exists yet, create it now (rare for already-approved, but safe)
                if new_bdc_id:
                    s_bdc_payment_collection.update_one(
                        {"order_oid": oid, "bdc_id": new_bdc_id},
                        {"$setOnInsert": {
                            "order_id": human_id,
                            "created_at": datetime.utcnow(),
                        },
                         "$set": {
                            "payment_type": payment_type or "Cash",
                            "amount": new_payable_amount if new_payable_amount is not None else 0.0,
                            "client_name": client_name or "—",
                            "product": order.get("product", ""),
                            "vehicle_number": order.get("vehicle_number", ""),
                            "driver_name": order.get("driver_name", ""),
                            "driver_phone": order.get("driver_phone", ""),
                            "quantity": order.get("quantity", ""),
                            "region": order.get("region", ""),
                            "delivery_status": "pending",
                            "shareholder": shareholder,
                            "bank_status": "pending",
                            "updated_at": datetime.utcnow(),
                         }},
                        upsert=True
                    )
    else:
        # If order is S-TAX only, ensure any old BDC payable for this order is removed
        s_bdc_payment_collection.delete_many({"order_oid": oid})

    # ---- 2) OMC returns in omc_payment ----
    # Only if returns are positive AND OMC name present (for s_tax/combo)
    if (returns_total and returns_total > 0) and new_omc_name:
        # If OMC changed, rename/move doc
        if (prev_omc_name or "") != new_omc_name:
            old_doc = omc_payment_collection.find_one({"order_oid": oid, "omc_name": prev_omc_name})
            if old_doc:
                # If a doc for the new omc_name already exists, update it; then delete old
                already = omc_payment_collection.find_one({"order_oid": oid, "omc_name": new_omc_name})
                set_updates = {
                    "omc_name": new_omc_name,
                    "amount": round(returns_total, 2),
                    "returns_price": round(returns_price, 2),
                    "returns_tax": round(returns_tax, 2),
                    "status": old_doc.get("status", "pending"),
                    "shareholder": shareholder,
                    "product": order.get("product", old_doc.get("product", "")),
                    "quantity": order.get("quantity", old_doc.get("quantity", "")),
                    "region": order.get("region", old_doc.get("region", "")),
                    "updated_at": datetime.utcnow(),
                }
                if already:
                    omc_payment_collection.update_one({"_id": already["_id"]}, {"$set": set_updates})
                    omc_payment_collection.delete_one({"_id": old_doc["_id"]})
                else:
                    omc_payment_collection.update_one({"_id": old_doc["_id"]}, {"$set": set_updates})
            else:
                # No old doc — just upsert a new one
                omc_payment_collection.update_one(
                    {"order_oid": oid, "omc_name": new_omc_name},
                    {"$setOnInsert": {"order_id": human_id, "created_at": datetime.utcnow()},
                     "$set": {
                         "amount": round(returns_total, 2),
                         "returns_price": round(returns_price, 2),
                         "returns_tax": round(returns_tax, 2),
                         "status": "pending",
                         "shareholder": shareholder,
                         "product": order.get("product", ""),
                         "quantity": order.get("quantity", ""),
                         "region": order.get("region", ""),
                         "updated_at": datetime.utcnow(),
                     }},
                    upsert=True
                )
        else:
            # OMC didn’t change; just update figures
            omc_payment_collection.update_one(
                {"order_oid": oid, "omc_name": new_omc_name},
                {"$setOnInsert": {"order_id": human_id, "created_at": datetime.utcnow()},
                 "$set": {
                     "amount": round(returns_total, 2),
                     "returns_price": round(returns_price, 2),
                     "returns_tax": round(returns_tax, 2),
                     "status": "pending",
                     "shareholder": shareholder,
                     "product": order.get("product", ""),
                     "quantity": order.get("quantity", ""),
                     "region": order.get("region", ""),
                     "updated_at": datetime.utcnow(),
                 }},
                upsert=True
            )
    else:
        # If no OMC (or zero returns), remove any existing omc_payment for this order
        omc_payment_collection.delete_many({"order_oid": oid})

    return jsonify({
        "success": True,
        "message": "Order and related payment records updated.",
        "approved": True,
        "order_id": human_order_id(order)
    })
