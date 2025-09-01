from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from bson import ObjectId, errors
from db import db
from datetime import datetime, date

orders_bp = Blueprint('orders', __name__, template_folder='templates')

orders_collection        = db['orders']
clients_collection       = db['clients']
bdc_collection           = db['bdc']
products_collection      = db['products']         # Products collection
omc_collection           = db['bd_omc']           # OMCs (with rep_phone)
s_bdc_payment_collection = db['s_bdc_payment']    # central BDC payment collection
omc_payment_collection   = db['omc_payment']      # simple OMC-side posting collection

# --------------- helpers ---------------
def _f(v):
    """parse float or return None"""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

def _nz(v):
    """None -> 0.0 without changing real zeros"""
    return v if v is not None else 0.0

def _as_dt(d):
    """best effort to parse a date/datetime or return None"""
    if isinstance(d, datetime):
        return d
    if isinstance(d, date):
        return datetime(d.year, d.month, d.day)
    if isinstance(d, str):
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(d, fmt)
            except Exception:
                pass
    return None

def human_order_id(order) -> str:
    """
    Resolve your human-generated order id (not Mongo _id).
    Tries common keys; otherwise creates a safe fallback.
    """
    for k in ("order_id", "order_no", "order_code", "order_number", "order_ref", "public_id"):
        v = order.get(k)
        if v:
            return str(v)
    # Fallback: ORD-YYMMDD-XXXXXX (last 6 of ObjectId)
    created = _as_dt(order.get("date")) or datetime.utcnow()
    ts = created.strftime("%y%m%d")
    tail = str(order.get("_id"))[-6:].upper()
    return f"ORD-{ts}-{tail}"

# --------------- pages ---------------
@orders_bp.route('/', methods=['GET'])
def view_orders():
    if 'role' not in session or session['role'] not in ['admin', 'assistant']:
        flash("Access denied.", "danger")
        return redirect(url_for('login.login'))

    orders = list(orders_collection.find({'status': 'pending'}).sort('date', -1))

    # BDCs with contact fields
    bdcs = list(
        bdc_collection.find({}, {'name': 1, 'rep_phone': 1, 'phone': 1}).sort('name', 1)
    )

    # OMCs with contact fields
    omcs = list(omc_collection.find({}, {'name': 1, 'rep_phone': 1}).sort('name', 1))

    for order in orders:
        # client could be stored as ObjectId or string
        try:
            client = clients_collection.find_one({'_id': ObjectId(order.get('client_id'))})
        except Exception:
            client = None

        if client:
            order['client_name'] = client.get('name', 'No Name')
            order['client_image_url'] = client.get('image_url', '')
            order['client_id'] = client.get('client_id', '')
            order['client_profile_url'] = None
        else:
            order['client_name'] = 'Unknown'
            order['client_image_url'] = ''
            order['client_profile_url'] = None

        # Server-side initial display (fallbacks)
        p     = _f(order.get('p_bdc_omc'))
        s     = _f(order.get('s_bdc_omc'))
        p_tax = _f(order.get('p_tax'))
        s_tax = _f(order.get('s_tax'))
        q     = _f(order.get('quantity')) or 0.0

        # per-L margins (only if both sides available)
        margin_price = (s - p) if (s is not None and p is not None) else None
        margin_tax   = (s_tax - p_tax) if (s_tax is not None and p_tax is not None) else None

        # expose both margins for the UI
        order['margin']      = round(margin_price, 2) if margin_price is not None else None
        order['margin_tax']  = round(margin_tax, 2)   if margin_tax   is not None else None

        # returns = Q × (sum of available margins)
        ret_price = (_nz(margin_price)) * q
        ret_tax   = (_nz(margin_tax)) * q
        ret_total = ret_price + ret_tax

        # store per-part + total for initial render
        order['returns_sbdc']  = round(ret_price, 2)   # price-margin × Q
        order['returns_stax']  = round(ret_tax, 2)     # tax-margin × Q
        order['returns_total'] = round(ret_total, 2)
        order['returns']       = round(ret_total, 2)   # legacy alias

    return render_template('partials/orders.html', orders=orders, bdcs=bdcs, omcs=omcs)

@orders_bp.route('/update/<order_id>', methods=['POST'])
def update_order(order_id):
    if 'role' not in session or session['role'] not in ['admin', 'assistant']:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    form = request.form
    mode = (form.get("order_type") or "combo").strip().lower()  # 's_bdc' | 's_tax' | 'combo'

    fields = {
        "omc": form.get("omc"),
        "bdc": form.get("bdc"),  # may be None when S-Tax
        "depot": form.get("depot"),
        "p_bdc_omc": form.get("p_bdc_omc"),
        "s_bdc_omc": form.get("s_bdc_omc"),
        "p_tax": form.get("p_tax"),
        "s_tax": form.get("s_tax"),
        "due_date": form.get("due_date"),
        "payment_type": (form.get("payment_type") or "").strip(),
        "payment_amount": form.get("payment_amount"),
        "shareholder": (form.get("shareholder") or "").strip(),

        # Optional: bank hints from the form (no auto-allocation here)
        "bank_id": (form.get("bank_id") or "").strip(),
        "bank_reference": (form.get("bank_reference") or "").strip(),
        "bank_paid_by": (form.get("bank_paid_by") or "").strip(),
        "bank_payment_date": (form.get("bank_payment_date") or "").strip(),  # YYYY-MM-DD
    }

    # ---------- REQUIRED FIELDS (aligned with business rules) ----------
    # DEPOT is always required
    if not fields["depot"]:
        return jsonify({"success": False, "error": "DEPOT is required."}), 400

    if mode not in ("s_bdc", "s_tax", "combo"):
        return jsonify({"success": False, "error": "Invalid order type."}), 400

    if mode == "s_tax":
        # OMC required, BDC NOT required
        if not fields["omc"]:
            return jsonify({"success": False, "error": "OMC is required for S-Tax order type."}), 400
    elif mode == "s_bdc":
        # BDC required, OMC NOT required
        if not fields["bdc"]:
            return jsonify({"success": False, "error": "BDC is required for S-BDC order type."}), 400
    else:  # combo
        # Both required
        if not fields["omc"] or not fields["bdc"]:
            return jsonify({"success": False, "error": "OMC and BDC are required for Combo order type."}), 400

    # Fetch order + client ...
    try:
        order = orders_collection.find_one({"_id": ObjectId(order_id)})
    except Exception:
        order = None
    if not order:
        return jsonify({"success": False, "error": "Order not found"}), 404

    client_name = ""
    try:
        client = clients_collection.find_one({"_id": ObjectId(order.get("client_id"))})
        client_name = client.get("name", "") if client else ""
    except Exception:
        client = None

    # Human order id for postings (NOT the Mongo _id)
    human_id = human_order_id(order)

    # Parse numeric inputs
    def _f_local(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def _nz_local(v):
        return v if v is not None else 0.0

    p     = _f_local(fields["p_bdc_omc"])   # P-BDC
    s     = _f_local(fields["s_bdc_omc"])   # S-BDC
    p_tax = _f_local(fields["p_tax"])       # P-Tax
    s_tax = _f_local(fields["s_tax"])       # S-Tax
    q     = _f_local(order.get("quantity")) or 0.0

    # Validate by order type (per price/tax inputs)
    if mode == "s_bdc" and s is None:
        return jsonify({"success": False, "error": "S-BDC is required for S-BDC type."}), 400
    if mode == "s_tax" and s_tax is None:
        return jsonify({"success": False, "error": "S-Tax is required for S-Tax type."}), 400
    if mode == "combo" and (s is None or s_tax is None):
        return jsonify({"success": False, "error": "S-BDC and S-Tax are required for Combo type."}), 400

    # ---- per-L margins ----
    margin_price = (s - p) if (s is not None and p is not None) else None
    margin_tax   = (s_tax - p_tax) if (s_tax is not None and p_tax is not None) else None

    # ---- total debt by order type ----
    if mode == "s_bdc":
        total_debt = _nz_local(s) * q
    elif mode == "s_tax":
        total_debt = _nz_local(s_tax) * q
    else:  # combo
        total_debt = (_nz_local(s) + _nz_local(s_tax)) * q

    # ---- RETURNS ----
    returns_price = _nz_local(margin_price) * q
    returns_tax   = _nz_local(margin_tax) * q
    returns_total = returns_price + returns_tax

    # Build update doc
    update_data = {
        "omc": fields["omc"],                  # may be None/'' for s_bdc
        "depot": fields["depot"],
        "shareholder": fields["shareholder"] or None,
        "p_bdc_omc": p,
        "s_bdc_omc": s,
        "p_tax": p_tax,
        "s_tax": s_tax,
        "order_type": mode,
        "total_debt": round(total_debt, 2),
        "returns_sbdc": round(returns_price, 2),
        "returns_stax": round(returns_tax, 2),
        "returns_total": round(returns_total, 2),
        "returns": round(returns_total, 2),
    }
    if margin_price is not None:
        update_data["margin_price"] = round(margin_price, 2)
        update_data["margin"] = round(margin_price, 2)  # legacy alias
    if margin_tax is not None:
        update_data["margin_tax"] = round(margin_tax, 2)

    # Due date
    if fields["due_date"]:
        try:
            update_data["due_date"] = datetime.strptime(fields["due_date"], "%Y-%m-%d")
        except ValueError:
            return jsonify({"success": False, "error": "Invalid date format"}), 400
    else:
        update_data["due_date"] = None

    # BDC lookup & set (when NOT S-Tax)
    bdc_id = None
    if mode != "s_tax":
        try:
            bdc_id = ObjectId(fields["bdc"])
        except Exception:
            return jsonify({"success": False, "error": "Invalid BDC ID"}), 400

        bdc = bdc_collection.find_one({"_id": bdc_id})
        if not bdc:
            return jsonify({"success": False, "error": "BDC not found"}), 404

        update_data["bdc_id"] = bdc_id
        update_data["bdc_name"] = bdc.get("name", "")

    # ---------------------------
    # Payment handling -> s_bdc_payment  (BDC payable)
    # Cash / From Account / Credit => create pending record (no auto-bank clearing)
    # ---------------------------
    payment_type_norm = (fields["payment_type"] or "").strip().lower()
    if mode != "s_tax" and payment_type_norm in ("cash", "from account", "credit"):
        if p is None:
            return jsonify({"success": False, "error": "P-BDC is required to compute payment amount"}), 400

        calc_amount = round(q * p, 2)
        s_bdc_payment_collection.insert_one({
            "order_oid": ObjectId(order_id),
            "order_id": human_id,                      # human order id
            "bdc_id": bdc_id,
            "payment_type": fields["payment_type"],    # original case
            "amount": calc_amount,
            "client_name": client_name or "—",
            "product": order.get("product", ""),
            "vehicle_number": order.get("vehicle_number", ""),
            "driver_name": order.get("driver_name", ""),
            "driver_phone": order.get("driver_phone", ""),
            "quantity": order.get("quantity", ""),
            "region": order.get("region", ""),
            "delivery_status": "pending",
            "shareholder": fields["shareholder"] or None,
            "bank_status": "pending",
            "date": datetime.utcnow()
        })

    # ---------------------------
    # OMC-side posting (their “returns”/margin receivable)
    # Only create if we actually have an OMC name
    # ---------------------------
    if returns_total and returns_total > 0 and fields["omc"]:
        omc_payment_collection.insert_one({
            "order_oid": ObjectId(order_id),
            "order_id": human_id,                 # human order id
            "omc_name": fields["omc"],
            "amount": round(returns_total, 2),
            "returns_price": round(returns_price, 2),
            "returns_tax": round(returns_tax, 2),
            "status": "pending",
            "shareholder": fields["shareholder"] or None,
            "product": order.get("product", ""),
            "quantity": order.get("quantity", ""),
            "region": order.get("region", ""),
            "created_at": datetime.utcnow()
        })

    # Status flags
    complete_fields = (update_data.get("total_debt") is not None) and (
        (mode == "s_tax" and ("returns_total" in update_data or "margin_tax" in update_data)) or
        (mode in ("s_bdc", "combo") and ("returns_total" in update_data or "margin" in update_data))
    )
    new_status = "approved" if complete_fields else "pending"
    update_data["status"] = new_status
    update_data["delivery_status"] = "pending"

    # stamp approved_at when transitioning to approved
    prev = orders_collection.find_one({"_id": ObjectId(order_id)}, {"status": 1, "approved_at": 1})
    if new_status == "approved" and (not prev or prev.get("status") != "approved"):
        update_data["approved_at"] = datetime.utcnow()

    orders_collection.update_one({"_id": ObjectId(order_id)}, {"$set": update_data})

    approved = (new_status == "approved")
    resp = {
        "success": True,
        "message": "Order updated" + (" and approved" if approved else " (still pending)"),
        "approved": approved
    }
    if approved:
        resp["approved_at"] = (update_data.get("approved_at") or (prev or {}).get("approved_at"))
        resp["invoice_url"] = url_for("orders.order_invoice", order_id=order_id)
        resp["order_id"] = human_id
    return jsonify(resp)

@orders_bp.route('/get_product_price', methods=['GET'])
def get_product_price():
    product_name = (request.args.get('name', '') or '').strip()
    if not product_name:
        return jsonify({'success': False, 'error': 'Missing product name'}), 400

    product = products_collection.find_one(
        {'name': {'$regex': f'^{product_name}$', '$options': 'i'}},
        {'p_price': 1, 's_price': 1, 'p_tax': 1, 's_tax': 1}
    )
    if not product:
        return jsonify({'success': False, 'error': 'Product not found'}), 404

    return jsonify({
        'success': True,
        'p_price': product.get('p_price', 0),
        's_price': product.get('s_price', 0),
        'p_tax':   product.get('p_tax', 0),
        's_tax':   product.get('s_tax', 0),
    })

# --------------- invoice page ---------------
@orders_bp.route('/invoice/<order_id>', methods=['GET'])
def order_invoice(order_id):
    try:
        oid = ObjectId(order_id)
    except Exception:
        flash("Invalid order id.", "danger")
        return redirect(url_for('orders.view_orders'))

    order = orders_collection.find_one({"_id": oid})
    if not order:
        flash("Order not found.", "danger")
        return redirect(url_for('orders.view_orders'))

    client = None
    cid = order.get("client_id")
    if cid:
        try:
            client = clients_collection.find_one({"_id": ObjectId(cid)})
        except Exception:
            client = clients_collection.find_one({"client_id": str(cid)})

    # Optional receipt ref if present (kept for compatibility; may be empty now)
    receipt_ref = None
    p_details = (order.get("payment_details") or [])
    if p_details:
        latest = sorted(p_details, key=lambda x: x.get("date") or datetime.min, reverse=True)[0]
        receipt_ref = latest.get("receipt_ref")

    return render_template(
        "partials/invoice.html",
        order=order,
        client=client or {},
        now=datetime.utcnow(),
        receipt_ref=receipt_ref
    )
