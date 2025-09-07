from flask import Blueprint, render_template, session, redirect, url_for, flash
from db import db
from bson import ObjectId
from datetime import datetime

approved_orders_bp = Blueprint('approved_orders', __name__, template_folder='templates')

orders_collection = db['orders']
clients_collection = db['clients']
payments_collection = db['payments']

def as_objid_or_none(v):
    """Return ObjectId if possible, else None."""
    try:
        if isinstance(v, ObjectId):
            return v
        if v is None:
            return None
        s = str(v)
        return ObjectId(s) if ObjectId.is_valid(s) else None
    except Exception:
        return None

def as_float(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default

@approved_orders_bp.route('/approved_orders')
def view_approved_orders():
    if 'role' not in session or session['role'] != 'admin':
        flash("Access denied.", "danger")
        return redirect(url_for('login.login'))

    # ✅ feature flag check
    

    # ✅ load approved orders
    orders = list(orders_collection.find({'status': 'approved'}).sort('date', -1))

    for order in orders:
        # --- client lookup (handle string/ObjectId) ---
        client_id = order.get('client_id')
        client_oid = as_objid_or_none(client_id)
        client = clients_collection.find_one({'_id': client_oid}) if client_oid else None

        order['client_name'] = (client or {}).get('name', 'Unknown')
        order['client_mongo_id'] = str((client or {}).get('_id', ''))

        # --- numeric fields ---
        margin = as_float(order.get('margin'))
        quantity = as_float(order.get('quantity'))
        order['returns'] = round(margin * quantity, 2)

        order['tax'] = as_float(order.get('tax'))
        order['p_tax'] = as_float(order.get('p_tax'))
        order['s_tax'] = as_float(order.get('s_tax'))
        order['p_bdc_omc'] = as_float(order.get('p_bdc_omc'))
        order['s_bdc_omc'] = as_float(order.get('s_bdc_omc'))
        order['total_debt'] = as_float(order.get('total_debt'))

        # --- payments sum (handle ObjectId vs string) ---
        this_oid = order.get('_id')  # this is an ObjectId
        # Some historical rows might have saved order_id as a *string*,
        # so we match either form to be safe.
        match_ids = [this_oid, str(this_oid)]

        amount_paid_result = payments_collection.aggregate([
            {
                '$match': {
                    'order_id': {'$in': match_ids},
                    'status': 'confirmed'
                }
            },
            {
                '$group': {
                    '_id': None,
                    'total_paid': {'$sum': '$amount'}
                }
            }
        ])

        paid_rows = list(amount_paid_result)
        order['amount_paid'] = round(as_float(paid_rows[0]['total_paid']) if paid_rows else 0.0, 2)
        order['amount_left'] = round(order['total_debt'] - order['amount_paid'], 2)

        # --- ensure date is python datetime for template ---
        dt = order.get('date')
        if isinstance(dt, dict) and '$date' in dt:  # in case a raw mongo dict leaked in
            try:
                ms = int(dt['$date'].get('$numberLong', 0))
                order['date'] = datetime.fromtimestamp(ms / 1000.0)
            except Exception:
                pass

    return render_template('approved_orders.html', orders=orders)
