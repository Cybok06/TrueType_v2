# admin_dashboard.py
from flask import Blueprint, render_template, redirect, url_for, session
from db import db, users_collection

admin_dashboard_bp = Blueprint('admin_dashboard', __name__, template_folder='templates')

# Collections (kept for future use)
clients_collection = db["clients"]
orders_collection = db["orders"]
payments_collection = db["payments"]
truck_payments_collection = db["truck_payments"]
tax_records_collection = db["tax_records"]
sbdc_collection = db["s_bdc_payment"]
payment_vouchers_collection = db["payment_vouchers"]

def _load_current_user():
    """
    Use session only to identify *who* is logged in (username/role),
    but NEVER to read permissions. Permissions come directly from DB.
    """
    username = session.get("username")
    if not username:
        return None
    user = users_collection.find_one(
        {"username": username},
        {"username": 1, "role": 1, "access": 1, "perms": 1}
    )
    return user

def _allowed_slugs_for(user):
    """
    Compute allowed slugs from user's stored access map or legacy perms list.
    Superadmin is considered only by username=='admin' or role flag (optional).
    """
    if not user:
        return set(), False

    is_super = (user.get("username") == "admin") or (user.get("role") == "superadmin")

    allowed = set()
    access = user.get("access")
    if isinstance(access, dict):
        allowed = {k for k, v in access.items() if v}
    else:
        perms = user.get("perms") or []
        allowed = set(perms)

    return allowed, bool(is_super)

@admin_dashboard_bp.route('/dashboard')
def dashboard():
    # If no user identified => send to login
    user = _load_current_user()
    if not user:
        return redirect(url_for('login.login'))

    # dY"' Pull permissions straight from DB for the logged user
    allowed_slugs, is_superadmin = _allowed_slugs_for(user)

    return render_template(
        'admin/admin_dashboard.html',
        # dY`% server-render permissions (no client fetch, no session perms)
        allowed_slugs=list(allowed_slugs),
        is_superadmin=is_superadmin
    )
