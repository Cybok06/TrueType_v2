from flask import Blueprint, render_template
from db import db
from datetime import datetime

assistant_dashboard_bp = Blueprint('assistant_dashboard', __name__, template_folder='templates')

# Collections
clients_collection = db["clients"]
orders_collection = db["orders"]
payments_collection = db["payments"]

# Assistant Dashboard Home
@assistant_dashboard_bp.route('/dashboard')
def dashboard():
    # Count unapproved orders
    unapproved_orders_count = orders_collection.count_documents({"status": "pending"})

    # Count overdue clients
    overdue_clients_count = clients_collection.count_documents({"status": "overdue"})

    # Count unconfirmed payments
    unconfirmed_payments_count = payments_collection.count_documents({"status": "pending"})

    return render_template(
        'assistant/assistant_dashboard.html',
        unapproved_orders_count=unapproved_orders_count,
        overdue_clients_count=overdue_clients_count,
        unconfirmed_payments_count=unconfirmed_payments_count
    )

