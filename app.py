from flask import Flask, redirect, url_for, session  # ✅ Required for logout

# === Auth/Login ===
from login import login_bp

# === Shared Features ===
from register_client import register_client_bp  # ✅ FIXED
from clientlist import clientlist_bp
from client_profile import client_profile_bp
from approved_orders import approved_orders_bp
from orders import orders_bp
from payments import payments_bp
from debtors import debtors_bp
from bank_profile import bank_profile_bp  # ✅ NEW
from bdc import bdc_bp
from home import home_bp
from shareholders import shareholders_bp
from tax import tax_bp
from manage_deliveries import manage_deliveries_bp
from products import products_bp
from bank_accounts import bank_accounts_bp
from truck import truck_bp
from truck_debtors import truck_debtors_bp  # ✅ NEW
from admin_truck_payments import admin_truck_payments_bp  # ✅ NEW
from share_links import shared_bp
from omc import omc_bp
from order_cancellation import cancel_bp
# app.py
from navbar import navbar_bp
from users import users_bp






# === Admin Features ===
from admin.admin_dashboard import admin_dashboard_bp
from admin.settings import admin_settings_bp

# === Assistant Features ===
from assistant.assistant_dashboard import assistant_dashboard_bp
from reports import reports_bp  # <-- import the new one


# === Client Features ===
from client.client_dashboard import client_dashboard_bp
from client.client_order import client_order_bp
from client.client_order_history import client_order_history_bp
from client.client_payment import client_payment_bp
from login_logs import login_logs_bp  # <-- import the new logs blueprint

# === Initialize App ===
app = Flask(__name__)
app.secret_key = '4b1b26eee81fd7da3be8efd2649c3b07140b511118b11009f243adabd4d61559'  # 🔐 Use env variable in production

# === Blueprint Registration ===

# Auth/Login
app.register_blueprint(login_bp)
app.register_blueprint(home_bp)

# Shared Features
app.register_blueprint(register_client_bp)
app.register_blueprint(clientlist_bp)
app.register_blueprint(client_profile_bp)
app.register_blueprint(approved_orders_bp)
app.register_blueprint(orders_bp, url_prefix='/orders')
app.register_blueprint(payments_bp)
app.register_blueprint(debtors_bp)
app.register_blueprint(bdc_bp)
app.register_blueprint(shareholders_bp)
app.register_blueprint(tax_bp)
app.register_blueprint(manage_deliveries_bp)
app.register_blueprint(products_bp)
app.register_blueprint(bank_accounts_bp)
app.register_blueprint(truck_bp)
app.register_blueprint(truck_debtors_bp)  # ✅ Registered
app.register_blueprint(admin_truck_payments_bp)  # ✅ Registered
app.register_blueprint(bank_profile_bp)  # ✅ Register bank profile route
app.register_blueprint(reports_bp)
app.register_blueprint(shared_bp)
app.register_blueprint(omc_bp)
app.register_blueprint(cancel_bp, url_prefix="") 
app.register_blueprint(navbar_bp)
     # ✅ routes: /orders/cancel/...
app.register_blueprint(users_bp)
app.register_blueprint(login_logs_bp)            # routes like /logs/login




# Admin
app.register_blueprint(admin_dashboard_bp, url_prefix='/admin')
app.register_blueprint(admin_settings_bp)

# Assistant
app.register_blueprint(assistant_dashboard_bp, url_prefix='/assistant')

# Client
app.register_blueprint(client_dashboard_bp, url_prefix='/client')
app.register_blueprint(client_order_bp, url_prefix='/client')
app.register_blueprint(client_order_history_bp, url_prefix='/client')
app.register_blueprint(client_payment_bp, url_prefix='/client')

# === Logout Route ===
@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login.login'))

# === Run App ===
if __name__ == '__main__':
    app.run(debug=True)
