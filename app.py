from flask import Flask, redirect, url_for, session, render_template
import os

# === Auth/Login ===
from login import login_bp

# === Shared Features ===
from register_client import register_client_bp
from clientlist import clientlist_bp
from client_profile import client_profile_bp
from approved_orders import approved_orders_bp
from orders import orders_bp
from payments import payments_bp
from debtors import debtors_bp
from bank_profile import bank_profile_bp
from bdc import bdc_bp
from home import home_bp
from shareholders import shareholders_bp
from tax import tax_bp
from manage_deliveries import manage_deliveries_bp
from products import products_bp
from bank_accounts import bank_accounts_bp  # legacy/general bank accounts (non-accounting)
from truck import truck_bp
from truck_debtors import truck_debtors_bp
from admin_truck_payments import admin_truck_payments_bp
from share_links import shared_bp
from omc import omc_bp
from order_cancellation import cancel_bp
from navbar import navbar_bp
from users import users_bp
from taxes import taxes_bp
from prices_bp import prices_bp
from frontdesk.leaves import leaves_bp
from admin_reports import admin_reports_bp


# === Accounting (separate blueprints) ===
from accounting_routes.accounts import accounting_bp               # Chart of Accounts, etc.
from accounting_routes.journals import journals_bp                 # Journal list/new/create
from accounting_routes.ledger import ledger_bp                     # General Ledger
from accounting_routes.customers import customers_bp               # AR Customers
from accounting_routes.ar_invoices import ar_invoices_bp as ar_invoices
from accounting_routes.ar_payments import ar_payments_bp as ar_payments
from accounting_routes.ar_aging import ar_aging_bp                 # AR Aging
from accounting_routes.ap_bills import ap_bills_bp as ap_bills     # AP Bills
from accounting_routes.bank_accounts import bank_accounts_bp as acc_bank_accounts_bp  # Accounting Bank Accounts
from accounting_routes.bank_recon import bank_recon_bp             # Bank Reconciliation
from accounting_routes.fixed_assets import fixed_assets_bp         # Fixed Assets Register
from accounting_routes.payroll_calculator import acc_payroll_calc
from accounting_routes.expenses import acc_expenses
from accounting_routes.balance_sheet import acc_balance_sheet
from accounting_routes.dashboard import acc_dashboard
from accounting_routes.profile import acc_profile
from accounting_routes.payment_voucher import payment_voucher_bp

# === Admin Features ===
from admin.admin_dashboard import admin_dashboard_bp
from admin.settings import admin_settings_bp

# === Assistant / Reports ===
from reports import reports_bp

# === Client Features ===
from client.client_dashboard import client_dashboard_bp
from client.client_order import client_order_bp
from client.client_order_history import client_order_history_bp
from client.client_payment import client_payment_bp
from login_logs import login_logs_bp
from taxes_history import taxes_hist_bp

# === Frontdesk (Alice) ===
from frontdesk.frontdesk_dashboard import frontdesk_dashboard_bp
from frontdesk.meetings import meetings_bp
from frontdesk.admin_meetings import admin_meetings_bp
from frontdesk.hris_employees import hris_employees_bp
from frontdesk.documents import documents_bp
from frontdesk.tasks import tasks_bp
from frontdesk.frontdesk_navbar import frontdesk_navbar_bp

# === Initialize App ===
app = Flask(__name__)
app.secret_key = os.getenv(
    "FLASK_SECRET_KEY",
    "4b1b26eee81fd7da3be8efd2649c3b07140b511118b11009f243adabd4d61559"
)

# === Root / Index ===
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/index")
def index_alias():
    return render_template("index.html")


# === Blueprint Registration ===

# Frontdesk
app.register_blueprint(frontdesk_dashboard_bp)
app.register_blueprint(frontdesk_navbar_bp)
app.register_blueprint(tasks_bp)
app.register_blueprint(meetings_bp)
app.register_blueprint(admin_meetings_bp)
app.register_blueprint(hris_employees_bp)
app.register_blueprint(documents_bp)
app.register_blueprint(leaves_bp)

# Auth/Login — mount on a prefix to avoid capturing '/'
app.register_blueprint(login_bp, url_prefix="/auth")

# Shared Features
app.register_blueprint(home_bp)
app.register_blueprint(register_client_bp)
app.register_blueprint(clientlist_bp)
app.register_blueprint(client_profile_bp)
app.register_blueprint(approved_orders_bp)
app.register_blueprint(orders_bp, url_prefix="/orders")
app.register_blueprint(payments_bp)
app.register_blueprint(debtors_bp)
app.register_blueprint(bdc_bp)
app.register_blueprint(shareholders_bp)
app.register_blueprint(tax_bp)
app.register_blueprint(manage_deliveries_bp)
app.register_blueprint(products_bp)
app.register_blueprint(truck_bp)
app.register_blueprint(truck_debtors_bp)
app.register_blueprint(admin_truck_payments_bp)
app.register_blueprint(bank_profile_bp)  # general bank profile screens
app.register_blueprint(reports_bp)
app.register_blueprint(shared_bp)
app.register_blueprint(omc_bp)
app.register_blueprint(cancel_bp)   # routes like /orders/cancel/...
app.register_blueprint(navbar_bp)
app.register_blueprint(users_bp)
app.register_blueprint(login_logs_bp)
app.register_blueprint(taxes_bp)
app.register_blueprint(taxes_hist_bp)
app.register_blueprint(prices_bp)
app.register_blueprint(bank_accounts_bp)  # general/legacy bank accounts routes (non-accounting)
app.register_blueprint(admin_reports_bp)

# Accounting (all under /accounting)
app.register_blueprint(ledger_bp,        url_prefix="/accounting")             # /accounting/ledger
app.register_blueprint(customers_bp,     url_prefix="/accounting")             # /accounting/customers
app.register_blueprint(ar_invoices,      url_prefix="/accounting")             # /accounting/ar/invoices
app.register_blueprint(ar_payments,      url_prefix="/accounting")             # /accounting/ar/payments
app.register_blueprint(ar_aging_bp,      url_prefix="/accounting")             # /accounting/ar/aging
app.register_blueprint(ap_bills,         url_prefix="/accounting")             # /accounting/ap/bills
app.register_blueprint(acc_payroll_calc, url_prefix="/accounting")
app.register_blueprint(
    acc_balance_sheet,
    url_prefix="/accounting",
    name="acc_balance_sheet"
)
# Accounting bank accounts – unique blueprint name at registration
app.register_blueprint(
    acc_bank_accounts_bp,
    url_prefix="/accounting",
    name="acc_bank_accounts"
)  # /accounting/bank-accounts
# Accounting dashboard (under /accounting)
app.register_blueprint(
    acc_dashboard,
    url_prefix="/accounting",  # => /accounting/dashboard
)
app.register_blueprint(bank_recon_bp,    url_prefix="/accounting")             # /accounting/bank-recon/<id>
app.register_blueprint(accounting_bp,    url_prefix="/accounting")             # /accounting/accounts, etc.
app.register_blueprint(journals_bp,      url_prefix="/accounting")             # /accounting/journals, ...
app.register_blueprint(acc_profile)
app.register_blueprint(payment_voucher_bp, url_prefix="/accounting/payment-vouchers")

# ✅ Fixed Assets mounted under /accounting/fixed-assets
app.register_blueprint(
    fixed_assets_bp,
    url_prefix="/accounting/fixed-assets"
)  # /accounting/fixed-assets/

# Admin
app.register_blueprint(admin_dashboard_bp, url_prefix="/admin")
app.register_blueprint(admin_settings_bp)

# Client
app.register_blueprint(client_dashboard_bp,      url_prefix="/client")
app.register_blueprint(client_order_bp,          url_prefix="/client")
app.register_blueprint(client_order_history_bp,  url_prefix="/client")
app.register_blueprint(client_payment_bp,        url_prefix="/client")
app.register_blueprint(acc_expenses)


# === Login shortcuts (optional) ===
@app.route("/login")
def login_shortcut():
    return redirect(url_for("login.login"))


# === Logout Route ===
@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login.login"))


# === Run App ===
if __name__ == "__main__":
    app.run(debug=True)
