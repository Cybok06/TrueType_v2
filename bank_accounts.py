from flask import Blueprint, render_template, request, jsonify
from bson import ObjectId
from db import db

bank_accounts_bp = Blueprint("bank_accounts", __name__, template_folder="templates")

accounts_col = db["bank_accounts"]

# ✅ View All Bank Accounts
@bank_accounts_bp.route("/bank-accounts", methods=["GET"])
def bank_accounts():
    accounts = list(accounts_col.find().sort("bank_name"))
    return render_template("partials/bank_accounts.html", accounts=accounts)

# ✅ Add New Account
@bank_accounts_bp.route("/bank-accounts/add", methods=["POST"])
def add_bank_account():
    data = request.form
    new_account = {
        "bank_name": data.get("bank_name"),
        "account_name": data.get("account_name"),
        "account_number": data.get("account_number"),
        "branch": data.get("branch")
    }
    accounts_col.insert_one(new_account)
    return jsonify({"success": True, "message": "Bank account added"})

# ✅ Edit Account
@bank_accounts_bp.route("/bank-accounts/edit/<id>", methods=["POST"])
def edit_bank_account(id):
    data = request.form
    update = {
        "bank_name": data.get("bank_name"),
        "account_name": data.get("account_name"),
        "account_number": data.get("account_number"),
        "branch": data.get("branch")
    }
    accounts_col.update_one({"_id": ObjectId(id)}, {"$set": update})
    return jsonify({"success": True, "message": "Bank account updated"})

# ✅ Delete Account
@bank_accounts_bp.route("/bank-accounts/delete/<id>", methods=["POST"])
def delete_bank_account(id):
    accounts_col.delete_one({"_id": ObjectId(id)})
    return jsonify({"success": True, "message": "Bank account deleted"})
