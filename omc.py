# omc.py
from flask import Blueprint, render_template, request, jsonify
from datetime import datetime
from bson import ObjectId
from db import db

# 📦 Collection
omc_col = db["bd_omc"]

# 🔹 Blueprint
omc_bp = Blueprint("omc", __name__, template_folder="templates")

# ---------- Helpers ----------
def _s(v): return (v or "").strip()

def _oid(s):
    try:
        return ObjectId(str(s))
    except Exception:
        return None

# 📄 List OMCs (no profile page)
@omc_bp.route("/omc", methods=["GET"])
def omc_list():
    omcs = list(omc_col.find().sort("name", 1))
    return render_template("partials/omc.html", omcs=omcs)

# ➕ Add OMC
@omc_bp.route("/omc/add", methods=["POST"])
def add_omc():
    try:
        data = request.get_json(force=True, silent=True) or {}
        name      = _s(data.get("name"))
        phone     = _s(data.get("phone"))
        location  = _s(data.get("location"))
        rep_name  = _s(data.get("rep_name"))
        rep_phone = _s(data.get("rep_phone"))

        if not all([name, phone, location, rep_name, rep_phone]):
            return jsonify({"status": "error", "message": "All fields are required."}), 400

        if omc_col.find_one({"name": name}):
            return jsonify({"status": "error", "message": "OMC already exists."}), 400

        omc_col.insert_one({
            "name": name,
            "phone": phone,
            "location": location,
            "rep_name": rep_name,
            "rep_phone": rep_phone,
            "date_created": datetime.utcnow()
        })
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ✏️ Update OMC
@omc_bp.route("/omc/update/<omc_id>", methods=["POST"])
def update_omc(omc_id):
    try:
        oid = _oid(omc_id)
        if not oid:
            return jsonify({"status": "error", "message": "Invalid OMC id."}), 400

        data = request.get_json(force=True, silent=True) or {}
        name      = _s(data.get("name"))
        phone     = _s(data.get("phone"))
        location  = _s(data.get("location"))
        rep_name  = _s(data.get("rep_name"))
        rep_phone = _s(data.get("rep_phone"))

        if not all([name, phone, location, rep_name, rep_phone]):
            return jsonify({"status": "error", "message": "All fields are required."}), 400

        # Prevent duplicate names (exclude current doc)
        if omc_col.find_one({"name": name, "_id": {"$ne": oid}}):
            return jsonify({"status": "error", "message": "Another OMC with this name already exists."}), 400

        res = omc_col.update_one(
            {"_id": oid},
            {"$set": {
                "name": name,
                "phone": phone,
                "location": location,
                "rep_name": rep_name,
                "rep_phone": rep_phone
            }}
        )
        if res.matched_count == 0:
            return jsonify({"status": "error", "message": "OMC not found."}), 404

        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
