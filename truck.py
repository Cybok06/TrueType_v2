from flask import Blueprint, render_template, request, jsonify
from datetime import datetime
from bson import ObjectId
from db import db

truck_bp = Blueprint("truck_bp", __name__)

trucks_col = db["trucks"]
clients_col = db["clients"]
truck_orders_col = db["truck_orders"]
truck_expenses_col = db["truck_expenses"]

@truck_bp.route("/trucks", methods=["GET"])
def view_trucks():
    trucks = list(trucks_col.find().sort("created_at", -1))
    clients = list(clients_col.find({}, {"_id": 1, "name": 1, "phone": 1}))
    orders = list(truck_orders_col.find().sort("created_at", -1))
    return render_template("partials/truck_page.html", trucks=trucks, clients=clients, orders=orders)

@truck_bp.route("/trucks/add", methods=["POST"])
def add_truck():
    data = request.get_json()
    truck_number = data.get("truck_number", "").strip().upper()

    if trucks_col.find_one({"truck_number": truck_number}):
        return jsonify({"success": False, "message": "Truck already exists."}), 400

    truck = {
        "truck_number": truck_number,
        "product": data.get("product", ""),
        "capacity": data.get("capacity", ""),
        "driver_name": data.get("driver_name", ""),
        "driver_phone": data.get("driver_phone", ""),
        "created_at": datetime.utcnow()
    }

    trucks_col.insert_one(truck)
    return jsonify({"success": True})

@truck_bp.route("/trucks/initiate_order", methods=["POST"])
def initiate_truck_order():
    data = request.get_json()
    truck_id = data.get("truck_id")
    destination = data.get("destination")
    total_debt = data.get("total_debt")

    if not all([truck_id, destination, total_debt]):
        return jsonify({"success": False, "message": "Missing required fields."}), 400

    try:
        truck = trucks_col.find_one({"_id": ObjectId(truck_id)})
        if not truck:
            return jsonify({"success": False, "message": "Truck not found."}), 404
    except Exception:
        return jsonify({"success": False, "message": "Invalid truck ID."}), 400

    client_id = data.get("client_id")
    client_name = data.get("client_name")
    client_phone = data.get("client_phone")
    client = None

    if client_id:
        try:
            client = clients_col.find_one({"_id": ObjectId(client_id)})
            if not client:
                return jsonify({"success": False, "message": "Client not found."}), 404
        except Exception:
            return jsonify({"success": False, "message": "Invalid client ID."}), 400
    elif client_name and client_phone:
        new_client = {
            "name": client_name.strip(),
            "phone": client_phone.strip(),
            "status": "external",
            "created_at": datetime.utcnow()
        }
        inserted = clients_col.insert_one(new_client)
        new_client["_id"] = inserted.inserted_id
        client = new_client
    else:
        return jsonify({"success": False, "message": "Please select or enter a client."}), 400

    truck_order = {
        "truck_id": str(truck["_id"]),
        "truck_number": truck["truck_number"],
        "driver_name": truck["driver_name"],
        "driver_phone": truck["driver_phone"],
        "client_id": str(client["_id"]),
        "client_name": client.get("name", ""),
        "client_phone": client.get("phone", ""),
        "destination": destination,
        "total_debt": float(total_debt),
        "status": "pending",
        "created_at": datetime.utcnow()
    }

    truck_orders_col.insert_one(truck_order)
    return jsonify({"success": True})

@truck_bp.route("/truck_orders/start/<order_id>", methods=["POST"])
def start_truck_order(order_id):
    try:
        now = datetime.utcnow()
        result = truck_orders_col.update_one(
            {"_id": ObjectId(order_id)},
            {"$set": {"status": "enroute", "started_at": now}}
        )
        if result.matched_count == 0:
            return jsonify({"success": False, "message": "Order not found."}), 404
        return jsonify({"success": True, "status": "enroute", "started_at": now.strftime("%Y-%m-%d %H:%M:%S")})
    except Exception:
        return jsonify({"success": False, "message": "Invalid order ID."}), 400

@truck_bp.route("/truck_orders/complete/<order_id>", methods=["POST"])
def complete_truck_order(order_id):
    try:
        now = datetime.utcnow()
        result = truck_orders_col.update_one(
            {"_id": ObjectId(order_id)},
            {"$set": {"status": "Delivered", "delivered_at": now}}
        )
        if result.matched_count == 0:
            return jsonify({"success": False, "message": "Order not found."}), 404
        return jsonify({
            "success": True,
            "status": "Delivered",
            "delivered_at": now.strftime("%Y-%m-%d %H:%M:%S")
        })
    except Exception:
        return jsonify({"success": False, "message": "Invalid order ID."}), 400

@truck_bp.route("/truck_orders/add_expense/<order_id>", methods=["POST"])
def add_truck_order_expense(order_id):
    try:
        data = request.get_json()
        label = data.get("label", "").strip()
        amount = data.get("amount", "").strip()

        if not label or not amount:
            return jsonify({"success": False, "message": "Missing expense details."}), 400

        try:
            amount = float(amount)
        except ValueError:
            return jsonify({"success": False, "message": "Amount must be a valid number."}), 400

        order = truck_orders_col.find_one({"_id": ObjectId(order_id)})
        if not order:
            return jsonify({"success": False, "message": "Order not found."}), 404

        expense = {
            "order_id": str(order["_id"]),
            "truck_number": order.get("truck_number", ""),
            "label": label,
            "amount": amount,
            "created_at": datetime.utcnow()
        }

        # ✅ Save to truck_expenses collection
        truck_expenses_col.insert_one(expense)

        # ✅ (Optional) Save under the order itself
        truck_orders_col.update_one(
            {"_id": ObjectId(order_id)},
            {"$push": {"expenses": expense}}
        )

        return jsonify({
            "success": True,
            "expense": {
                "label": expense["label"],
                "amount": f"{expense['amount']:.2f}",
                "created_at": expense["created_at"].strftime("%Y-%m-%d %H:%M:%S")
            }
        })

    except Exception as e:
        return jsonify({"success": False, "message": f"Unexpected error: {str(e)}"}), 500
