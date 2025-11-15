from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from datetime import datetime
from bson import ObjectId, Regex
from db import db
import random, string, re
from pymongo.errors import DuplicateKeyError, OperationFailure

client_order_bp = Blueprint('client_order', __name__, template_folder='templates')

orders_collection        = db["orders"]
products_collection      = db["products"]
trucks_collection        = db["trucks"]
truck_orders_collection  = db["truck_orders"]
truck_numbers_collection = db["truck_numbers"]

# --------------------------
# Indexes (make idempotent)
# --------------------------
def ensure_indexes():
    try:
        orders_collection.create_index("order_id", unique=True, sparse=True, background=True)
    except Exception:
        pass

    # Truck numbers (per-client recent address book)
    try:
        info = truck_numbers_collection.index_information()
        for name, spec in info.items():
            keys = spec.get("key") or spec.get("key_pattern") or []
            if isinstance(keys, dict):
                keys = list(keys.items())
            is_only_norm = (len(keys) == 1 and keys[0][0] == "vehicle_number_norm" and keys[0][1] == 1)
            if is_only_norm and spec.get("unique"):
                try:
                    truck_numbers_collection.drop_index(name)
                except Exception:
                    pass

        if "vehicle_number_norm_unique" in info:
            try:
                truck_numbers_collection.drop_index("vehicle_number_norm_unique")
            except Exception:
                pass

        truck_numbers_collection.create_index(
            [("client_id", 1), ("vehicle_number_norm", 1)],
            name="client_vehicle_norm_unique",
            unique=True,
            background=True
        )
        truck_numbers_collection.create_index(
            [("vehicle_number_norm", 1)],
            name="vehicle_number_norm_idx",
            background=True
        )
    except OperationFailure:
        pass

    # Trucks: helpful lookups
    try:
        trucks_collection.create_index([("truck_number", 1)], name="truck_number_idx", background=True)
        trucks_collection.create_index([("truck_number_norm", 1)], name="truck_number_norm_idx", background=True)
    except Exception:
        pass


ensure_indexes()


def _to_int_qty(q):
    if not q:
        return None
    return int(str(q).replace(",", "").strip())


def _maybe_oid(val):
    try:
        return ObjectId(val)
    except Exception:
        return val


def _generate_order_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))


def _norm_plate(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", s).upper()


_VALID_ORDER_TYPES = {"s_tax", "s_bdc", "combo"}


@client_order_bp.route('/submit_order', methods=['GET', 'POST'])
def submit_order():
    if 'client_id' not in session:
        flash("Please log in to place an order", "danger")
        return redirect(url_for('client_login'))

    if request.method == 'POST':
        product        = request.form.get('product')
        quantity       = _to_int_qty(request.form.get('quantity'))
        region         = request.form.get('region')
        vehicle_number = (request.form.get('vehicle_number') or "").strip()
        driver_name    = (request.form.get('driver_name') or "").strip()
        driver_phone   = (request.form.get('driver_phone') or "").strip()
        order_type     = (request.form.get('order_type') or '').strip().lower()

        # Hire Truck selection comes as the truck's _id (as string)
        truck_id_raw   = (request.form.get('truck_id') or "").strip()
        is_hire_truck  = bool(truck_id_raw)

        # Basic validation
        if not all([product, quantity, region, vehicle_number, driver_name, driver_phone, order_type]):
            flash("All fields are required.", "danger")
            return redirect(url_for('client_order.submit_order'))

        if order_type not in _VALID_ORDER_TYPES:
            flash("Invalid order type selected.", "danger")
            return redirect(url_for('client_order.submit_order'))

        # ---------- Resolve HIRE TRUCK only when explicitly selected ----------
        truck = None
        if is_hire_truck:
            tid = _maybe_oid(truck_id_raw)
            if isinstance(tid, ObjectId):
                truck = trucks_collection.find_one({"_id": tid})
            if truck is None:
                is_hire_truck = False
                flash("Selected hire truck could not be verified. Order recorded without a hired truck.", "warning")

        # Snapshot product pricing/taxes
        prod_doc = products_collection.find_one(
            {"name": Regex(f"^{re.escape(product)}$", "i")},
            {"s_price": 1, "p_price": 1, "s_tax": 1, "p_tax": 1, "name": 1}
        )
        snapshot_s_price = (prod_doc or {}).get("s_price")
        snapshot_p_price = (prod_doc or {}).get("p_price")
        snapshot_s_tax   = (prod_doc or {}).get("s_tax")
        snapshot_p_tax   = (prod_doc or {}).get("p_tax")

        client_oid = _maybe_oid(session['client_id'])
        vehicle_number_norm = _norm_plate(vehicle_number)

        # Base order
        base_order = {
            "client_id": client_oid,
            "product": product,
            "vehicle_number": vehicle_number,
            "driver_name": driver_name,
            "driver_phone": driver_phone,
            "quantity": quantity,
            "region": region,
            "status": "pending",
            "date": datetime.utcnow(),
            "order_type": order_type,
            "product_s_price": snapshot_s_price,
            "product_p_price": snapshot_p_price,
            "product_s_tax":   snapshot_s_tax,
            "product_p_tax":   snapshot_p_tax,
            "hired_truck": bool(is_hire_truck),
            "truck_verified": bool(truck),
            "truck_number_norm": vehicle_number_norm
        }
        if truck:
            base_order["truck_id"] = truck["_id"]

        # Insert order with collision-proof order_id
        while True:
            code = _generate_order_id()
            doc = dict(base_order)
            doc["order_id"] = code
            try:
                result = orders_collection.insert_one(doc)
                order_mongo_id = result.inserted_id
                break
            except DuplicateKeyError:
                continue

        # Create truck_orders ONLY when hire truck was selected and verified
        if truck:
            trk_driver_name  = (truck.get("driver_name") or driver_name or "").strip()
            trk_driver_phone = (truck.get("driver_phone") or driver_phone or "").strip()

            truck_orders_collection.insert_one({
                "order_ref": str(order_mongo_id),
                "order_id": code,
                "client_id": str(client_oid) if isinstance(client_oid, ObjectId) else client_oid,
                "truck_id": str(truck["_id"]),
                "truck_number": truck.get("truck_number") or vehicle_number,
                "driver_name": trk_driver_name,
                "driver_phone": trk_driver_phone,
                "quantity": quantity,
                "region": region,
                "status": "pending",
                "created_at": datetime.utcnow()
            })
            flash(f"Order submitted successfully with a hired truck. Your Order ID is {code}", "success")
        else:
            flash(f"Order submitted successfully. Your Order ID is {code}", "info")

        # Upsert into per-client recent trucks (address book)
        flt = {"client_id": client_oid, "vehicle_number_norm": vehicle_number_norm}
        upsert_doc = {
            "client_id": client_oid,
            "vehicle_number": vehicle_number,
            "vehicle_number_norm": vehicle_number_norm,
            "destination": region,
            "driver_name": driver_name,
            "driver_phone": driver_phone,
            "updated_at": datetime.utcnow(),
        }

        for _ in range(2):
            try:
                truck_numbers_collection.update_one(
                    flt,
                    {"$set": upsert_doc, "$setOnInsert": {"created_at": datetime.utcnow()}},
                    upsert=True
                )
                break
            except DuplicateKeyError:
                truck_numbers_collection.update_one(flt, {"$set": upsert_doc}, upsert=False)
                break

        return redirect(url_for('client_order.submit_order'))

    # ---------- GET: include per-client RECENT TRUCKS + all trucks ----------
    products = list(products_collection.find({}, {"name": 1, "description": 1}))
    cid = _maybe_oid(session['client_id'])

    # Normalize trucks: convert _id to string and only expose clean values
    trucks = []
    for t in trucks_collection.find({}, {"truck_number": 1, "capacity": 1, "driver_name": 1, "driver_phone": 1}):
        trucks.append({
            "id": str(t.get("_id")),
            "truck_number": t.get("truck_number", ""),
            "capacity": t.get("capacity", ""),
            "driver_name": t.get("driver_name", ""),
            "driver_phone": t.get("driver_phone", "")
        })

    # Recent trucks
    recents_cursor = truck_numbers_collection.find(
        {"client_id": cid},
        {
            "_id": 0,
            "vehicle_number": 1,
            "destination": 1,
            "driver_name": 1,
            "driver_phone": 1,
            "vehicle_number_norm": 1,
            "updated_at": 1
        }
    ).sort("updated_at", -1).limit(100)

    seen = set()
    recent_trucks = []
    for doc in recents_cursor:
        norm = doc.get("vehicle_number_norm")
        if norm in seen:
            continue
        seen.add(norm)
        recent_trucks.append({
            "vehicle_number": doc.get("vehicle_number", ""),
            "destination": doc.get("destination", ""),
            "driver_name": doc.get("driver_name", ""),
            "driver_phone": doc.get("driver_phone", "")
        })
        if len(recent_trucks) >= 20:
            break

    return render_template(
        'client/client_order.html',
        products=products,
        trucks=trucks,
        recent_trucks=recent_trucks
    )
