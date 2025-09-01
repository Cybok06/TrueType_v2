
from flask import Blueprint, render_template, request, jsonify
from bson import ObjectId
from datetime import datetime
from db import db
import re

products_bp = Blueprint("products", __name__, template_folder="templates")
products_collection = db["products"]
clients_collection  = db["clients"]

_GH_DEFAULT_CC = "233"  # Ghana

# -------------------- helpers --------------------
def _digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def _normalize_msisdn(raw: str, default_cc: str = _GH_DEFAULT_CC) -> str | None:
    if not raw:
        return None
    d = _digits_only(raw)
    if not d:
        return None
    if d.startswith(default_cc):
        return d
    if d.startswith("0") and len(d) >= 10:
        return default_cc + d[1:]
    if len(d) in (9,):
        return default_cc + d
    if len(d) >= 11 and not d.startswith("0"):
        return d
    return None

def _to_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default

# 🔍 Render the Products Page
@products_bp.route("/products", methods=["GET"])
def manage_products():
    return render_template("partials/products.html")

# 📥 Load Products via AJAX
@products_bp.route("/products/load", methods=["GET"])
def load_products():
    products = list(products_collection.find().sort("date_added", -1))
    out = []
    for p in products:
        # normalize basic fields
        s_price = _to_float(p.get("s_price"))
        p_price = _to_float(p.get("p_price"))
        s_tax   = _to_float(p.get("s_tax"))
        p_tax   = _to_float(p.get("p_tax"))

        # build history with dates
        formatted_history = []
        for entry in p.get("price_history", []):
            ts = entry.get("timestamp") or datetime.utcnow()
            if not isinstance(ts, datetime):
                try:
                    ts = datetime.fromisoformat(str(ts))
                except Exception:
                    ts = datetime.utcnow()
            formatted_history.append({
                "s_price": _to_float(entry.get("s_price")),
                "p_price": _to_float(entry.get("p_price")),
                "s_tax":   _to_float(entry.get("s_tax")),
                "p_tax":   _to_float(entry.get("p_tax")),
                "date": ts.strftime("%Y-%m-%d")
            })

        out.append({
            "_id": str(p["_id"]),
            "name": p.get("name") or "",
            "description": p.get("description") or "",
            "s_price": s_price,
            "p_price": p_price,
            "s_tax": s_tax,
            "p_tax": p_tax,
            "date_added": (p.get("date_added") or datetime.utcnow()).strftime("%Y-%m-%d"),
            "price_history": formatted_history
        })
    return jsonify(out)

# ➕ Add Product with Price+Tax History
@products_bp.route("/products/add", methods=["POST"])
def add_product():
    data = request.get_json(force=True, silent=True) or {}
    name = (data.get("name") or "").strip()
    description = (data.get("description") or "").strip()

    if not name:
        return jsonify({"success": False, "message": "Product name is required."}), 400

    s_price = _to_float(data.get("s_price"))
    p_price = _to_float(data.get("p_price"))
    s_tax   = _to_float(data.get("s_tax"))
    p_tax   = _to_float(data.get("p_tax"))

    now = datetime.utcnow()
    product = {
        "name": name,
        "description": description,
        "s_price": s_price,
        "p_price": p_price,
        "s_tax": s_tax,
        "p_tax": p_tax,
        "date_added": now,
        "price_history": [{
            "s_price": s_price,
            "p_price": p_price,
            "s_tax": s_tax,
            "p_tax": p_tax,
            "timestamp": now
        }]
    }

    result = products_collection.insert_one(product)
    return jsonify({"success": True, "product": {"_id": str(result.inserted_id)}})

# ✏️ Update Product and Append to Price+Tax History
@products_bp.route("/products/update/<product_id>", methods=["POST"])
def update_product(product_id):
    try:
        oid = ObjectId(product_id)
    except Exception:
        return jsonify({"success": False, "message": "Invalid product id."}), 400

    data = request.get_json(force=True, silent=True) or {}
    name = (data.get("name") or "").strip()
    description = (data.get("description") or "").strip()

    s_price = _to_float(data.get("s_price"))
    p_price = _to_float(data.get("p_price"))
    s_tax   = _to_float(data.get("s_tax"))
    p_tax   = _to_float(data.get("p_tax"))

    now = datetime.utcnow()
    update_fields = {
        "name": name,
        "description": description,
        "s_price": s_price,
        "p_price": p_price,
        "s_tax": s_tax,
        "p_tax": p_tax
    }

    result = products_collection.update_one(
        {"_id": oid},
        {
            "$set": update_fields,
            "$push": {
                "price_history": {
                    "s_price": s_price,
                    "p_price": p_price,
                    "s_tax": s_tax,
                    "p_tax": p_tax,
                    "timestamp": now
                }
            }
        }
    )
    return jsonify({"success": result.modified_count == 1})

# ❌ Delete Product
@products_bp.route("/products/delete/<product_id>", methods=["DELETE"])
def delete_product(product_id):
    try:
        oid = ObjectId(product_id)
    except Exception:
        return jsonify({"success": False, "message": "Invalid product id."}), 400
    result = products_collection.delete_one({"_id": oid})
    return jsonify({"success": result.deleted_count == 1})

# -------- Clients for sharing (unchanged) --------
@products_bp.route("/products/clients", methods=["GET"])
def list_clients_for_share():
    docs = list(clients_collection.find({}, {
        "name": 1, "client_id": 1, "phone": 1, "phone_number": 1, "whatsapp": 1, "mobile": 1, "phones": 1
    }).sort("name", 1))

    results = []
    for d in docs:
        cid = str(d.get("_id"))
        name = d.get("name") or d.get("client_id") or cid

        raw_list = []
        for key in ("phone", "phone_number", "whatsapp", "mobile"):
            v = d.get(key)
            if isinstance(v, str) and v.strip():
                raw_list.append(v.strip())
        if isinstance(d.get("phones"), list):
            for v in d["phones"]:
                if isinstance(v, str) and v.strip():
                    raw_list.append(v.strip())

        wa_numbers = []
        for raw in raw_list:
            n = _normalize_msisdn(raw)
            if n and n not in wa_numbers:
                wa_numbers.append(n)

        if wa_numbers:
            results.append({
                "_id": cid,
                "name": name,
                "phones": raw_list,
                "wa_numbers": wa_numbers,
                "primary_wa": wa_numbers[0]
            })
    return jsonify(results)

# -------- Default share message (kept simple) --------
@products_bp.route("/products/share/default_message", methods=["GET"])
def default_share_message():
    product_id = request.args.get("product_id")
    if not product_id:
        return jsonify({"success": False, "message": "product_id is required"}), 400
    try:
        p = products_collection.find_one({"_id": ObjectId(product_id)}, {"name":1, "s_price":1, "p_price":1})
    except Exception:
        p = None
    if not p:
        return jsonify({"success": False, "message": "Product not found"}), 404

    name = p.get("name") or "Product"
    s_price = _to_float(p.get("s_price"))
    p_price = _to_float(p.get("p_price"))
    msg = f"Price update — {name}\nS-Price: {s_price:.2f}"
    return jsonify({"success": True, "message": msg})

# -------- Build WhatsApp share links (unchanged) --------
@products_bp.route("/products/share/build", methods=["POST"])
def build_share_links():
    data = request.get_json(force=True, silent=True) or {}
    product_id = data.get("product_id")
    custom_msg = (data.get("message") or "").strip()
    client_ids = data.get("client_ids") or []

    if not product_id or not client_ids:
        return jsonify({"success": False, "message": "product_id and client_ids are required"}), 400

    try:
        p = products_collection.find_one({"_id": ObjectId(product_id)}, {"name":1, "s_price":1})
    except Exception:
        p = None
    if not p:
        return jsonify({"success": False, "message": "Product not found"}), 404

    if not custom_msg:
        name = p.get("name") or "Product"
        s_price = _to_float(p.get("s_price"))
        custom_msg = f"Price update — {name}\nS-Price: {s_price:.2f}"

    oids = []
    for cid in client_ids:
        try:
            oids.append(ObjectId(cid))
        except Exception:
            continue
    if not oids:
        return jsonify({"success": False, "message": "No valid client ids."}), 400

    docs = list(clients_collection.find({"_id": {"$in": oids}}, {
        "name":1, "phone":1, "phone_number":1, "whatsapp":1, "mobile":1, "phones":1
    }))

    links = []
    for d in docs:
        cid = str(d["_id"])
        name = d.get("name") or cid

        raw_list = []
        for key in ("phone", "phone_number", "whatsapp", "mobile"):
            v = d.get(key)
            if isinstance(v, str) and v.strip():
                raw_list.append(v.strip())
        if isinstance(d.get("phones"), list):
            for v in d["phones"]:
                if isinstance(v, str) and v.strip():
                    raw_list.append(v.strip())

        wa_number = None
        for raw in raw_list:
            n = _normalize_msisdn(raw)
            if n:
                wa_number = n
                break
        if not wa_number:
            continue

        text = custom_msg.replace("&", "%26").replace("#", "%23").replace("+", "%2B")
        url = f"https://wa.me/{wa_number}?text={text}"
        links.append({"client_id": cid, "name": name, "number": wa_number, "url": url})

    return jsonify({"success": True, "links": links, "message": custom_msg})

