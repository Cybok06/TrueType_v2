# prices_bp.py
from flask import Blueprint, render_template, request, jsonify, url_for, redirect, flash
from bson import ObjectId
from datetime import datetime
import secrets
from urllib.parse import quote

from db import db

prices_bp = Blueprint("prices_bp", __name__, template_folder="templates")

# --- Collections ---
bdc_collection            = db["bdc"]                   # existing: { _id, name, rep_phone? }
bdc_prices_collection     = db["bdc_prices"]            # price history (logs)
bdc_current_prices        = db["bdc_current_prices"]    # one doc per (bdc_id, product) = current price
bdc_share_tokens          = db["bdc_share_tokens"]      # shareable links for posting

# --- Indexes (safe to re-run on startup) ---
def _ensure_indexes():
    try:
        # history lookup & time-series
        bdc_prices_collection.create_index(
            [("bdc_id", 1), ("product", 1), ("created_at", -1)],
            name="by_bdc_product_time"
        )
        # current uniqueness
        bdc_current_prices.create_index(
            [("bdc_id", 1), ("product", 1)],
            unique=True,
            name="uniq_current_bdc_product"
        )
        # for high/low lists
        bdc_current_prices.create_index(
            [("product", 1), ("price", -1)],
            name="by_product_price_desc"
        )
        # OPTIONAL quality-of-life: if you later query by location server-side
        # bdc_current_prices.create_index([("product", 1), ("location", 1)], name="by_product_location")

        # share tokens
        bdc_share_tokens.create_index("token", unique=True, name="uniq_token")
        bdc_share_tokens.create_index([("bdc_id", 1), ("active", 1)], name="by_bdc_active")
    except Exception:
        pass

_ensure_indexes()

# ---------- Helpers ----------
PRODUCTS = ["PMS", "AGO"]

def _now():
    return datetime.utcnow()

def _valid_token(token: str):
    if not token:
        return None
    return bdc_share_tokens.find_one({"token": token, "active": True})

def _iso_z(dt):
    try:
        return dt.isoformat() + "Z"
    except Exception:
        return None

def _pct_change(new_val, old_val):
    try:
        if old_val is None or old_val == 0:
            return None
        return round(((float(new_val) - float(old_val)) / float(old_val)) * 100.0, 4)
    except Exception:
        return None

def _clean_text(s: str, limit: int = 180):
    """Trim, collapse whitespace, and cap length; return None if empty after cleaning."""
    if not s:
        return None
    s = " ".join(str(s).split())[:limit].strip()
    return s or None

# ---------- Pages ----------
@prices_bp.get("/prices")
def prices_board():
    """
    Board page:
    - Public: /prices
    - Shared view (for a BDC): /prices?token=<share_token>
      In shared view, the template gets shared_view=True and can HIDE the "Share" button.
    """
    token = (request.args.get("token") or "").strip()
    shared_view = False
    shared_bdc = None

    if token:
        tdoc = _valid_token(token)
        if tdoc:
            shared_view = True
            shared_bdc = bdc_collection.find_one({"_id": tdoc["bdc_id"]}, {"name": 1})

    # Keep full list here to allow admins to generate share links to *any* BDC.
    bdcs = list(bdc_collection.find({}, {"name": 1, "rep_phone": 1}).sort("name", 1))
    return render_template(
        "prices/board.html",
        bdcs=bdcs,                           # for Share modal only
        products=PRODUCTS,
        shared_view=shared_view,
        shared_bdc_name=(shared_bdc or {}).get("name") if shared_bdc else None,
        token=token if shared_view else None,
    )

@prices_bp.post("/prices/share_link")
def prices_share_link():
    """
    Generate or reuse a share URL bound to a specific BDC (no login flow).
    SECURITY: If this call is made from a shared view (i.e., URL has a valid token),
    we DENY it. BDCs cannot re-share or create links.
    """
    # Deny sharing if a valid shared token is present in query/form
    shared_token = (request.args.get("token") or request.form.get("token") or "").strip()
    if shared_token and _valid_token(shared_token):
        return jsonify({"ok": False, "error": "Sharing is disabled on shared links."}), 403

    bdc_id = request.form.get("bdc_id")
    if not bdc_id:
        return jsonify({"ok": False, "error": "Missing bdc_id"}), 400
    try:
        oid = ObjectId(bdc_id)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid bdc_id"}), 400

    bdc = bdc_collection.find_one({"_id": oid}, {"name": 1, "rep_phone": 1})
    if not bdc:
        return jsonify({"ok": False, "error": "BDC not found"}), 404

    existing = bdc_share_tokens.find_one({"bdc_id": oid, "active": True})
    if existing:
        token = existing["token"]
    else:
        token = secrets.token_urlsafe(24)
        bdc_share_tokens.insert_one({
            "bdc_id": oid,
            "token": token,
            "active": True,
            "created_at": _now(),
            "last_used_at": None
        })

    share_url = url_for("prices_bp.price_post_page", token=token, _external=True)
    msg = f"Hello, please post your latest PMS/AGO price here (include your location & note): {share_url}"
    wa_url = "https://wa.me/?text=" + quote(msg, safe="")

    return jsonify({
        "ok": True,
        "bdc": {"_id": str(bdc["_id"]), "name": bdc.get("name")},
        "share_url": share_url,
        "wa_url": wa_url
    })

@prices_bp.get("/prices/post")
def price_post_page():
    """Minimal page a BDC opens from a shared link to submit a price."""
    token = (request.args.get("token") or "").strip()
    if not token:
        flash("Missing token.", "danger")
        return redirect(url_for("prices_bp.prices_board"))

    t = _valid_token(token)
    if not t:
        flash("Invalid or inactive link.", "danger")
        return redirect(url_for("prices_bp.prices_board"))

    bdc = bdc_collection.find_one({"_id": t["bdc_id"]}, {"name": 1})
    name = bdc.get("name") if bdc else "BDC"
    return render_template("prices/post.html", token=token, bdc_name=name, products=PRODUCTS)

@prices_bp.post("/prices/post")
def price_post_submit():
    """
    Save a new price (log) AND update the 'current' price for (bdc_id, product).
    The newest post *overcomes* the previous current while logs are preserved.
    Also stores change stats against the previous current.
    """
    token = (request.form.get("token") or "").strip()
    product = (request.form.get("product") or "").strip().upper()
    try:
        price = float(request.form.get("price"))
    except Exception:
        price = None

    # NEW: optional fields
    raw_location = request.form.get("location")
    raw_desc = request.form.get("description")
    location = _clean_text(raw_location, limit=120)      # e.g., "Tema Depot", "Takoradi"
    description = _clean_text(raw_desc, limit=300)       # short note

    if not token:
        return jsonify({"ok": False, "error": "Missing token"}), 400
    if product not in PRODUCTS:
        return jsonify({"ok": False, "error": "Product must be PMS or AGO"}), 400
    if price is None or price <= 0:
        return jsonify({"ok": False, "error": "Enter a valid price"}), 400
    # Practical sanity bounds (adjust as needed)
    if price < 1 or price > 100:
        return jsonify({"ok": False, "error": "That price looks off. Try again."}), 422

    t = _valid_token(token)
    if not t:
        return jsonify({"ok": False, "error": "Invalid or inactive link"}), 403

    now = _now()
    # 1) Save history (log)
    doc = {
        "bdc_id": t["bdc_id"],
        "product": product,
        "price": round(price, 4),
        "created_at": now,
        "source_ip": request.headers.get("X-Forwarded-For") or request.remote_addr,
        "user_agent": request.headers.get("User-Agent"),
        # NEW: store context on each log
        "location": location,
        "description": description,
    }
    bdc_prices_collection.insert_one(doc)

    # 2) Update "current" with change stats (overcome previous)
    prev = bdc_current_prices.find_one({"bdc_id": t["bdc_id"], "product": product})
    prev_price = float(prev["price"]) if prev and ("price" in prev) else None
    change_abs = round(price - prev_price, 4) if prev_price is not None else None
    change_pct = _pct_change(price, prev_price)

    # Keep latest location/description at "current" level for quick display/filtering
    bdc_current_prices.update_one(
        {"bdc_id": t["bdc_id"], "product": product},
        {"$set": {
            "bdc_id": t["bdc_id"],
            "product": product,
            "price": round(price, 4),
            "updated_at": now,
            "prev_price": prev_price,
            "change_abs": change_abs,
            "change_pct": change_pct,
            "location": location,
            "description": description,
        }},
        upsert=True
    )

    bdc_share_tokens.update_one({"_id": t["_id"]}, {"$set": {"last_used_at": now}})

    return jsonify({
        "ok": True,
        "message": "Price submitted",
        "product": product,
        "price": round(price, 4),
        "location": location,
        "description": description
    })

# ---------- APIs for the board (FAST + FILTERED + RECENT3/CHANGE) ----------

@prices_bp.get("/prices/api/board_data")
def api_board_data():
    """
    ONE-SHOT endpoint for the trading UI:
    Returns CURRENT price (from bdc_current_prices), recent history logs, and
    derived stats (last three prices before current, and delta % vs previous).

    Query:
      - history_points: int (default 40; min 5, max 200)  -> still used for sparklines
      - products: csv among PMS,AGO (default both)
      - token: optional (to indicate shared view)
    Response:
      {
        ok: true,
        shared_view: bool,
        top_lists: { PMS: [{_id, name, price, time, change_pct}], AGO: [...] },
        bdcs: [
          {
            _id, name,
            prices: {
              PMS: {
                current: { price, time, location?, description? },
                recent3: [ {price,time}, ... up to 3 ],
                change: { abs, pct, dir },
                series: [{t,y}, ...]
              },
              AGO: { ... }
            }
          }, ...
        ]
      }
    """
    token = (request.args.get("token") or "").strip()
    shared_view = bool(_valid_token(token)) if token else False

    # history window bounds
    try:
        limit = int(request.args.get("history_points", 40))
        if limit < 5:
            limit = 5
        if limit > 200:
            limit = 200
    except Exception:
        limit = 40

    # select products
    prods_raw = (request.args.get("products") or "").strip().upper()
    if prods_raw:
        prods = [p for p in [s.strip() for s in prods_raw.split(",")] if p in PRODUCTS]
        if not prods:
            prods = PRODUCTS[:]
    else:
        prods = PRODUCTS[:]

    # -------- CURRENT (only BDCs that have a current record for selected products) --------
    cur_docs = list(bdc_current_prices.find(
        {"product": {"$in": prods}},
        {
            "bdc_id": 1, "product": 1, "price": 1, "updated_at": 1,
            "change_abs": 1, "change_pct": 1,
            # NEW
            "location": 1, "description": 1
        }
    ))

    if not cur_docs:
        return jsonify({"ok": True, "shared_view": shared_view, "top_lists": {}, "bdcs": []})

    bdc_ids = sorted(list({d["bdc_id"] for d in cur_docs}))
    # map current {(bdc_id, product) -> current payload}
    current_map = {
        (d["bdc_id"], d["product"]): {
            "price": float(d.get("price", 0.0)),
            "time": _iso_z(d.get("updated_at")),
            "change_abs": d.get("change_abs", None),
            "change_pct": d.get("change_pct", None),
            # NEW
            "location": d.get("location"),
            "description": d.get("description"),
        }
        for d in cur_docs
    }

    # Fetch BDC names for those that have current
    bdc_docs = list(bdc_collection.find({"_id": {"$in": bdc_ids}}, {"name": 1}))
    bdc_name = {d["_id"]: (d.get("name") or "BDC") for d in bdc_docs}

    # -------- SERIES (batched) for sparklines and recent3 calc --------
    series_pipe = [
        {"$match": {"product": {"$in": prods}, "bdc_id": {"$in": bdc_ids}}},
        {"$sort": {"bdc_id": 1, "product": 1, "created_at": -1}},
        {"$group": {
            "_id": {"bdc_id": "$bdc_id", "product": "$product"},
            "points": {"$push": {"t": "$created_at", "y": "$price"}}
        }},
        {"$project": {"points": {"$slice": ["$points", max(limit, 4)]}}}  # keep at least 4 for recent3+current
    ]
    series_rows = list(bdc_prices_collection.aggregate(series_pipe))

    series_map = {}
    for r in series_rows:
        key = (r["_id"]["bdc_id"], r["_id"]["product"])
        pts = r.get("points") or []
        # reverse to chronological + cast
        pts = list(reversed([{"t": _iso_z(p["t"]), "y": float(p["y"])} for p in pts if p.get("t")]))
        series_map[key] = pts

    # -------- Assemble bdcs array (only those with any selected product 'current') --------
    out_bdcs = []
    for bid in bdc_ids:
        row = {"_id": str(bid), "name": bdc_name.get(bid, "BDC"), "prices": {}}
        any_prod = False
        for prod in prods:
            cur_payload = current_map.get((bid, prod))
            if not cur_payload:
                # skip products without current for this bdc
                continue

            any_prod = True
            pts = series_map.get((bid, prod), [])

            # recent4: up to last four chronological points (includes current as the last)
            if pts:
                recent4 = pts[-4:]
                current_point = recent4[-1]
                prev_point = recent4[-2] if len(recent4) >= 2 else None
                # last three BEFORE current:
                recent3 = recent4[:-1][-3:]
            else:
                # fallback to only current
                current_point = {"t": cur_payload["time"], "y": cur_payload["price"]}
                prev_point = None
                recent3 = []

            # change vs immediate previous
            if cur_payload.get("change_pct") is not None and cur_payload.get("change_abs") is not None:
                change_abs = float(cur_payload["change_abs"])
                change_pct = float(cur_payload["change_pct"])
            else:
                if prev_point and (prev_point["y"] != 0):
                    change_abs = round(current_point["y"] - prev_point["y"], 4)
                    change_pct = round(((current_point["y"] - prev_point["y"]) / prev_point["y"]) * 100.0, 4)
                else:
                    change_abs = None
                    change_pct = None

            if change_abs is None:
                dir_flag = "na"
            elif change_abs > 0:
                dir_flag = "up"
            elif change_abs < 0:
                dir_flag = "down"
            else:
                dir_flag = "flat"

            row["prices"][prod] = {
                "current": {
                    "price": current_point["y"],
                    "time": current_point["t"],
                    # NEW (flow to UI)
                    "location": cur_payload.get("location"),
                    "description": cur_payload.get("description"),
                },
                "recent3": [{"price": p["y"], "time": p["t"]} for p in recent3],
                "change": {"abs": change_abs, "pct": change_pct, "dir": dir_flag},
                "series": pts  # full chronological for charts (up to 'limit')
            }

        if any_prod:
            out_bdcs.append(row)

    # -------- Build top_lists per product (sorted DESC by current price) --------
    top_lists = {}
    for prod in prods:
        rows = []
        for bid in bdc_ids:
            cur_payload = current_map.get((bid, prod))
            if cur_payload and (cur_payload.get("price") is not None):
                rows.append({
                    "_id": str(bid),
                    "name": bdc_name.get(bid, "BDC"),
                    "price": float(cur_payload["price"]),
                    "time": cur_payload.get("time"),
                    "change_pct": cur_payload.get("change_pct")
                })
        rows.sort(key=lambda x: x["price"], reverse=True)
        top_lists[prod] = rows

    return jsonify({
        "ok": True,
        "shared_view": shared_view,
        "top_lists": top_lists,
        "bdcs": out_bdcs
    })

# --- Backward compatible endpoints (still usable by the UI) ---

@prices_bp.get("/prices/api/latest")
def api_latest():
    """
    Return current price per BDC per product, ONLY for BDCs that have posted.
    Output: { bdcs: [{ _id, name, prices: { PMS: {price, time, location?, description?}, AGO: {..} } }] }
    """
    cur_docs = list(bdc_current_prices.find(
        {"product": {"$in": PRODUCTS}},
        {"bdc_id": 1, "product": 1, "price": 1, "updated_at": 1, "location": 1, "description": 1}
    ))
    if not cur_docs:
        return jsonify({"ok": True, "bdcs": []})

    bdc_ids = sorted(list({d["bdc_id"] for d in cur_docs}))
    current_map = {(d["bdc_id"], d["product"]): {
        "price": float(d.get("price", 0.0)),
        "time": _iso_z(d.get("updated_at")),
        "location": d.get("location"),
        "description": d.get("description"),
    } for d in cur_docs}

    bdc_docs = list(bdc_collection.find({"_id": {"$in": bdc_ids}}, {"name": 1}))
    bdc_name = {d["_id"]: (d.get("name") or "BDC") for d in bdc_docs}

    out = []
    for bid in bdc_ids:
        row = {"_id": str(bid), "name": bdc_name.get(bid, "BDC"), "prices": {}}
        for prod in PRODUCTS:
            p = current_map.get((bid, prod))
            if p:
                row["prices"][prod] = p
        out.append(row)

    return jsonify({"ok": True, "bdcs": out})

@prices_bp.get("/prices/api/history")
def api_history():
    """Return time series for one bdc+product (default 60 points)."""
    bdc_id = request.args.get("bdc_id")
    product = (request.args.get("product") or "").upper()
    limit = int(request.args.get("limit") or 60)

    try:
        oid = ObjectId(bdc_id)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid bdc_id"}), 400
    if product not in PRODUCTS:
        return jsonify({"ok": False, "error": "Product must be PMS or AGO"}), 400
    if limit < 5:
        limit = 5
    if limit > 200:
        limit = 200

    cur = bdc_prices_collection.find(
        {"bdc_id": oid, "product": product},
        {"price": 1, "created_at": 1}
    ).sort("created_at", -1).limit(limit)

    # reverse to chronological for the chart
    points = [{
        "t": _iso_z(d["created_at"]),
        "y": float(d["price"])
    } for d in reversed(list(cur))]

    return jsonify({"ok": True, "series": points})
