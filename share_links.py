# share_links.py  (UPDATED)
from flask import Blueprint, request, render_template, redirect, url_for, session, jsonify
from bson import ObjectId
from db import db
from datetime import datetime, timedelta
import secrets, re
from werkzeug.security import generate_password_hash, check_password_hash

shared_bp = Blueprint("shared_links", __name__, template_folder="templates")

orders         = db["orders"]
clients        = db["clients"]
bdc_col        = db["bdc"]
omc_col        = db["bd_omc"]           # <-- used for OMC validation/listing
shared_links   = db["shared_links"]     # collection for shared link documents

STATUS_OPTIONS = [
    "Ordered", "Approved", "GoodStanding", "Depot Manager",
    "BRV check pass", "BRV check unpass", "Loading", "Loaded",
    "Moved", "Released"
]

# ---------- helpers ----------
def _now():
    return datetime.utcnow()

def _clean(s: str) -> str:
    return (s or "").strip()

def _require_5_digit(s: str) -> bool:
    return bool(re.fullmatch(r"\d{5}", s or ""))

def _token():
    # url‑safe token
    return secrets.token_urlsafe(24)

def _is_link_valid(link_doc):
    if not link_doc:
        return False
    if link_doc.get("revoked_at"):
        return False
    exp = link_doc.get("expires_at")
    if exp and exp < _now():
        return False
    return True

def _safe_oid(val):
    try:
        return ObjectId(val)
    except Exception:
        return None

def _mode_of(doc) -> str:
    """
    Returns one of:
      - "bdc_multi": allowed_bdcs (list of names)
      - "omc":       allowed_omc  (single OMC name)
    Backward compatibility:
      If legacy field "bdc_name" exists and no mode set -> treat as bdc_multi [bdc_name]
    """
    m = (doc.get("mode") or "").strip().lower()
    if m in ("bdc_multi", "omc"):
        return m
    # legacy single BDC
    if doc.get("bdc_name"):
        return "bdc_multi"
    # default fallback (should not happen)
    return "bdc_multi"

def _get_allowed(doc):
    """
    Returns (mode, payload)
      - if mode == "bdc_multi" -> payload: list[str]  (BDC names)
      - if mode == "omc"       -> payload: str       (OMC name)
    """
    m = _mode_of(doc)
    if m == "omc":
        return m, _clean(doc.get("allowed_omc") or doc.get("omc_name") or "")
    # bdc_multi
    bdcs = doc.get("allowed_bdcs")
    if not bdcs and doc.get("bdc_name"):
        bdcs = [doc["bdc_name"]]  # legacy single name
    bdcs = [b for b in (bdcs or []) if _clean(b)]
    return "bdc_multi", bdcs

# ---------- Admin UI: create link (browser page) ----------
@shared_bp.route("/deliveries/share/new", methods=["GET", "POST"])
def new_share_link_form():
    """
    GET  -> show form to create a share link (now supports Multi-BDC OR Single-OMC)
    POST -> validate inputs, create link, render result page with final URL
    """
    if request.method == "GET":
        bdc_names = sorted({
            (d.get("name") or "").strip()
            for d in bdc_col.find({}, {"name": 1})
            if d.get("name")
        })
        omc_names = sorted({
            (o.get("name") or "").strip()
            for o in omc_col.find({}, {"name": 1})
            if o.get("name")
        })
        return render_template(
            "shared/new_share_link_form.html",
            bdc_names=bdc_names,
            omc_names=omc_names,
            error=None
        )

    # POST -> Create link via same validation as API (below)
    mode = _clean(request.form.get("mode") or "bdc_multi").lower()  # "bdc_multi" | "omc"
    passcode = _clean(request.form.get("passcode"))
    try:
        expires_in_days = int(request.form.get("expires_in_days") or 7)
    except Exception:
        expires_in_days = 7

    error = None
    allowed_bdcs = []
    allowed_omc = ""

    if mode == "bdc_multi":
        # accept either multiple <select> (name="bdc_names") OR a single csv text (name="bdc_names_csv")
        raw_list = request.form.getlist("bdc_names")
        csv_txt  = _clean(request.form.get("bdc_names_csv"))
        if csv_txt:
            raw_list += [x.strip() for x in csv_txt.split(",") if x.strip()]
        allowed_bdcs = sorted(set([_clean(x) for x in raw_list if _clean(x)]))
        if not allowed_bdcs:
            error = "Select at least one BDC."
    elif mode == "omc":
        allowed_omc = _clean(request.form.get("omc_name"))
        if not allowed_omc:
            error = "OMC is required."
    else:
        error = "Invalid mode."

    if not _require_5_digit(passcode):
        error = error or "Passcode must be exactly 5 digits."
    if not (1 <= expires_in_days <= 90):
        error = error or "Expiry must be between 1 and 90 days."

    if error:
        bdc_names = sorted({
            (d.get("name") or "").strip()
            for d in bdc_col.find({}, {"name": 1})
            if d.get("name")
        })
        omc_names = sorted({
            (o.get("name") or "").strip()
            for o in omc_col.find({}, {"name": 1})
            if o.get("name")
        })
        return render_template(
            "shared/new_share_link_form.html",
            bdc_names=bdc_names,
            omc_names=omc_names,
            error=error
        )

    token = _token()
    base_doc = {
        "token": token,
        "mode": mode,  # "bdc_multi" | "omc"
        "allowed_bdcs": allowed_bdcs if mode == "bdc_multi" else None,
        "allowed_omc": allowed_omc if mode == "omc" else None,
        # legacy single bdc_name stays empty for new links
        "bdc_name": None,
        "pass_hash": generate_password_hash(passcode),
        "created_at": _now(),
        "expires_at": _now() + timedelta(days=expires_in_days),
        "revoked_at": None,
        "created_by": session.get("user_id"),  # optional
        "audit": [{"type": "create", "at": _now(), "by": session.get("user_id")}]
    }
    shared_links.insert_one(base_doc)

    shared_url = url_for("shared_links.shared_landing", token=token, _external=True)

    # For the result page we keep a friendly label to show what was scoped
    scope_label = (
        ", ".join(allowed_bdcs) if mode == "bdc_multi"
        else f"OMC: {allowed_omc}"
    )

    return render_template(
        "shared/new_share_link_result.html",
        bdc_name=scope_label,               # reusing template label
        shared_url=shared_url,
        passcode=passcode,                  # show once to admin
        expires_at=base_doc["expires_at"],
        token=token
    )

# ---------- Programmatic API: create a share link ----------
# POST /deliveries/share/create
@shared_bp.route("/deliveries/share/create", methods=["POST"])
def create_share_link():
    """
    Accepts:
      mode = "bdc_multi" | "omc"
      if mode == "bdc_multi":
        - bdc_names[] (repeatable) OR bdc_names_csv="A,B,C"
      if mode == "omc":
        - omc_name=<name>
      Common:
        - passcode (exactly 5 digits)
        - expires_in_days (1..90, default 7)
    """
    mode = _clean(request.form.get("mode") or "bdc_multi").lower()
    passcode = _clean(request.form.get("passcode"))

    if not _require_5_digit(passcode):
        return jsonify({"success": False, "message": "Passcode must be exactly 5 digits."}), 400

    try:
        days = int(request.form.get("expires_in_days") or 7)
        if days < 1 or days > 90:
            raise ValueError()
    except Exception:
        return jsonify({"success": False, "message": "expires_in_days must be 1–90."}), 400

    allowed_bdcs, allowed_omc = [], ""
    if mode == "bdc_multi":
        raw_list = request.form.getlist("bdc_names")
        csv_txt  = _clean(request.form.get("bdc_names_csv"))
        if csv_txt:
            raw_list += [x.strip() for x in csv_txt.split(",") if x.strip()]
        allowed_bdcs = sorted(set([_clean(x) for x in raw_list if _clean(x)]))
        if not allowed_bdcs:
            return jsonify({"success": False, "message": "Select at least one BDC."}), 400
    elif mode == "omc":
        allowed_omc = _clean(request.form.get("omc_name"))
        if not allowed_omc:
            return jsonify({"success": False, "message": "OMC is required."}), 400
    else:
        return jsonify({"success": False, "message": "Invalid mode."}), 400

    token = _token()
    doc = {
        "token": token,
        "mode": mode,
        "allowed_bdcs": allowed_bdcs if mode == "bdc_multi" else None,
        "allowed_omc": allowed_omc if mode == "omc" else None,
        "bdc_name": None,  # legacy not used for new links
        "pass_hash": generate_password_hash(passcode),
        "created_at": _now(),
        "expires_at": _now() + timedelta(days=days),
        "revoked_at": None,
        "created_by": session.get("user_id"),
        "audit": [{"type": "create", "at": _now(), "by": session.get("user_id")}]
    }
    shared_links.insert_one(doc)

    return jsonify({
        "success": True,
        "url": url_for("shared_links.shared_landing", token=token, _external=True),
        "expires_at": doc["expires_at"].isoformat() + "Z",
        "mode": mode,
        "allowed_bdcs": doc.get("allowed_bdcs"),
        "allowed_omc": doc.get("allowed_omc"),
        "token": token
    })

# ---------- Admin: revoke link ----------
@shared_bp.route("/deliveries/share/<token>/revoke", methods=["POST"])
def revoke_share_link(token):
    link = shared_links.find_one({"token": token})
    if not link:
        return jsonify({"success": False, "message": "Link not found."}), 404
    if link.get("revoked_at"):
        return jsonify({"success": False, "message": "Already revoked."}), 400
    shared_links.update_one({"_id": link["_id"]}, {"$set": {"revoked_at": _now()}})
    return jsonify({"success": True})

# ---------- Partner: landing (asks for passcode) ----------
@shared_bp.route("/deliveries/shared/<token>", methods=["GET", "POST"])
def shared_landing(token):
    link = shared_links.find_one({"token": token})
    if not _is_link_valid(link):
        return render_template("shared/invalid_link.html"), 410  # Gone/invalid

    # Minimal in-session rate limiting for pass attempts
    key_attempts = f"pass_attempts:{token}"
    attempts = session.get(key_attempts, 0)

    if request.method == "POST":
        if attempts >= 10:
            return render_template("shared/passcode.html", token=token,
                                   bdc_name=_scope_label(link),
                                   error="Too many attempts. Try again later."), 429

        passcode = _clean(request.form.get("passcode"))
        session[key_attempts] = attempts + 1

        if not _require_5_digit(passcode):
            return render_template("shared/passcode.html", token=token,
                                   bdc_name=_scope_label(link),
                                   error="Enter exactly 5 digits.")

        if not check_password_hash(link["pass_hash"], passcode):
            return render_template("shared/passcode.html", token=token,
                                   bdc_name=_scope_label(link),
                                   error="Incorrect passcode.")

        # Success: mark unlocked and set session
        session[f"shared_unlocked:{token}"] = True
        shared_links.update_one(
            {"_id": link["_id"]},
            {"$push": {"audit": {"type": "unlock", "at": _now(), "ip": request.remote_addr}}}
        )
        return redirect(url_for("shared_links.shared_manage", token=token))

    # GET -> show passcode form
    return render_template("shared/passcode.html", token=token,
                           bdc_name=_scope_label(link), error=None)

def _scope_label(link):
    mode, payload = _get_allowed(link)
    if mode == "omc":
        return f"OMC: {payload or '—'}"
    # bdc_multi
    if not payload:
        return "BDC: —"
    if len(payload) == 1:
        return f"{payload[0]}"
    return f"{len(payload)} BDCs"

# ---------- Partner: manage deliveries (restricted) ----------
@shared_bp.route("/deliveries/shared/<token>/manage", methods=["GET"])
def shared_manage(token):
    link = shared_links.find_one({"token": token})
    if not _is_link_valid(link):
        return render_template("shared/invalid_link.html"), 410
    if not session.get(f"shared_unlocked:{token}"):
        return redirect(url_for("shared_links.shared_landing", token=token))

    mode, payload = _get_allowed(link)

    # Only approved orders visible; filter by mode
    filters = {"status": "approved"}
    if mode == "bdc_multi":
        filters["bdc_name"] = {"$in": payload} if payload else "__none__"
    elif mode == "omc":
        filters["omc"] = payload or "__none__"

    projection = {
        "_id": 1, "client_id": 1, "bdc_name": 1, "omc": 1, "product": 1,
        "vehicle_number": 1, "driver_name": 1, "driver_phone": 1,
        "quantity": 1, "region": 1,
        "delivery_status": 1, "tts_status": 1, "npa_status": 1,
        "date": 1, "delivered_date": 1
    }

    items = list(orders.find(filters, projection).sort("date", -1))

    # client names map
    client_ids = []
    for o in items:
        cid = o.get("client_id")
        if isinstance(cid, ObjectId):
            client_ids.append(cid)
        else:
            try:
                client_ids.append(ObjectId(str(cid)))
            except Exception:
                pass
    cmap = {str(c["_id"]): c.get("name", "Unknown")
            for c in clients.find({"_id": {"$in": list(set(client_ids))}}, {"name": 1})}

    deliveries = []
    for o in items:
        cid = str(o.get("client_id")) if o.get("client_id") else ""
        deliveries.append({
            "order_id": str(o["_id"]),
            "bdc_name": o.get("bdc_name", ""),
            "omc": o.get("omc", ""),
            "client_name": cmap.get(cid, "Unknown"),
            "product": o.get("product", ""),
            "vehicle_number": o.get("vehicle_number", ""),
            "driver_name": o.get("driver_name", ""),
            "driver_phone": o.get("driver_phone", ""),
            "quantity": o.get("quantity", 0),
            "region": o.get("region", ""),
            "delivery_status": o.get("delivery_status", "pending"),
            "tts_status": o.get("tts_status"),
            "npa_status": o.get("npa_status"),
            "date": o.get("date"),
            "delivered_date": o.get("delivered_date")
        })

    return render_template(
        "shared/manage_deliveries_shared.html",
        bdc_name=_scope_label(link),      # title/subheader already expects 'bdc_name' label
        deliveries=deliveries,
        status_options=STATUS_OPTIONS,
        token=token
    )

# ---------- Partner: update (restricted + server-side scope check) ----------
@shared_bp.route("/deliveries/shared/<token>/update_status/<order_id>", methods=["POST"])
def shared_update_status(token, order_id):
    link = shared_links.find_one({"token": token})
    if not _is_link_valid(link):
        return jsonify({"success": False, "message": "Invalid or expired link."}), 410
    if not session.get(f"shared_unlocked:{token}"):
        return jsonify({"success": False, "message": "Locked."}), 403

    oid = _safe_oid(order_id)
    if not oid:
        return jsonify({"success": False, "message": "Invalid order id."}), 400

    tts = _clean(request.form.get("tts_status"))
    npa = _clean(request.form.get("npa_status"))

    if not tts and not npa:
        return jsonify({"success": False, "message": "Provide TTS and/or NPA status."}), 400

    # Load order minimal fields
    order_doc = orders.find_one({"_id": oid}, {"bdc_name": 1, "omc": 1})
    if not order_doc:
        return jsonify({"success": False, "message": "Order not found."}), 404

    # Ensure the order is inside the allowed scope
    mode, payload = _get_allowed(link)
    if mode == "bdc_multi":
        allowed = payload or []
        if order_doc.get("bdc_name") not in allowed:
            return jsonify({"success": False, "message": "Order not allowed for this link (BDC scope)."}), 403
    elif mode == "omc":
        if (order_doc.get("omc") or "").strip() != (payload or ""):
            return jsonify({"success": False, "message": "Order not allowed for this link (OMC scope)."}), 403

    update_fields = {}
    if tts:
        update_fields["tts_status"] = tts
    if npa:
        update_fields["npa_status"] = npa

    history_entry = {
        "tts_status": tts or None,
        "npa_status": npa or None,
        "by_shared_token": token,
        "timestamp": _now()
    }

    res = orders.update_one(
        {"_id": oid},
        {"$set": update_fields, "$push": {"delivery_history": history_entry}}
    )

    # audit the share link doc
    shared_links.update_one(
        {"_id": link["_id"]},
        {"$push": {"audit": {
            "type": "update", "at": _now(), "order_id": str(oid),
            "ip": request.remote_addr, "tts": tts or None, "npa": npa or None
        }}}
    )

    return jsonify({"success": res.modified_count == 1, "message": "Updated" if res.modified_count == 1 else "No change"})
