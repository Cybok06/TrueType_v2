from flask import Blueprint, render_template, request, redirect, flash, session, jsonify, url_for
from datetime import datetime, timedelta, timezone
from db import db
import requests
import secrets
from urllib.parse import quote

register_client_bp = Blueprint('register_client', __name__, template_folder='templates')

clients_collection      = db.clients
invite_links_collection = db.invite_links  # collection for expiring links

ARKESEL_API_KEY = "c1JKV21keG1DdnJZQW1zc2ks"
DEFAULT_IMAGE_URL = "https://cdn-icons-png.flaticon.com/256/3135/3135715.png"

# ------------- Time helpers (normalize to UTC naive) -------------
def _utcnow():
    """Naive UTC now (no tzinfo) to match default Mongo behavior."""
    return datetime.utcnow()

def _to_utc_naive(dt):
    """Normalize any datetime to naive UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Assume it's already UTC-naive (Mongo default)
        return dt
    # Convert aware->UTC then strip tzinfo
    return dt.astimezone(timezone.utc).replace(tzinfo=None)

def _expires_at_from_doc(invite_doc):
    """Get expires_at from doc, normalized to naive UTC."""
    dt = invite_doc.get("expires_at")
    return _to_utc_naive(dt)

# ------------- Misc helpers -------------
def _parse_duration(amount, unit):
    try:
        amt = int(amount)
    except (TypeError, ValueError):
        amt = 15
    unit = (unit or "minutes").lower()
    if unit.startswith("day"):
        return timedelta(days=amt)
    if unit.startswith("hour"):
        return timedelta(hours=amt)
    return timedelta(minutes=amt)

def _clean_phone_for_sms(phone: str) -> str | None:
    if not phone:
        return None
    p = phone.strip().replace(" ", "").replace("-", "")
    if p.startswith("0") and len(p) == 10:
        p = "233" + p[1:]
    if p.startswith("233") and len(p) == 12:
        return p
    return None

# ✅ Generate unique client ID in format TTYYXXX####
def generate_unique_client_id(phone):
    year = datetime.now().year % 100
    last3 = (phone or "")[-3:].zfill(3)
    prefix = f"TT{year:02d}{last3}"
    count = clients_collection.count_documents({"client_id": {"$regex": f"^{prefix}"}})
    suffix = str(count + 1).zfill(4)
    return f"{prefix}{suffix}"

# ✅ Send SMS with Arkesel
def send_registration_sms(name, phone, client_id):
    try:
        phone_number = _clean_phone_for_sms(phone)
        if not phone_number:
            print("❌ Invalid phone number for SMS:", phone)
            return False

        first_name = name.split()[0] if name else "Client"
        message = (
            f"Welcome to TrueType Services, {first_name}!\n\n"
            f"Your account has been successfully created.\n"
            f"Login Details:\n"
            f"Client ID: {client_id}\n"
            f"Password: {phone}\n\n"
            f"Use these to log in at https://truetypegh.com/login\n"
            f"Thank you!"
        )

        sms_url = (
            "https://sms.arkesel.com/sms/api?action=send-sms"
            f"&api_key={ARKESEL_API_KEY}"
            f"&to={phone_number}"
            f"&from=TrueType"
            f"&sms={quote(message)}"
        )
        response = requests.get(sms_url, timeout=15)
        print("Arkesel SMS response:", response.text)
        return response.status_code == 200 and '"code":"ok"' in response.text
    except Exception as e:
        print("SMS error:", str(e))
        return False

def _invite_doc_by_token(token: str):
    if not token:
        return None
    return invite_links_collection.find_one({"token": token})

# ------------- Create Shareable Link (admin only) -------------
@register_client_bp.route('/share/register-link', methods=['POST'])
def create_register_share_link():
    role = session.get("role")
    username = session.get("username")
    if role != "admin":
        return jsonify({"message": "Access denied."}), 403

    amount = request.form.get('amount') or (request.json or {}).get('amount')
    unit   = request.form.get('unit')   or (request.json or {}).get('unit')

    td = _parse_duration(amount, unit)
    now = _utcnow()
    expires_at = now + td  # naive UTC

    token = secrets.token_urlsafe(20)
    doc = {
        "token": token,
        "created_by": {"role": role, "username": username},
        "created_at": now,          # naive UTC
        "expires_at": expires_at,   # naive UTC
        "active": True,
        "purpose": "public_client_registration"
    }
    invite_links_collection.insert_one(doc)

    url = url_for('register_client.public_register_landing', token=token, _external=True)

    return jsonify({
        "url": url,
        # Provide ISO with Z to hint UTC in the frontend
        "expires_at_iso": expires_at.isoformat() + "Z"
    }), 200

# ------------- Public landing (no login) -------------
@register_client_bp.route('/register/<token>', methods=['GET'])
def public_register_landing(token):
    invit = _invite_doc_by_token(token)
    now = _utcnow()  # naive UTC
    expires_at = _expires_at_from_doc(invit) if invit else None

    if (not invit) or (not invit.get("active", True)) or (expires_at is None) or (now >= expires_at):
        return render_template(
            "partials/link_expired.html",
            title="Registration Link Expired",
            reason="This registration link is no longer valid.",
        ), 410

    return render_template(
        'partials/register_client.html',
        role="public",
        invite_token=token,
        expires_at=(expires_at.isoformat() + "Z")
    )

# ------------- Public submit -------------
@register_client_bp.route('/public/register_client/<token>', methods=['POST'])
def public_register_submit(token):
    invit = _invite_doc_by_token(token)
    now = _utcnow()
    expires_at = _expires_at_from_doc(invit) if invit else None

    if (not invit) or (not invit.get("active", True)) or (expires_at is None) or (now >= expires_at):
        msg = "❌ Link expired."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return msg, 410
        return render_template("partials/link_expired.html", title="Registration Link Expired", reason=msg), 410

    return _perform_registration(created_by={"role": "invite", "token": token})

# ------------- Admin -------------
@register_client_bp.route('/admin/register_client', methods=['GET', 'POST'])
def register_client():
    if request.method == 'POST':
        role = session.get("role", "unknown")
        return _perform_registration(created_by={"role": role, "username": session.get("username", "unknown")})

    return render_template('partials/register_client.html', role=session.get('role'))

# ------------- Shared registration core -------------
def _perform_registration(created_by: dict):
    name          = request.form.get('name', '').strip()
    phone         = request.form.get('phone', '').strip()
    email         = request.form.get('email', '').strip()
    location      = request.form.get('location', '').strip()
    image_url     = request.form.get('image_url') or DEFAULT_IMAGE_URL

    id_type           = request.form.get('id_type', '').strip()
    id_number         = request.form.get('id_number', '').strip()
    house_address     = request.form.get('house_address', '').strip()
    next_of_kin       = request.form.get('next_of_kin', '').strip()
    next_of_kin_phone = request.form.get('next_of_kin_phone', '').strip()
    relationship      = request.form.get('relationship', '').strip()

    if not (name and phone and id_type and id_number and next_of_kin and next_of_kin_phone and relationship):
        msg = "❗ Required fields are missing!"
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return msg, 400
        flash(msg, 'danger')
        return redirect(request.path)

    client_id = generate_unique_client_id(phone)
    if clients_collection.find_one({"client_id": client_id}):
        msg = f"❌ A client with ID {client_id} already exists."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return msg, 400
        flash(msg, 'danger')
        return redirect(request.path)

    creator_meta = created_by.copy()

    client_data = {
        'name': name,
        'phone': phone,
        'email': email or None,
        'location': location or None,
        'image_url': image_url,
        'client_id': client_id,
        'status': 'active',
        'date_registered': _utcnow(),  # naive UTC
        'created_by': creator_meta,
        'id_type': id_type,
        'id_number': id_number,
        'house_address': house_address or None,
        'next_of_kin': next_of_kin,
        'next_of_kin_phone': next_of_kin_phone,
        'relationship': relationship
    }

    try:
        clients_collection.insert_one(client_data)
        sms_sent = send_registration_sms(name, phone, client_id)
        if not sms_sent:
            print("⚠️ SMS failed or invalid number.")
    except Exception as e:
        print("Registration error:", str(e))
        error_msg = "❌ Client registration failed. Please try again."
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return error_msg, 500
        flash(error_msg, 'danger')
        return redirect(request.path)

    success_msg = f"✅ Client registered successfully with ID: {client_id}"
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({
            "message": success_msg,
            "client_id": client_id,
            "phone": phone
        }), 200

    flash(success_msg, 'success')
    return redirect(request.path)

