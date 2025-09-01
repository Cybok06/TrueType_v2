from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from db import users_collection, clients_collection, db
from werkzeug.security import check_password_hash
from bson import ObjectId
from datetime import datetime, timezone
import urllib.request
import urllib.parse
import json

login_bp = Blueprint('login', __name__, template_folder='templates')

# New: collection for login logs
login_logs_collection = db["login_logs"]


def _status(entity, default="active"):
    return (entity or {}).get("status", default).strip().lower()


def _is_active(entity):
    return _status(entity) == "active"


# -------- Helpers for logging --------
def _now_utc():
    return datetime.now(timezone.utc)

def _req_ip():
    """Best-effort client IP extraction (handles proxies)."""
    # Common proxy headers
    xff = request.headers.get("X-Forwarded-For", "") or request.headers.get("X-Real-IP", "")
    ip = (xff.split(",")[0].strip() if xff else None) or request.remote_addr
    return ip, xff

def _ua():
    return request.headers.get("User-Agent", "")

def _ref():
    return request.referrer or ""

def _pick_headers():
    wanted = ["User-Agent", "Referer", "Origin", "X-Forwarded-For", "X-Real-IP", "CF-Connecting-IP", "CF-IPCountry"]
    h = {}
    for k in wanted:
        v = request.headers.get(k)
        if v:
            h[k] = v
    return h

def _geo_lookup(ip: str) -> dict:
    """
    Best-effort IP geolocation. Tries ipapi.co then ipinfo.io (no key needed for basic demo).
    If outbound blocked or times out, returns {'source':'none','ok':False}.
    """
    def fetch_json(url, timeout=1.2):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8", "ignore"))
        except Exception:
            return None

    if not ip:
        return {"source": "none", "ok": False}

    # ipapi.co
    j = fetch_json(f"https://ipapi.co/{urllib.parse.quote(ip)}/json/")
    if j and isinstance(j, dict) and not j.get("error"):
        return {
            "source": "ipapi",
            "ok": True,
            "ip": ip,
            "city": j.get("city"),
            "region": j.get("region"),
            "country": j.get("country_name") or j.get("country"),
            "lat": j.get("latitude"),
            "lon": j.get("longitude"),
            "org": j.get("org"),
            "asn": j.get("asn"),
        }

    # ipinfo.io (free tier may rate-limit)
    j2 = fetch_json(f"https://ipinfo.io/{urllib.parse.quote(ip)}/json")
    if j2 and isinstance(j2, dict) and not j2.get("error"):
        loc = (j2.get("loc") or "").split(",")
        lat = loc[0] if len(loc) == 2 else None
        lon = loc[1] if len(loc) == 2 else None
        return {
            "source": "ipinfo",
            "ok": True,
            "ip": ip,
            "city": j2.get("city"),
            "region": j2.get("region"),
            "country": j2.get("country"),
            "lat": lat, "lon": lon,
            "org": j2.get("org"),
        }

    return {"source": "none", "ok": False, "ip": ip}

def _log_login_attempt(
    *, kind: str,  # 'admin' | 'assistant' | 'client' | 'external' | 'unknown'
    username_attempted: str,
    success: bool,
    reason: str,   # 'ok' | 'blocked' | 'inactive' | 'bad_password' | 'not_found' | 'invalid_credentials'
    session_user_id: ObjectId | None = None,
    client_id: ObjectId | None = None,
    extra: dict | None = None
):
    ip, xff = _req_ip()
    log_doc = {
        "ts": _now_utc(),
        "who": {
            "username": username_attempted,
            "kind": kind,
        },
        "result": {
            "success": bool(success),
            "reason": reason,
        },
        "req": {
            "ip": ip,
            "forwarded_for": xff,
            "ua": _ua(),
            "ref": _ref(),
            "method": request.method,
            "path": request.path,
            "headers": _pick_headers(),
        },
        "geo": _geo_lookup(ip),
    }
    if session_user_id:
        log_doc["session_user_id"] = str(session_user_id)
    if client_id:
        log_doc["client_id"] = str(client_id)
    if extra:
        log_doc["extra"] = extra

    try:
        login_logs_collection.insert_one(log_doc)
    except Exception:
        # Never break login flow because of logging failure
        pass


@login_bp.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()

        # Clear previous session early
        session.clear()

        # === Admin / Assistant Login ===
        user = users_collection.find_one({"username": username})

        if user:
            pw_ok = check_password_hash(user.get("password", "") or "", password)
            if pw_ok:
                # Blocked/Inactive gate for ALL staff roles
                if not _is_active(user):
                    s = _status(user)
                    if s == "blocked":
                        _log_login_attempt(kind=user.get('role') or 'admin',
                                           username_attempted=username,
                                           success=False, reason="blocked",
                                           session_user_id=user.get("_id"))
                        flash("Your account is blocked. Contact an administrator.", "danger")
                    else:
                        _log_login_attempt(kind=user.get('role') or 'admin',
                                           username_attempted=username,
                                           success=False, reason="inactive",
                                           session_user_id=user.get("_id"))
                        flash("Your account is inactive. Contact an administrator.", "warning")
                    return redirect(url_for('login.login'))

                # Success
                session['username'] = username
                session['role'] = user.get('role', 'assistant')
                session['name'] = user.get("name", "User")

                _log_login_attempt(kind=session['role'], username_attempted=username,
                                   success=True, reason="ok",
                                   session_user_id=user.get("_id"))

                role = session['role']
                if role == 'admin':
                    return redirect(url_for('admin_dashboard.dashboard'))
                elif role == 'assistant':
                    return redirect(url_for('assistant_dashboard.dashboard'))
                else:
                    _log_login_attempt(kind='unknown', username_attempted=username,
                                       success=False, reason="unauthorized_role",
                                       session_user_id=user.get("_id"))
                    flash("Unauthorized role.", "warning")
                    session.clear()
                    return redirect(url_for('login.login'))
            else:
                # Wrong password for existing staff user
                _log_login_attempt(kind=user.get('role') or 'admin',
                                   username_attempted=username,
                                   success=False, reason="bad_password",
                                   session_user_id=user.get("_id"))
                # Continue to try client paths but we already know creds are wrong for staff

        # === Registered Client Login (client_id + phone as password) ===
        client = clients_collection.find_one({"client_id": username})
        if client:
            if (client.get("phone") or "") == password:
                # Enforce status
                s = _status(client)
                if s != "active":
                    if s == "blocked":
                        _log_login_attempt(kind='client', username_attempted=username,
                                           success=False, reason="blocked", client_id=client.get("_id"))
                        flash("Your client account is blocked. Please contact support.", "danger")
                    else:
                        _log_login_attempt(kind='client', username_attempted=username,
                                           success=False, reason="inactive", client_id=client.get("_id"))
                        flash("Your client account is inactive. Please contact support.", "warning")
                    return redirect(url_for('login.login'))

                # Success (client)
                session['role'] = 'client'
                session['client_id'] = str(client['_id'])
                session['client_code'] = client.get('client_id')
                session['client_name'] = client.get('name')

                _log_login_attempt(kind='client', username_attempted=username,
                                   success=True, reason="ok", client_id=client.get("_id"))
                return redirect(url_for('client_dashboard.dashboard'))
            else:
                _log_login_attempt(kind='client', username_attempted=username,
                                   success=False, reason="bad_password", client_id=client.get("_id"))

        # === External Client Login (name + phone) ===
        # Only allow if explicitly status == "external" and not blocked/inactive
        external = clients_collection.find_one({
            "name": {"$regex": f"^{username}$", "$options": "i"},
            "phone": password
        })
        if external:
            s = _status(external)
            if s == "blocked":
                _log_login_attempt(kind='external', username_attempted=username,
                                   success=False, reason="blocked", client_id=external.get("_id"))
                flash("Your external account is blocked. Please contact support.", "danger")
                return redirect(url_for('login.login'))
            if s == "inactive":
                _log_login_attempt(kind='external', username_attempted=username,
                                   success=False, reason="inactive", client_id=external.get("_id"))
                flash("Your external account is inactive. Please contact support.", "warning")
                return redirect(url_for('login.login'))

            if s == "external":
                session['role'] = 'external'
                session['external_id'] = str(external['_id'])
                session['external_name'] = external.get('name')
                session['external_phone'] = external.get('phone')

                _log_login_attempt(kind='external', username_attempted=username,
                                   success=True, reason="ok", client_id=external.get("_id"))
                return redirect(url_for('external.external_dashboard'))

        # If no match at all
        _log_login_attempt(kind='unknown', username_attempted=username,
                           success=False, reason="invalid_credentials",
                           extra={"entered_password_len": len(password)})
        flash("Invalid credentials", "danger")
        return redirect(url_for('login.login'))

    # GET
    return render_template('login.html')
