from flask import Blueprint, render_template, request, redirect, session, url_for, flash
from db import db
from datetime import datetime, timedelta
from bson import ObjectId
import os
import requests
from urllib.parse import quote

client_payment_bp = Blueprint("client_payment", __name__, template_folder="templates")

payments_col        = db["payments"]
truck_payments_col  = db["truck_payments"]
orders_col          = db["orders"]
bank_accounts_col   = db["bank_accounts"]
clients_col         = db["clients"]  # <-- for client details

# ───────────────────────── Config ─────────────────────────
ARKESEL_API_KEY = os.getenv("ARKESEL_API_KEY", "c1JKV21kDdnJZQW1zc2JpVks")
ADMIN_NOTIFY_MSISDN = "0277336609"  # destination for notifications
DUP_CONFIRM_TTL_MIN = 10            # user can confirm by re-submitting within this window

# ───────────────────── Helper functions ───────────────────
def _to_f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0

def _fmt_amt(v):
    try:
        return f"{float(v):,.2f}"
    except Exception:
        return "0.00"

def _clean_phone_for_sms(phone: str) -> str | None:
    """
    Return Ghana MSISDN in 233XXXXXXXXX format or None.
    """
    if not phone:
        return None
    p = phone.strip().replace(" ", "").replace("-", "")
    if p.startswith("+"):
        p = p[1:]
    if p.startswith("0") and len(p) == 10:
        p = "233" + p[1:]
    if p.startswith("233") and len(p) == 12:
        return p
    return None

def _send_sms(msisdn: str, message: str) -> bool:
    """
    Fire-and-forget SMS via Arkesel. Returns True on 'ok' response.
    """
    try:
        to = _clean_phone_for_sms(msisdn)
        if not to:
            print("❌ Invalid destination MSISDN:", msisdn)
            return False
        url = (
            "https://sms.arkesel.com/sms/api?action=send-sms"
            f"&api_key={ARKESEL_API_KEY}"
            f"&to={to}"
            f"&from=TrueType"
            f"&sms={quote(message)}"
        )
        resp = requests.get(url, timeout=15)
        ok = (resp.status_code == 200 and '"code":"ok"' in resp.text)
        if not ok:
            print("⚠️ SMS not accepted:", resp.status_code, resp.text)
        return ok
    except Exception as e:
        print("⚠️ SMS error:", e)
        return False

def _build_admin_payment_sms(*, receipt_ref: str, payment_type: str, amount: float,
                             client_name: str, client_id: str, client_phone: str,
                             order_code: str | None, bank_name: str, account_last4: str,
                             proof_url: str, created_at: datetime) -> str:
    order_line = order_code if order_code else "N/A"
    phone_disp = client_phone or "-"
    return (
        "Payment Alert\n"
        f"Receipt: {receipt_ref}\n"
        f"Type: {payment_type}\n"
        f"Amount: GHS {_fmt_amt(amount)}\n"
        f"Client: {client_name} ({client_id})\n"
        f"Phone: {phone_disp}\n"
        f"Order ID: {order_line}\n"
        f"Bank: {bank_name} ({account_last4})\n"
        f"Proof: {proof_url}\n"
        f"Time: {created_at.strftime('%Y-%m-%d %H:%M:%S')} UTC"
    )

def _latest_unconfirmed_payment(client_id_val, order_oid: ObjectId):
    """
    Return the most recent unconfirmed payment document for this client+order
    (status != 'confirmed'), or None if none exist.
    """
    return payments_col.find_one(
        {
            "client_id": client_id_val,
            "order_id": order_oid,
            "status": {"$ne": "confirmed"},
        },
        sort=[("date", -1)]
    )

def _dup_confirm_ready(order_oid_str: str) -> bool:
    """
    Has the user already seen the duplicate warning for this order recently?
    If yes (within TTL), allow the next POST to pass as a confirmed resubmission.
    """
    store = session.get("dup_confirm_store") or {}
    slot = store.get(order_oid_str)
    if not slot:
        return False
    try:
        ts = datetime.fromisoformat(slot.get("ts"))
    except Exception:
        return False
    return (datetime.utcnow() - ts) <= timedelta(minutes=DUP_CONFIRM_TTL_MIN)

def _remember_dup_warning(order_oid_str: str):
    store = session.get("dup_confirm_store") or {}
    store[order_oid_str] = {"ts": datetime.utcnow().isoformat()}
    session["dup_confirm_store"] = store

# ───────────────────────── Route ──────────────────────────
@client_payment_bp.route("/payment", methods=["GET", "POST"])
def client_payment():
    client_id = session.get("client_id")

    # ✅ Check login
    if not client_id:
        flash("⚠ Session expired. Please log in again.", "warning")
        return redirect(url_for("login.login"))

    # Support both storage styles in orders: ObjectId or string
    oid = ObjectId(client_id) if ObjectId.is_valid(client_id) else None
    client_match = {"$in": ([oid, str(client_id)] if oid else [str(client_id)])}
    client_for_payments = (oid or client_id)

    # Fetch client profile for notifications (safe defaults)
    client_doc = clients_col.find_one(
        {"_id": oid} if oid else {"client_id": str(client_id)}
    ) or {}
    client_name  = client_doc.get("name", "Client")
    client_code  = client_doc.get("client_id", str(client_id))
    client_phone = client_doc.get("phone", "")

    if request.method == "POST":
        payment_type   = (request.form.get("payment_type") or "").strip().lower()  # "order" or "truck"
        amount_s       = (request.form.get("amount") or "").strip()
        bank_name      = (request.form.get("bank_name") or "").strip()
        account_last4  = (request.form.get("account_last4") or "").strip()
        proof_url      = (request.form.get("proof_url") or "").strip()
        sel_order_id_s = (request.form.get("order_id") or "").strip()

        if not all([amount_s, bank_name, account_last4, proof_url]):
            flash("⚠ All fields are required.", "danger")
            return redirect(url_for("client_payment.client_payment"))

        amount = _to_f(amount_s)
        if amount <= 0:
            flash("⚠ Invalid amount format.", "danger")
            return redirect(url_for("client_payment.client_payment"))

        created_at = datetime.utcnow()

        payment_base = {
            "client_id": client_for_payments,  # keep ObjectId when we can
            "amount": amount,
            "bank_name": bank_name,
            "account_last4": account_last4,
            "proof_url": proof_url,
            "status": "pending",
            "date": created_at
        }

        try:
            if payment_type == "truck":
                # Insert truck payment (no duplicate guard for trucks)
                ins = truck_payments_col.insert_one(payment_base)
                receipt_ref = f"PMT-{str(ins.inserted_id)[-6:].upper()}"
                # Build SMS for admin
                sms_text = _build_admin_payment_sms(
                    receipt_ref=receipt_ref,
                    payment_type="Truck",
                    amount=amount,
                    client_name=client_name,
                    client_id=client_code,
                    client_phone=client_phone,
                    order_code=None,
                    bank_name=bank_name,
                    account_last4=account_last4,
                    proof_url=proof_url,
                    created_at=created_at,
                )
                _send_sms(ADMIN_NOTIFY_MSISDN, sms_text)

                flash("✅ Truck payment submitted successfully!", "success")

            else:
                # Order payment requires explicit order selection
                if not sel_order_id_s or not ObjectId.is_valid(sel_order_id_s):
                    flash("⚠ Please select a valid order to pay for.", "danger")
                    return redirect(url_for("client_payment.client_payment"))

                sel_oid = ObjectId(sel_order_id_s)

                # Ensure the selected order belongs to this client
                owned = orders_col.find_one({"_id": sel_oid, "client_id": client_match})
                if not owned:
                    flash("⚠ Selected order not found for your account.", "danger")
                    return redirect(url_for("client_payment.client_payment"))

                # Duplicate-payment guard: is there any unconfirmed payment already?
                existing = _latest_unconfirmed_payment(client_for_payments, sel_oid)
                order_oid_str = str(sel_oid)
                if existing and not _dup_confirm_ready(order_oid_str):
                    # ask for confirmation (no insert yet)
                    last_amt = _fmt_amt(existing.get("amount", 0))
                    last_dt  = existing.get("date")
                    last_dt_str = last_dt.strftime("%Y-%m-%d %H:%M:%S") if isinstance(last_dt, datetime) else str(last_dt or "")
                    flash(
                        f"⚠ You already submitted a payment for this order that is not confirmed yet "
                        f"(GHS {last_amt} on {last_dt_str}). "
                        f"If you really want to send another payment for the same order, submit again within "
                        f"{DUP_CONFIRM_TTL_MIN} minutes to confirm.",
                        "warning"
                    )
                    _remember_dup_warning(order_oid_str)
                    return redirect(url_for("client_payment.client_payment"))

                # Human-friendly order code (fallback to ObjectId)
                order_code = owned.get("order_id") or str(owned["_id"])

                # Save payment with order reference
                doc = dict(payment_base)
                doc["order_id"] = sel_oid
                ins = payments_col.insert_one(doc)

                # Generate a simple receipt reference
                receipt_ref = f"PMT-{str(ins.inserted_id)[-6:].upper()}"

                # Build and send SMS to admin number
                sms_text = _build_admin_payment_sms(
                    receipt_ref=receipt_ref,
                    payment_type="Order",
                    amount=amount,
                    client_name=client_name,
                    client_id=client_code,
                    client_phone=client_phone,
                    order_code=order_code,
                    bank_name=bank_name,
                    account_last4=account_last4,
                    proof_url=proof_url,
                    created_at=created_at,
                )
                _send_sms(ADMIN_NOTIFY_MSISDN, sms_text)

                # clear confirmation latch for this order (avoid open-ended confirmations)
                store = session.get("dup_confirm_store") or {}
                if order_oid_str in store:
                    try:
                        del store[order_oid_str]
                        session["dup_confirm_store"] = store
                    except Exception:
                        pass

                flash("✅ Payment submitted successfully!", "success")

        except Exception as e:
            flash(f"❌ Error saving payment: {str(e)}", "danger")

        return redirect(url_for("client_payment.client_payment"))

    # ------------------------- GET: Build “orders with debt” -------------------------
    orders = list(
        orders_col.find({"client_id": client_match}).sort("date", -1)
    )

    # Map: order_id -> confirmed paid total
    def confirmed_paid_for(order_oid):
        cur = payments_col.find({
            "client_id": client_for_payments,
            "order_id": order_oid,
            "status": "confirmed"
        })
        return sum(_to_f(p.get("amount")) for p in cur)

    orders_with_debt = []
    full_outstanding_total = 0.0

    for o in orders:
        total_debt = _to_f(o.get("total_debt"))
        if total_debt <= 0:
            continue

        paid = confirmed_paid_for(o["_id"])
        outstanding = round(max(total_debt - paid, 0.0), 2)
        if outstanding > 0:
            orders_with_debt.append({
                "_id": str(o["_id"]),                                  # for form value
                "code": o.get("order_id") or str(o["_id"]),            # human code to display
                "product": o.get("product", ""),
                "date": o.get("date"),
                "total_debt": round(total_debt, 2),
                "paid": round(paid, 2),
                "outstanding": outstanding
            })
            full_outstanding_total += outstanding

    # Sort by most recent order date
    orders_with_debt.sort(key=lambda x: x["date"] or datetime.min, reverse=True)

    # Build a small map for front-end auto-fill (order -> outstanding)
    order_balance_map = {row["_id"]: row["outstanding"] for row in orders_with_debt}

    # ✅ Fetch and combine both payment types (history)
    order_payments = list(payments_col.find({"client_id": client_for_payments}).sort("date", -1))
    truck_payments = list(truck_payments_col.find({"client_id": client_for_payments}).sort("date", -1))

    combined_payments = []
    for p in order_payments:
        combined_payments.append({
            "type": "Order",
            "date": (p.get("date") or datetime.min).strftime("%Y-%m-%d %H:%M:%S"),
            "amount": _to_f(p.get("amount")),
            "bank_name": p.get("bank_name", "-"),
            "account_last4": p.get("account_last4", ""),
            "proof_url": p.get("proof_url", "#"),
            "status": p.get("status", "pending"),
            "feedback": p.get("feedback", "")
        })
    for p in truck_payments:
        combined_payments.append({
            "type": "Truck",
            "date": (p.get("date") or datetime.min).strftime("%Y-%m-%d %H:%M:%S"),
            "amount": _to_f(p.get("amount")),
            "bank_name": p.get("bank_name", "-"),
            "account_last4": p.get("account_last4", ""),
            "proof_url": p.get("proof_url", "#"),
            "status": p.get("status", "pending"),
            "feedback": p.get("feedback", "")
        })

    # Sort history by date string (safe because YYYY-MM-DD HH:MM:SS)
    combined_payments.sort(key=lambda x: x["date"], reverse=True)

    # ✅ Load available bank accounts
    bank_accounts = list(bank_accounts_col.find({}, {
        "bank_name": 1, "account_name": 1, "account_number": 1, "_id": 0
    }).sort("bank_name"))

    return render_template(
        "client/client_payment.html",
        payments=combined_payments,
        # For the UI dropdown and “Full payment” auto-fill:
        orders_with_debt=orders_with_debt,
        full_outstanding_total=round(full_outstanding_total, 2),
        order_balance_map=order_balance_map,
        bank_accounts=bank_accounts
    )

