# accounting_routes/payment_voucher.py
from __future__ import annotations

from flask import (
    Blueprint, render_template, request,
    redirect, url_for, flash, abort, jsonify
)
from bson import ObjectId
from datetime import datetime
from decimal import Decimal, InvalidOperation
from werkzeug.utils import secure_filename
import traceback
import requests

from db import db

payment_vouchers_col = db["payment_vouchers"]
images_col = db["images"]  # traceability logs (same pattern as your other module)

# NOTE:
# - template_folder points to project_root/templates/accounting
# - NO url_prefix here; it will be added in app.py when registering
payment_voucher_bp = Blueprint(
    "payment_voucher",
    __name__,
    template_folder="../templates/accounting",
)

# ===== Cloudflare Images (hardcoded as requested) =====
CF_ACCOUNT_ID   = "63e6f91eec9591f77699c4b434ab44c6"
CF_IMAGES_TOKEN = "Brz0BEfl_GqEUjEghS2UEmLZhK39EUmMbZgu_hIo"
CF_HASH         = "h9fmMoa1o2c2P55TcWJGOg"
DEFAULT_VARIANT = "public"

ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}


def _allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _ensure_indexes() -> None:
    """Safe indexes."""
    try:
        payment_vouchers_col.create_index([("created_at", -1)])
        payment_vouchers_col.create_index([("date", -1)])
        payment_vouchers_col.create_index([("to_name", 1)])
        payment_vouchers_col.create_index([("pv_number", 1)], unique=True)  # prevent duplicates
        images_col.create_index([("created_at", -1)])
    except Exception:
        # ignore if no permissions / already exists
        pass


_ensure_indexes()


def _to_decimal(val, default: str = "0") -> Decimal:
    """Safely convert any value from the form to Decimal."""
    try:
        return Decimal(str(val or default))
    except (InvalidOperation, TypeError):
        return Decimal(default)


# ============================================================
# PV Numbering: TTGH-YY-MM-0001  (YY=year, MM=month)
# Sequence increments per (year, month)
# ============================================================
def _pv_prefix_for_now(now: datetime | None = None) -> str:
    """TTGH-YY-MM-"""
    now = now or datetime.today()
    yy = f"{now.year % 100:02d}"   # 2025 -> 25
    mm = f"{now.month:02d}"        # 12 -> 12
    return f"TTGH-{yy}-{mm}-"


def _next_pv_number(now: datetime | None = None) -> str:
    """
    Generate next PV number like:
      TTGH-25-12-0001
    where 25=year, 12=month, 0001 increments within that month.
    """
    now = now or datetime.today()
    prefix = _pv_prefix_for_now(now)

    last = payment_vouchers_col.find_one(
        {"pv_number": {"$regex": f"^{prefix}"}},
        sort=[("pv_number", -1)]
    )

    last_seq = 0
    if last and isinstance(last.get("pv_number"), str):
        try:
            last_seq = int(last["pv_number"].split("-")[-1])
        except Exception:
            last_seq = 0

    return f"{prefix}{last_seq + 1:04d}"


@payment_voucher_bp.post("/upload_image")
def upload_image():
    """
    Upload image (signature/invoice) to Cloudflare Images.
    Expects form-data: image=<file>
    Returns:
      { success, image_url, image_id, variant }
    """
    try:
        if "image" not in request.files:
            return jsonify({"success": False, "error": "No file part in request"}), 400

        image = request.files["image"]
        if not image or image.filename == "":
            return jsonify({"success": False, "error": "No selected file"}), 400

        if not _allowed_file(image.filename):
            return jsonify({"success": False, "error": "File type not allowed"}), 400

        # 1) get one-time direct upload URL
        direct_url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/images/v2/direct_upload"
        headers = {"Authorization": f"Bearer {CF_IMAGES_TOKEN}"}
        data = {}

        res = requests.post(direct_url, headers=headers, data=data, timeout=20)
        try:
            j = res.json()
        except Exception:
            return jsonify({"success": False, "error": "Cloudflare (direct_upload) returned non-JSON"}), 502

        if not j.get("success"):
            return jsonify({"success": False, "error": "Cloudflare direct_upload failed", "details": j}), 400

        upload_url = j["result"]["uploadURL"]
        image_id = j["result"]["id"]

        # 2) upload file bytes to that uploadURL
        up = requests.post(
            upload_url,
            files={
                "file": (
                    secure_filename(image.filename),
                    image.stream,
                    image.mimetype or "application/octet-stream",
                )
            },
            timeout=60
        )
        try:
            uj = up.json()
        except Exception:
            return jsonify({"success": False, "error": "Cloudflare (upload) returned non-JSON"}), 502

        if not uj.get("success"):
            return jsonify({"success": False, "error": "Cloudflare upload failed", "details": uj}), 400

        variant = request.args.get("variant", DEFAULT_VARIANT)
        image_url = f"https://imagedelivery.net/{CF_HASH}/{image_id}/{variant}"

        images_col.insert_one({
            "provider": "cloudflare_images",
            "source": "payment_voucher",
            "image_id": image_id,
            "variant": variant,
            "url": image_url,
            "original_filename": secure_filename(image.filename),
            "mimetype": image.mimetype,
            "created_at": datetime.utcnow(),
        })

        return jsonify({"success": True, "image_url": image_url, "image_id": image_id, "variant": variant})

    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@payment_voucher_bp.get("/")
def form_page():
    """
    Payment Voucher main page:
    - Shows the create-new form
    - Lists recent vouchers (for quick access).
    """
    pv_number = _next_pv_number()
    today = datetime.today().strftime("%Y-%m-%d")

    recent = list(
        payment_vouchers_col.find({})
        .sort("created_at", -1)
        .limit(25)
    )

    return render_template(
        "payment_voucher_form.html",
        pv_number=pv_number,
        today=today,
        vouchers=recent,
    )


@payment_voucher_bp.post("/")
def create_voucher():
    """Handle submission of a new payment voucher."""
    form = request.form

    # Always compute server-side too (don’t trust hidden input)
    pv_number = _next_pv_number()

    date_str = form.get("date") or datetime.today().strftime("%Y-%m-%d")
    to_name = (form.get("to_name") or "").strip()
    pay_method = form.get("pay_method", "tfr")  # 'cash' or 'tfr'
    tfr_no = (form.get("tfr_no") or "").strip()
    bank_name = (form.get("bank_name") or "").strip()

    # Line items
    desc_list = form.getlist("line_description[]")
    amt_list = form.getlist("line_amount[]")

    line_items = []
    subtotal = Decimal("0")

    for desc, amt in zip(desc_list, amt_list):
        desc = (desc or "").strip()
        if not desc and not amt:
            continue
        amount = _to_decimal(amt)
        if amount < 0:
            continue
        line_items.append({"description": desc, "amount": float(amount)})
        subtotal += amount

    if not line_items:
        flash("Please add at least one line item.", "danger")
        return redirect(url_for("payment_voucher.form_page"))

    # Tax percentages
    vat_pct = _to_decimal(form.get("vat_pct"), "0")
    wht_pct = _to_decimal(form.get("wht_pct"), "0")

    vat_amount = (subtotal * vat_pct / Decimal("100")).quantize(Decimal("0.01"))
    wht_amount = (subtotal * wht_pct / Decimal("100")).quantize(Decimal("0.01"))
    total_payable = (subtotal + vat_amount - wht_amount).quantize(Decimal("0.01"))

    amount_in_words = (form.get("amount_in_words") or "").strip()

    prepared_by = (form.get("prepared_by") or "").strip()
    authorised_by = (form.get("authorised_by") or "").strip()
    approved_by = (form.get("approved_by") or "").strip()
    received_by = (form.get("received_by") or "").strip()
    recipient_details = (form.get("recipient_details") or "").strip()

    # Signature URLs
    signatures = {
        "prepared":  (form.get("sig_prepared_url") or "").strip(),
        "authorised": (form.get("sig_authorised_url") or "").strip(),
        "approved":  (form.get("sig_approved_url") or "").strip(),
        "received":  (form.get("sig_received_url") or "").strip(),
    }

    # Invoice image URL (page two)
    invoice_url = (form.get("invoice_url") or "").strip()

    try:
        date_obj = datetime.fromisoformat(date_str)
    except Exception:
        date_obj = datetime.today()

    doc = {
        "pv_number": pv_number,
        "date": date_obj,
        "to_name": to_name,
        "pay_method": pay_method,
        "tfr_no": tfr_no,
        "bank_name": bank_name,

        "line_items": line_items,
        "subtotal": float(subtotal),
        "vat_pct": float(vat_pct),
        "vat_amount": float(vat_amount),
        "wht_pct": float(wht_pct),
        "wht_amount": float(wht_amount),
        "total_payable": float(total_payable),

        "amount_in_words": amount_in_words,

        "prepared_by": prepared_by,
        "authorised_by": authorised_by,
        "approved_by": approved_by,
        "received_by": received_by,

        "signatures": signatures,
        "invoice_url": invoice_url,

        "recipient_details": recipient_details,
        "created_at": datetime.utcnow(),
    }

    # Retry loop in case of rare pv_number collision (unique index)
    for _ in range(4):
        try:
            res = payment_vouchers_col.insert_one(doc)
            flash("Payment voucher created.", "success")
            return redirect(url_for("payment_voucher.view_voucher", voucher_id=str(res.inserted_id)))
        except Exception:
            # likely duplicate pv_number under concurrency — regenerate & retry
            doc["pv_number"] = _next_pv_number()
            continue

    flash("Could not create voucher. Please try again.", "danger")
    return redirect(url_for("payment_voucher.form_page"))


@payment_voucher_bp.get("/<voucher_id>")
def view_voucher(voucher_id: str):
    """Display one voucher in printable layout (invoice on page two if present)."""
    try:
        oid = ObjectId(voucher_id)
    except Exception:
        abort(404)

    voucher = payment_vouchers_col.find_one({"_id": oid})
    if not voucher:
        abort(404)

    return render_template("payment_voucher_view.html", v=voucher)
