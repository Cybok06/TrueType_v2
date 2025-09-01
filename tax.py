from flask import Blueprint, render_template, request, jsonify, send_file
from datetime import datetime, timedelta
from bson import ObjectId, errors
from io import BytesIO
from db import db
import calendar
import re
import requests

tax_bp = Blueprint("tax", __name__, template_folder="templates")

orders_col = db["orders"]
tax_col   = db["tax_records"]

# ---------- helpers ----------
def _f(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default

def _fmt(n):
    try:
        return f"{float(n):,.2f}"
    except Exception:
        return "0.00"

def _str_oid(v):
    try:
        return str(v) if isinstance(v, ObjectId) else str(ObjectId(v))
    except Exception:
        return None

def _month_buckets():
    return {m: 0.0 for m in list(calendar.month_name)[1:]}

def _ptax_per_l(order: dict) -> float:
    """Get P-Tax per litre from the order (supports 'p_tax' and 'p-tax')."""
    for k in ("p_tax", "p-tax"):
        if k in order and order.get(k) is not None:
            val = _f(order.get(k), None)
            if val is not None:
                return float(val)
    return 0.0

def _order_ptax_due(order: dict) -> float:
    """Due = P-Tax per L × quantity."""
    q = _f(order.get("quantity"), 0.0)
    ptax = _ptax_per_l(order)
    return round(q * ptax, 2)

def _parse_date_start(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None

def _parse_date_end(s):
    """Parse and set to end-of-day so filters include the full 'to' date."""
    dt = _parse_date_start(s)
    if not dt:
        return None
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)

def _paid_type_query():
    # robust & index-friendly: match p-tax, p_tax, p tax (any case)
    return {"type": {"$regex": r"^p[\s_-]*tax$", "$options": "i"}}

def _paid_sum_for_order(oid: ObjectId) -> float:
    """Sum of all P-Tax payments recorded for this order."""
    try:
        pipe = [
            {"$match": {"order_oid": oid, **_paid_type_query()}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
        ]
        row = next(tax_col.aggregate(pipe), None)
        return float(row.get("total", 0.0)) if row else 0.0
    except Exception:
        return 0.0

# ---------- views ----------
@tax_bp.route("/tax", methods=["GET"])
def tax_dashboard():
    # ===== UNPAID (and partially paid) =====
    # Eligible orders: those with a positive p_tax per litre (robust to field name)
    base_query = {
        "$or": [
            {"p_tax": {"$gt": 0}},
            {"p-tax": {"$gt": 0}},
        ]
    }
    projection = {
        "_id": 1, "order_id": 1, "omc": 1, "quantity": 1,
        "p_tax": 1, "p-tax": 1,
        "due_date": 1, "date": 1,
    }
    eligible_orders = list(orders_col.find(base_query, projection).sort("date", -1))

    unpaid_rows, total_unpaid_sum = [], 0.0
    for o in eligible_orders:
        oid = o.get("_id")
        due = _order_ptax_due(o)              # p_tax × qty
        already_paid = _paid_sum_for_order(oid)  # sum of all P-Tax payments
        remaining = max(0.0, round(due - already_paid, 2))

        if remaining > 0:
            total_unpaid_sum += remaining
            unpaid_rows.append({
                "_id": _str_oid(oid),
                "order_id": o.get("order_id", "—"),
                "omc": o.get("omc", "—"),
                "due_amount": remaining,
                "due_amount_fmt": _fmt(remaining),
                "payment_status": "Pending" if already_paid == 0 else "Partially Paid",
                "payment_badge": "warning" if already_paid == 0 else "info",
                "due_date": o.get("due_date"),
                "date": o.get("date"),
                "quantity_fmt": _fmt(_f(o.get("quantity"), 0.0)),
                # Keep the original key name most templates use; now it shows P-Tax per L
                "s_price_fmt": _fmt(_ptax_per_l(o)),
                "already_paid_fmt": _fmt(already_paid),
            })

    # ===== FILTERS for PAID table =====
    omc_f      = (request.args.get("omc") or "").strip()
    paid_by_f  = (request.args.get("paid_by") or "").strip()
    date_from_s= (request.args.get("date_from") or "").strip()
    date_to_s  = (request.args.get("date_to") or "").strip()
    amt_min_s  = (request.args.get("amount_min") or "").strip()
    amt_max_s  = (request.args.get("amount_max") or "").strip()

    paid_query = {"$and": [_paid_type_query()]}
    if omc_f:
        paid_query["$and"].append({"omc": omc_f})
    if paid_by_f:
        paid_query["$and"].append({"paid_by": {"$regex": re.escape(paid_by_f), "$options": "i"}})

    df = _parse_date_start(date_from_s)
    dt = _parse_date_end(date_to_s)
    if df and dt:
        paid_query["$and"].append({"payment_date": {"$gte": df, "$lte": dt}})
    elif df:
        paid_query["$and"].append({"payment_date": {"$gte": df}})
    elif dt:
        paid_query["$and"].append({"payment_date": {"$lte": dt}})

    try:
        if amt_min_s:
            paid_query["$and"].append({"amount": {"$gte": float(amt_min_s)}})
    except ValueError:
        pass
    try:
        if amt_max_s:
            paid_query["$and"].append({"amount": {"$lte": float(amt_max_s)}})
    except ValueError:
        pass

    # ===== PAID rows (filtered) =====
    paid_rows, total_paid_sum = [], 0.0
    for t in tax_col.find(paid_query, {
        "_id": 0, "type": 1, "amount": 1, "payment_date": 1, "reference": 1,
        "paid_by": 1, "omc": 1, "order_id": 1, "order_oid": 1
    }).sort("payment_date", -1):
        amt = _f(t.get("amount"), 0.0)
        total_paid_sum += amt
        pd = t.get("payment_date")
        if isinstance(pd, str):
            try:
                pd_dt = datetime.strptime(pd, "%Y-%m-%d")
            except Exception:
                pd_dt = None
        else:
            pd_dt = pd if isinstance(pd, datetime) else None

        paid_rows.append({
            "omc": t.get("omc", "—"),
            "order_id": t.get("order_id", "—"),
            "amount": amt,
            "amount_fmt": _fmt(amt),
            "payment_date": pd,
            "payment_date_str": pd_dt.strftime("%Y-%m-%d") if pd_dt else str(pd or "—"),
            "reference": t.get("reference", "—"),
            "paid_by": t.get("paid_by", "—"),
        })

    # ===== CARDS: totals per OMC (ALL P-Tax, not filtered) =====
    pipeline_cards = [
        {"$match": _paid_type_query()},
        {"$group": {"_id": "$omc", "total": {"$sum": "$amount"}}},
        {"$sort": {"total": -1}},
    ]
    omc_cards = []
    for d in tax_col.aggregate(pipeline_cards):
        name = d.get("_id") or "—"
        total = float(d.get("total") or 0.0)
        if total > 0:
            omc_cards.append({"omc": name, "total": total, "total_fmt": _fmt(total)})

    # ===== Trend (all P-Tax) =====
    trend = _month_buckets()
    for row in tax_col.find(_paid_type_query(), {"amount": 1, "payment_date": 1}):
        dtp = row.get("payment_date")
        try:
            if isinstance(dtp, str):
                try:
                    dtp = datetime.strptime(dtp, "%Y-%m-%d")
                except Exception:
                    continue
            if not isinstance(dtp, datetime):
                continue
            trend[calendar.month_name[dtp.month]] += _f(row.get("amount"), 0.0)
        except Exception:
            continue

    return render_template(
        "partials/tax_dashboard.html",
        unpaid_rows=unpaid_rows,
        total_unpaid_sum=_fmt(total_unpaid_sum),
        paid_rows=paid_rows,
        total_paid_sum=_fmt(total_paid_sum),
        omc_cards=omc_cards,
        filters={
            "omc": omc_f, "paid_by": paid_by_f,
            "date_from": date_from_s, "date_to": date_to_s,
            "amount_min": amt_min_s, "amount_max": amt_max_s,
        },
        trend_data=trend
    )

@tax_bp.route("/tax/pay", methods=["POST"])
def pay_ptax():
    """
    Accepts partial payments. Validates:
    - order exists and has P-Tax
    - amount > 0
    - amount <= remaining
    Inserts payment (type=P-Tax) and updates order flags when fully paid.
    Returns JSON with due, already_paid (after), remaining (after).
    """
    try:
        order_oid = (request.form.get("order_oid") or "").strip()
        amount = _f(request.form.get("amount"))
        reference = (request.form.get("reference") or "").strip()
        paid_by = (request.form.get("paid_by") or "").strip()
        payment_date_str = (request.form.get("payment_date") or "").strip()

        if not order_oid:
            return jsonify({"status": "error", "message": "Missing order id"}), 400
        try:
            oid = ObjectId(order_oid)
        except (errors.InvalidId, Exception):
            return jsonify({"status": "error", "message": "Invalid order id"}), 400

        order = orders_col.find_one({"_id": oid})
        if not order:
            return jsonify({"status": "error", "message": "Order not found"}), 404

        has_ptax_value = _ptax_per_l(order) > 0
        if not has_ptax_value:
            return jsonify({"status": "error", "message": "Order has no P-Tax to pay"}), 400

        due = _order_ptax_due(order)  # p_tax × qty
        if amount <= 0:
            return jsonify({"status": "error", "message": "Amount must be greater than 0"}), 400

        already_paid_before = _paid_sum_for_order(oid)
        remaining_before = max(0.0, round(due - already_paid_before, 2))
        if remaining_before <= 0:
            return jsonify({"status": "error", "message": "P-Tax already fully paid"}), 400

        if amount > remaining_before:
            return jsonify({"status": "error", "message": f"Amount exceeds remaining balance (GH₵ {_fmt(remaining_before)})"}), 400

        # payment date
        pay_dt = datetime.utcnow()
        if payment_date_str:
            try:
                pay_dt = datetime.strptime(payment_date_str, "%Y-%m-%d")
            except ValueError:
                return jsonify({"status": "error", "message": "Invalid payment date"}), 400

        # insert payment as P-Tax
        tax_col.insert_one({
            "type": "P-Tax",
            "amount": round(float(amount), 2),
            "payment_date": pay_dt,
            "reference": reference or None,
            "paid_by": paid_by or None,
            "omc": order.get("omc"),
            "order_id": order.get("order_id"),
            "order_oid": oid,
            "submitted_at": datetime.utcnow()
        })

        # recompute totals AFTER insert
        already_paid_after = _paid_sum_for_order(oid)
        remaining_after = max(0.0, round(due - already_paid_after, 2))

        # Set canonical P-Tax flags; also mirror to s_tax_* to keep legacy readers safe
        update_doc = {
            # canonical P-Tax fields
            "p_tax_paid_amount": round(float(already_paid_after), 2),
            "p_tax_paid_at": pay_dt,
            "p_tax_reference": reference or order.get("p_tax_reference"),
            "p_tax_paid_by": paid_by or order.get("p_tax_paid_by"),
            "p_tax_payment": "paid" if remaining_after <= 0 else "partial",
            "p-tax-payment": "paid" if remaining_after <= 0 else "partial",
            # legacy mirrors (optional; remove later when UI is migrated)
            "s_tax_paid_amount": round(float(already_paid_after), 2),
            "s_tax_paid_at": pay_dt,
            "s_tax_reference": reference or order.get("s_tax_reference"),
            "s_tax_paid_by": paid_by or order.get("s_tax_paid_by"),
            "s_tax_payment": "paid" if remaining_after <= 0 else "partial",
            "s-tax-payment": "paid" if remaining_after <= 0 else "partial",
        }
        orders_col.update_one({"_id": oid}, {"$set": update_doc})

        return jsonify({
            "status": "success",
            "due": round(due, 2),
            "already_paid": round(already_paid_after, 2),
            "remaining": round(remaining_after, 2)
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@tax_bp.route("/tax/add", methods=["POST"])
def add_tax():
    try:
        tax_type     = (request.form.get("type") or "").strip()
        amount       = _f(request.form.get("amount"))
        payment_date = (request.form.get("payment_date") or "").strip()
        reference    = (request.form.get("reference") or "").strip() or None
        paid_by      = (request.form.get("paid_by") or "").strip() or None

        if not tax_type:
            return jsonify({"status": "error", "message": "Type is required"}), 400
        if amount <= 0:
            return jsonify({"status": "error", "message": "Amount must be greater than 0"}), 400

        pay_dt = datetime.utcnow()
        if payment_date:
            try:
                pay_dt = datetime.strptime(payment_date, "%Y-%m-%d")
            except ValueError:
                return jsonify({"status": "error", "message": "Invalid payment date"}), 400

        new_tax = {
            "type": tax_type,  # allow manual entries for P-Tax or others
            "amount": round(amount, 2),
            "payment_date": pay_dt,
            "reference": reference,
            "paid_by": paid_by,
            "submitted_at": datetime.utcnow()
        }
        tax_col.insert_one(new_tax)
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ===== Export filtered PAID transactions to PDF (server-side) =====
@tax_bp.route("/tax/export.pdf", methods=["GET"])
def export_tax_pdf():
    omc_f      = (request.args.get("omc") or "").strip()
    paid_by_f  = (request.args.get("paid_by") or "").strip()
    date_from_s= (request.args.get("date_from") or "").strip()
    date_to_s  = (request.args.get("date_to") or "").strip()
    amt_min_s  = (request.args.get("amount_min") or "").strip()
    amt_max_s  = (request.args.get("amount_max") or "").strip()

    q = {"$and": [_paid_type_query()]}
    if omc_f:     q["$and"].append({"omc": omc_f})
    if paid_by_f: q["$and"].append({"paid_by": {"$regex": re.escape(paid_by_f), "$options": "i"}})

    df = _parse_date_start(date_from_s)
    dt = _parse_date_end(date_to_s)
    if df and dt:   q["$and"].append({"payment_date": {"$gte": df, "$lte": dt}})
    elif df:        q["$and"].append({"payment_date": {"$gte": df}})
    elif dt:        q["$and"].append({"payment_date": {"$lte": dt}})

    try:
        if amt_min_s: q["$and"].append({"amount": {"$gte": float(amt_min_s)}})
    except ValueError:
        pass
    try:
        if amt_max_s: q["$and"].append({"amount": {"$lte": float(amt_max_s)}})
    except ValueError:
        pass

    rows = list(tax_col.find(q).sort("payment_date", -1))

    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.pdfgen import canvas
        from reportlab.lib.units import mm
        from reportlab.lib.utils import ImageReader
        from reportlab.pdfbase.pdfmetrics import stringWidth

        # Try to fetch logo (AVIF may fail; ignore if so)
        logo_bytes = None
        try:
            resp = requests.get("https://res.cloudinary.com/dl2ipzxyk/image/upload/v1751107241/logo_ijmteg.avif", timeout=6)
            if resp.ok:
                logo_bytes = resp.content
        except Exception:
            logo_bytes = None

        buf = BytesIO()
        W, H = landscape(A4)
        left_margin, right_margin = 15*mm, 15*mm
        top_margin, bottom_margin = 12*mm, 12*mm
        table_width = W - left_margin - right_margin

        col_defs = [
            ("Date", 28*mm),
            ("OMC", 70*mm),
            ("Order ID", 28*mm),
            ("Paid By", 38*mm),
            ("Reference", 75*mm),
            ("Amount (GH₵)", 30*mm),
        ]
        total_widths = sum(w for _, w in col_defs)
        scale = table_width / total_widths
        col_defs = [(h, w*scale) for h, w in col_defs]

        def draw_header(c, page_num):
            y = H - top_margin
            x = left_margin
            if logo_bytes:
                try:
                    img = ImageReader(BytesIO(logo_bytes))
                    logo_h = 10*mm
                    iw, ih = img.getSize()
                    ratio = logo_h / ih
                    logo_w = iw * ratio
                    c.drawImage(img, x, y - logo_h, width=logo_w, height=logo_h, mask='auto')
                    x += logo_w + 6
                except Exception:
                    pass
            c.setFont("Helvetica-Bold", 13)
            c.drawString(x, y - 3*mm, "TrueType Services")
            c.setFont("Helvetica", 10)
            title = "P-Tax Payments Report"
            if omc_f:
                title += f" — {omc_f}"
            c.drawString(left_margin, y - 12*mm, title)
            c.setFont("Helvetica", 8)
            c.drawRightString(W - right_margin, y - 12*mm, f"Generated: {datetime.utcnow():%Y-%m-%d %H:%M UTC}  |  Page {page_num}")

            c.setLineWidth(0.6)
            header_y = y - 18*mm
            c.line(left_margin, header_y, left_margin + table_width, header_y)
            c.setFont("Helvetica-Bold", 9)
            xh = left_margin
            for head, w in col_defs:
                c.drawString(xh + 2, header_y - 9, head)
                xh += w
            c.line(left_margin, header_y - 12, left_margin + table_width, header_y - 12)
            return header_y - 14

        def draw_table(c):
            y = draw_header(c, draw_table.page)
            c.setFont("Helvetica", 9)
            line_height = 10
            x_cols = [left_margin]
            for _, w in col_defs:
                x_cols.append(x_cols[-1] + w)

            total_amt = 0.0
            for r in rows:
                if y < bottom_margin + 20*mm:
                    c.showPage()
                    draw_table.page += 1
                    y = draw_header(c, draw_table.page)
                    c.setFont("Helvetica", 9)

                pd = r.get("payment_date")
                date_str = pd.strftime("%Y-%m-%d") if isinstance(pd, datetime) else str(pd or "—")
                vals = [
                    date_str,
                    r.get("omc", "—"),
                    r.get("order_id", "—"),
                    r.get("paid_by", "—"),
                    r.get("reference", "—"),
                    _fmt(_f(r.get("amount"), 0.0)),
                ]
                for i, val in enumerate(vals):
                    txt = str(val)
                    maxw = col_defs[i][1] - 4
                    while stringWidth(txt, "Helvetica", 9) > maxw and len(txt) > 3:
                        txt = txt[:-4] + "…"
                    x = x_cols[i] + 2
                    c.drawString(x, y, txt)
                c.setLineWidth(0.3)
                c.line(left_margin, y - 2, left_margin + table_width, y - 2)
                y -= line_height
                total_amt += _f(r.get("amount"), 0.0)

            c.setFont("Helvetica-Bold", 9)
            c.line(left_margin, y - 2, left_margin + table_width, y - 2)
            c.drawRightString(x_cols[-1], y - 10, f"TOTAL: GH₵ {_fmt(total_amt)}")

        c = canvas.Canvas(buf, pagesize=landscape(A4))
        draw_table.page = 1
        draw_table(c)
        c.showPage()
        c.save()
        buf.seek(0)

        safe_omc = re.sub(r'\W+', '_', omc_f.lower()) if omc_f else ""
        filename = f"p_tax_payments_{safe_omc}.pdf" if safe_omc else "p_tax_payments.pdf"
        return send_file(buf, mimetype="application/pdf", as_attachment=True, download_name=filename)
    except Exception as e:
        return jsonify({"status": "error", "message": f"PDF generation failed: {e}. Install 'reportlab'."}), 500
