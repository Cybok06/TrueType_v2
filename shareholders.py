from flask import Blueprint, render_template, request, jsonify
from datetime import datetime, timedelta
from collections import defaultdict
from bson import ObjectId
from db import db

shareholders_bp = Blueprint('shareholders', __name__, template_folder='templates')

orders_col = db['orders']
shared_tax_col = db['shared_tax']  # manual tax rates storage

# ---------------------
# Config
# ---------------------
SHAREHOLDERS = ["Rex", "Simon", "Paul"]
SHARE_SPLIT = {"Rex": 0.35, "Simon": 0.35, "Paul": 0.30}  # used ONLY for splitting NPA Component

# ---------------------
# Helpers
# ---------------------
def _f(v):
    """Parse to float, handle strings like '12,300.50' and None."""
    try:
        if isinstance(v, str):
            v = v.replace(",", "").strip()
        return float(v)
    except Exception:
        return 0.0


def _today_utc():
    now = datetime.utcnow()
    return datetime(now.year, now.month, now.day)


def _is_neutral_shareholder(val) -> bool:
    """Case-insensitive Neutral check."""
    try:
        return str(val).strip().lower() == "neutral"
    except Exception:
        return False


def _order_total_returns(o):
    """
    Use new 'total_returns' if present, else 'returns_total', else fallback margin*qty.
    NOTE: This returns the numeric value ONLY; callers decide whether to include/exclude based on shareholder.
    """
    if o.get("total_returns") is not None:
        return _f(o.get("total_returns"))
    if o.get("returns_total") is not None:
        return _f(o.get("returns_total"))
    return round(_f(o.get("margin")) * _f(o.get("quantity")), 2)


def month_range(ym):
    """'YYYY-MM' -> (start_dt, end_dt_exclusive)"""
    y, m = [int(x) for x in ym.split("-")]
    start = datetime(y, m, 1)
    end = datetime(y + (m // 12), (m % 12) + 1, 1)
    return start, end


def distinct_products():
    prods = orders_col.distinct("product")
    return sorted([p for p in prods if isinstance(p, str) and p.strip()])

# ---------------------
# Existing summary blocks (with Neutral returns excluded)
# ---------------------
def filter_orders_for_returns(period, start_date, end_date):
    now = datetime.utcnow()
    query = {"status": "approved"}

    if period == "today":
        query["date"] = {"$gte": _today_utc()}
    elif period == "week":
        query["date"] = {"$gte": now - timedelta(days=7)}
    elif period == "month":
        query["date"] = {"$gte": datetime(now.year, now.month, 1)}
    elif period == "custom" and start_date and end_date:
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
            query["date"] = {"$gte": start, "$lt": end}
        except ValueError:
            pass
    # else 'all' => no date filter
    return list(orders_col.find(query))


def build_contributions(orders):
    """Totals and per-shareholder contributions. Neutral *returns* are excluded from all totals and percentages.
    Order and quantity counts continue to include only named shareholders listed in SHAREHOLDERS.
    """
    # Compute totals excluding Neutral returns
    eligible_orders = [o for o in orders if not _is_neutral_shareholder(o.get("shareholder"))]

    total_orders = len(orders)  # unchanged: overall approved orders in period
    total_quantity = sum(_f(order.get("quantity")) for order in orders)
    total_returns = round(sum(_order_total_returns(order) for order in eligible_orders), 2)

    contributions = {name: {"orders": 0, "quantity": 0, "returns": 0.0} for name in SHAREHOLDERS}
    for order in eligible_orders:
        name = order.get("shareholder")
        if name not in contributions:
            continue  # ignore unknown shareholders and Neutral
        qty = _f(order.get("quantity"))
        ret = _order_total_returns(order)
        contributions[name]["orders"] += 1
        contributions[name]["quantity"] += int(round(qty))
        contributions[name]["returns"] += round(ret, 2)

    for name in SHAREHOLDERS:
        returns = contributions[name]["returns"]
        contributions[name]["percentage_of_returns"] = round((returns / total_returns) * 100, 2) if total_returns else 0.0

    # Shared returns pool (if you still display this) now excludes Neutral amounts
    shared_returns = {name: round(SHARE_SPLIT[name] * total_returns, 2) for name in SHAREHOLDERS}

    return total_orders, int(round(total_quantity)), total_returns, contributions, shared_returns


def build_volume_data(volume_period, volume_start, volume_end):
    now = datetime.utcnow()
    volume_query = {"status": "approved"}

    if volume_period == "today":
        volume_query["date"] = {"$gte": _today_utc()}
    elif volume_period == "week":
        volume_query["date"] = {"$gte": now - timedelta(days=7)}
    elif volume_period == "month":
        volume_query["date"] = {"$gte": datetime(now.year, now.month, 1)}
    elif volume_period == "custom" and volume_start and volume_end:
        try:
            vs = datetime.strptime(volume_start, "%Y-%m-%d")
            ve = datetime.strptime(volume_end, "%Y-%m-%d") + timedelta(days=1)
            volume_query["date"] = {"$gte": vs, "$lt": ve}
        except ValueError:
            pass
    # else 'all' => no date filter

    volume_orders = list(orders_col.find(volume_query))
    volume_data = defaultdict(int)
    for order in volume_orders:
        name = order.get("shareholder")
        if name in SHAREHOLDERS:  # Neutral excluded implicitly
            volume_data[name] += int(round(_f(order.get("quantity"))))

    return volume_data

# ---------------------
# Tax storage (shared_tax collection)
# ---------------------
def load_shared_tax(product):
    doc = shared_tax_col.find_one({"product": product})
    if not doc:
        return None
    return {
        "total_tax": _f(doc.get("total_tax")),
        "gra_tax": _f(doc.get("gra_tax")),
        "npa_life_tax": _f(doc.get("npa_life_tax")),
        "npa_component_tax": _f(doc.get("npa_component_tax")),
    }


def save_shared_tax(product, total_tax, gra_tax, npa_life_tax, npa_component_tax):
    shared_tax_col.update_one(
        {"product": product},
        {"$set": {
            "product": product,
            "total_tax": _f(total_tax),
            "gra_tax": _f(gra_tax),
            "npa_life_tax": _f(npa_life_tax),
            "npa_component_tax": _f(npa_component_tax),
            "updated_at": datetime.utcnow()
        }},
        upsert=True
    )


def derive_rates_for_product(product, start, end):
    # 1) overrides
    qt = request.args.get("total_tax_override")
    qg = request.args.get("gra_tax_override")
    qnl = request.args.get("npa_life_override")
    qnc = request.args.get("npa_component_override")
    if all(x is not None and str(x).strip() != "" for x in [qt, qg, qnl, qnc]):
        return _f(qt), _f(qg), _f(qnl), _f(qnc)

    # 2) shared_tax storage
    stored = load_shared_tax(product)
    if stored:
        total_tax = stored.get("total_tax", 0.0)
        gra_tax = stored.get("gra_tax", 0.0)
        npa_life_tax = stored.get("npa_life_tax", None)
        npa_component_tax = stored.get("npa_component_tax", None)
        if npa_life_tax is None:
            npa_life_tax = max(total_tax - gra_tax, 0.0)
        if npa_component_tax is None:
            npa_component_tax = max(total_tax - gra_tax, 0.0)
        return total_tax, gra_tax, npa_life_tax, npa_component_tax

    # No data
    return 0.0, 0.0, 0.0, 0.0

# ---------------------
# Per‑product monthly/custom tax breakdown (+ multi‑product)
# ---------------------
def parse_tax_period_args():
    month = (request.args.get("month_tax") or "").strip()
    s = (request.args.get("custom_tax_start") or "").strip()
    e = (request.args.get("custom_tax_end") or "").strip()

    if month:
        try:
            start, end = month_range(month)
            return start, end, "month", month
        except Exception:
            pass

    if s and e:
        try:
            start = datetime.strptime(s, "%Y-%m-%d")
            end = datetime.strptime(e, "%Y-%m-%d") + timedelta(days=1)
            return start, end, "custom", None
        except ValueError:
            pass

    now = datetime.utcnow()
    start = datetime(now.year, now.month, 1)
    end = datetime(now.year + (now.month // 12), (now.month % 12) + 1, 1)
    return start, end, "month", f"{now.year:04d}-{now.month:02d}"


def parse_selected_products():
    products = request.args.getlist("tax_product")
    if not products:
        csv = (request.args.get("tax_products") or "").strip()
        if csv:
            products = [p.strip() for p in csv.split(",") if p.strip()]
    if not products:
        single = (request.args.get("tax_product") or "").strip()
        if single:
            products = [single]

    if len(products) == 1 and products[0].lower() == "all":
        products = distinct_products()

    dp = set(distinct_products())
    products = [p for p in products if p in dp]
    if not products and dp:
        products = [sorted(dp)[0]]
    return products


def fetch_orders_for_tax(product, start, end):
    q_base = {"status": "approved", "product": product, "date": {"$gte": start, "$lt": end}}
    # Case-insensitive Neutral handling
    neutral_regex = {"$regex": "^neutral$", "$options": "i"}
    main_q = dict(q_base, **{"shareholder": {"$not": neutral_regex}})
    neutral_q = dict(q_base, **{"shareholder": neutral_regex})
    main_orders = list(orders_col.find(main_q))
    neutral_orders = list(orders_col.find(neutral_q))
    return main_orders, neutral_orders


def summarize_orders_for_tax(orders):
    total_volume = 0
    total_returns = 0.0
    for o in orders:
        total_volume += int(round(_f(o.get("quantity"))))
        total_returns += _order_total_returns(o)
    return total_volume, round(total_returns, 2)


def build_tax_breakdown_for_product(product, start, end):
    main_orders, neutral_orders = fetch_orders_for_tax(product, start, end)
    vol_main, _ = summarize_orders_for_tax(main_orders)
    vol_neutral, returns_neutral = summarize_orders_for_tax(neutral_orders)

    total_tax, gra_tax, npa_life_tax, npa_component_tax = derive_rates_for_product(product, start, end)

    life_component_tax = max(npa_life_tax - npa_component_tax, 0.0)

    # Amounts (exclude neutral by using vol_main only)
    amt_total_tax = round(total_tax * vol_main, 2)
    amt_gra_tax = round(gra_tax * vol_main, 2)
    amt_npa_life = round(npa_life_tax * vol_main, 2)
    amt_life_component = round(life_component_tax * vol_main, 2)
    amt_npa_component = round(npa_component_tax * vol_main, 2)

    rows = [
        {"label": "Total Tax",       "rate": round(total_tax, 4),        "amount": amt_total_tax},
        {"label": "GRA",             "rate": round(gra_tax, 4),          "amount": amt_gra_tax},
        {"label": "NPA/Life",        "rate": round(npa_life_tax, 4),     "amount": amt_npa_life},
        {"label": "Life Component",  "rate": round(life_component_tax,4),"amount": amt_life_component},
        {"label": "NPA Component",   "rate": round(npa_component_tax,4), "amount": amt_npa_component},
    ]

    # Split strictly on NPA Component (exclude neutral volume)
    split_rows = []
    for name, pct in SHARE_SPLIT.items():
        rate = round(npa_component_tax * pct, 4)   # per L
        amount = round(rate * vol_main, 2)         # total for that shareholder
        split_rows.append({"name": name, "percent": int(pct*100), "rate": rate, "amount": amount})

    return {
        "product": product,
        "volume_main": vol_main,
        "volume_neutral": vol_neutral,
        "neutral_total_returns": returns_neutral,
        "rates": {
            "total_tax": round(total_tax, 4),
            "gra_tax": round(gra_tax, 4),
            "npa_life": round(npa_life_tax, 4),
            "npa_component": round(npa_component_tax, 4),
            "life_component": round(life_component_tax, 4),
        },
        "rows": rows,
        "split_rows": split_rows
    }


def build_tax_breakdown(products, start, end):
    return [build_tax_breakdown_for_product(p, start, end) for p in products]

# ✅ Main Route
@shareholders_bp.route('/shareholders')
def view_shareholders():
    # Summary filters
    period = request.args.get("period", "all")
    start_date = request.args.get("start")
    end_date = request.args.get("end")

    # Volume chart filters
    volume_period = request.args.get("volume_period", "all")
    volume_start = request.args.get("volume_start")
    volume_end = request.args.get("volume_end")

    # Per‑product tax filters
    tax_start, tax_end, tax_period_kind, tax_month_str = parse_tax_period_args()
    selected_products = parse_selected_products()
    all_products = distinct_products()

    # Build existing sections
    orders = filter_orders_for_returns(period, start_date, end_date)
    total_orders, total_quantity, total_returns, contributions, shared_returns = build_contributions(orders)
    volume_data = build_volume_data(volume_period, volume_start, volume_end)

    # Build new (multi‑product) tax section
    tax_breakdowns = build_tax_breakdown(selected_products, tax_start, tax_end)

    return render_template(
        "partials/shareholders.html",
        total_orders=total_orders,
        total_quantity=total_quantity,
        total_returns=total_returns,                  # Neutral returns excluded
        contributions=contributions,
        shared_returns=shared_returns,                # Based on excluded total_returns
        period=period,
        start_date=start_date,
        end_date=end_date,
        volume_data=volume_data,
        volume_period=volume_period,
        volume_start=volume_start,
        volume_end=volume_end,
        tax_all_products=all_products,
        tax_selected_products=selected_products,
        tax_breakdowns=tax_breakdowns,
        tax_period_kind=tax_period_kind,
        tax_month_str=tax_month_str,
        tax_start_str=tax_start.strftime("%Y-%m-%d"),
        tax_end_str=(tax_end - timedelta(days=1)).strftime("%Y-%m-%d"),
    )

# ✅ Upsert manual tax rates
@shareholders_bp.route('/shareholders/shared_tax_update', methods=['POST'])
def shareholders_shared_tax_update():
    data = request.get_json(silent=True) or request.form
    product = (data.get('product') or '').strip()
    total_tax = data.get('total_tax')
    gra_tax = data.get('gra_tax')
    npa_life_tax = data.get('npa_life_tax')
    npa_component_tax = data.get('npa_component_tax')

    if not product:
        return jsonify({"success": False, "error": "product is required"}), 400
    missing = [k for k, v in {
        "total_tax": total_tax,
        "gra_tax": gra_tax,
        "npa_life_tax": npa_life_tax,
        "npa_component_tax": npa_component_tax
    }.items() if v is None]
    if missing:
        return jsonify({"success": False, "error": f"missing fields: {', '.join(missing)}"}), 400

    try:
        save_shared_tax(product, total_tax, gra_tax, npa_life_tax, npa_component_tax)
        saved = load_shared_tax(product) or {
            "total_tax": _f(total_tax),
            "gra_tax": _f(gra_tax),
            "npa_life_tax": _f(npa_life_tax),
            "npa_component_tax": _f(npa_component_tax),
        }
        return jsonify({"success": True, "product": product, "saved": saved})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ✅ Debug JSON
@shareholders_bp.route("/shareholders/tax_debug.json")
def shareholders_tax_debug():
    selected_products = parse_selected_products()
    tax_start, tax_end, tax_period_kind, tax_month_str = parse_tax_period_args()

    blocks = build_tax_breakdown(selected_products, tax_start, tax_end)
    return {
        "products": selected_products,
        "period_kind": tax_period_kind,
        "month": tax_month_str,
        "start": tax_start.strftime("%Y-%m-%d"),
        "end_inclusive": (tax_end - timedelta(days=1)).strftime("%Y-%m-%d"),
        "breakdowns": blocks
    }
