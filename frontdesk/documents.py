# frontdesk/documents.py
from flask import Blueprint, render_template, request, jsonify, send_file, abort, url_for
from bson import ObjectId
from datetime import datetime, timedelta
from io import BytesIO
import gridfs, secrets, re

from db import db  # your existing Mongo connection

DOCS_META   = db["fd_docs"]           # file metadata (one per upload)
DOCS_SHARES = db["fd_doc_shares"]     # share tokens
fs = gridfs.GridFS(db, collection="hr_storage")  # reuse same GridFS bucket

documents_bp = Blueprint(
    "documents_bp",
    __name__,
    url_prefix="/frontdesk"
)

# ---------------- Helpers ----------------
def _now():
    return datetime.utcnow()

def _parse_date(s):
    if not s: return None
    try:
        # Accept YYYY-MM-DD or ISO
        if len(s) == 10:
            return datetime.strptime(s, "%Y-%m-%d")
        return datetime.fromisoformat(s.replace("Z",""))
    except Exception:
        return None

def _split_tags(raw):
    if not raw: return []
    if isinstance(raw, list):
        parts = raw
    else:
        parts = re.split(r"[,\n]+", str(raw))
    clean, seen = [], set()
    for p in [x.strip() for x in parts]:
        if not p: continue
        k = p.lower()
        if k in seen: continue
        seen.add(k)
        clean.append(p)
    return clean

def _serialize_meta(m):
    return {
        "id": str(m["_id"]),
        "title": m.get("title") or m.get("filename"),
        "filename": m.get("filename"),
        "mimetype": m.get("mimetype"),
        "size": m.get("size") or 0,
        "category": m.get("category"),
        "tags": m.get("tags") or [],
        "notes": m.get("notes") or "",
        "uploaded_at": m.get("uploaded_at").isoformat() if m.get("uploaded_at") else None,
        "uploader": m.get("uploader"),
        "is_image": bool((m.get("mimetype") or "").startswith("image/")),
        "is_pdf": (m.get("mimetype") or "").lower().startswith("application/pdf"),
    }

# ---------------- Page ----------------
@documents_bp.get("/documents")
def documents_page():
    return render_template("frontdesk_pages/documents.html")

# ---------------- APIs ----------------
@documents_bp.post("/api/docs")
def api_docs_upload():
    """
    Multipart form:
      - file (required)
      - title, category, tags (comma or array), notes
    """
    if "file" not in request.files:
        return jsonify({"error":"no file"}), 400
    file = request.files["file"]
    title = request.form.get("title") or file.filename
    category = (request.form.get("category") or "").strip() or None
    tags = _split_tags(request.form.get("tags"))
    notes = request.form.get("notes") or ""

    # Optional: pull user from session if available
    uploader = None
    try:
        from flask import session
        uploader = session.get("username")
    except Exception:
        pass

    blob_id = fs.put(file.stream, filename=file.filename, content_type=file.mimetype)
    meta = {
        "file_id": blob_id,
        "title": title,
        "filename": file.filename,
        "mimetype": file.mimetype or "application/octet-stream",
        "size": file.content_length,
        "category": category,
        "tags": tags,
        "notes": notes,
        "uploader": uploader,
        "uploaded_at": _now(),
    }
    ins = DOCS_META.insert_one(meta)
    meta["_id"] = ins.inserted_id
    return jsonify({"ok": True, "doc": _serialize_meta(meta)}), 201

@documents_bp.get("/api/docs")
def api_docs_list():
    """
    Query params:
      q, tag, category, type (image|pdf|other), date_from, date_to, sort, page, per
    """
    q = (request.args.get("q") or "").strip().lower()
    tag = (request.args.get("tag") or "").strip()
    category = (request.args.get("category") or "").strip()
    ftype = (request.args.get("type") or "").strip().lower()
    date_from = _parse_date(request.args.get("date_from"))
    date_to = _parse_date(request.args.get("date_to"))
    sort = (request.args.get("sort") or "-uploaded_at").strip().lower()  # -uploaded_at (default), uploaded_at, title
    page = max(1, int(request.args.get("page") or 1))
    per  = min(60, max(6, int(request.args.get("per") or 24)))

    q_mongo = {}
    if tag:
        q_mongo["tags"] = tag
    if category:
        q_mongo["category"] = category
    if ftype == "image":
        q_mongo["mimetype"] = {"$regex": r"^image/", "$options":"i"}
    elif ftype == "pdf":
        q_mongo["mimetype"] = {"$regex": r"^application/pdf$", "$options":"i"}
    elif ftype == "other":
        q_mongo["mimetype"] = {"$not": {"$regex": r"^(image/|application/pdf)$", "$options":"i"}}

    if date_from and date_to:
        q_mongo["uploaded_at"] = {"$gte": date_from, "$lte": date_to + timedelta(days=1)}
    elif date_from:
        q_mongo["uploaded_at"] = {"$gte": date_from}
    elif date_to:
        q_mongo["uploaded_at"] = {"$lte": date_to + timedelta(days=1)}

    # text-ish filter (title/filename/tags/notes/category/uploader)
    if q:
        q_mongo["$or"] = [
            {"title": {"$regex": re.escape(q), "$options":"i"}},
            {"filename": {"$regex": re.escape(q), "$options":"i"}},
            {"notes": {"$regex": re.escape(q), "$options":"i"}},
            {"category": {"$regex": re.escape(q), "$options":"i"}},
            {"tags": {"$elemMatch": {"$regex": re.escape(q), "$options":"i"}}},
            {"uploader": {"$regex": re.escape(q), "$options":"i"}},
        ]

    sort_spec = [("uploaded_at", -1)]
    if sort == "uploaded_at":
        sort_spec = [("uploaded_at", 1)]
    elif sort == "title":
        sort_spec = [("title", 1)]
    elif sort == "-title":
        sort_spec = [("title", -1)]
    elif sort == "size":
        sort_spec = [("size", 1)]
    elif sort == "-size":
        sort_spec = [("size", -1)]

    total = DOCS_META.count_documents(q_mongo)
    docs = DOCS_META.find(q_mongo).sort(sort_spec).skip((page-1)*per).limit(per)
    items = [_serialize_meta(x) for x in docs]
    return jsonify({"items": items, "page": page, "per": per, "total": total})

@documents_bp.get("/api/docs/taxonomy")
def api_docs_taxonomy():
    # Distinct tags and categories for filters
    tags = sorted({t for d in DOCS_META.find({}, {"tags":1}) for t in (d.get("tags") or [])}, key=lambda s: s.lower())
    cats = sorted({(d.get("category") or "") for d in DOCS_META.find({}, {"category":1}) if d.get("category")}, key=lambda s: s.lower())
    return jsonify({"tags": tags, "categories": cats})

@documents_bp.get("/api/docs/<meta_id>/download")
def api_docs_download(meta_id):
    try:
        mid = ObjectId(meta_id)
    except Exception:
        return abort(400)
    m = DOCS_META.find_one({"_id": mid})
    if not m: return abort(404)
    try:
        blob = fs.get(m["file_id"])
    except Exception:
        return abort(404)

    return send_file(
        BytesIO(blob.read()),
        mimetype=m.get("mimetype") or "application/octet-stream",
        download_name=m.get("filename") or "file.bin",
        as_attachment=True
    )

@documents_bp.get("/api/docs/<meta_id>/preview")
def api_docs_preview(meta_id):
    """Inline preview for images/PDF."""
    try:
        mid = ObjectId(meta_id)
    except Exception:
        return abort(400)
    m = DOCS_META.find_one({"_id": mid})
    if not m: return abort(404)
    try:
        blob = fs.get(m["file_id"])
    except Exception:
        return abort(404)
    return send_file(
        BytesIO(blob.read()),
        mimetype=m.get("mimetype") or "application/octet-stream",
        download_name=m.get("filename") or "preview",
        as_attachment=False
    )

@documents_bp.delete("/api/docs/<meta_id>")
def api_docs_delete(meta_id):
    try:
        mid = ObjectId(meta_id)
    except Exception:
        return jsonify({"error":"invalid id"}), 400
    m = DOCS_META.find_one({"_id": mid})
    if not m: return jsonify({"error":"not found"}), 404
    try:
        fs.delete(m["file_id"])
    except Exception:
        pass
    DOCS_META.delete_one({"_id": mid})
    return jsonify({"ok": True})

# ---------------- Sharing (WhatsApp) ----------------
@documents_bp.post("/api/docs/<meta_id>/share")
def api_docs_share(meta_id):
    """
    Generates a short-lived public link you can paste anywhere (including WhatsApp).
    Returns both the share URL and a WhatsApp deep-link with prefilled text.
    """
    try:
        mid = ObjectId(meta_id)
    except Exception:
        return jsonify({"error":"invalid id"}), 400
    m = DOCS_META.find_one({"_id": mid})
    if not m: return jsonify({"error":"not found"}), 404

    token = secrets.token_urlsafe(18)
    expires_at = _now() + timedelta(days=3)  # 3 days validity
    DOCS_SHARES.insert_one({
        "token": token,
        "meta_id": mid,
        "created_at": _now(),
        "expires_at": expires_at,
        "uses": 0
    })
    share_url = url_for("documents_bp.docs_share_open", token=token, _external=True)
    wa_text = f"Sharing *{m.get('title') or m.get('filename')}*:\n{share_url}"
    wa_url = f"https://wa.me/?text={requests.utils.requote_uri(wa_text) if 'requests' in globals() else share_url}"
    # (If requests isn't installed, link will still open to WhatsApp with the URL.)

    return jsonify({"ok": True, "share_url": share_url, "whatsapp_url": wa_url, "expires_at": expires_at.isoformat()})

@documents_bp.get("/share/<token>")
def docs_share_open(token):
    rec = DOCS_SHARES.find_one({"token": token})
    if not rec: return abort(404)
    if rec.get("expires_at") and _now() > rec["expires_at"]:
        return abort(410)  # gone
    m = DOCS_META.find_one({"_id": rec["meta_id"]})
    if not m: return abort(404)
    try:
        blob = fs.get(m["file_id"])
    except Exception:
        return abort(404)

    DOCS_SHARES.update_one({"_id": rec["_id"]}, {"$inc": {"uses": 1}})
    # Serve inline when viewable, attachment otherwise
    is_inline = (m.get("mimetype","").startswith("image/") or m.get("mimetype","").lower()=="application/pdf")
    return send_file(
        BytesIO(blob.read()),
        mimetype=m.get("mimetype") or "application/octet-stream",
        download_name=m.get("filename") or "file.bin",
        as_attachment=not is_inline
    )

__all__ = ["documents_bp"]
