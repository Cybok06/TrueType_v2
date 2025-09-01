from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify
from bson import ObjectId, errors as bson_errors
from werkzeug.security import generate_password_hash, check_password_hash
from db import db

admin_settings_bp = Blueprint('admin_settings', __name__, template_folder='templates/partials')

users_collection = db['users']
settings_collection = db['settings']


# ✅ System Settings Page
@admin_settings_bp.route('/admin/settings')
def settings():
    if 'role' not in session or session['role'] != 'admin':
        return redirect(url_for('login.login'))

    assistants = list(users_collection.find({'role': 'assistant'}))
    settings_doc = settings_collection.find_one() or {}
    return render_template('partials/settings.html', assistants=assistants, settings=settings_doc)


# ✅ AJAX: Update global boolean setting
@admin_settings_bp.route('/admin/settings/update', methods=['POST'])
def update_setting():
    if 'role' not in session or session['role'] != 'admin':
        return jsonify(success=False, message="Unauthorized"), 403

    try:
        data = request.get_json()
        setting = data.get('setting')
        value = data.get('value')

        if not setting or value is None:
            return jsonify(success=False, message="Missing setting name or value"), 400

        if isinstance(value, str):
            value = value.lower() == 'true'

        settings_collection.update_one({}, {'$set': {setting: value}}, upsert=True)
        return jsonify(success=True, message=f"'{setting}' updated to {value}")
    except Exception as e:
        return jsonify(success=False, message=f"Error: {str(e)}"), 500


# ✅ AJAX: Change admin password
@admin_settings_bp.route('/admin/settings/change_password', methods=['POST'])
def change_admin_password():
    if 'role' not in session or session['role'] != 'admin':
        return jsonify(success=False, message="Unauthorized"), 403

    new_password = request.form.get('new_password')
    username = session.get('username')

    if not new_password:
        return jsonify(success=False, message="Password cannot be empty.")

    try:
        existing_admin = users_collection.find_one({'username': username, 'role': 'admin'})
        if not existing_admin:
            return jsonify(success=False, message="Admin not found.")

        if check_password_hash(existing_admin['password'], new_password):
            return jsonify(success=False, message="New password must be different from the current one.")

        hashed_pw = generate_password_hash(new_password)
        users_collection.update_one(
            {'username': username, 'role': 'admin'},
            {'$set': {'password': hashed_pw}}
        )
        return jsonify(success=True, message="Password updated successfully.")
    except Exception as e:
        return jsonify(success=False, message=f"Error: {str(e)}"), 500


# ✅ AJAX: Change assistant password
@admin_settings_bp.route('/admin/settings/assistant/<user_id>/change_password', methods=['POST'])
def change_assistant_password(user_id):
    if 'role' not in session or session['role'] != 'admin':
        return jsonify(success=False, message="Unauthorized"), 403

    new_password = request.form.get('new_password')
    if not new_password:
        return jsonify(success=False, message="Password cannot be empty.")

    try:
        result = users_collection.update_one(
            {'_id': ObjectId(user_id), 'role': 'assistant'},
            {'$set': {'password': generate_password_hash(new_password)}}
        )
        if result.matched_count:
            return jsonify(success=True, message="Assistant password updated.")
        else:
            return jsonify(success=False, message="Assistant not found.")
    except bson_errors.InvalidId:
        return jsonify(success=False, message="Invalid assistant ID.")
    except Exception as e:
        return jsonify(success=False, message=f"Error: {str(e)}"), 500


# ✅ AJAX: Update assistant permissions
@admin_settings_bp.route('/admin/settings/assistant/<user_id>/update_permissions', methods=['POST'])
def update_permissions(user_id):
    if 'role' not in session or session['role'] != 'admin':
        return jsonify(success=False, message="Unauthorized"), 403

    data = request.get_json()
    permissions = data.get('permissions', [])

    try:
        result = users_collection.update_one(
            {'_id': ObjectId(user_id), 'role': 'assistant'},
            {'$set': {'permissions': permissions}}
        )
        if result.matched_count:
            return jsonify(success=True, message="Permissions updated successfully.")
        else:
            return jsonify(success=False, message="Assistant not found.")
    except bson_errors.InvalidId:
        return jsonify(success=False, message="Invalid assistant ID.")
    except Exception as e:
        return jsonify(success=False, message=f"Error: {str(e)}"), 500


# ✅ AJAX: Add new assistant
@admin_settings_bp.route('/admin/settings/add_assistant', methods=['POST'])
def add_assistant():
    if 'role' not in session or session['role'] != 'admin':
        return jsonify(success=False, message="Unauthorized"), 403

    username = request.form.get('username')
    password = request.form.get('password')

    if not username or not password:
        return jsonify(success=False, message="Username and password are required.")

    if users_collection.find_one({'username': username}):
        return jsonify(success=False, message="Username already exists.")

    hashed_pw = generate_password_hash(password)
    users_collection.insert_one({
        'username': username,
        'password': hashed_pw,
        'role': 'assistant',
        'status': 'active'
    })

    return jsonify(success=True, message="Assistant added successfully.")


# ✅ AJAX: Lock/unlock assistant account
@admin_settings_bp.route('/admin/settings/assistant/<user_id>/lock', methods=['POST'])
def lock_assistant_account(user_id):
    if 'role' not in session or session['role'] != 'admin':
        return jsonify(success=False, message="Unauthorized"), 403

    try:
        data = request.get_json()
        locked = data.get('locked')

        if locked not in [True, False]:
            return jsonify(success=False, message="Invalid value for lock status."), 400

        new_status = "locked" if locked else "active"

        result = users_collection.update_one(
            {'_id': ObjectId(user_id), 'role': 'assistant'},
            {'$set': {'status': new_status}}
        )
        if result.matched_count:
            return jsonify(success=True, message=f"Account {new_status}.")
        else:
            return jsonify(success=False, message="Assistant not found.")
    except bson_errors.InvalidId:
        return jsonify(success=False, message="Invalid assistant ID.")
    except Exception as e:
        return jsonify(success=False, message=f"Error: {str(e)}"), 500
