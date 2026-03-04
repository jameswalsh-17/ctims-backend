from flask import Blueprint, session, jsonify
from decorators import login_required

auth_bp = Blueprint("auth", __name__)

@auth_bp.route("/session", methods=["GET"])
def session_status():
    if "user_id" not in session:
        return jsonify({"logged_in": False}), 200
    return jsonify({
        "logged_in": True,
        "user_id": session.get("user_id"),
        "username": session.get("username"),
        "role": session.get("role"),
    }), 200