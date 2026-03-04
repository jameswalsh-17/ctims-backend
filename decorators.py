from functools import wraps
from flask import session, jsonify
from permissions import can


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return jsonify({"error": "not logged in"}), 401
        return f(*args, **kwargs)
    return wrapper


def require_perm(resource: str, action: str):
    """
    resource: 'users' | 'cows' | 'health' | 'breeding' | 'locations' | 'audit' | 'dashboard'
    action: 'read' | 'write' | 'delete'
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if "user_id" not in session:
                return jsonify({"error": "not logged in"}), 401

            role = session.get("role")
            if not role:
                return jsonify({"error": "not logged in"}), 401

            if not can(role, resource, action):
                return jsonify({"error": "forbidden"}), 403

            return f(*args, **kwargs)
        return wrapper
    return decorator

