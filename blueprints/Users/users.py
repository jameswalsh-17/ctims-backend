from flask import Blueprint, request, jsonify, session
from werkzeug.security import generate_password_hash, check_password_hash

from db import get_conn
from audit import log_audit
from validators import ALLOWED_ROLES, validate_password, validate_email
from decorators import login_required

users_bp = Blueprint("users", __name__)

def username_taken(cur, username: str, exclude_user_id: int | None = None) -> bool:
    if exclude_user_id is None:
        cur.execute("SELECT user_id FROM dbo.users WHERE username = ?", (username,))
    else:
        cur.execute(
            "SELECT user_id FROM dbo.users WHERE username = ? AND user_id <> ?",
            (username, exclude_user_id),
        )
    return cur.fetchone() is not None


def email_taken(cur, email: str, exclude_user_id: int | None = None) -> bool:
    if exclude_user_id is None:
        cur.execute("SELECT user_id FROM dbo.users WHERE email = ?", (email,))
    else:
        cur.execute(
            "SELECT user_id FROM dbo.users WHERE email = ? AND user_id <> ?",
            (email, exclude_user_id),
        )
    return cur.fetchone() is not None


@users_bp.route("/register", methods=["POST"])
def register():
    data = request.get_json(silent=True) or {}

    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role = (data.get("role") or "Farm Labourer").strip()  
    email = (data.get("email") or "").strip().lower()

    if not username or not password or not email:
        return jsonify({"error": "username, password and email are required"}), 400

    if role not in ALLOWED_ROLES:
        return jsonify({"error": "invalid role"}), 400

    if not validate_password(password):
        return jsonify({
            "error": "password must be 8+ chars and include 1 uppercase and 1 number or special character"
        }), 400

    if not validate_email(email):
        return jsonify({"error": "invalid email"}), 400

    password_hash = generate_password_hash(password)

    try:
        conn = get_conn()
        cur = conn.cursor()

        if username_taken(cur, username):
            conn.close()
            return jsonify({"error": "username already exists"}), 409

        if email_taken(cur, email):
            conn.close()
            return jsonify({"error": "email already exists"}), 409

        cur.execute(
            "INSERT INTO dbo.users (username, password_hash, role, email, status) VALUES (?, ?, ?, ?, 'Approved')",
            (username, password_hash, role, email),
        )
        conn.commit()

        user_id = None
        try:
            cur.execute("SELECT CAST(SCOPE_IDENTITY() AS INT)")
            row = cur.fetchone()
            if row and row[0] is not None:
                user_id = int(row[0])
        except Exception:
            user_id = None

        if user_id is None:
            cur.execute("SELECT user_id FROM dbo.users WHERE username = ?", (username,))
            row = cur.fetchone()
            if row and row[0] is not None:
                user_id = int(row[0])

        conn.close()

        log_audit(
            action="CREATE",
            entity="USER",
            entity_id=str(user_id) if user_id is not None else "unknown",
            details=f"New user registered: username={username}, role={role}, email={email}",
        )

        return jsonify({
            "message": "registered",
            "user_id": user_id,
            "username": username,
            "role": role,
            "email": email
        }), 201

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@users_bp.route("/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}

    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if not username or not password:
        return jsonify({"error": "username and password are required"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            "SELECT user_id, password_hash, role, email FROM dbo.users WHERE username = ?",
            (username,),
        )
        row = cur.fetchone()
        conn.close()

        if not row:
            return jsonify({"error": "invalid username or password"}), 401

        user_id, password_hash, role, email = row

        if not check_password_hash(password_hash, password):
            return jsonify({"error": "invalid username or password"}), 401

        session["user_id"] = int(user_id)
        session["username"] = username
        session["role"] = role
        session["email"] = email

        log_audit(
            action="LOGIN",
            entity="USER",
            entity_id=str(user_id),
            details=f"Logged in username={username}",
        )

        return jsonify({
            "message": "logged in",
            "user_id": int(user_id),
            "username": username,
            "role": role,
            "email": email
        }), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@users_bp.route("/logout", methods=["POST"])
@login_required
def logout():
    log_audit(
        action="LOGOUT",
        entity="USER",
        entity_id=str(session.get("user_id")),
        details="Logged out",
    )
    session.clear()
    return jsonify({"message": "logged out"}), 200


@users_bp.route("/me", methods=["GET"])
@login_required
def me():
    return jsonify({
        "user_id": session["user_id"],
        "username": session["username"],
        "role": session.get("role"),
        "email": session.get("email"),
    }), 200


@users_bp.route("/me", methods=["PUT"])
@login_required
def update_me():
    data = request.get_json(silent=True) or {}
    user_id = int(session["user_id"])

    new_username = (data.get("username") or "").strip() or None
    new_password = data.get("password")  
    new_email = (data.get("email") or "").strip().lower() or None
    new_role = (data.get("role") or "").strip() or None 

    if new_email is not None and not validate_email(new_email):
        return jsonify({"error": "invalid email"}), 400

    if new_password is not None:
        if not validate_password(new_password):
            return jsonify({
                "error": "password must be 8+ chars and include 1 uppercase and 1 number or special character"
            }), 400
        new_password_hash = generate_password_hash(new_password)
    else:
        new_password_hash = None

    if new_role is not None:
        if session.get("role") != "Admin":
            return jsonify({"error": "only Admin can change roles"}), 403
        if new_role not in ALLOWED_ROLES:
            return jsonify({"error": "invalid role"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()

        if new_username is not None and username_taken(cur, new_username, user_id):
            conn.close()
            return jsonify({"error": "username already exists"}), 409

        if new_email is not None and email_taken(cur, new_email, user_id):
            conn.close()
            return jsonify({"error": "email already exists"}), 409

        cur.execute("""
            UPDATE dbo.users
            SET username = COALESCE(?, username),
                email = COALESCE(?, email),
                role = COALESCE(?, role),
                password_hash = COALESCE(?, password_hash)
            WHERE user_id = ?
        """, (new_username, new_email, new_role, new_password_hash, user_id))

        conn.commit()
        conn.close()

        if new_username is not None:
            session["username"] = new_username
        if new_email is not None:
            session["email"] = new_email
        if new_role is not None and session.get("role") == "Admin":
            session["role"] = new_role

        log_audit("UPDATE", "USER", str(user_id), "Updated own user details")

        return jsonify({"message": "user updated"}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@users_bp.route("/", methods=["GET"])
@login_required
def list_users():
    if session.get("role") != "Admin":
        return jsonify({"error": "forbidden"}), 403

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id, username, role, email
            FROM dbo.users
            ORDER BY user_id DESC
        """)
        rows = cur.fetchall()
        conn.close()

        users = [{"user_id": r[0], "username": r[1], "role": r[2], "email": r[3]} for r in rows]
        return jsonify(users), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@users_bp.route("/<int:target_user_id>", methods=["GET"])
@login_required
def get_user(target_user_id: int):
    if session.get("role") != "Admin":
        return jsonify({"error": "forbidden"}), 403

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id, username, role, email
            FROM dbo.users
            WHERE user_id = ?
        """, (target_user_id,))
        row = cur.fetchone()
        conn.close()

        if not row:
            return jsonify({"error": "user not found"}), 404

        return jsonify({"user_id": row[0], "username": row[1], "role": row[2], "email": row[3]}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@users_bp.route("/<int:target_user_id>", methods=["PUT"])
@login_required
def admin_update_user(target_user_id: int):
    if session.get("role") != "Admin":
        return jsonify({"error": "forbidden"}), 403

    data = request.get_json(silent=True) or {}

    new_username = (data.get("username") or "").strip() or None
    new_role = (data.get("role") or "").strip() or None
    new_email = (data.get("email") or "").strip().lower() or None
    new_password = data.get("password") 

    if new_role is not None and new_role not in ALLOWED_ROLES:
        return jsonify({"error": "invalid role"}), 400

    if new_email is not None and not validate_email(new_email):
        return jsonify({"error": "invalid email"}), 400

    if new_password is not None:
        if not validate_password(new_password):
            return jsonify({
                "error": "password must be 8+ chars and include 1 uppercase and 1 number or special character"
            }), 400
        new_password_hash = generate_password_hash(new_password)
    else:
        new_password_hash = None

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT user_id FROM dbo.users WHERE user_id = ?", (target_user_id,))
        if not cur.fetchone():
            conn.close()
            return jsonify({"error": "user not found"}), 404

        if new_username is not None and username_taken(cur, new_username, target_user_id):
            conn.close()
            return jsonify({"error": "username already exists"}), 409

        if new_email is not None and email_taken(cur, new_email, target_user_id):
            conn.close()
            return jsonify({"error": "email already exists"}), 409

        cur.execute("""
            UPDATE dbo.users
            SET username = COALESCE(?, username),
                email = COALESCE(?, email),
                role = COALESCE(?, role),
                password_hash = COALESCE(?, password_hash)
            WHERE user_id = ?
        """, (new_username, new_email, new_role, new_password_hash, target_user_id))

        conn.commit()
        conn.close()

        log_audit("UPDATE", "USER", str(target_user_id), "Admin updated user details")

        return jsonify({"message": "user updated (admin)"}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@users_bp.route("/<int:target_user_id>", methods=["DELETE"])
@login_required
def admin_delete_user(target_user_id: int):
    if session.get("role") != "Admin":
        return jsonify({"error": "forbidden"}), 403

    if target_user_id == session.get("user_id"):
        return jsonify({"error": "you cannot delete your own account"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        
        cur.execute("DELETE FROM dbo.users WHERE user_id = ?", (target_user_id,))
        conn.commit()
        deleted = cur.rowcount
        conn.close()

        if deleted == 0:
            return jsonify({"error": "user not found"}), 404

        log_audit("DELETE", "USER", str(target_user_id), "Admin deleted user")

        return jsonify({"message": "user deleted"}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500
    

@users_bp.route("/request-access", methods=["POST"])
def request_access():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role = (data.get("role") or "Farm Labourer").strip()
    email = (data.get("email") or "").strip().lower()

    if not username or not password or not email:
        return jsonify({"error": "Username, password and email are required"}), 400

    if not validate_password(password):
        return jsonify({"error": "Password too weak"}), 400

    password_hash = generate_password_hash(password)

    try:
        conn = get_conn()
        cur = conn.cursor()
        if username_taken(cur, username) or email_taken(cur, email):
            conn.close()
            return jsonify({"error": "Username or email already exists"}), 409


        cur.execute(
            "INSERT INTO dbo.users (username, password_hash, role, email, status) VALUES (?, ?, ?, ?, 'Pending')",
            (username, password_hash, role, email),
        )
        conn.commit()
        conn.close()

        log_audit("CREATE", "USER", username, f"Public access request from {username}")
        return jsonify({"message": "Request submitted successfully."}), 201
    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500
    

@users_bp.route("/pending", methods=["GET"])
@login_required
def list_pending_users():
    if session.get("role") != "Admin":
        return jsonify({"error": "forbidden"}), 403
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id, username, role, email FROM dbo.users WHERE status = 'Pending'")
    rows = cur.fetchall()
    conn.close()
    users = [{"user_id": r[0], "username": r[1], "role": r[2], "email": r[3]} for r in rows]
    return jsonify(users), 200


@users_bp.route("/<int:target_user_id>/approve", methods=["PUT"])
@login_required
def approve_user(target_user_id: int):
    if session.get("role") != "Admin":
        return jsonify({"error": "forbidden"}), 403
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE dbo.users SET status = 'Approved' WHERE user_id = ?", (target_user_id,))
    conn.commit()
    conn.close()
    log_audit("UPDATE", "USER", str(target_user_id), "Admin approved user access")
    return jsonify({"message": "User approved"}), 200

@users_bp.route("/<int:target_user_id>/reject", methods=["DELETE"])
@login_required
def reject_user(target_user_id: int):
    if session.get("role") != "Admin":
        return jsonify({"error": "forbidden"}), 403
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM dbo.users WHERE user_id = ?", (target_user_id,))
    conn.commit()
    conn.close()
    log_audit("DELETE", "USER", str(target_user_id), "Admin rejected user request")
    return jsonify({"message": "User request rejected"}), 200