from flask import Blueprint, jsonify, request
from db import get_conn
from decorators import login_required

audit_bp = Blueprint("audit", __name__)

@audit_bp.route("/", methods=["GET"])
@login_required
def list_audit():
    limit = request.args.get("limit", "50")
    try:
        limit = int(limit)
    except ValueError:
        limit = 50

    limit = max(1, min(limit, 200))

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(f"""
            SELECT TOP {limit} audit_id, user_id, action, entity, entity_id, details, created_at
            FROM dbo.audit_log
            ORDER BY created_at DESC, audit_id DESC
        """)
        rows = cur.fetchall()
        conn.close()

        logs = []
        for r in rows:
            logs.append({
                "audit_id": r[0],
                "user_id": r[1],
                "action": r[2],
                "entity": r[3],
                "entity_id": r[4],
                "details": r[5],
                "created_at": str(r[6]) if r[6] else None
            })

        return jsonify(logs), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500
