from flask import Blueprint, request, jsonify
from datetime import date
from db import get_conn
from decorators import login_required, require_perm
from audit import log_audit
from validators import parse_date_yyyy_mm_dd

breeding_bp = Blueprint("breeding", __name__)

ALLOWED_OUTCOMES = {"pending", "pregnant", "not_pregnant", "aborted", "calved"}


def _row_to_dict(r):
    return {
        "breeding_id": r[0],
        "cow_id": r[1],
        "service_date": str(r[2]) if r[2] else None,
        "outcome": r[3],
        "created_at": str(r[4]) if r[4] else None,
        "tag_number": r[5],
    }

@breeding_bp.route("/", methods=["POST"])
@login_required
@require_perm("breeding", "write")
def create_breeding_record():
    data = request.get_json(silent=True) or {}

    cow_id = data.get("cow_id")
    if not cow_id:
        return jsonify({"error": "cow_id is required"}), 400

    service_date_in = data.get("service_date")
    if not service_date_in:
        return jsonify({"error": "service_date is required (YYYY-MM-DD)"}), 400

    service_date = parse_date_yyyy_mm_dd(service_date_in)
    if not service_date:
        return jsonify({"error": "service_date must be YYYY-MM-DD"}), 400

    if service_date > date.today():
        return jsonify({"error": "service_date cannot be in the future"}), 400

    outcome = (data.get("outcome") or "pending").strip().lower()
    if outcome not in ALLOWED_OUTCOMES:
        return jsonify({"error": f"invalid outcome (allowed: {sorted(ALLOWED_OUTCOMES)})"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT cow_id FROM dbo.cows WHERE cow_id = ?", (cow_id,))
        if not cur.fetchone():
            conn.close()
            return jsonify({"error": "cow not found"}), 404

        cur.execute("""
            INSERT INTO dbo.breeding_records (cow_id, service_date, outcome)
            VALUES (?, ?, ?)
        """, (cow_id, service_date, outcome))

        conn.commit()
        cur.execute("SELECT CAST(SCOPE_IDENTITY() AS INT)")
        row = cur.fetchone()
        breeding_id = int(row[0]) if row and row[0] is not None else None
        conn.close()

        log_audit(
            "CREATE",
            "BREEDING_RECORD",
            str(breeding_id) if breeding_id is not None else "unknown",
            f"cow_id={cow_id}, service_date={service_date_in}, outcome={outcome}"
        )

        return jsonify({"message": "breeding record created", "breeding_id": breeding_id}), 201

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500

@breeding_bp.route("/", methods=["GET"])
@login_required
@require_perm("breeding", "read")
def list_breeding_records():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT br.breeding_id, br.cow_id, br.service_date, br.outcome, br.created_at,
                   c.tag_number
            FROM dbo.breeding_records br
            JOIN dbo.cows c ON c.cow_id = br.cow_id
            ORDER BY br.service_date DESC, br.breeding_id DESC
        """)

        rows = cur.fetchall()
        conn.close()

        return jsonify([_row_to_dict(r) for r in rows]), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500

@breeding_bp.route("/cow/<int:cow_id>", methods=["GET"])
@login_required
@require_perm("breeding", "read")
def list_breeding_for_cow(cow_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT br.breeding_id, br.cow_id, br.service_date, br.outcome, br.created_at,
                   c.tag_number
            FROM dbo.breeding_records br
            JOIN dbo.cows c ON c.cow_id = br.cow_id
            WHERE br.cow_id = ?
            ORDER BY br.service_date DESC, br.breeding_id DESC
        """, (cow_id,))

        rows = cur.fetchall()
        conn.close()

        return jsonify([_row_to_dict(r) for r in rows]), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@breeding_bp.route("/<int:breeding_id>", methods=["GET"])
@login_required
@require_perm("breeding", "read")
def get_breeding_record(breeding_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT br.breeding_id, br.cow_id, br.service_date, br.outcome, br.created_at,
                   c.tag_number
            FROM dbo.breeding_records br
            JOIN dbo.cows c ON c.cow_id = br.cow_id
            WHERE br.breeding_id = ?
        """, (breeding_id,))

        r = cur.fetchone()
        conn.close()

        if not r:
            return jsonify({"error": "breeding record not found"}), 404

        return jsonify(_row_to_dict(r)), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@breeding_bp.route("/<int:breeding_id>", methods=["PUT"])
@login_required
@require_perm("breeding", "write")
def update_breeding_record(breeding_id: int):
    data = request.get_json(silent=True) or {}

    service_date_in = data.get("service_date")
    outcome_in = data.get("outcome")

    service_date = None
    if service_date_in is not None:
        service_date = parse_date_yyyy_mm_dd(service_date_in)
        if not service_date:
            return jsonify({"error": "service_date must be YYYY-MM-DD"}), 400
        if service_date > date.today():
            return jsonify({"error": "service_date cannot be in the future"}), 400

    outcome = None
    if outcome_in is not None:
        outcome = str(outcome_in).strip().lower()
        if outcome not in ALLOWED_OUTCOMES:
            return jsonify({"error": f"invalid outcome (allowed: {sorted(ALLOWED_OUTCOMES)})"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT breeding_id FROM dbo.breeding_records WHERE breeding_id = ?", (breeding_id,))
        if not cur.fetchone():
            conn.close()
            return jsonify({"error": "breeding record not found"}), 404

        cur.execute("""
            UPDATE dbo.breeding_records
            SET service_date = COALESCE(?, service_date),
                outcome = COALESCE(?, outcome)
            WHERE breeding_id = ?
        """, (service_date, outcome, breeding_id))

        conn.commit()
        conn.close()

        log_audit("UPDATE", "BREEDING_RECORD", str(breeding_id), "Updated breeding record")

        return jsonify({"message": "breeding record updated"}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@breeding_bp.route("/<int:breeding_id>", methods=["DELETE"])
@login_required
@require_perm("breeding", "write")
def delete_breeding_record(breeding_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("DELETE FROM dbo.breeding_records WHERE breeding_id = ?", (breeding_id,))
        conn.commit()
        deleted = cur.rowcount
        conn.close()

        if deleted == 0:
            return jsonify({"error": "breeding record not found"}), 404

        log_audit("DELETE", "BREEDING_RECORD", str(breeding_id), "Deleted breeding record")

        return jsonify({"message": "breeding record deleted"}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500
