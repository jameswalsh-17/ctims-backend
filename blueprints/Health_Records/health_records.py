from flask import Blueprint, request, jsonify, session
from db import get_conn
from decorators import login_required, require_perm
from audit import log_audit
from validators import parse_date_yyyy_mm_dd

health_bp = Blueprint("health", __name__)

def _get_user_id_by_username_and_role(cur, username: str, role: str):
    cur.execute(
        "SELECT user_id FROM dbo.users WHERE username = ? AND role = ?",
        (username, role),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def _parse_weight(value):
    if value is None or value == "":
        return None
    try:
        w = round(float(value), 2)
    except ValueError:
        raise ValueError("weight must be a number")
    if w < 0:
        raise ValueError("weight cannot be negative")
    return float(f"{w:.2f}")


def _record_to_dict(r):
    return {
        "health_id": r[0],
        "cow_id": r[1],
        "last_visit": str(r[2]) if r[2] else None,
        "follow_up_date": str(r[3]) if r[3] else None,
        "vet_user_id": r[4],
        "vet_assistant_user_id": r[5],
        "reason": r[6],
        "diagnosis": r[7],
        "treatment": r[8],
        "weight": float(r[9]) if r[9] is not None else None,
        "notes": r[10],
        "created_at": str(r[11]) if r[11] else None,
        "vet_username": r[12],
        "vet_assistant_username": r[13],
        "tag_number": r[14],
        "vaccination_type": r[15] 
    }


@health_bp.route("/vets", methods=["GET"])
@login_required
def get_vets():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT username, role 
            FROM dbo.users 
            WHERE role IN ('Veterinary Surgeon', 'Veterinary Assistance')
        """)
        rows = cur.fetchall()
        conn.close()

        vets = [{"username": r[0], "role": r[1]} for r in rows]
        return jsonify(vets), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@health_bp.route("/", methods=["POST"])
@login_required
@require_perm("health", "write")
def add_health_record():
    data = request.get_json(silent=True) or {}

    cow_id = data.get("cow_id")
    if not cow_id:
        return jsonify({"error": "cow_id is required"}), 400

    last_visit_in = data.get("last_visit")
    follow_up_in = data.get("follow_up_date")

    last_visit = parse_date_yyyy_mm_dd(last_visit_in) if last_visit_in else None
    follow_up_date = parse_date_yyyy_mm_dd(follow_up_in) if follow_up_in else None

    if last_visit_in and not last_visit:
        return jsonify({"error": "last_visit must be YYYY-MM-DD"}), 400
    if follow_up_in and not follow_up_date:
        return jsonify({"error": "follow_up_date must be YYYY-MM-DD"}), 400
    if last_visit and follow_up_date and follow_up_date <= last_visit:
        return jsonify({"error": "follow_up_date must be after last_visit"}), 400

    vet_username = (data.get("vet") or "").strip()
    if not vet_username:
        return jsonify({"error": "vet is required"}), 400

    vet_assistant_username = (data.get("vet_assistant") or "").strip() or None

    reason = (data.get("reason") or "").strip() or None
    diagnosis = (data.get("diagnosis") or "").strip() or None
    treatment = (data.get("treatment") or "").strip() or None
    notes = (data.get("notes") or "").strip() or None
    vaccination_type = (data.get("vaccination_type") or "").strip() or None

    try:
        weight = _parse_weight(data.get("weight"))
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT cow_id FROM dbo.cows WHERE cow_id = ?", (cow_id,))
        if not cur.fetchone():
            conn.close()
            return jsonify({"error": "cow not found"}), 404

        vet_user_id = _get_user_id_by_username_and_role(cur, vet_username, "Veterinary Surgeon")
        if not vet_user_id:
            conn.close()
            return jsonify({"error": "vet must be a user with role 'Veterinary Surgeon'"}), 400

        vet_assistant_user_id = None
        if vet_assistant_username:
            vet_assistant_user_id = _get_user_id_by_username_and_role(
                cur, vet_assistant_username, "Veterinary Assistance"
            )
            if not vet_assistant_user_id:
                conn.close()
                return jsonify({"error": "vet_assistant must be a user with role 'Veterinary Assistance'"}), 400

        cur.execute("""
            INSERT INTO dbo.health_records
                (cow_id, last_visit, follow_up_date, vet_user_id, vet_assistant_user_id,
                 reason, diagnosis, treatment, weight, notes, vaccination_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            cow_id, last_visit, follow_up_date, vet_user_id, vet_assistant_user_id,
            reason, diagnosis, treatment, weight, notes, vaccination_type
        ))

        conn.commit()
        cur.execute("SELECT CAST(SCOPE_IDENTITY() AS INT)")
        row = cur.fetchone()
        health_id = int(row[0]) if row and row[0] is not None else None
        conn.close()

        log_audit(
            "CREATE",
            "HEALTH_RECORD",
            str(health_id) if health_id is not None else "unknown",
            f"cow_id={cow_id}, last_visit={last_visit_in}, follow_up={follow_up_in}, vet={vet_username}"
        )

        return jsonify({"message": "health record created", "health_id": health_id}), 201

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@health_bp.route("/", methods=["GET"])
@login_required
@require_perm("health", "read")
def list_all_health_records():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT hr.health_id,
                   hr.cow_id,
                   hr.last_visit,
                   hr.follow_up_date,
                   hr.vet_user_id,
                   hr.vet_assistant_user_id,
                   hr.reason,
                   hr.diagnosis,
                   hr.treatment,
                   hr.weight,
                   hr.notes,
                   hr.created_at,
                   v.username AS vet_username,
                   va.username AS vet_assistant_username,
                   c.tag_number,
                   hr.vaccination_type
            FROM dbo.health_records hr
            JOIN dbo.cows c ON c.cow_id = hr.cow_id
            LEFT JOIN dbo.users v ON v.user_id = hr.vet_user_id
            LEFT JOIN dbo.users va ON va.user_id = hr.vet_assistant_user_id
            ORDER BY COALESCE(hr.last_visit, hr.created_at) DESC, hr.health_id DESC
        """)

        rows = cur.fetchall()
        conn.close()

        return jsonify([_record_to_dict(r) for r in rows]), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@health_bp.route("/cow/<int:cow_id>", methods=["GET"])
@login_required
@require_perm("health", "read")
def list_health_for_cow(cow_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT hr.health_id,
                   hr.cow_id,
                   hr.last_visit,
                   hr.follow_up_date,
                   hr.vet_user_id,
                   hr.vet_assistant_user_id,
                   hr.reason,
                   hr.diagnosis,
                   hr.treatment,
                   hr.weight,
                   hr.notes,
                   hr.created_at,
                   v.username AS vet_username,
                   va.username AS vet_assistant_username,
                   c.tag_number,
                   hr.vaccination_type
            FROM dbo.health_records hr
            JOIN dbo.cows c ON c.cow_id = hr.cow_id
            LEFT JOIN dbo.users v ON v.user_id = hr.vet_user_id
            LEFT JOIN dbo.users va ON va.user_id = hr.vet_assistant_user_id
            WHERE hr.cow_id = ?
            ORDER BY COALESCE(hr.last_visit, hr.created_at) DESC, hr.health_id DESC
        """, (cow_id,))

        rows = cur.fetchall()
        conn.close()

        return jsonify([_record_to_dict(r) for r in rows]), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@health_bp.route("/<int:health_id>", methods=["GET"])
@login_required
@require_perm("health", "read")
def get_health_record(health_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT hr.health_id,
                   hr.cow_id,
                   hr.last_visit,
                   hr.follow_up_date,
                   hr.vet_user_id,
                   hr.vet_assistant_user_id,
                   hr.reason,
                   hr.diagnosis,
                   hr.treatment,
                   hr.weight,
                   hr.notes,
                   hr.created_at,
                   v.username AS vet_username,
                   va.username AS vet_assistant_username,
                   c.tag_number,
                   hr.vaccination_type
            FROM dbo.health_records hr
            JOIN dbo.cows c ON c.cow_id = hr.cow_id
            LEFT JOIN dbo.users v ON v.user_id = hr.vet_user_id
            LEFT JOIN dbo.users va ON va.user_id = hr.vet_assistant_user_id
            WHERE hr.health_id = ?
        """, (health_id,))

        r = cur.fetchone()
        conn.close()

        if not r:
            return jsonify({"error": "health record not found"}), 404

        return jsonify(_record_to_dict(r)), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@health_bp.route("/<int:health_id>", methods=["PUT"])
@login_required
@require_perm("health", "write")
def update_health_record(health_id: int):
    data = request.get_json(silent=True) or {}

    last_visit_in = data.get("last_visit")
    follow_up_in = data.get("follow_up_date")

    last_visit = parse_date_yyyy_mm_dd(last_visit_in) if last_visit_in else None
    follow_up_date = parse_date_yyyy_mm_dd(follow_up_in) if follow_up_in else None

    if last_visit_in and not last_visit:
        return jsonify({"error": "last_visit must be YYYY-MM-DD"}), 400
    if follow_up_in and not follow_up_date:
        return jsonify({"error": "follow_up_date must be YYYY-MM-DD"}), 400

    vet_username = data.get("vet")
    if vet_username is not None:
        vet_username = str(vet_username).strip()
        if not vet_username:
            return jsonify({"error": "vet cannot be empty"}), 400

    vet_assistant_username = data.get("vet_assistant")
    if vet_assistant_username is not None:
        vet_assistant_username = str(vet_assistant_username).strip() or None

    reason = data.get("reason")
    diagnosis = data.get("diagnosis")
    treatment = data.get("treatment")
    notes = data.get("notes")
    vaccination_type = (data.get("vaccination_type") or "").strip() or None

    weight_present = "weight" in data
    try:
        weight = _parse_weight(data.get("weight")) if weight_present else None
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT last_visit, follow_up_date FROM dbo.health_records WHERE health_id = ?", (health_id,))
        existing = cur.fetchone()
        if not existing:
            conn.close()
            return jsonify({"error": "health record not found"}), 404

        current_last_visit, current_follow_up = existing

        final_last = last_visit if last_visit_in is not None else current_last_visit
        final_follow = follow_up_date if follow_up_in is not None else current_follow_up

        if final_last and final_follow and final_follow <= final_last:
            conn.close()
            return jsonify({"error": "follow_up_date must be after last_visit"}), 400

        vet_user_id = None
        if vet_username is not None:
            vet_user_id = _get_user_id_by_username_and_role(cur, vet_username, "Veterinary Surgeon")
            if not vet_user_id:
                conn.close()
                return jsonify({"error": "vet must be a user with role 'Veterinary Surgeon'"}), 400

        vet_asst_present = "vet_assistant" in data
        vet_assistant_user_id = None
        if vet_asst_present:
            if vet_assistant_username:
                vet_assistant_user_id = _get_user_id_by_username_and_role(
                    cur, vet_assistant_username, "Veterinary Assistance"
                )
                if not vet_assistant_user_id:
                    conn.close()
                    return jsonify({"error": "vet_assistant must be a user with role 'Veterinary Assistance'"}), 400
            else:
                vet_assistant_user_id = None

        cur.execute("""
            UPDATE dbo.health_records
            SET last_visit = COALESCE(?, last_visit),
                follow_up_date = COALESCE(?, follow_up_date),
                vet_user_id = COALESCE(?, vet_user_id),
                vet_assistant_user_id = CASE
                    WHEN ? = 1 THEN ?
                    ELSE vet_assistant_user_id
                END,
                reason = COALESCE(?, reason),
                diagnosis = COALESCE(?, diagnosis),
                treatment = COALESCE(?, treatment),
                weight = CASE
                    WHEN ? = 1 THEN ?
                    ELSE weight
                END,
                notes = COALESCE(?, notes),
                vaccination_type = COALESCE(?, vaccination_type)    
            WHERE health_id = ?
        """, (
            last_visit, follow_up_date, vet_user_id,
            1 if vet_asst_present else 0, vet_assistant_user_id,
            reason, diagnosis, treatment,
            1 if weight_present else 0, weight,
            notes,
            vaccination_type,
            health_id
        ))

        conn.commit()
        conn.close()

        log_audit("UPDATE", "HEALTH_RECORD", str(health_id), "Updated health record")

        return jsonify({"message": "health record updated"}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@health_bp.route("/<int:health_id>", methods=["DELETE"])
@login_required
@require_perm("health", "write")
def delete_health_record(health_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("DELETE FROM dbo.health_records WHERE health_id = ?", (health_id,))
        conn.commit()
        deleted = cur.rowcount
        conn.close()

        if deleted == 0:
            return jsonify({"error": "health record not found"}), 404

        log_audit("DELETE", "HEALTH_RECORD", str(health_id), "Deleted health record")

        return jsonify({"message": "health record deleted"}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500
