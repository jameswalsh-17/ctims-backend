from flask import Blueprint, jsonify
from db import get_conn
from decorators import login_required

dashboard_bp = Blueprint("dashboard", __name__)

@dashboard_bp.route("/summary", methods=["GET"])
@login_required
def summary():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM dbo.cows")
        total_cows = cur.fetchone()[0]

        cur.execute("""
            SELECT status, COUNT(*)
            FROM dbo.cows
            GROUP BY status
        """)
        status_rows = cur.fetchall()
        cows_by_status = {r[0]: r[1] for r in status_rows}

        cur.execute("SELECT COUNT(*) FROM dbo.health_records")
        total_health = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM dbo.breeding_records")
        total_breeding = cur.fetchone()[0]

        cur.execute("""
            SELECT TOP 5 hr.health_id, hr.cow_id, c.tag_number,
                   hr.treatment_date, hr.vaccination_type, hr.notes
            FROM dbo.health_records hr
            JOIN dbo.cows c ON c.cow_id = hr.cow_id
            ORDER BY hr.treatment_date DESC, hr.health_id DESC
        """)
        recent_health_rows = cur.fetchall()
        recent_health = []
        for r in recent_health_rows:
            recent_health.append({
                "health_id": r[0],
                "cow_id": r[1],
                "tag_number": r[2],
                "treatment_date": str(r[3]) if r[3] else None,
                "vaccination_type": r[4],
                "notes": r[5]
            })

        cur.execute("""
            SELECT TOP 5 br.breeding_id, br.cow_id, c.tag_number,
                   br.service_date, br.outcome
            FROM dbo.breeding_records br
            JOIN dbo.cows c ON c.cow_id = br.cow_id
            ORDER BY br.service_date DESC, br.breeding_id DESC
        """)
        recent_breeding_rows = cur.fetchall()
        recent_breeding = []
        for r in recent_breeding_rows:
            recent_breeding.append({
                "breeding_id": r[0],
                "cow_id": r[1],
                "tag_number": r[2],
                "service_date": str(r[3]) if r[3] else None,
                "outcome": r[4]
            })

        conn.close()

        return jsonify({
            "total_cows": total_cows,
            "cows_by_status": cows_by_status,
            "total_health_records": total_health,
            "total_breeding_records": total_breeding,
            "recent_health_records": recent_health,
            "recent_breeding_records": recent_breeding
        }), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500
