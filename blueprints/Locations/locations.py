from flask import Blueprint, request, jsonify
from db import get_conn
from decorators import login_required, require_perm
from audit import log_audit
from validators import parse_date_yyyy_mm_dd

locations_bp = Blueprint("locations", __name__)


def _row_to_dict(r):
    return {
        "location_id": r[0],
        "farm_name": r[1],
        "address_line": r[2],
        "town": r[3],
        "county": r[4],
        "postcode": r[5],
        "date_registered": str(r[6]) if r[6] else None,
        "last_inspection_date": str(r[7]) if r[7] else None,
    }


@locations_bp.route("/", methods=["POST"])
@login_required
@require_perm("locations", "write")
def create_location():
    data = request.get_json(silent=True) or {}

    farm_name = (data.get("farm_name") or "").strip()
    address_line = (data.get("address_line") or "").strip()
    town = (data.get("town") or "").strip()
    county = (data.get("county") or "").strip()
    postcode = (data.get("postcode") or "").strip()
    last_inspection_date_in = data.get("last_inspection_date")

    if not farm_name:
        return jsonify({"error": "farm_name is required"}), 400
    if not address_line or not town or not county or not postcode:
        return jsonify({"error": "address_line, town, county, postcode are required"}), 400

    last_inspection_date = None
    if last_inspection_date_in:
        last_inspection_date = parse_date_yyyy_mm_dd(last_inspection_date_in)
        if not last_inspection_date:
            return jsonify({"error": "last_inspection_date must be YYYY-MM-DD"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()


        cur.execute("SELECT LocationID FROM dbo.locations WHERE FarmName = ?", (farm_name,))
        if cur.fetchone():
            conn.close()
            return jsonify({"error": "farm_name already exists"}), 409

        cur.execute("""
            INSERT INTO dbo.locations (FarmName, AddressLine, Town, County, Postcode, LastInspectionDate)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (farm_name, address_line, town, county, postcode, last_inspection_date))

        conn.commit()

        conn.commit()

        location_id = None


        cur.execute("SELECT CAST(SCOPE_IDENTITY() AS INT)")
        row = cur.fetchone()
        if row and row[0] is not None:
            location_id = int(row[0])


        if location_id is None:
            cur.execute("SELECT LocationID FROM dbo.locations WHERE FarmName = ?", (farm_name,))
            row = cur.fetchone()
            if row and row[0] is not None:
                location_id = int(row[0])

        conn.close()

        log_audit("CREATE", "LOCATION", str(location_id) if location_id is not None else "unknown",
                  f"Created location FarmName={farm_name}")

        return jsonify({"message": "location created", "location_id": location_id}), 201

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@locations_bp.route("/", methods=["GET"])
@login_required
@require_perm("locations", "read")
def list_locations():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT LocationID, FarmName, AddressLine, Town, County, Postcode, DateRegistered, LastInspectionDate
            FROM dbo.locations
            ORDER BY FarmName ASC
        """)
        rows = cur.fetchall()
        conn.close()

        return jsonify([_row_to_dict(r) for r in rows]), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@locations_bp.route("/<int:location_id>", methods=["GET"])
@login_required
@require_perm("locations", "read")
def get_location(location_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT LocationID, FarmName, AddressLine, Town, County, Postcode, DateRegistered, LastInspectionDate
            FROM dbo.locations
            WHERE LocationID = ?
        """, (location_id,))
        r = cur.fetchone()
        conn.close()

        if not r:
            return jsonify({"error": "location not found"}), 404

        return jsonify(_row_to_dict(r)), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@locations_bp.route("/<int:location_id>", methods=["PUT"])
@login_required
@require_perm("locations", "write")
def update_location(location_id: int):
    data = request.get_json(silent=True) or {}

    farm_name = data.get("farm_name")
    address_line = data.get("address_line")
    town = data.get("town")
    county = data.get("county")
    postcode = data.get("postcode")
    last_inspection_date_in = data.get("last_inspection_date")

    last_inspection_date = None
    last_inspection_present = "last_inspection_date" in data
    if last_inspection_present and last_inspection_date_in:
        last_inspection_date = parse_date_yyyy_mm_dd(last_inspection_date_in)
        if not last_inspection_date:
            return jsonify({"error": "last_inspection_date must be YYYY-MM-DD"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT LocationID FROM dbo.locations WHERE LocationID = ?", (location_id,))
        if not cur.fetchone():
            conn.close()
            return jsonify({"error": "location not found"}), 404

        if farm_name is not None:
            farm_name = str(farm_name).strip()
            if not farm_name:
                conn.close()
                return jsonify({"error": "farm_name cannot be empty"}), 400
            cur.execute("""
                SELECT LocationID FROM dbo.locations
                WHERE FarmName = ? AND LocationID <> ?
            """, (farm_name, location_id))
            if cur.fetchone():
                conn.close()
                return jsonify({"error": "farm_name already exists"}), 409

        cur.execute("""
            UPDATE dbo.locations
            SET FarmName = COALESCE(?, FarmName),
                AddressLine = COALESCE(?, AddressLine),
                Town = COALESCE(?, Town),
                County = COALESCE(?, County),
                Postcode = COALESCE(?, Postcode),
                LastInspectionDate = CASE
                    WHEN ? = 1 THEN ?
                    ELSE LastInspectionDate
                END
            WHERE LocationID = ?
        """, (
            (str(farm_name).strip() if farm_name is not None else None),
            (str(address_line).strip() if address_line is not None else None),
            (str(town).strip() if town is not None else None),
            (str(county).strip() if county is not None else None),
            (str(postcode).strip() if postcode is not None else None),
            1 if last_inspection_present else 0,
            last_inspection_date,
            location_id
        ))

        conn.commit()
        conn.close()

        log_audit("UPDATE", "LOCATION", str(location_id), "Updated location")

        return jsonify({"message": "location updated"}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@locations_bp.route("/<int:location_id>", methods=["DELETE"])
@login_required
@require_perm("locations", "write")
def delete_location(location_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("DELETE FROM dbo.locations WHERE LocationID = ?", (location_id,))
        conn.commit()
        deleted = cur.rowcount
        conn.close()

        if deleted == 0:
            return jsonify({"error": "location not found"}), 404

        log_audit("DELETE", "LOCATION", str(location_id), "Deleted location")

        return jsonify({"message": "location deleted"}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500