import os
import uuid
from datetime import datetime
from flask import Blueprint, request, jsonify, session

from azure.storage.blob import BlobServiceClient, ContentSettings

from db import get_conn
from decorators import login_required, require_perm
from audit import log_audit
from validators import normalize_tag, ALLOWED_BREEDS

cows_bp = Blueprint("cows", __name__)

ALLOWED_STATUS = {"active", "sold", "deceased"}
ALLOWED_SEX = {"male", "female"}

ALLOWED_IMAGE_MIME = {"image/jpeg", "image/png", "image/webp"}
MAX_IMAGE_BYTES = 5 * 1024 * 1024  


def _resolve_location_id(cur, farm_name: str) -> int | None:
    cur.execute("SELECT LocationID FROM dbo.locations WHERE FarmName = ?", (farm_name,))
    row = cur.fetchone()
    return int(row[0]) if row else None


def _get_container_client():
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_STORAGE_CONTAINER", "cowimages")
    if not conn_str:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING not set")
    bsc = BlobServiceClient.from_connection_string(conn_str)
    return bsc.get_container_client(container_name)


def _get_ext_for_mime(mime: str) -> str:
    return {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/webp": "webp",
    }[mime]


def _get_cow_owner_and_tag(cur, cow_id: int):
    cur.execute("SELECT user_id, tag_number, image_url FROM dbo.cows WHERE cow_id = ?", (cow_id,))
    row = cur.fetchone()
    if not row:
        return None
    return int(row[0]), row[1], row[2]

def _ensure_owner_or_admin(cur, cow_id: int):
    info = _get_cow_owner_and_tag(cur, cow_id)
    if not info:
        return None, (jsonify({"error": "cow not found"}), 404)

    owner_user_id, tag_number, image_url = info

    if session.get("role") == "Admin":
        return (owner_user_id, tag_number, image_url), None

    if owner_user_id != int(session["user_id"]):
        return None, (jsonify({"error": "forbidden"}), 403)

    return (owner_user_id, tag_number, image_url), None


def _extract_blob_name_from_url(image_url: str) -> str | None:
    """
    Extract blob path from full URL:
    https://<account>.blob.core.windows.net/<container>/<blob_path>
    -> returns <blob_path>
    """
    if not image_url:
        return None
    container = os.getenv("AZURE_STORAGE_CONTAINER", "cowimages")
    marker = f"/{container}/"
    idx = image_url.find(marker)
    if idx == -1:
        return None
    return image_url[idx + len(marker):]


def _delete_blob_by_url(image_url: str) -> None:
    if not image_url:
        return

    container = os.getenv("AZURE_STORAGE_CONTAINER", "cowimages")
    marker = f"/{container}/"
    idx = image_url.find(marker)
    if idx == -1:
        return  

    blob_name = image_url[idx + len(marker):]

    container_client = _get_container_client()
    blob_client = container_client.get_blob_client(blob_name)

    try:
        blob_client.delete_blob()
    except Exception:
        
        pass


@cows_bp.route("/", methods=["POST"])
@login_required
@require_perm("cows", "write")
def create_cow():
    data = request.get_json(silent=True) or {}

    try:
        tag_number = normalize_tag(data.get("tag_number"))
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400

    breed = (data.get("breed") or "").strip()
    if breed not in ALLOWED_BREEDS:
        return jsonify({"error": "invalid breed"}), 400

    sex = (data.get("sex") or "").strip().lower()
    sex = sex if sex else None
    if sex is not None and sex not in ALLOWED_SEX:
        return jsonify({"error": "invalid sex (use male or female)"}), 400

    date_of_birth = data.get("date_of_birth")

    status = (data.get("status") or "active").strip().lower()
    if status not in ALLOWED_STATUS:
        return jsonify({"error": "invalid status (use: active, sold, deceased)"}), 400

    farm_name = (data.get("farm_name") or data.get("location") or "").strip()
    if not farm_name:
        return jsonify({"error": "farm_name (or location) is required"}), 400

    owner_user_id = int(session["user_id"])

    try:
        conn = get_conn()
        cur = conn.cursor()

        location_id = _resolve_location_id(cur, farm_name)
        if not location_id:
            conn.close()
            return jsonify({"error": "farm_name must exist in Locations table"}), 400

        cur.execute(
            """
            INSERT INTO dbo.cows (tag_number, breed, sex, date_of_birth, status, user_id, LocationID)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (tag_number, breed, sex, date_of_birth, status, owner_user_id, location_id),
        )
        conn.commit()

        cow_id = None
        cur.execute("SELECT CAST(SCOPE_IDENTITY() AS INT)")
        row = cur.fetchone()
        if row and row[0] is not None:
            cow_id = int(row[0])

        if cow_id is None:
            cur.execute("SELECT cow_id FROM dbo.cows WHERE tag_number = ?", (tag_number,))
            row = cur.fetchone()
            if row and row[0] is not None:
                cow_id = int(row[0])

        conn.close()

        log_audit("CREATE", "COW", str(cow_id) if cow_id is not None else "unknown",
                  f"Created cow tag={tag_number}, farm={farm_name}")

        return jsonify({"message": "cow created", "cow_id": cow_id, "tag_number": tag_number}), 201

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500

@cows_bp.route("/", methods=["GET"])
@login_required
@require_perm("cows", "read")
def list_cows():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT c.cow_id,
                   c.tag_number,
                   c.breed,
                   c.sex,
                   c.date_of_birth,
                   c.status,
                   c.user_id,
                   c.LocationID,
                   l.FarmName,
                   c.image_url,
                   c.image_uploaded_at
            FROM dbo.cows c
            LEFT JOIN dbo.locations l ON l.LocationID = c.LocationID
            ORDER BY c.cow_id DESC
            """
        )
        rows = cur.fetchall()
        conn.close()

        cows = []
        for r in rows:
            cows.append(
                {
                    "cow_id": r[0],
                    "tag_number": r[1],
                    "breed": r[2],
                    "sex": r[3],
                    "date_of_birth": str(r[4]) if r[4] else None,
                    "status": r[5],
                    "owner_user_id": r[6],
                    "location_id": r[7],
                    "farm_name": r[8],
                    "image_url": r[9],
                    "image_uploaded_at": str(r[10]) if r[10] else None,
                }
            )

        return jsonify(cows), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500

@cows_bp.route("/<int:cow_id>", methods=["GET"])
@login_required
@require_perm("cows", "read")
def get_cow(cow_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT c.cow_id,
                   c.tag_number,
                   c.breed,
                   c.sex,
                   c.date_of_birth,
                   c.status,
                   c.user_id,
                   c.LocationID,
                   l.FarmName,
                   c.image_url,
                   c.image_uploaded_at,
                   c.created_at,    
                   c.updated_at
            FROM dbo.cows c
            LEFT JOIN dbo.locations l ON l.LocationID = c.LocationID
            WHERE c.cow_id = ?
            """,
            (cow_id,),
        )
        r = cur.fetchone()
        conn.close()

        if not r:
            return jsonify({"error": "cow not found"}), 404

        return jsonify(
            {
                "cow_id": r[0],
                "tag_number": r[1],
                "breed": r[2],
                "sex": r[3],
                "date_of_birth": str(r[4]) if r[4] else None,
                "status": r[5],
                "owner_user_id": r[6],
                "location_id": r[7],
                "farm_name": r[8],
                "image_url": r[9],
                "image_uploaded_at": str(r[10]) if r[10] else None,
                "created_at": str(r[11]) if r[11] else None,
                "updated_at": str(r[12]) if r[12] else None,
            }
        ), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@cows_bp.route("/<int:cow_id>/profile", methods=["GET"])
@login_required
@require_perm("cows", "read")
def cow_profile(cow_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT c.cow_id,
                   c.tag_number,
                   c.breed,
                   c.sex,
                   c.date_of_birth,
                   c.status,
                   c.user_id,
                   c.LocationID,
                   l.FarmName,
                   l.AddressLine,
                   l.Town,
                   l.County,
                   l.Postcode,
                   l.DateRegistered,
                   l.LastInspectionDate,
                   c.image_url,
                   c.image_uploaded_at
            FROM dbo.cows c
            LEFT JOIN dbo.locations l ON l.LocationID = c.LocationID
            WHERE c.cow_id = ?
            """,
            (cow_id,),
        )
        cow = cur.fetchone()
        if not cow:
            conn.close()
            return jsonify({"error": "cow not found"}), 404

        cow_data = {
            "cow_id": cow[0],
            "tag_number": cow[1],
            "breed": cow[2],
            "sex": cow[3],
            "date_of_birth": str(cow[4]) if cow[4] else None,
            "status": cow[5],
            "owner_user_id": cow[6],
            "location_id": cow[7],
            "image_url": cow[15],
            "image_uploaded_at": str(cow[16]) if cow[16] else None,
            "location": {
                "farm_name": cow[8],
                "address_line": cow[9],
                "town": cow[10],
                "county": cow[11],
                "postcode": cow[12],
                "date_registered": str(cow[13]) if cow[13] else None,
                "last_inspection_date": str(cow[14]) if cow[14] else None,
            },
        }

        cur.execute(
            """
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
                   va.username AS vet_assistant_username
            FROM dbo.health_records hr
            LEFT JOIN dbo.users v ON v.user_id = hr.vet_user_id
            LEFT JOIN dbo.users va ON va.user_id = hr.vet_assistant_user_id
            WHERE hr.cow_id = ?
            ORDER BY COALESCE(hr.last_visit, hr.created_at) DESC, hr.health_id DESC
            """,
            (cow_id,),
        )
        hr_rows = cur.fetchall()
        health_records = []
        for r in hr_rows:
            health_records.append(
                {
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
                }
            )

        cur.execute(
            """
            SELECT breeding_id, cow_id, service_date, outcome, created_at
            FROM dbo.breeding_records
            WHERE cow_id = ?
            ORDER BY service_date DESC, breeding_id DESC
            """,
            (cow_id,),
        )
        br_rows = cur.fetchall()
        breeding_records = []
        for r in br_rows:
            breeding_records.append(
                {
                    "breeding_id": r[0],
                    "cow_id": r[1],
                    "service_date": str(r[2]) if r[2] else None,
                    "outcome": r[3],
                    "created_at": str(r[4]) if r[4] else None,
                }
            )

        conn.close()

        return jsonify({"cow": cow_data, "health_records": health_records, "breeding_records": breeding_records}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500

@cows_bp.route("/<int:cow_id>", methods=["PUT"])
@login_required
@require_perm("cows", "write")
def update_cow(cow_id: int):
    data = request.get_json(silent=True) or {}
    logged_in_user_id = int(session["user_id"])

    breed = data.get("breed")
    if breed is not None:
        breed = str(breed).strip()
        if breed not in ALLOWED_BREEDS:
            return jsonify({"error": "invalid breed"}), 400

    sex = data.get("sex")
    if sex is not None:
        sex = str(sex).strip().lower()
        if sex not in ALLOWED_SEX:
            return jsonify({"error": "invalid sex (use male or female)"}), 400

    date_of_birth = data.get("date_of_birth")

    status = data.get("status")
    if status is not None:
        status = str(status).strip().lower()
        if status not in ALLOWED_STATUS:
            return jsonify({"error": "invalid status (use: active, sold, deceased)"}), 400

    farm_name_in = data.get("farm_name")
    if farm_name_in is None and "location" in data:
        farm_name_in = data.get("location")

    location_present = farm_name_in is not None
    new_location_id = None

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT user_id FROM dbo.cows WHERE cow_id = ?", (cow_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return jsonify({"error": "cow not found"}), 404

        if location_present:
            farm_name = str(farm_name_in).strip()
            if not farm_name:
                conn.close()
                return jsonify({"error": "farm_name/location cannot be empty"}), 400

            new_location_id = _resolve_location_id(cur, farm_name)
            if not new_location_id:
                conn.close()
                return jsonify({"error": "farm_name must exist in Locations table"}), 400

        cur.execute(
            """
            UPDATE dbo.cows
            SET breed = COALESCE(?, breed),
                sex = COALESCE(?, sex),
                date_of_birth = COALESCE(?, date_of_birth),
                status = COALESCE(?, status),
                LocationID = CASE WHEN ? = 1 THEN ? ELSE LocationID END,
                updated_at = SYSUTCDATETIME()
            WHERE cow_id = ?
            """,
            (
                breed,
                sex,
                date_of_birth,
                status,
                1 if location_present else 0,
                new_location_id,
                cow_id,
            ),
        )

        conn.commit()
        conn.close()

        log_audit("UPDATE", "COW", str(cow_id), "Updated cow")
        return jsonify({"message": "cow updated"}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500

@cows_bp.route("/<int:cow_id>", methods=["DELETE"])
@login_required
@require_perm("cows", "write")
def delete_cow(cow_id: int):
    logged_in_user_id = int(session["user_id"])

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT user_id, tag_number FROM dbo.cows WHERE cow_id = ?", (cow_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return jsonify({"error": "cow not found"}), 404

        owner_user_id = int(row[0])
        tag_number = row[1]


        cur.execute("DELETE FROM dbo.cows WHERE cow_id = ?", (cow_id,))
        conn.commit()
        conn.close()

        log_audit("DELETE", "COW", str(cow_id), f"Deleted cow tag={tag_number}")
        return jsonify({"message": "cow deleted"}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500

@cows_bp.route("/<int:cow_id>/image", methods=["POST"])
@login_required
@require_perm("cows", "write")
def upload_cow_image(cow_id: int):
    if "image" not in request.files:
        return jsonify({"error": "image file is required (form-data key: image)"}), 400

    file = request.files["image"]
    if not file or file.filename == "":
        return jsonify({"error": "no file selected"}), 400

    content_type = (file.mimetype or "").lower()
    if content_type not in ALLOWED_IMAGE_MIME:
        return jsonify({"error": "invalid file type (allowed: jpeg, png, webp)"}), 400

    data = file.read()
    if len(data) > MAX_IMAGE_BYTES:
        return jsonify({"error": "file too large (max 5MB)"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()

        info, err = _ensure_owner_or_admin(cur, cow_id)
        if err:
            conn.close()
            return err
        owner_user_id, tag_number, existing_url = info

        container_client = _get_container_client()

        ext = _get_ext_for_mime(content_type)
        blob_name = f"cows/{cow_id}/{uuid.uuid4().hex}.{ext}"

        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(
            data,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
        )

        image_url = blob_client.url

        cur.execute(
            """
            UPDATE dbo.cows
            SET image_url = ?, image_uploaded_at = SYSUTCDATETIME()
            WHERE cow_id = ?
            """,
            (image_url, cow_id),
        )
        conn.commit()
        conn.close()

        log_audit("UPLOAD", "COW_IMAGE", str(cow_id), f"Uploaded image for cow tag={tag_number}")

        return jsonify({"message": "image uploaded", "cow_id": cow_id, "image_url": image_url}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500

@cows_bp.route("/<int:cow_id>/image", methods=["GET"])
@login_required
@require_perm("cows", "read")
def get_cow_image(cow_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            "SELECT image_url, image_uploaded_at FROM dbo.cows WHERE cow_id = ?",
            (cow_id,),
        )
        row = cur.fetchone()
        conn.close()

        if not row:
            return jsonify({"error": "cow not found"}), 404

        return jsonify({
            "cow_id": cow_id,
            "image_url": row[0],
            "image_uploaded_at": str(row[1]) if row[1] else None
        }), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500


@cows_bp.route("/<int:cow_id>/image", methods=["PUT"])
@login_required
@require_perm("cows", "write")
def replace_cow_image(cow_id: int):
    if "image" not in request.files:
        return jsonify({"error": "image file is required (form-data key: image)"}), 400

    file = request.files["image"]
    if not file or file.filename == "":
        return jsonify({"error": "no file selected"}), 400

    content_type = (file.mimetype or "").lower()
    if content_type not in ALLOWED_IMAGE_MIME:
        return jsonify({"error": "invalid file type (allowed: jpeg, png, webp)"}), 400

    data = file.read()
    if len(data) > MAX_IMAGE_BYTES:
        return jsonify({"error": "file too large (max 5MB)"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()

        info, err = _ensure_owner_or_admin(cur, cow_id)  
        if err:
            conn.close()
            return err

        _, tag_number, old_image_url = info

        if old_image_url:
            _delete_blob_by_url(old_image_url)

        container_client = _get_container_client()
        ext = _get_ext_for_mime(content_type)
        blob_name = f"cows/{cow_id}/{uuid.uuid4().hex}.{ext}"

        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(
            data,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
        )

        new_url = blob_client.url

        cur.execute(
            """
            UPDATE dbo.cows
            SET image_url = ?, image_uploaded_at = SYSUTCDATETIME()
            WHERE cow_id = ?
            """,
            (new_url, cow_id),
        )
        conn.commit()
        conn.close()

        log_audit("UPDATE", "COW_IMAGE", str(cow_id), f"Replaced image for cow tag={tag_number}")

        return jsonify({"message": "image replaced", "cow_id": cow_id, "image_url": new_url}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500

@cows_bp.route("/<int:cow_id>/image", methods=["DELETE"])
@login_required
@require_perm("cows", "write")
def delete_cow_image(cow_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()

        info, err = _ensure_owner_or_admin(cur, cow_id)
        if err:
            conn.close()
            return err
        owner_user_id, tag_number, image_url = info

        if image_url:
            blob_name = _extract_blob_name_from_url(image_url)
            if blob_name:
                container_client = _get_container_client()
                blob_client = container_client.get_blob_client(blob_name)
                try:
                    blob_client.delete_blob()
                except Exception:
                    pass

        cur.execute(
            """
            UPDATE dbo.cows
            SET image_url = NULL, image_uploaded_at = NULL
            WHERE cow_id = ?
            """,
            (cow_id,),
        )
        conn.commit()
        conn.close()

        log_audit("DELETE", "COW_IMAGE", str(cow_id), f"Deleted image for cow tag={tag_number}")

        return jsonify({"message": "image deleted", "cow_id": cow_id}), 200

    except Exception as e:
        return jsonify({"error": "server error", "details": str(e)}), 500
    