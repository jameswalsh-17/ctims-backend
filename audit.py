from typing import Optional
from flask import session
from db import get_conn

def log_audit(action: str, entity: str, entity_id: Optional[str] = None, details: Optional[str] = None) -> None:
    """
    Writes an audit event to dbo.audit_log.
    Uses session user_id if available, otherwise stores NULL.
    Never throws errors back to the user (audit should not break the app).
    """
    user_id = session.get("user_id")  

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO dbo.audit_log (user_id, action, entity, entity_id, details)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, action, entity, entity_id, details))
        conn.commit()
        conn.close()
    except Exception:
        pass
