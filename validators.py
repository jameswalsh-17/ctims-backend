import re
from datetime import datetime

ALLOWED_ROLES = {
    "Admin",
    "Farm Owner", "Farm Manager", "Farm Assistance",
    "Farm Labourer", "Milker", "Tractor & Machinery Operator",
    "Veterinary Surgeon", "Veterinary Assistance",
    "AI Technician"
}

ALLOWED_BREEDS = {
    "Holstein Friesian",
    "Aberdeen Angus",
    "Limousin",
    "Charolais",
    "Hereford",
    "Simmental",
    "Belgian Blue",
    "Jersey",
    "Ayrshire",
    "British Friesian",
    "Brown Swiss",
    "Guernsey",
    "Norwegian Red",
    "Wagyu",
    "Highland",
    "Galloway",
    "Belted Galloway",
    "Dexter",
    "Irish Moiled",
    "Shorthorn",
    "Salers",
    "Blonde d'Aquitaine",
    "Montbéliarde",
    "Brahman",
    "Texas Longhorn",
    "Red Poll",
    "South Devon",
    "Welsh Black",
    "Chianina",
    "Murray Grey",
}

PASSWORD_RE = re.compile(r"^(?=.*[A-Z])(?=.*[\d\W]).{8,}$")

def validate_password(pw: str) -> bool:
    return bool(PASSWORD_RE.match(pw or ""))

def parse_date_yyyy_mm_dd(s: str):
    if s is None:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None

def normalize_tag(tag: str) -> str:
    """
    Accepts input with or without spaces.
    Stores as: 'UK 0 1234567 1234 1' (spaces exactly).
    NOTE: Your example implies 13 digits after prefix (1-7-4-1 groups).
    If you truly need 14 digits, tell me and I’ll adjust grouping.
    """
    if not tag:
        raise ValueError("tag_number is required")

    raw = re.sub(r"\s+", "", tag).upper()  
    if not (raw.startswith("UK") or raw.startswith("XI")):
        raise ValueError("tag_number must start with UK or XI")

    prefix = raw[:2]
    digits = raw[2:]
    if not digits.isdigit():
        raise ValueError("tag_number must contain only digits after prefix")

    if len(digits) == 13:
        a = digits[0]
        b = digits[1:8]
        c = digits[8:12]
        d = digits[12]
        return f"{prefix} {a} {b} {c} {d}"
    if len(digits) == 14:
        a = digits[0]
        b = digits[1:8]
        c = digits[8:12]
        d = digits[12:14]
        return f"{prefix} {a} {b} {c} {d}"

    raise ValueError("tag_number must have 13 (example) or 14 digits after UK/XI")

import re

ALLOWED_EMAIL_DOMAINS = {
    "gmail.com",
    "hotmail.com",
    "outlook.com",
    "icloud.com",
    "yahoo.com",
    "live.com",
    "apple.com",
}


def validate_email(email: str) -> bool:
    if not email:
        return False

    email = email.strip().lower()

    pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    if not re.match(pattern, email):
        return False

    domain = email.split("@")[-1]
    return domain in ALLOWED_EMAIL_DOMAINS