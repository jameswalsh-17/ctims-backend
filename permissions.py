FULL_ACCESS = {
    "Admin",
    "Farm Owner", "Farm Manager", "Farm Assistance",
}

READ_ONLY = {
    "Farm Labourer", "Milker", "Tractor & Machinery Operator",
}

VET_ACCESS = {
    "Veterinary Surgeon", "Veterinary Assistance",
}

AI_TECH = {"AI Technician"}


def can(role: str, resource: str, action: str) -> bool:
    """
    resource: 'users' | 'cows' | 'health' | 'breeding' | 'locations' | 'audit' | 'dashboard'
    action: 'read' | 'write' | 'delete'
    """

    if role in FULL_ACCESS:
        return True

    if role in READ_ONLY:
        return action == "read"

    if role in VET_ACCESS:
        if resource in {"health", "breeding"}:
            return action in {"read", "write", "delete"}
        return action == "read"

    if role in AI_TECH:
        if resource == "breeding":
            return action in {"read", "write", "delete"}
        return action == "read"

    return False

