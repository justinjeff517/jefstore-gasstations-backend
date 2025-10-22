import os
from pymongo import MongoClient

from bson import json_util as bsonju

def _response(payload, status=200):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": bsonju.dumps(payload, ensure_ascii=False),
    }

def main(args):
    role = str(args.get("role", "")).strip()
    employee_number = str(args.get("employee_number", "")).strip()
    name = str(args.get("name", "")).strip()

    if not role:
        return _response({"ok": False, "reason": "missing_role"}, 400)


    uri = os.getenv("MONGODB_URI")
    dbn = os.getenv("MONGODB_DATABASE")
    if not uri or not dbn:
        return _response({"ok": False, "reason": "missing_env"}, 500)

    col = MongoClient(uri)[dbn]["sales_invoices"]

    docs = list(col.find({"is_empty": True}, {"employees": 1}))
    if len(docs) != 1:
        return _response(
            {"ok": False, "reason": "expected_single_empty_form", "found": len(docs)},
            400,
        )

    doc = docs[0]
    doc_id = doc.get("_id")
    employees = doc.get("employees")

    if isinstance(employees, dict):
        employees = [employees]
    elif not isinstance(employees, list):
        employees = []

    role_key = role.lower()
    idx = next(
        (
            i
            for i, e in enumerate(employees)
            if isinstance(e, dict)
            and str(e.get("role", "")).strip().lower() == role_key
        ),
        None,
    )

    if idx is None:
        return _response(
            {
                "ok": False,
                "reason": "role_not_in_empty_form",
                "role": role,
                "available_roles": [
                    str((e or {}).get("role", "")).strip()
                    for e in employees
                    if isinstance(e, dict)
                ],
            },
            400,
        )

    current = employees[idx] if isinstance(employees[idx], dict) else {}
    employees[idx] = {
        "role": current.get("role") or role,
        "employee_number": employee_number or current.get("employee_number"),
        "name": name or current.get("name"),
    }

    res = col.update_one({"_id": doc_id}, {"$set": {"employees": employees}})

    return _response(
        {
            "ok": True,
            "matched_count": res.matched_count,
            "modified_count": res.modified_count,
            "updated_employee": employees[idx],
        }
    )
