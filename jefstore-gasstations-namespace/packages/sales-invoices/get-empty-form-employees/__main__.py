import os, json
from datetime import datetime
from pymongo import MongoClient
from bson import json_util as bsonju

# DigitalOcean Functions / OpenWhisk entrypoint
def main(args):
    try:
        uri = os.getenv("MONGODB_URI")
        dbn = os.getenv("MONGODB_DATABASE")
        coll = os.getenv("MONGODB_COLLECTION", "sales_invoices")
        if not uri or not dbn:
            return _resp(500, {"ok": False, "error": "Missing MONGODB_URI or MONGODB_DATABASE"})

        c = MongoClient(uri)[dbn][coll]

        docs = list(
            c.find(
                {"is_empty": True},
                {"employees": 1, "id": 1, "current_form_qr_code": 1, "created": 1, "_id": 0},
            )
        )

        if len(docs) != 1:
            return _resp(
                400,
                {
                    "ok": False,
                    "error": "Expected exactly 1 empty form",
                    "found": len(docs),
                    "docs": docs,  # BSON-safe via json_util in _resp
                },
            )

        doc = docs[0]
        employees = doc.get("employees")

        if isinstance(employees, dict):
            employees = [employees]
        elif not isinstance(employees, list):
            employees = []

        out = {

            "employees": employees,
        }

        return _resp(200, out)

    except Exception as e:
        return _resp(500, {"ok": False, "error": str(e)})


def _resp(status, obj):
    body = bsonju.dumps(obj, indent=2, ensure_ascii=False)
    return {
        "statusCode": status,
        "headers": {
            "content-type": "application/json; charset=utf-8",
            "access-control-allow-origin": "*",
            "access-control-allow-methods": "GET,POST,OPTIONS",
        },
        "body": body,
    }
