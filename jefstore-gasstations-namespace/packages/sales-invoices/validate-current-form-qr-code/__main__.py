import os, json
from pymongo import MongoClient

def main(args):
    uri = os.getenv("MONGODB_URI")
    dbname = os.getenv("MONGODB_DATABASE")
    qr = (args or {}).get("current_form_qr_code")

    if not uri or not dbname:
        return {"statusCode": 500, "body": json.dumps({"ok": False, "error": "Missing MONGODB_URI or MONGODB_DATABASE"})}
    if not qr:
        return {"statusCode": 400, "body": json.dumps({"ok": False, "error": "Missing current_form_qr_code"})}

    col = MongoClient(uri)[dbname]["sales_invoices"]
    doc = col.find_one({"current_form_qr_code": qr}, {"is_empty": 1, "_id": 0})

    exists = bool(doc)
    is_empty = bool(doc and doc.get("is_empty", False))
    status = "EMPTY" if (exists and is_empty) else ("FILLED" if exists else "NOT_FOUND")

    # Per rule: when not found -> is_empty=False and exists=False
    if not exists:
        is_empty = False

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "current_form_qr_code": qr,
            "exists": exists,
            "is_empty": is_empty,
            "status": status
        })
    }
